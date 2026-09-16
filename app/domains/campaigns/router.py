from uuid import UUID
from typing import Optional

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
    status,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from uuid6 import uuid7

from fastapi import File, UploadFile

from app.core.database import get_db
from app.core.redis import get_redis_pool
from app.domains.auth.dependencies import get_current_user
from app.domains.campaigns.models import (
    HiringCampaign,
    CampaignStatus,
    EmploymentType,
)
from app.domains.campaigns.schemas import BatchStatusResponse, BatchUploadResponse
from app.domains.campaigns.service import (
    CampaignService,
    create_batch_tracker,
    enqueue_resume_upload_batch,
    enqueue_campaign_screening,
    get_batch_status,
    stage_files_to_s3,
    validate_jd_file,
    extract_jd_from_upload,
    normalize_jd_text,
    validate_resume_uploads,
)
from app.domains.users.models import User

router = APIRouter(prefix="/campaigns")


# -----------------------------------------------------------------------
# Campaign CRUD
# -----------------------------------------------------------------------

@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_campaign(
    job_title: str = Form(...),
    location: str | None = Form(None),
    employment_type: EmploymentType = Form(EmploymentType.FULL_TIME),
    role_summary: str | None = Form(None),
    jd_text: str | None = Form(None),
    jd_file: UploadFile | None = File(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if jd_text is None and jd_file is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Either jd_text or jd_file must be provided.",
        )

    if jd_text is not None and jd_file is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide either jd_text or jd_file, not both.",
        )

    source_jd_text = jd_text

    if jd_file is not None:
        validate_jd_file(jd_file)
        source_jd_text = await extract_jd_from_upload(jd_file)

    if source_jd_text is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="JD text is required after parsing the upload.",
        )

    source_jd_text = normalize_jd_text(source_jd_text)

    campaign = await CampaignService.create_campaign(
        db=db,
        job_title=job_title,
        location=location,
        employment_type=employment_type,
        role_summary=role_summary,
        jd_text=source_jd_text,
        user_id=current_user.id,
    )

    return campaign


# -----------------------------------------------------------------------
# Candidate Resume Upload
# -----------------------------------------------------------------------

@router.post(
    "/{campaign_id}/candidates/upload",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=BatchUploadResponse,
)
async def upload_candidates(
    campaign_id: UUID,
    files: list[UploadFile] = File(..., description="One or more resume files (PDF, DOCX, TXT) or a single ZIP file"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Accept one or more resume files (or a single zip) for async processing.

    Returns a ``batch_id`` immediately.  Processing happens in the background
    worker — poll the status endpoint to track progress.
    """

    # 1. Verify campaign exists and belongs to this user
    result = await db.execute(
        select(HiringCampaign).where(
            HiringCampaign.id == campaign_id,
            HiringCampaign.user_id == current_user.id,
        )
    )
    campaign = result.scalar_one_or_none()
    if campaign is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Campaign not found.",
        )

    # 2. Validate uploads (extension + MIME + zip safety is deferred to worker)
    validate_resume_uploads(files)

    # 3. Generate batch ID
    batch_id = f"batch_{uuid7()}"

    # 4. Stage files to S3
    s3_prefix, source_type = await stage_files_to_s3(batch_id, files)

    # 5. Create Redis batch tracker
    redis = await get_redis_pool()
    try:
        await create_batch_tracker(redis, batch_id, file_count=len(files))

        # 6. Enqueue the background job
        from app.domains.campaigns.service import enqueue_resume_upload_batch
        await enqueue_resume_upload_batch(
            redis,
            batch_id=batch_id,
            campaign_id=str(campaign_id),
            s3_prefix=s3_prefix,
            source_type=source_type,
        )
    finally:
        await redis.aclose()

    return BatchUploadResponse(
        batch_id=batch_id,
        status="QUEUED",
        accepted_files=len(files),
    )


# -----------------------------------------------------------------------
# Batch Status Polling
# -----------------------------------------------------------------------

@router.get(
    "/{campaign_id}/candidates/upload/{batch_id}/status",
    response_model=BatchStatusResponse,
)
async def batch_status(
    campaign_id: UUID,
    batch_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Poll the processing status of a resume upload batch.
    
    When COMPLETED, returns the list of parsed candidates.
    """

    redis = await get_redis_pool()
    try:
        data = await get_batch_status(redis, batch_id)
    finally:
        await redis.aclose()

    if data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Batch not found.",
        )

    status_val = data.get("status", "UNKNOWN")
    candidates = None

    if status_val == "COMPLETED":
        # Fetch parsed candidates for this campaign
        from app.domains.campaigns.models import Candidate, CandidateStatus
        result = await db.execute(
            select(Candidate).where(
                Candidate.campaign_id == campaign_id,
                Candidate.status == CandidateStatus.RESUME_PARSED
            )
        )
        candidates = result.scalars().all()

    return BatchStatusResponse(
        batch_id=batch_id,
        status=status_val,
        total_files=int(data.get("total_files", 0)),
        processed=int(data.get("processed", 0)),
        failed=int(data.get("failed", 0)),
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
        finished_at=data.get("finished_at"),
        candidates=candidates,
    )


# -----------------------------------------------------------------------
# Candidate Management & Screening
# -----------------------------------------------------------------------

from app.domains.campaigns.schemas import CandidateResponse, ScreeningRequest

@router.get(
    "/{campaign_id}/candidates",
    response_model=list[CandidateResponse]
)
async def get_candidates(
    campaign_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Fetch all candidates for a campaign."""
    from app.domains.campaigns.models import Candidate
    
    result = await db.execute(
        select(Candidate).where(Candidate.campaign_id == campaign_id)
    )
    return result.scalars().all()


@router.post(
    "/{campaign_id}/candidates/screen",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=BatchUploadResponse,
)
async def screen_candidates(
    campaign_id: UUID,
    request: ScreeningRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Trigger the screening phase for the specified candidates."""
    # 1. Verify campaign exists and belongs to this user
    result = await db.execute(
        select(HiringCampaign).where(
            HiringCampaign.id == campaign_id,
            HiringCampaign.user_id == current_user.id,
        )
    )
    if not result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Campaign not found.",
        )

    # 2. Generate batch ID for screening progress
    batch_id = f"screen_{uuid7()}"

    # 3. Create Redis batch tracker (estimate file count as 0 initially, worker updates it)
    redis = await get_redis_pool()
    try:
        await create_batch_tracker(redis, batch_id, file_count=0)

        # 4. Enqueue the screening job
        from app.domains.campaigns.service import enqueue_campaign_screening
        
        # Convert UUIDs to strings for arq JSON serialization
        candidate_ids_str = [str(cid) for cid in request.candidate_ids] if request.candidate_ids else None
        
        await enqueue_campaign_screening(
            redis,
            batch_id=batch_id,
            campaign_id=str(campaign_id),
            candidate_ids=candidate_ids_str,
        )
    finally:
        await redis.aclose()

    return BatchUploadResponse(
        batch_id=batch_id,
        status="QUEUED",
        accepted_files=len(request.candidate_ids) if request.candidate_ids else 0,
    )


@router.get(
    "/{campaign_id}/candidates/screen/{batch_id}/status",
    response_model=BatchStatusResponse,
)
async def screening_batch_status(
    campaign_id: UUID,
    batch_id: str,
    current_user: User = Depends(get_current_user),
):
    """Poll the processing status of a screening batch."""
    redis = await get_redis_pool()
    try:
        data = await get_batch_status(redis, batch_id)
    finally:
        await redis.aclose()

    if data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Batch not found.",
        )

    return BatchStatusResponse(
        batch_id=batch_id,
        status=data.get("status", "UNKNOWN"),
        total_files=int(data.get("total_files", 0)),
        processed=int(data.get("processed", 0)),
        failed=int(data.get("failed", 0)),
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
        finished_at=data.get("finished_at"),
    )