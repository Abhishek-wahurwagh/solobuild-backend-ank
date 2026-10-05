from typing import Any
from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
    status,
    Body,
)
from pydantic import Json
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from uuid6 import uuid7

from app.core.database import get_db
from app.core.redis import get_redis_client
from app.domains.auth.dependencies import get_current_user
from app.domains.campaigns.models import (
    Campaign,
    Candidate,
    WorkflowStepStatus,
)
from app.domains.campaigns.schemas import (
    BatchStatusResponse,
    BatchUploadResponse,
    CampaignFieldsUpdate,
    CampaignResponse,
    CampaignListResponse,
    CandidateResponse,
    ScreeningBatchResponse,
    ScreeningRequest,
    CallingRequest,
    CallingBatchResponse,
    CallingBatchControlResponse,
)
from app.domains.campaigns.service import (
    build_campaign_raw_text,
    create_batch_tracker,
    enqueue_document_upload_batch,
    enqueue_campaign_screening,
    enqueue_campaign_calling,
    extract_campaign_requirements_llm,
    get_batch_status,
    stage_files_to_s3,
    validate_document_uploads,
    extract_document_fields_llm,
    pause_calling_batch,
    resume_calling_batch,
)
from app.domains.ingestion.service import create_ingestion_batch
from app.domains.ingestion.models import (
    IngestionBatch,
    IngestionBatchStatus,
    IngestionItem,
    IngestionItemStatus,
)
from app.domains.users.models import User

router = APIRouter(prefix="/campaigns")


# -----------------------------------------------------------------------
# Campaign CRUD
# -----------------------------------------------------------------------

@router.post("/", status_code=status.HTTP_201_CREATED, response_model=CampaignResponse)
async def create_campaign(
    title: str = Form(...),
    raw_text: str | None = Form(None),
    required_fields: Json[dict[str, Any]] | None = Form(None),
    file: UploadFile | None = File(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    campaign_id = uuid7()
    resulting_raw_text, file_url = await build_campaign_raw_text(
        raw_text,
        file,
        campaign_id=campaign_id,
    )
    if not resulting_raw_text:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide either raw_text or a requirement file.",
        )

    extracted_fields = await extract_document_fields_llm(resulting_raw_text)
    campaign = Campaign(
        id=campaign_id,
        title=title,
        raw_text=resulting_raw_text,
        required_fields={**extracted_fields, **(required_fields or {})},
        file_url=file_url,
        created_by_user_id=current_user.id,
    )
    db.add(campaign)
    await db.commit()
    await db.refresh(campaign)
    return campaign


@router.get("/", response_model=list[CampaignListResponse])
async def list_campaigns(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(Campaign)
        .where(Campaign.created_by_user_id == current_user.id)
        .order_by(Campaign.created_at.desc())
    )
    return result.scalars().all()


@router.get("/{campaign_id}", response_model=CampaignResponse)
async def get_campaign(
    campaign_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(Campaign).where(
            Campaign.id == campaign_id,
            Campaign.created_by_user_id == current_user.id,
        )
    )
    campaign = result.scalar_one_or_none()
    if campaign is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return campaign


@router.patch("/{campaign_id}", response_model=CampaignResponse)
async def update_campaign(
    campaign_id: UUID,
    title: str = Form(...),
    raw_text: str | None = Form(None),
    required_fields: Json[dict[str, Any]] | None = Form(None),
    file: UploadFile | None = File(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update campaign raw text / requirement file. Accepts multipart/form-data."""
    result = await db.execute(
        select(Campaign).where(
            Campaign.id == campaign_id,
            Campaign.created_by_user_id == current_user.id,
        )
    )
    campaign = result.scalar_one_or_none()
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    if raw_text is not None or file is not None:
        campaign.raw_text, campaign.file_url = await build_campaign_raw_text(
            raw_text,
            file,
            campaign_id=campaign.id,
        )
        extracted_fields = await extract_campaign_requirements_llm(
            campaign.raw_text,
        )
        campaign.required_fields = {**extracted_fields, **(required_fields or {})}
    elif required_fields is not None:
        campaign.required_fields = {
            **(campaign.required_fields or {}),
            **required_fields,
        }

    if title:
        campaign.title = title

    await db.commit()
    await db.refresh(campaign)
    return campaign


@router.patch("/{campaign_id}/fields", response_model=CampaignResponse)
async def update_campaign_fields(
    campaign_id: UUID,
    body: CampaignFieldsUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Merge incoming values into campaign required_fields without discarding existing data."""
    result = await db.execute(
        select(Campaign).where(
            Campaign.id == campaign_id,
            Campaign.created_by_user_id == current_user.id,
        )
    )
    campaign = result.scalar_one_or_none()
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    campaign.required_fields = {
        **(campaign.required_fields or {}),
        **body.required_fields,
    }

    await db.commit()
    await db.refresh(campaign)
    return campaign


# -----------------------------------------------------------------------
# Candidate CRUD & Upload
# -----------------------------------------------------------------------

@router.patch("/{campaign_id}/candidates/{candidate_id}", response_model=CandidateResponse)
async def update_candidate(
    campaign_id: UUID,
    candidate_id: UUID,
    name: str | None = Body(None),
    email: str | None = Body(None),
    phone: str | None = Body(None),
    extracted_fields: dict | None = Body(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    campaign_result = await db.execute(
        select(Campaign).where(
            Campaign.id == campaign_id,
            Campaign.created_by_user_id == current_user.id,
        )
    )
    if campaign_result.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Campaign not found")

    result = await db.execute(
        select(Candidate).where(
            Candidate.id == candidate_id,
            Candidate.campaign_id == campaign_id,
        )
    )
    candidate = result.scalar_one_or_none()
    if not candidate:
        raise HTTPException(status_code=404, detail="Candidate not found")

    if name is not None:
        candidate.name = name
    if email is not None:
        candidate.email = email
    if phone is not None:
        candidate.phone = phone
    if extracted_fields is not None:
        candidate.extracted_fields = extracted_fields

    await db.commit()
    await db.refresh(candidate)
    return candidate


@router.post(
    "/{campaign_id}/candidates/upload",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=BatchUploadResponse,
)
async def upload_candidates(
    campaign_id: UUID,
    files: list[UploadFile] = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(Campaign).where(
            Campaign.id == campaign_id,
            Campaign.created_by_user_id == current_user.id,
        )
    )
    campaign = result.scalar_one_or_none()
    if campaign is None:
        raise HTTPException(status_code=404, detail="Campaign not found.")

    valid_files, invalid_files = await validate_document_uploads(files)
    if not valid_files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "message": "No uploaded files passed validation.",
                "rejected_files": invalid_files,
            },
        )
    batch_uuid = uuid7()
    batch_id = f"batch_{batch_uuid}"
    s3_prefix, source_type, staged_files, upload_failures = await stage_files_to_s3(
        batch_id,
        valid_files,
    )
    rejected_files = [*invalid_files, *upload_failures]

    if not staged_files:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "message": "None of the candidate files could be stored.",
                "rejected_files": rejected_files,
            },
        )

    ingestion_items = [
        {
            "source_key": source_key,
            "display_name": display_name,
        }
        for source_key, display_name in staged_files
    ]
    await create_ingestion_batch(
        db,
        batch_uuid=batch_uuid,
        context_type="campaign_candidates",
        context_id=campaign_id,
        created_by_user_id=current_user.id,
        items=ingestion_items,
    )
    await db.commit()

    redis = await get_redis_client()
    try:
        await create_batch_tracker(redis, batch_id, file_count=len(staged_files))
        await enqueue_document_upload_batch(
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
        accepted_candidates=len(staged_files),
        rejected_candidates=len(rejected_files),
        rejected_files=rejected_files,
    )


@router.post(
    "/{campaign_id}/candidates/upload/{batch_id}/retry",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=BatchUploadResponse,
)
async def retry_failed_upload(
    campaign_id: UUID,
    batch_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    campaign_result = await db.execute(
        select(Campaign).where(
            Campaign.id == campaign_id,
            Campaign.created_by_user_id == current_user.id,
        )
    )
    if campaign_result.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Campaign not found.")

    try:
        ingestion_batch_id = UUID(batch_id.removeprefix("batch_"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid batch ID.") from exc

    batch = await db.get(IngestionBatch, ingestion_batch_id)
    if batch is None or batch.context_id != campaign_id:
        raise HTTPException(status_code=404, detail="Batch not found.")

    item_result = await db.execute(
        select(IngestionItem).where(
            IngestionItem.batch_id == ingestion_batch_id,
            IngestionItem.status.in_(
                [IngestionItemStatus.FAILED, IngestionItemStatus.RETRYABLE]
            ),
        )
    )
    retry_items = item_result.scalars().all()
    if not retry_items:
        raise HTTPException(status_code=409, detail="Batch has no failed files to retry.")

    source_result = await db.execute(
        select(IngestionItem.source_key).where(
            IngestionItem.batch_id == ingestion_batch_id,
            IngestionItem.member_path.is_(None),
        ).limit(1)
    )
    source_key = source_result.scalar_one_or_none()
    if not source_key:
        raise HTTPException(status_code=409, detail="Batch source is unavailable.")

    s3_prefix = source_key.rsplit("/", 1)[0]
    for item in retry_items:
        item.status = IngestionItemStatus.PENDING
        item.current_stage = None
        item.last_error = None
    batch.status = IngestionBatchStatus.QUEUED
    await db.commit()

    redis = await get_redis_client()
    try:
        await create_batch_tracker(redis, batch_id, file_count=len(retry_items))
        await enqueue_document_upload_batch(
            redis,
            batch_id=batch_id,
            campaign_id=str(campaign_id),
            s3_prefix=s3_prefix,
            source_type="retry",
        )
    finally:
        await redis.aclose()

    return BatchUploadResponse(
        batch_id=batch_id,
        status="QUEUED",
        accepted_candidates=len(retry_items),
        rejected_candidates=0,
    )


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
    campaign_result = await db.execute(
        select(Campaign).where(
            Campaign.id == campaign_id,
            Campaign.created_by_user_id == current_user.id,
        )
    )
    if campaign_result.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Campaign not found.")

    redis = await get_redis_client()
    try:
        data = await get_batch_status(redis, batch_id)
    finally:
        await redis.aclose()

    if data is None:
        raise HTTPException(status_code=404, detail="Batch not found.")

    status_val = data.get("status", "UNKNOWN")
    failed_candidates: list[str] = []

    try:
        ingestion_batch_id = UUID(batch_id.removeprefix("batch_"))
        failed_result = await db.execute(
            select(IngestionItem.display_name).where(
                IngestionItem.batch_id == ingestion_batch_id,
                IngestionItem.status.in_(
                    [IngestionItemStatus.FAILED, IngestionItemStatus.RETRYABLE]
                ),
            )
        )
        failed_candidates = list(failed_result.scalars().all())
    except ValueError:
        pass

    return BatchStatusResponse(
        batch_id=batch_id,
        status=status_val,
        total_candidates=int(data.get("total_candidates", 0)),
        processed=int(data.get("processed", 0)),
        failed=int(data.get("failed", 0)),
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
        finished_at=data.get("finished_at"),
        failed_candidates=failed_candidates
    )


# -----------------------------------------------------------------------
# Candidate Screening
# -----------------------------------------------------------------------

@router.get(
    "/{campaign_id}/candidates",
    response_model=list[CandidateResponse]
)
async def get_candidates(
    campaign_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Verify the requesting user owns this campaign
    campaign_result = await db.execute(
        select(Campaign).where(
            Campaign.id == campaign_id,
            Campaign.created_by_user_id == current_user.id,
        )
    )
    if campaign_result.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Campaign not found.")

    result = await db.execute(
        select(Candidate).where(Candidate.campaign_id == campaign_id)
    )
    return result.scalars().all()


@router.post(
    "/{campaign_id}/candidates/screen",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ScreeningBatchResponse,
)
async def screen_candidates(
    campaign_id: UUID,
    request: ScreeningRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(Campaign).where(
            Campaign.id == campaign_id,
            Campaign.created_by_user_id == current_user.id,
        )
    )
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Campaign not found.")

    batch_id = f"screen_{uuid7()}"
    redis = await get_redis_client()
    try:
        await create_batch_tracker(redis, batch_id, file_count=0)
        candidate_ids_str = [str(cid) for cid in request.candidate_ids] if request.candidate_ids else None

        await enqueue_campaign_screening(
            redis,
            batch_id=batch_id,
            campaign_id=str(campaign_id),
            candidate_ids=candidate_ids_str,
        )
    finally:
        await redis.aclose()

    return ScreeningBatchResponse(
        batch_id=batch_id,
        status="QUEUED",
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
    redis = await get_redis_client()
    try:
        data = await get_batch_status(redis, batch_id)
    finally:
        await redis.aclose()

    if data is None:
        raise HTTPException(status_code=404, detail="Batch not found.")

    return BatchStatusResponse(
        batch_id=batch_id,
        status=data.get("status", "UNKNOWN"),
        total_candidates=int(data.get("total_candidates", 0)),
        processed=int(data.get("processed", 0)),
        failed=int(data.get("failed", 0)),
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
        finished_at=data.get("finished_at"),
    )

@router.post(
    "/{campaign_id}/candidates/call",
    response_model=CallingBatchResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def call_candidates(
    campaign_id: UUID,
    request: CallingRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(Campaign).where(
            Campaign.id == campaign_id,
            Campaign.created_by_user_id == current_user.id,
        )
    )
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Campaign not found.")

    # Mark candidates as outbound_call / PENDING in the DB
    candidate_query = select(Candidate).where(
        Candidate.campaign_id == campaign_id,
        Candidate.phone.isnot(None),
    )
    if request.candidate_ids is not None:
        candidate_query = candidate_query.where(Candidate.id.in_(request.candidate_ids))

    candidate_results = await db.execute(candidate_query)
    candidates = candidate_results.scalars().all()
    for candidate in candidates:
        candidate.workflow_step = "outbound_call"
        candidate.step_status = WorkflowStepStatus.PENDING
    await db.commit()

    batch_id = f"call_{uuid7()}"
    redis = await get_redis_client()
    try:
        await create_batch_tracker(redis, batch_id, file_count=len(candidates))
        candidate_ids_str = (
            [str(c.id) for c in candidates]
            if request.candidate_ids is not None
            else None
        )

        await enqueue_campaign_calling(
            redis,
            batch_id=batch_id,
            campaign_id=str(campaign_id),
            candidate_ids=candidate_ids_str,
        )
    finally:
        await redis.aclose()

    return CallingBatchResponse(
        batch_id=batch_id,
        status="QUEUED",
    )


@router.get(
    "/{campaign_id}/candidates/call/{batch_id}/status",
    response_model=BatchStatusResponse,
)
async def calling_batch_status(
    campaign_id: UUID,
    batch_id: str,
    current_user: User = Depends(get_current_user),
):
    redis = await get_redis_client()
    try:
        data = await get_batch_status(redis, batch_id)
    finally:
        await redis.aclose()

    if data is None:
        raise HTTPException(status_code=404, detail="Batch not found.")

    return BatchStatusResponse(
        batch_id=batch_id,
        status=data.get("status", "UNKNOWN"),
        total_candidates=int(data.get("total_candidates", 0)),
        processed=int(data.get("processed", 0)),
        failed=int(data.get("failed", 0)),
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
        finished_at=data.get("finished_at"),
    )


@router.post(
    "/{campaign_id}/candidates/call/{batch_id}/pause",
    response_model=CallingBatchControlResponse,
    status_code=status.HTTP_200_OK,
)
async def pause_calling(
    campaign_id: UUID,
    batch_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Pause an in-progress calling batch. Updates uncalled candidates in DB to PAUSED
    and cleanly stops worker dispatching."""
    result = await db.execute(
        select(Campaign).where(
            Campaign.id == campaign_id,
            Campaign.created_by_user_id == current_user.id,
        )
    )
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Campaign not found.")

    redis = await get_redis_client()
    try:
        data = await get_batch_status(redis, batch_id)
        if data is None:
            raise HTTPException(status_code=404, detail="Batch not found.")
        if data.get("status") not in ("PROCESSING", "QUEUED"):
            raise HTTPException(
                status_code=409,
                detail=f"Cannot pause a batch in '{data.get('status')}' state.",
            )
        await pause_calling_batch(db, redis, batch_id=batch_id, campaign_id=campaign_id)
    finally:
        await redis.aclose()

    return CallingBatchControlResponse(batch_id=batch_id, status="PAUSED")


@router.post(
    "/{campaign_id}/candidates/call/{batch_id}/resume",
    response_model=CallingBatchControlResponse,
    status_code=status.HTTP_200_OK,
)
async def resume_calling(
    campaign_id: UUID,
    batch_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Resume a previously paused calling batch. Resets PAUSED candidates to PENDING
    and re-enqueues calling in ARQ."""
    result = await db.execute(
        select(Campaign).where(
            Campaign.id == campaign_id,
            Campaign.created_by_user_id == current_user.id,
        )
    )
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Campaign not found.")

    redis = await get_redis_client()
    try:
        data = await get_batch_status(redis, batch_id)
        if data is None:
            raise HTTPException(status_code=404, detail="Batch not found.")
        if data.get("status") != "PAUSED":
            raise HTTPException(
                status_code=409,
                detail=f"Cannot resume a batch in '{data.get('status')}' state.",
            )
        await resume_calling_batch(db, redis, batch_id=batch_id, campaign_id=campaign_id)
    finally:
        await redis.aclose()

    return CallingBatchControlResponse(batch_id=batch_id, status="PROCESSING")
