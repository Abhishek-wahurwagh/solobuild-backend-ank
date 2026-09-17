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
    Body,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from uuid6 import uuid7

from app.core.database import get_db
from app.core.redis import get_redis_pool
from app.domains.auth.dependencies import get_current_user
from app.domains.campaigns.models import (
    Campaign,
    Candidate,
    WorkflowStepStatus,
)
from app.domains.campaigns.schemas import BatchStatusResponse, BatchUploadResponse, CampaignResponse, CandidateResponse, ScreeningRequest
from app.domains.campaigns.service import (
    create_batch_tracker,
    enqueue_document_upload_batch,
    enqueue_campaign_screening,
    get_batch_status,
    stage_files_to_s3,
    validate_document_uploads,
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
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    campaign = Campaign(
        title=title,
        raw_text=raw_text,
        created_by_user_id=current_user.id,
    )
    db.add(campaign)
    await db.commit()
    await db.refresh(campaign)
    return campaign


@router.patch("/{campaign_id}", response_model=CampaignResponse)
async def update_campaign(
    campaign_id: UUID,
    raw_text: str | None = Body(None),
    required_fields: dict | None = Body(None),
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
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    if raw_text is not None:
        campaign.raw_text = raw_text
    if required_fields is not None:
        campaign.required_fields = required_fields

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

    validate_document_uploads(files)
    batch_id = f"batch_{uuid7()}"
    s3_prefix, source_type = await stage_files_to_s3(batch_id, files)

    redis = await get_redis_pool()
    try:
        await create_batch_tracker(redis, batch_id, file_count=len(files))
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
        accepted_files=len(files),
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
    redis = await get_redis_pool()
    try:
        data = await get_batch_status(redis, batch_id)
    finally:
        await redis.aclose()

    if data is None:
        raise HTTPException(status_code=404, detail="Batch not found.")

    status_val = data.get("status", "UNKNOWN")
    candidates = None

    if status_val == "COMPLETED":
        result = await db.execute(
            select(Candidate).where(
                Candidate.campaign_id == campaign_id,
                Candidate.workflow_step == "document_extraction"
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
    result = await db.execute(
        select(Campaign).where(
            Campaign.id == campaign_id,
            Campaign.created_by_user_id == current_user.id,
        )
    )
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Campaign not found.")

    batch_id = f"screen_{uuid7()}"
    redis = await get_redis_pool()
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
    redis = await get_redis_pool()
    try:
        data = await get_batch_status(redis, batch_id)
    finally:
        await redis.aclose()

    if data is None:
        raise HTTPException(status_code=404, detail="Batch not found.")

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

# -----------------------------------------------------------------------
# Webhooks
# -----------------------------------------------------------------------

from app.domains.campaigns.schemas import CallWebhookPayload
from app.domains.campaigns.service import process_call_webhook

@router.post("/webhooks/call-completed", status_code=status.HTTP_200_OK)
async def call_completed_webhook(
    payload: CallWebhookPayload,
    db: AsyncSession = Depends(get_db),
):
    """
    Webhook endpoint to receive call completion data from Voice Provider (e.g. Vobiz).
    """
    await process_call_webhook(db, payload)
    return {"status": "ok"}