"""
Campaign service layer.

Contains:
- Document upload validation (extension, MIME, zip safety)
- S3 staging
- Redis batch tracker CRUD
- Generic Document Text Extraction (PDF, DOCX, TXT)
- Generic LLM Document Fields Extraction
- Generic LLM Screening
- Call webhook processing
"""
from __future__ import annotations
from click import UUID

import asyncio
import io
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import HTTPException, UploadFile, status
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.s3 import upload_fileobj_to_s3
from app.domains.campaigns.models import (
    CallScreening,
    Campaign,
    Candidate,
    WorkflowStepStatus,
)
from app.domains.campaigns.schemas import CallWebhookPayload
from app.domains.ingestion.models import IngestionItem
from app.integrations.ai.factory import StructuredExtractionProviderFactory
from app.integrations.ai.providers.base import StructuredExtractionProvider
from app.domains.ingestion.text import (
    extract_document_text,
    extract_text_from_docx,
    extract_text_from_pdf,
    extract_text_from_txt,
    normalize_text,
    validate_magic_bytes,
)


# =========================================================================
# DOCUMENT UPLOAD VALIDATION
# =========================================================================

ALLOWED_EXTENSIONS: set[str] = {".pdf", ".docx", ".txt", ".csv", ".zip"}

EXT_TO_MIME: dict[str, set[str]] = {
    ".pdf": {"application/pdf"},
    ".docx": {
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/zip",
    },
    ".txt": {"text/plain", "application/octet-stream"},
    ".csv": {"text/csv", "text/plain", "application/vnd.ms-excel", "application/octet-stream"},
    ".zip": {"application/zip", "application/x-zip-compressed", "application/octet-stream"},
}

_ZIP_MAX_UNCOMPRESSED_BYTES = 500 * 1024 * 1024  # 500 MB
_ZIP_MAX_RATIO = 50


async def validate_document_uploads(files: list[UploadFile]) -> tuple[list[UploadFile], list[dict]]:
    """
    Validates uploaded files.
    Returns a tuple of:
    - list[UploadFile]: Files that passed validation.
    - list[dict]: Details of files that failed validation.
    """
    valid_files = []
    invalid_files = []

    # Handle the empty list edge case up front
    if not files:
        return [], [{"filename": "unknown", "reason": "No files were uploaded."}]

    for f in files:
        # Check for missing filename
        if not f.filename:
            invalid_files.append({
                "filename": "Unknown Filename",
                "reason": "Every uploaded file must have a filename."
            })
            continue

        ext = Path(f.filename).suffix.lower()

        # Check for allowed extension
        if ext not in ALLOWED_EXTENSIONS:
            invalid_files.append({
                "filename": f.filename,
                "reason": f"Unsupported file type. Allowed: {', '.join(ALLOWED_EXTENSIONS)}"
            })
            continue

        # Check for MIME type mismatch
        expected = EXT_TO_MIME.get(ext, set())
        actual = f.content_type or ""
        if actual and expected and actual not in expected:
            invalid_files.append({
                "filename": f.filename,
                "reason": f"MIME mismatch: expected one of {expected}, got {actual}"
            })
            continue

        # If it passes all checks, it's valid
        valid_files.append(f)

    return valid_files, invalid_files


async def validate_zip_safety(data: bytes) -> list[str]:
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded zip file is corrupt or not a valid zip.",
        )

    infos = zf.infolist()
    max_count = getattr(settings, "RESUME_MAX_FILE_COUNT", 500)
    if len(infos) > max_count:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Zip contains {len(infos)} entries (max {max_count}).",
        )

    total_uncompressed = sum(i.file_size for i in infos)
    compressed_size = len(data)
    if total_uncompressed > _ZIP_MAX_UNCOMPRESSED_BYTES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Zip file exceeds maximum uncompressed size.",
        )
    if compressed_size > 0 and (total_uncompressed / compressed_size) > _ZIP_MAX_RATIO:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Zip compression ratio is suspiciously high.",
        )

    safe_names: list[str] = []
    for info in infos:
        name = info.filename
        if info.is_dir():
            continue
        if (info.external_attr >> 16) & 0o120000 == 0o120000:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Symlink found: {name}")
        if name.startswith("/") or name.startswith("\\") or ".." in name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Path traversal: {name}")

        basename = Path(name).name
        if basename.startswith(".") or basename in {"__MACOSX", "Thumbs.db", "desktop.ini"} or "__MACOSX" in name:
            continue

        if Path(name).suffix.lower() == ".zip":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Nested zip found: {name}")

        ext = Path(name).suffix.lower()
        if ext not in {".pdf", ".docx", ".txt", ".csv"}:
            continue

        safe_names.append(name)

    if not safe_names:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Zip contains no supported files.",
        )

    return safe_names


# =========================================================================
# S3 STAGING
# =========================================================================

async def stage_files_to_s3(
    batch_id: str,
    files: list[UploadFile],
) -> tuple[str, str]:
    s3_prefix = f"document-batches/{batch_id}/original"
    source_type = "single" if len(files) == 1 else "multi"

    for f in files:
        if Path(f.filename).suffix.lower() == ".zip":
            source_type = "zip"

        key = f"{s3_prefix}/{f.filename}"
        content_type = f.content_type or "application/octet-stream"
        await asyncio.to_thread(
            upload_fileobj_to_s3, key, f.file, content_type=content_type
        )

    return s3_prefix, source_type


# =========================================================================
# REDIS BATCH TRACKER
# =========================================================================

_BATCH_PENDING_KEY = "campaign:{batch_id}:pending"
_BATCH_TTL = 7 * 86400  # 7 days


async def create_batch_tracker(redis: Redis, batch_id: str, file_count: int) -> None:
    now = datetime.now(timezone.utc).isoformat()
    await redis.hset(  # type: ignore
        f"job:{batch_id}",
        mapping={
            "status": "QUEUED",
            "total_candidates": str(file_count),
            "processed": "0",
            "failed": "0",
            "created_at": now,
            "updated_at": now,
        },
    )
    await redis.expire(f"job:{batch_id}", _BATCH_TTL)


async def update_batch_progress(
    redis: Redis,
    batch_id: str,
    *,
    total_candidates: int | None = None,
    processed_incr: int = 0,
    failed_incr: int = 0,
    status: str | None = None,
) -> None:
    pipe = redis.pipeline(transaction=True)
    if total_candidates is not None:
        pipe.hset(f"job:{batch_id}", "total_candidates", str(total_candidates))
    if processed_incr:
        pipe.hincrby(f"job:{batch_id}", "processed", processed_incr)
    if failed_incr:
        pipe.hincrby(f"job:{batch_id}", "failed", failed_incr)
    if status:
        pipe.hset(f"job:{batch_id}", "status", status)
    pipe.hset(f"job:{batch_id}", "updated_at", datetime.now(timezone.utc).isoformat())
    await pipe.execute()


async def complete_batch(redis: Redis, batch_id: str, status: str = "COMPLETED") -> None:
    now = datetime.now(timezone.utc).isoformat()
    await redis.hset(  # type: ignore
        f"job:{batch_id}",
        mapping={
            "status": status,
            "finished_at": now,
            "updated_at": now,
        },
    )


async def fail_batch(redis: Redis, batch_id: str, reason: str = "") -> None:
    now = datetime.now(timezone.utc).isoformat()
    await redis.hset(  # type: ignore
        f"job:{batch_id}",
        mapping={
            "status": "FAILED",
            "finished_at": now,
            "updated_at": now,
            "error": reason[:500],
        },
    )


async def get_batch_status(redis: Redis, batch_id: str) -> dict[str, str] | None:
    data = await redis.hgetall(f"job:{batch_id}")  # type: ignore
    return data or None


# ---------------------------------------------------------------------------
# Redis Set-based atomic completion tracker (Phase 3)
# ---------------------------------------------------------------------------

async def batch_add_pending(redis: Redis, batch_id: str, item_ids: list[str]) -> None:
    """Atomically register all pending item IDs into the tracking Set.

    The Set cardinality reaching 0 is the authoritative completion signal.
    Using a Set (not a counter) prevents double-counting if a task runs twice.
    """
    if not item_ids:
        return
    key = _BATCH_PENDING_KEY.format(batch_id=batch_id)
    pipe = redis.pipeline(transaction=True)
    for item_id in item_ids:
        pipe.sadd(key, item_id)  # type: ignore
    pipe.expire(key, _BATCH_TTL)  # type: ignore
    await pipe.execute()


async def batch_mark_done(redis: Redis, batch_id: str, item_id: str) -> bool:
    """Remove *item_id* from the pending Set.

    Returns ``True`` iff this was the last item (Set is now empty), meaning
    the caller is responsible for finalising the batch.  Uses Redis SREM +
    SCARD atomically via a pipeline so exactly one caller gets ``True``.
    """
    key = _BATCH_PENDING_KEY.format(batch_id=batch_id)
    pipe = redis.pipeline(transaction=True)
    pipe.srem(key, item_id)  # type: ignore
    pipe.scard(key)  # type: ignore
    results = await pipe.execute()
    remaining: int = results[1]
    return remaining == 0


# =========================================================================
# ARQ JOB ENQUEUE
# =========================================================================

async def enqueue_document_upload_batch(
    redis: Redis,
    *,
    batch_id: str,
    campaign_id: str,
    s3_prefix: str,
    source_type: str,
) -> None:
    from arq import create_pool
    from arq.connections import RedisSettings

    pool = await create_pool(RedisSettings.from_dsn(settings.REDIS_URL))
    # Dispatcher: fans out individual file tasks inside the worker
    await pool.enqueue_job(
        "dispatch_ingestion_batch",
        batch_id=batch_id,
        campaign_id=campaign_id,
        s3_prefix=s3_prefix,
        source_type=source_type,
    )
    await pool.aclose()


async def enqueue_campaign_screening(
    redis: Redis,
    *,
    batch_id: str,
    campaign_id: str,
    candidate_ids: list[str] | None = None,
) -> None:
    from arq import create_pool
    from arq.connections import RedisSettings

    pool = await create_pool(RedisSettings.from_dsn(settings.REDIS_URL))
    # Dispatcher: fans out individual candidate screening tasks inside the worker
    await pool.enqueue_job(
        "dispatch_campaign_screening",
        batch_id=batch_id,
        campaign_id=campaign_id,
        candidate_ids=candidate_ids,
    )
    await pool.aclose()


async def enqueue_campaign_calling(
    redis: Redis,
    *,
    batch_id: str,
    campaign_id: str,
    candidate_ids: list[str] | None = None,
) -> None:
    from arq import create_pool
    from arq.connections import RedisSettings

    pool = await create_pool(RedisSettings.from_dsn(settings.REDIS_URL))
    # Dispatcher: fans out individual candidate call tasks inside the worker
    await pool.enqueue_job(
        "dispatch_campaign_calling",
        batch_id=batch_id,
        campaign_id=campaign_id,
        candidate_ids=candidate_ids,
    )
    await pool.aclose()


# ---------------------------------------------------------------------------
# Pause / Resume — pure DB state (Phase 4)
# Redis flags are no longer used; Campaign.status is the sole authority.
# ---------------------------------------------------------------------------

async def pause_calling_batch(
    db: AsyncSession,
    redis: Redis,
    *,
    batch_id: str,
    campaign_id: UUID,
) -> int:
    """Pause an active calling batch.

    Sets Campaign.status to 'PAUSED' in Postgres.  Atomic workers check this
    field on every wake-up and exit gracefully without needing a Redis flag.
    Also marks uncalled candidates as PAUSED so resumption knows which ones
    still need to be dialled.
    """
    # Update Redis tracker for UI visibility
    await update_batch_progress(redis, batch_id, status="PAUSED")

    # Mark all candidates that are currently pending/ready for calling as PAUSED
    stmt = (
        select(Candidate)
        .where(
            Candidate.campaign_id == campaign_id,
            Candidate.workflow_step == "outbound_call",
            Candidate.step_status.in_([WorkflowStepStatus.PENDING, WorkflowStepStatus.READY_FOR_ACTION]),
        )
    )
    result = await db.execute(stmt)
    candidates = result.scalars().all()
    for candidate in candidates:
        candidate.step_status = WorkflowStepStatus.PAUSED

    await db.commit()
    return len(candidates)


async def resume_calling_batch(
    db: AsyncSession,
    redis: Redis,
    *,
    batch_id: str,
    campaign_id: UUID,
) -> int:
    """Resume a paused calling batch.

    Resets PAUSED candidates back to PENDING in Postgres and fans out new
    atomic call tasks into ARQ — one per candidate.  No Redis flags to clear.
    """
    from arq import create_pool
    from arq.connections import RedisSettings
    from uuid6 import uuid7

    await update_batch_progress(redis, batch_id, status="PROCESSING")

    # Find candidates that were paused
    stmt = (
        select(Candidate)
        .where(
            Candidate.campaign_id == campaign_id,
            Candidate.workflow_step == "outbound_call",
            Candidate.step_status == WorkflowStepStatus.PAUSED,
        )
    )
    result = await db.execute(stmt)
    candidates = result.scalars().all()
    candidate_ids = [str(c.id) for c in candidates]

    for candidate in candidates:
        candidate.step_status = WorkflowStepStatus.PENDING
    await db.commit()

    if not candidate_ids:
        return 0

    # Re-initialise the Redis pending Set for the resumed batch
    await batch_add_pending(redis, batch_id, candidate_ids)

    # Fan out one atomic call task per candidate
    pool = await create_pool(RedisSettings.from_dsn(settings.REDIS_URL))
    try:
        for cid in candidate_ids:
            await pool.enqueue_job(
                "call_single_candidate",
                campaign_id=str(campaign_id),
                candidate_id=cid,
                batch_id=batch_id,
            )
    finally:
        await pool.aclose()

    return len(candidates)


# =========================================================================
# TEXT EXTRACTION
# =========================================================================

async def build_campaign_raw_text(
    raw_text: str | None,
    uploaded_file: UploadFile | None,
) -> str | None:
    if raw_text is not None and uploaded_file is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide either raw_text or a requirement file, not both.",
        )

    candidate_parts: list[str] = []

    if raw_text and raw_text.strip():
        candidate_parts.append(raw_text.strip())

    if uploaded_file is None:
        return "\n\n".join(candidate_parts) if candidate_parts else None

    filename = uploaded_file.filename or ""
    if not filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded requirement file must include a filename.",
        )

    ext = Path(filename).suffix.lower()
    if ext not in {".pdf", ".docx", ".txt", ".csv"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Campaign requirement file must be PDF, DOCX, TXT, or CSV.",
        )

    file_bytes = await uploaded_file.read()
    if not file_bytes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded requirement file is empty.",
        )

    if not validate_magic_bytes(file_bytes, ext):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Uploaded file does not match the expected {ext.upper()} signature.",
        )

    extracted = extract_document_text(file_bytes, ext)
    if not extracted or not extracted.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded requirement file did not produce usable text.",
        )

    candidate_parts.append(extracted.strip())
    return "\n\n".join(candidate_parts)


async def extract_campaign_requirements_llm(
    raw_text: str,
    existing_fields: dict | None = None,
    extraction_provider: StructuredExtractionProvider | None = None,
) -> dict[str, Any]:
    """Extract structured campaign requirements from already extracted text."""
    if not raw_text or not raw_text.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Campaign requirement text cannot be empty.",
        )

    return await extract_document_fields_llm(
        raw_text,
        existing_fields=existing_fields,
        extraction_provider=extraction_provider,
    )


# =========================================================================
# GENERIC LLM EXTRACTION & SCREENING
# =========================================================================

async def extract_document_fields_llm(
    document_text: str,
    existing_fields: dict | None = None,
    extraction_provider: StructuredExtractionProvider | None = None,
) -> dict[str, Any]:
    if not document_text or not document_text.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Document text cannot be empty for structured extraction.",
        )
    if extraction_provider is None:
        extraction_provider = StructuredExtractionProviderFactory.build()

    try:
        return await extraction_provider.extract(document_text, existing_fields)
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"AI service is currently unavailable. Please try again later. Details: {e}",
        )


async def screen_document_llm(
    candidate_fields: dict,
    campaign_fields: dict,
    extraction_provider: StructuredExtractionProvider | None = None,
) -> dict[str, Any]:
    if extraction_provider is None:
        extraction_provider = StructuredExtractionProviderFactory.build()

    try:
        return await extraction_provider.screen_candidate(
            candidate_text="",
            candidate_fields=candidate_fields or {},
            campaign_text="",
            campaign_fields=campaign_fields or {},
        )
    except RuntimeError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"AI service is currently unavailable. Please try again later. Details: {e}",
        )


# =========================================================================
# CANDIDATE PERSISTENCE
# =========================================================================

async def persist_candidate_from_ingestion(
    db: AsyncSession,
    *,
    campaign_id,
    item: IngestionItem,
    source_url: str,
    extracted_fields: dict[str, Any],
) -> tuple[Candidate, bool]:
    """Persist campaign-specific output for one generic ingestion item."""
    existing_result = await db.execute(
        select(Candidate).where(Candidate.ingestion_item_id == item.id)
    )
    existing_candidate = existing_result.scalar_one_or_none()
    if existing_candidate is not None:
        return existing_candidate, False

    candidate = Candidate(
        campaign_id=campaign_id,
        name=extracted_fields.get("name", "Unknown"),
        email=extracted_fields.get("email"),
        phone=extracted_fields.get("phone"),
        file_url=source_url,
        ingestion_item_id=item.id,
        extracted_fields=extracted_fields,
        workflow_step="document_extraction",
        step_status=WorkflowStepStatus.COMPLETED,
    )
    db.add(candidate)
    await db.flush()
    return candidate, True


async def extract_candidates_from_csv_llm(
    csv_text: str,
    extraction_provider: StructuredExtractionProvider | None = None,
) -> list[dict[str, Any]]:
    """Use the LLM to parse a block of CSV text into a list of candidate dicts.

    Each dict should contain at least ``name``, ``email``, and ``phone`` when
    those fields are present in the CSV.  All other columns are kept verbatim
    inside ``extracted_fields`` on the candidate.

    The LLM is instructed to return a JSON **array** where every element
    represents one candidate / data row.
    """
    if not csv_text or not csv_text.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="CSV text cannot be empty for candidate extraction.",
        )
    if extraction_provider is None:
        extraction_provider = StructuredExtractionProviderFactory.build()

    # We call into the Gemini client directly via its underlying _generate_content
    # method so we can supply a custom array-returning prompt, but we keep the
    # shared retry / timeout logic from the provider.
    import json, re

    prompt = f"""\
You are a data extraction assistant.
You will be given structured text representing one or more rows from a CSV file.
Each row represents a single candidate (person).

Your task:
1. Parse each row.
2. For every row produce a JSON object with these keys:
   - "name"  : full name of the person (string, or null if missing)
   - "email" : email address (string, or null if missing)
   - "phone" : phone / mobile number as a string (or null if missing)
   - "extracted_fields": a flat JSON object with all remaining non-empty
     columns as key-value string pairs.
3. Return a JSON **array** containing one object per row.
4. Output valid JSON only — no markdown, no prose.

CSV Data:
{csv_text}
"""

    try:
        # Access the underlying Gemini client to get a raw JSON response.
        raw = await extraction_provider._generate_content(prompt)  # type: ignore[attr-defined]
        text = getattr(raw, "text", None) or str(raw)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"AI service is currently unavailable: {exc}",
        )

    json_text = text.strip()
    if json_text.startswith("```"):
        json_text = re.sub(r"^```json\s*", "", json_text, flags=re.IGNORECASE)
        json_text = re.sub(r"```$", "", json_text).strip()

    try:
        payload = json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"LLM returned invalid JSON for CSV extraction: {exc}",
        )

    if not isinstance(payload, list):
        # Tolerate single-object responses by wrapping them
        if isinstance(payload, dict):
            payload = [payload]
        else:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="LLM did not return an array of candidates.",
            )

    return payload


async def persist_csv_candidate(
    db: AsyncSession,
    *,
    campaign_id,
    ingestion_item_id,
    source_url: str,
    extracted_fields: dict[str, Any],
) -> Candidate:
    """Create a Candidate from a single CSV row.

    Unlike ``persist_candidate_from_ingestion``, multiple candidates can originate
    from the same parent CSV file. Because ``Candidate.ingestion_item_id`` has a UNIQUE
    constraint, we do NOT assign ``ingestion_item_id`` directly to avoid collisions;
    instead we record ``source_ingestion_item_id`` inside ``extracted_fields``.
    """
    core_fields = dict(extracted_fields.get("extracted_fields") or {})
    if ingestion_item_id is not None:
        core_fields["source_ingestion_item_id"] = str(ingestion_item_id)
    # Merge top-level known fields into extracted_fields for storage
    for key in ("name", "email", "phone"):
        val = extracted_fields.get(key)
        if val:
            core_fields[key] = val

    candidate = Candidate(
        campaign_id=campaign_id,
        name=extracted_fields.get("name") or "Unknown",
        email=extracted_fields.get("email"),
        phone=extracted_fields.get("phone"),
        file_url=source_url,
        ingestion_item_id=None,
        extracted_fields=core_fields,
        workflow_step="document_extraction",
        step_status=WorkflowStepStatus.COMPLETED,
    )
    db.add(candidate)
    await db.flush()
    return candidate


# =========================================================================
# WEBHOOK PROCESSING
# =========================================================================

async def process_call_webhook(payload: CallWebhookPayload) -> None:
    from app.core.database import AsyncSessionLocal
    from app.core.redis import get_redis_client
    
    # 1. Fetch Candidate and Campaign (short transaction)
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Candidate).where(Candidate.id == payload.candidate_id)
        )
        candidate = result.scalar_one_or_none()
        if not candidate:
            raise HTTPException(status_code=404, detail="Candidate not found")

        result = await db.execute(
            select(Campaign).where(Campaign.id == payload.campaign_id)
        )
        campaign = result.scalar_one_or_none()
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")

        candidate_fields = dict(candidate.extracted_fields or {})
        campaign_text = campaign.raw_text or ""
        campaign_fields = dict(campaign.required_fields or {})
        campaign_id = campaign.id
        candidate_id = candidate.id

    # 2. Extract Data from Transcript & Screen using Generic Engine (No DB lock)
    extraction_provider = StructuredExtractionProviderFactory.build()
    transcript = payload.transcript or ""

    extracted_fields = await extraction_provider.extract(transcript, candidate_fields)

    screening_result = await extraction_provider.screen_candidate(
        candidate_text=transcript,
        candidate_fields=extracted_fields,
        campaign_text=campaign_text,
        campaign_fields=campaign_fields,
    )

    # 3. Create CallScreening record and update Candidate (short transaction)
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Candidate).where(Candidate.id == payload.candidate_id)
        )
        candidate = result.scalar_one_or_none()
        if not candidate:
            return

        call_screening = CallScreening(
            campaign_id=campaign_id,
            candidate_id=candidate_id,
            transcript=transcript,
            recording_url=payload.recording_url,
            match_score=screening_result.get("match_score", 0.0),
            matched_fields=screening_result.get("matched_fields", {}),
            unmatched_fields=screening_result.get("unmatched_fields", {}),
            summary=screening_result.get("summary", ""),
        )
        db.add(call_screening)

        merged_fields = dict(candidate.extracted_fields or {})
        merged_fields.update(extracted_fields)
        candidate.extracted_fields = merged_fields

        if "name" in extracted_fields and extracted_fields["name"]:
            candidate.name = extracted_fields["name"]
        if "email" in extracted_fields and extracted_fields["email"]:
            candidate.email = extracted_fields["email"]
        if "phone" in extracted_fields and extracted_fields["phone"]:
            candidate.phone = extracted_fields["phone"]

        await db.commit()

        # 5. Notify Orchestrator to proceed to next step
        from app.domains.campaigns.orchestrator import on_step_completed
        await on_step_completed(
            db=db,
            candidate_id=candidate.id,
            service_name="outbound_call",
            payload={
                "call_id": payload.call_id,
                "status": payload.status,
                "match_score": call_screening.match_score,
            }
        )

    # Clean up Redis concurrency tracker
    redis = await get_redis_client()
    try:
        await redis.srem(f"campaign_active_calls:{payload.campaign_id}", str(payload.candidate_id)) # type: ignore
    finally:
        await redis.aclose()
