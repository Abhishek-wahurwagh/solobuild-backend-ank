"""
ARQ background worker - Atomic Task Fan-Out architecture.


Worker functions

Dispatchers (lightweight, fan-out only):
  dispatch_ingestion_batch         queues one ingest_single_file task per file
  dispatch_campaign_screening      queues one screen_single_candidate task per candidate
  dispatch_campaign_calling        queues one call_single_candidate task per candidate

Atomic workers (process exactly ONE item):
  ingest_single_file               downloads & pipelines a single document (or fans out CSV chunks)
  process_csv_chunk                extracts & persists candidates for a single CSV chunk
  screen_single_candidate          LLM-screens a single candidate
  call_single_candidate            initiates an outbound call for a single candidate

Maintenance:
  reconcile_zombie_tasks           cron job; re-queues stuck IN_PROGRESS candidates

Design notes

 Each atomic worker calls batch_mark_done after finishing (success or terminal failure).
  When the returned cardinality reaches 0, that worker finalises the batch.
 Rate-limiting for calling uses arq.Retry(defer=...) instead of asyncio.sleep,
  so the worker slot is released immediately when concurrency is full.
 Pause state lives exclusively in Postgres (Candidate.step_status == PAUSED).
  There are no Redis pause flags; atomic workers check Campaign status on each run.
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from arq import ArqRedis, Retry
from arq import create_pool
from arq.connections import RedisSettings
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from uuid6 import uuid7

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.core.redis import get_redis_client
from app.core.s3 import download_s3_object, download_s3_prefix, upload_bytes_to_s3
from app.domains.users.models import User
from app.domains.campaigns.models import (
    Campaign,
    Candidate,
    DocumentScreening,
    WorkflowStepStatus,
)
from app.domains.campaigns.orchestrator import on_step_completed
from app.domains.campaigns.service import (
    adjust_batch_total_once,
    batch_add_pending,
    batch_mark_done,
    batch_mark_done_with_progress,
    complete_batch,
    extract_candidates_from_csv_llm,
    extract_document_fields_llm,
    fail_batch,
    persist_candidate_from_ingestion,
    persist_csv_candidate,
    process_call_webhook,
    screen_document_llm,
    update_batch_progress,
)
from app.domains.ingestion.files import safe_extract_zip
from app.domains.ingestion.files import collect_processable_files
from app.domains.ingestion.models import (
    IngestionBatch,
    IngestionChunk,
    IngestionItem,
    IngestionItemStatus,
)
from app.domains.ingestion.pipeline import run_document_pipeline
from app.domains.ingestion.text import csv_rows_to_text, extract_csv_rows
from app.domains.telephony.schemas import CallInitiationRequest
from app.domains.campaigns.schemas import CallWebhookPayload
from app.domains.telephony.service import initiate_outbound_call

logger = logging.getLogger("arq.worker.document")

# Maximum simultaneous outbound calls per campaign
_MAX_CONCURRENT_CALLS: int = 3
# How long to defer a call task when all slots are full (seconds)
_CALL_RETRY_DEFER_SECS: float = 5.0
# Safety-net TTL for the active-calls Redis Set (seconds)
_ACTIVE_CALLS_TTL: int = 3600
# Candidates stuck in IN_PROGRESS for longer than this are re-queued by the cron
_ZOMBIE_THRESHOLD_HOURS: int = 2


# ===========================================================================
# HELPERS
# ===========================================================================

def _arq_pool_settings() -> RedisSettings:
    return RedisSettings.from_dsn(settings.REDIS_URL)


async def _get_arq_pool() -> ArqRedis:
    return await create_pool(_arq_pool_settings())


# ===========================================================================
# DISPATCHERS  (fan-out only  run in < 1 s, no processing)
# ===========================================================================

async def dispatch_ingestion_batch(
    ctx: dict[str, Any],
    *,
    batch_id: str,
    campaign_id: str,
    s3_prefix: str,
    source_type: str,
) -> dict[str, Any]:
    """Query pending IngestionItems, initialise Redis tracking Set, fan out atomic tasks."""
    redis = await get_redis_client()
    try:
        await update_batch_progress(redis, batch_id, status="PROCESSING")

        ingestion_batch_id = UUID(batch_id.removeprefix("batch_"))

        async with AsyncSessionLocal() as db:
            item_result = await db.execute(
                select(IngestionItem).where(
                    IngestionItem.batch_id == ingestion_batch_id,
                    IngestionItem.status == IngestionItemStatus.PENDING,
                )
            )
            items = item_result.scalars().all()
            batch = await db.get(IngestionBatch, ingestion_batch_id)
            if batch:
                batch.status = "PROCESSING"
            await db.commit()

        if not items:
            await fail_batch(redis, batch_id, "No pending ingestion items found for batch.")
            return {"batch_id": batch_id, "status": "FAILED", "reason": "no items"}

        item_ids = [str(item.id) for item in items]
        await update_batch_progress(redis, batch_id, total_candidates=len(items))
        await batch_add_pending(redis, batch_id, item_ids)

        pool = await _get_arq_pool()
        try:
            for item in items:
                await pool.enqueue_job(
                    "ingest_single_file",
                    batch_id=batch_id,
                    campaign_id=campaign_id,
                    ingestion_item_id=str(item.id),
                    s3_prefix=s3_prefix,
                    source_key=item.source_key,
                    display_name=item.display_name,
                )
        finally:
            await pool.aclose()

        logger.info(
            "dispatch_ingestion_batch %s: dispatched %d tasks (source_type=%s).",
            batch_id,
            len(items),
            source_type,
        )
        return {"batch_id": batch_id, "status": "DISPATCHED", "task_count": len(items)}

    except Exception as exc:
        logger.exception("dispatch_ingestion_batch %s: unhandled error", batch_id)
        await fail_batch(redis, batch_id, str(exc))
        raise
    finally:
        await redis.aclose()


async def dispatch_campaign_screening(
    ctx: dict[str, Any],
    *,
    batch_id: str,
    campaign_id: str,
    candidate_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Query eligible candidates, initialise Redis tracking Set, fan out atomic tasks."""
    redis = await get_redis_client()
    try:
        await update_batch_progress(redis, batch_id, status="PROCESSING")

        async with AsyncSessionLocal() as db:
            query = select(Candidate).where(Candidate.campaign_id == UUID(campaign_id))
            if candidate_ids:
                query = query.where(Candidate.id.in_([UUID(cid) for cid in candidate_ids]))
            result = await db.execute(query)
            candidates = result.scalars().all()

        if not candidates:
            await fail_batch(redis, batch_id, "No eligible candidates found.")
            return {"batch_id": batch_id, "status": "FAILED"}

        cids = [str(c.id) for c in candidates]
        await update_batch_progress(redis, batch_id, total_candidates=len(cids))
        await batch_add_pending(redis, batch_id, cids)

        pool = await _get_arq_pool()
        try:
            for cid in cids:
                await pool.enqueue_job(
                    "screen_single_candidate",
                    campaign_id=campaign_id,
                    candidate_id=cid,
                    batch_id=batch_id,
                )
        finally:
            await pool.aclose()

        logger.info("dispatch_campaign_screening %s: dispatched %d tasks.", batch_id, len(cids))
        return {"batch_id": batch_id, "status": "DISPATCHED", "task_count": len(cids)}

    except Exception as exc:
        logger.exception("dispatch_campaign_screening %s: unhandled error", batch_id)
        await fail_batch(redis, batch_id, str(exc))
        raise
    finally:
        await redis.aclose()


async def dispatch_campaign_calling(
    ctx: dict[str, Any],
    *,
    batch_id: str,
    campaign_id: str,
    candidate_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Query callable candidates, initialise Redis tracking Set, fan out atomic tasks."""
    redis = await get_redis_client()
    try:
        await update_batch_progress(redis, batch_id, status="PROCESSING")

        if candidate_ids == []:
            await complete_batch(redis, batch_id)
            return {"batch_id": batch_id, "status": "COMPLETED", "task_count": 0}

        async with AsyncSessionLocal() as db:
            query = select(Candidate).where(
                Candidate.campaign_id == UUID(campaign_id),
                Candidate.phone.isnot(None),
            )
            if candidate_ids:
                query = query.where(Candidate.id.in_([UUID(cid) for cid in candidate_ids]))
            result = await db.execute(query)
            candidates = result.scalars().all()

        if not candidates:
            await complete_batch(redis, batch_id)
            return {"batch_id": batch_id, "status": "COMPLETED", "task_count": 0}

        cids = [str(c.id) for c in candidates]
        await update_batch_progress(redis, batch_id, total_candidates=len(cids))
        await batch_add_pending(redis, batch_id, cids)

        pool = await _get_arq_pool()
        try:
            for cid in cids:
                await pool.enqueue_job(
                    "call_single_candidate",
                    campaign_id=campaign_id,
                    candidate_id=cid,
                    batch_id=batch_id,
                )
        finally:
            await pool.aclose()

        logger.info("dispatch_campaign_calling %s: dispatched %d tasks.", batch_id, len(cids))
        return {"batch_id": batch_id, "status": "DISPATCHED", "task_count": len(cids)}

    except Exception as exc:
        logger.exception("dispatch_campaign_calling %s: unhandled error", batch_id)
        await fail_batch(redis, batch_id, str(exc))
        raise
    finally:
        await redis.aclose()


# ===========================================================================
# ATOMIC WORKERS  (process exactly ONE item per invocation)
# ===========================================================================

async def ingest_single_file(
    ctx: dict[str, Any],
    *,
    batch_id: str,
    campaign_id: str,
    ingestion_item_id: str,
    s3_prefix: str,
    source_key: str,
    display_name: str,
) -> dict[str, Any]:
    """Download and pipeline exactly one document file.

    Removes the item from the Redis pending Set on definitive success or
    terminal failure.  Terminal failure is detected inline by comparing
    ``ctx['job_try']`` against ``settings.WORKER_MAX_TRIES``; when the last
    attempt is exhausted we mark the item FAILED and remove it from the Set so
    the batch can still reach completion rather than hanging forever.

    Supports PDF, DOCX, TXT, CSV, and ZIP files.  ZIPs are extracted in-place:
    each member file gets its own IngestionItem row and is fanned out as a
    separate ``ingest_single_file`` task, mirroring the multi-chunk CSV pattern.
    """
    redis = await get_redis_client()
    tmp_dir: Path | None = None

    try:
        tmp_dir = Path(tempfile.mkdtemp(prefix=f"ingest_{ingestion_item_id}_"))
        safe_display_name = Path(display_name.replace("\\", "/")).name
        if (
            not safe_display_name
            or safe_display_name in {".", ".."}
            or ":" in safe_display_name
        ):
            raise ValueError("Ingestion item has an invalid display name.")
        tmp_file = tmp_dir / safe_display_name

        await asyncio.to_thread(download_s3_object, source_key, tmp_file)

        async with AsyncSessionLocal() as db:
            item = await db.get(IngestionItem, UUID(ingestion_item_id))
            if item is None:
                raise RuntimeError(f"IngestionItem {ingestion_item_id} not found.")
            if item.status == IngestionItemStatus.COMPLETED:
                # Idempotent — already done (e.g. duplicate delivery)
                is_last = await batch_mark_done_with_progress(
                    redis,
                    batch_id,
                    ingestion_item_id,
                )
                if is_last:
                    await _finalise_ingestion_batch(redis, batch_id)
                return {"ingestion_item_id": ingestion_item_id, "status": "ALREADY_DONE"}
            item.status = IngestionItemStatus.PROCESSING
            item.attempt_count += 1
            item.current_stage = "llm_extraction"
            await db.commit()

        async with AsyncSessionLocal() as db:
            item = await db.get(IngestionItem, UUID(ingestion_item_id))
            if item is None:
                raise RuntimeError(f"IngestionItem {ingestion_item_id} vanished.")

            ext = tmp_file.suffix.lower()

            # ----------------------------------------------------------------
            # ZIP: extract members, create per-file IngestionItems, fan out
            # ----------------------------------------------------------------
            if ext == ".zip":
                extracted_files = await asyncio.to_thread(
                    safe_extract_zip, tmp_file, tmp_dir
                )

                if not extracted_files:
                    raise RuntimeError(
                        f"ZIP {display_name} contained no processable files after extraction."
                    )

                # Build IngestionItem rows for each extracted member file.
                # member_path stores the relative path inside the ZIP so the
                # unique constraint (batch_id, source_key, member_path) allows
                # multiple members from the same ZIP source_key.
                ingestion_batch_id = UUID(batch_id.removeprefix("batch_"))
                member_items: list[IngestionItem] = []
                for member_path in extracted_files:
                    # Relative path inside __expanded__ dir
                    rel = member_path.relative_to(tmp_dir / "__expanded__")
                    member_s3_key = f"{s3_prefix}/zip_members/{ingestion_item_id}/{rel.as_posix()}"

                    # Upload the extracted file to S3 so downstream workers can download it
                    file_bytes = member_path.read_bytes()
                    await asyncio.to_thread(
                        upload_bytes_to_s3,
                        member_s3_key,
                        file_bytes,
                    )

                    member_result = await db.execute(
                        select(IngestionItem).where(
                            IngestionItem.batch_id == ingestion_batch_id,
                            IngestionItem.source_key == member_s3_key,
                            IngestionItem.member_path == rel.as_posix(),
                        )
                    )
                    member_item = member_result.scalar_one_or_none()
                    if member_item is None:
                        member_item = IngestionItem(
                            batch_id=ingestion_batch_id,
                            source_key=member_s3_key,
                            display_name=member_path.name,
                            member_path=rel.as_posix(),
                        )
                        db.add(member_item)
                    elif member_item.status != IngestionItemStatus.COMPLETED:
                        member_item.status = IngestionItemStatus.PENDING
                        member_item.last_error = None
                    member_items.append(member_item)

                await db.flush()
                await db.commit()

                pending_member_items = [
                    member_item
                    for member_item in member_items
                    if member_item.status != IngestionItemStatus.COMPLETED
                ]

                # Register member task IDs into the Redis pending Set before
                # fanning out so cardinality is correct when first member completes
                await batch_add_pending(
                    redis,
                    batch_id,
                    [str(member_item.id) for member_item in pending_member_items],
                )
                # Replace the ZIP work unit with one unit for each extracted member.
                await adjust_batch_total_once(
                    redis,
                    batch_id,
                    f"zip:{ingestion_item_id}",
                    len(pending_member_items) - 1,
                )

                pool = await _get_arq_pool()
                try:
                    for member_item in pending_member_items:
                        await pool.enqueue_job(
                            "ingest_single_file",
                            batch_id=batch_id,
                            campaign_id=campaign_id,
                            ingestion_item_id=str(member_item.id),
                            s3_prefix=s3_prefix,
                            source_key=member_item.source_key,
                            display_name=member_item.display_name,
                        )
                finally:
                    await pool.aclose()

                item.status = IngestionItemStatus.COMPLETED
                item.current_stage = None
                item.last_error = None
                await db.commit()

                # ZIP parent is now a "processed" unit — use batch_mark_done_with_progress
                # so the processed counter stays consistent with the UI display.
                is_last = await batch_mark_done_with_progress(redis, batch_id, ingestion_item_id)
                if is_last:
                    await _finalise_ingestion_batch(redis, batch_id)

                logger.info(
                    "ingest_single_file: ZIP %s extracted %d member files, fanned out tasks.",
                    display_name,
                    len(member_items),
                )
                return {
                    "ingestion_item_id": ingestion_item_id,
                    "status": "FANNED_OUT",
                    "member_count": len(member_items),
                }

            # ----------------------------------------------------------------
            # CSV: all files use durable chunk jobs, including a one-chunk file.
            # ----------------------------------------------------------------
            elif ext == ".csv":
                csv_bytes = tmp_file.read_bytes()
                headers, rows = extract_csv_rows(csv_bytes)
                if not rows:
                    raise RuntimeError("CSV contains no data rows.")
                chunk_size = getattr(settings, "CSV_CHUNK_SIZE", 50)

                if rows:
                    chunk_result = await db.execute(
                        select(IngestionChunk).where(
                            IngestionChunk.ingestion_item_id == UUID(ingestion_item_id)
                        )
                    )
                    existing_chunks = sorted(
                        chunk_result.scalars().all(),
                        key=lambda chunk: chunk.chunk_index,
                    )
                    if not existing_chunks:
                        chunk_slices = [
                            (start, rows[start : start + chunk_size])
                            for start in range(0, len(rows), chunk_size)
                        ]
                        existing_chunks = [
                            IngestionChunk(
                                ingestion_item_id=UUID(ingestion_item_id),
                                chunk_index=index,
                                row_start=start,
                                row_count=len(chunk_rows),
                                status="PENDING",
                            )
                            for index, (start, chunk_rows) in enumerate(chunk_slices)
                        ]
                        db.add_all(existing_chunks)
                    else:
                        expected_start = 0
                        for index, chunk in enumerate(existing_chunks):
                            if (
                                chunk.chunk_index != index
                                or chunk.row_start != expected_start
                                or chunk.row_count <= 0
                            ):
                                raise RuntimeError(
                                    "Stored CSV chunk layout is invalid; refusing to "
                                    "reprocess with a different row partition."
                                )
                            expected_start += chunk.row_count
                        if expected_start != len(rows):
                            raise RuntimeError(
                                "CSV content changed after chunking; refusing to "
                                "reuse stale chunk results."
                            )
                    num_chunks = len(existing_chunks)
                    for chunk in existing_chunks:
                        if chunk.status == "FAILED":
                            chunk.status = "PENDING"
                            chunk.last_error = None
                    item.status = IngestionItemStatus.PROCESSING
                    await db.commit()
                    pending_chunks = [
                        chunk
                        for chunk in existing_chunks
                        if chunk.status != "COMPLETED"
                    ]
                    pending_chunk_slices = [
                        (
                            chunk,
                            chunk.row_start,
                            rows[chunk.row_start : chunk.row_start + chunk.row_count],
                        )
                        for chunk in pending_chunks
                    ]
                    chunk_task_ids = [
                        f"csv_chunk_{ingestion_item_id}_{chunk.chunk_index}"
                        for chunk in pending_chunks
                    ]

                    # Register all chunk tasks into Redis pending Set
                    await batch_add_pending(redis, batch_id, chunk_task_ids)
                    # Replace this CSV file work unit (counted as 1 by the dispatcher)
                    # with one unit per actual candidate row so the UI shows a meaningful
                    # total.  adjust_batch_total_once is idempotent — safe across retries.
                    await adjust_batch_total_once(
                        redis,
                        batch_id,
                        f"csv:{ingestion_item_id}",
                        len(rows) - 1,
                    )

                    pool = await _get_arq_pool()
                    try:
                        for chunk, chunk_start, chunk_rows in pending_chunk_slices:
                            chunk_text = csv_rows_to_text(headers, chunk_rows)
                            await pool.enqueue_job(
                                "process_csv_chunk",
                                batch_id=batch_id,
                                campaign_id=campaign_id,
                                ingestion_item_id=ingestion_item_id,
                                chunk_task_id=(
                                    f"csv_chunk_{ingestion_item_id}_{chunk.chunk_index}"
                                ),
                                source_key=source_key,
                                display_name=display_name,
                                chunk_text=chunk_text,
                                chunk_index=chunk.chunk_index,
                                row_start=chunk_start,
                                row_count=len(chunk_rows),
                            )
                    finally:
                        await pool.aclose()

                    # Mark the parent file IngestionItem done in the tracking Set
                    is_last = await batch_mark_done(redis, batch_id, ingestion_item_id)
                    if is_last:
                        await _finalise_ingestion_batch(redis, batch_id)

                    logger.info(
                        "ingest_single_file: fanned out %d chunk tasks for CSV %s (%d rows)",
                        num_chunks,
                        display_name,
                        len(rows),
                    )
                    return {
                        "ingestion_item_id": ingestion_item_id,
                        "status": "FANNED_OUT",
                        "chunk_count": num_chunks,
                        "total_rows": len(rows),
                    }

            # ----------------------------------------------------------------
            # PDF / DOCX / TXT: single document → single candidate
            # ----------------------------------------------------------------
            else:
                async def process_fields(
                    extracted_fields: dict[str, Any],
                    _db: AsyncSession = db,
                    _item: IngestionItem = item,
                ) -> None:
                    candidate, created = await persist_candidate_from_ingestion(
                        _db,
                        campaign_id=UUID(campaign_id),
                        item=_item,
                        source_url=f"s3://{settings.AWS_BUCKET_NAME}/{source_key}",
                        extracted_fields=extracted_fields,
                    )
                    if created:
                        await on_step_completed(
                            _db,
                            candidate.id,
                            "document_extraction",
                            payload={"file": display_name, "extracted_fields": extracted_fields},
                        )

                await run_document_pipeline(
                    tmp_file.read_bytes(),
                    ext,
                    extract_fields=extract_document_fields_llm,
                    process_fields=process_fields,
                )

            item.status = IngestionItemStatus.COMPLETED
            item.current_stage = None
            item.last_error = None
            await db.commit()

        is_last = await batch_mark_done_with_progress(
            redis,
            batch_id,
            ingestion_item_id,
        )
        if is_last:
            await _finalise_ingestion_batch(redis, batch_id)

        logger.info("ingest_single_file: item %s completed.", ingestion_item_id)
        return {"ingestion_item_id": ingestion_item_id, "status": "COMPLETED"}

    except Exception as exc:
        logger.exception("ingest_single_file: item %s failed.", ingestion_item_id)

        # Determine whether this is the terminal (last) attempt.
        # ctx['job_try'] is the 1-based attempt number provided by ARQ.
        job_try: int = ctx.get("job_try", 1)
        is_terminal = job_try >= settings.WORKER_MAX_TRIES

        already_completed = False
        try:
            async with AsyncSessionLocal() as db:
                item = await db.get(IngestionItem, UUID(ingestion_item_id))
                already_completed = bool(
                    item and item.status == IngestionItemStatus.COMPLETED
                )
                if item and not already_completed:
                    if is_terminal:
                        item.status = IngestionItemStatus.FAILED
                    else:
                        item.status = IngestionItemStatus.RETRYABLE
                    item.last_error = str(exc)[:1000]
                    await db.commit()
        except Exception:
            # DB unavailable during error handling — log and continue so the
            # Redis pending Set is still cleaned up on terminal failures.
            logger.exception(
                "ingest_single_file: failed to update DB status for item %s", ingestion_item_id
            )

        if already_completed:
            if Path(display_name.replace("\\", "/")).suffix.lower() == ".zip":
                is_last = await batch_mark_done(redis, batch_id, ingestion_item_id)
            else:
                is_last = await batch_mark_done_with_progress(
                    redis,
                    batch_id,
                    ingestion_item_id,
                )
            if is_last:
                await _finalise_ingestion_batch(redis, batch_id)
            return {
                "ingestion_item_id": ingestion_item_id,
                "status": "ALREADY_DONE",
            }

        if is_terminal:
            # Remove this item from the pending Set so the batch can still
            # complete rather than hanging forever waiting for a job that
            # will never succeed.
            logger.warning(
                "ingest_single_file: item %s reached terminal failure (try %d/%d), "
                "removing from pending Set.",
                ingestion_item_id,
                job_try,
                settings.WORKER_MAX_TRIES,
            )
            is_last = await batch_mark_done_with_progress(
                redis,
                batch_id,
                ingestion_item_id,
                failed=True,
            )
            if is_last:
                await _finalise_ingestion_batch(redis, batch_id)
        else:
            # Not terminal yet — let ARQ retry; do NOT remove from Set
            raise

    finally:
        if tmp_dir and tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)
        await redis.aclose()


async def process_csv_chunk(
    ctx: dict[str, Any],
    *,
    batch_id: str,
    campaign_id: str,
    ingestion_item_id: str,
    chunk_task_id: str,
    source_key: str,
    display_name: str,
    chunk_text: str,
    chunk_index: int,
    row_start: int,
    row_count: int,
) -> dict[str, Any]:
    """Atomic worker: extracts and persists candidates for one CSV chunk.

    Removes chunk_task_id from the Redis pending Set upon completion.
    If it is the last item in the Set, finalises the batch.
    """
    redis = await get_redis_client()
    try:
        async with AsyncSessionLocal() as db:
            chunk_result = await db.execute(
                select(IngestionChunk).where(
                    IngestionChunk.ingestion_item_id == UUID(ingestion_item_id),
                    IngestionChunk.chunk_index == chunk_index,
                )
            )
            chunk = chunk_result.scalar_one_or_none()
            if chunk is None:
                raise RuntimeError(
                    f"CSV chunk {chunk_index} for ingestion item "
                    f"{ingestion_item_id} was not registered."
                )
            if chunk.status == "COMPLETED":
                is_last = await batch_mark_done_with_progress(
                    redis,
                    batch_id,
                    chunk_task_id,
                )
                if is_last:
                    await _finalise_ingestion_batch(redis, batch_id)
                return {"chunk_task_id": chunk_task_id, "status": "ALREADY_DONE"}

        candidates_data = chunk.result
        if candidates_data is None:
            extracted = await extract_candidates_from_csv_llm(chunk_text)
            if len(extracted) > row_count:
                raise RuntimeError(
                    f"CSV chunk {chunk_index} returned more candidates than input rows."
                )
            async with AsyncSessionLocal() as db:
                chunk_result = await db.execute(
                    select(IngestionChunk)
                    .where(
                        IngestionChunk.ingestion_item_id == UUID(ingestion_item_id),
                        IngestionChunk.chunk_index == chunk_index,
                    )
                    .with_for_update()
                )
                chunk = chunk_result.scalar_one_or_none()
                if chunk is None:
                    raise RuntimeError(
                        f"CSV chunk {chunk_index} for ingestion item "
                        f"{ingestion_item_id} was not registered."
                    )
                if chunk.result is None:
                    chunk.result = extracted
                candidates_data = chunk.result
                await db.commit()

        async with AsyncSessionLocal() as db:
            chunk_result = await db.execute(
                select(IngestionChunk)
                .where(
                    IngestionChunk.ingestion_item_id == UUID(ingestion_item_id),
                    IngestionChunk.chunk_index == chunk_index,
                )
                .with_for_update()
            )
            chunk = chunk_result.scalar_one_or_none()
            if chunk is None:
                raise RuntimeError(
                    f"CSV chunk {chunk_index} for ingestion item "
                    f"{ingestion_item_id} was not registered."
                )
            if chunk.status != "COMPLETED":
                for candidate_index, cand_fields in enumerate(candidates_data):
                    candidate, created = await persist_csv_candidate(
                        db,
                        campaign_id=UUID(campaign_id),
                        ingestion_item_id=UUID(ingestion_item_id),
                        source_row_key=f"{ingestion_item_id}:{row_start + candidate_index}",
                        source_url=f"s3://{settings.AWS_BUCKET_NAME}/{source_key}",
                        extracted_fields=cand_fields,
                    )
                    if created:
                        await on_step_completed(
                            db,
                            candidate.id,
                            "document_extraction",
                            payload={"file": display_name, "extracted_fields": cand_fields},
                        )
                chunk.status = "COMPLETED"
                chunk.last_error = None
                await db.commit()

        is_last = await batch_mark_done_with_progress(
            redis,
            batch_id,
            chunk_task_id,
        )
        if is_last:
            await _finalise_ingestion_batch(redis, batch_id)

        logger.info(
            "process_csv_chunk: %s completed (%d candidates).",
            chunk_task_id,
            len(candidates_data),
        )
        return {
            "chunk_task_id": chunk_task_id,
            "status": "COMPLETED",
            "candidates_count": len(candidates_data),
        }

    except Exception as exc:
        logger.exception("process_csv_chunk: %s failed.", chunk_task_id)
        job_try: int = ctx.get("job_try", 1)
        is_terminal = job_try >= settings.WORKER_MAX_TRIES
        chunk_completed = False
        async with AsyncSessionLocal() as db:
            chunk_result = await db.execute(
                select(IngestionChunk).where(
                    IngestionChunk.ingestion_item_id == UUID(ingestion_item_id),
                    IngestionChunk.chunk_index == chunk_index,
                )
            )
            chunk = chunk_result.scalar_one_or_none()
            chunk_completed = bool(chunk and chunk.status == "COMPLETED")
            if chunk and not chunk_completed:
                chunk.status = "FAILED" if is_terminal else "RETRYABLE"
                chunk.last_error = str(exc)[:1000]
                await db.commit()

        if chunk_completed:
            is_last = await batch_mark_done_with_progress(
                redis,
                batch_id,
                chunk_task_id,
            )
            if is_last:
                await _finalise_ingestion_batch(redis, batch_id)
        elif is_terminal:
            is_last = await batch_mark_done_with_progress(
                redis,
                batch_id,
                chunk_task_id,
                failed=True,
            )
            if is_last:
                await _finalise_ingestion_batch(redis, batch_id)
        else:
            raise
    finally:
        await redis.aclose()


async def screen_single_candidate(
    ctx: dict[str, Any],
    *,
    campaign_id: str,
    candidate_id: str,
    batch_id: str,
) -> dict[str, Any]:
    """LLM-screen exactly one candidate.

    Checks Campaign existence before doing work.  Uses the Redis pending Set
    for batch-completion detection  atomically safe across many workers.
    """
    redis = await get_redis_client()
    try:
        async with AsyncSessionLocal() as db:
            campaign_result = await db.execute(
                select(Campaign).where(Campaign.id == UUID(campaign_id))
            )
            campaign = campaign_result.scalar_one_or_none()
            if not campaign:
                logger.error("screen_single_candidate: campaign %s not found.", campaign_id)
                await update_batch_progress(redis, batch_id, failed_incr=1)
                is_last = await batch_mark_done(redis, batch_id, candidate_id)
                if is_last:
                    await complete_batch(redis, batch_id, status="PARTIAL")
                return {"candidate_id": candidate_id, "status": "FAILED", "reason": "campaign not found"}

            candidate = await db.get(Candidate, UUID(candidate_id))
            if not candidate:
                logger.warning("screen_single_candidate: candidate %s not found.", candidate_id)
                await update_batch_progress(redis, batch_id, failed_incr=1)
                is_last = await batch_mark_done(redis, batch_id, candidate_id)
                if is_last:
                    await complete_batch(redis, batch_id, status="PARTIAL")
                return {"candidate_id": candidate_id, "status": "SKIPPED"}

            # Extraction completion must not prevent the candidate's first screening.
            if (
                candidate.step_status == WorkflowStepStatus.COMPLETED
                and candidate.workflow_step != "document_extraction"
            ):
                is_last = await batch_mark_done(redis, batch_id, candidate_id)
                if is_last:
                    await complete_batch(redis, batch_id)
                return {"candidate_id": candidate_id, "status": "ALREADY_DONE"}

            candidate.workflow_step = "document_screening"
            candidate.step_status = WorkflowStepStatus.IN_PROGRESS
            await db.commit()

            screening_payload = await screen_document_llm(
                candidate_fields=candidate.extracted_fields or {},
                campaign_fields=campaign.required_fields or {},
            )

            db.add(DocumentScreening(
                campaign_id=campaign.id,
                candidate_id=candidate.id,
                match_score=screening_payload.get("match_score"),
                matched_fields=screening_payload.get("matched_fields", {}),
                unmatched_fields=screening_payload.get("unmatched_fields", {}),
                summary=screening_payload.get("summary", ""),
            ))
            candidate.step_status = WorkflowStepStatus.COMPLETED
            await db.commit()

            await on_step_completed(db, candidate.id, "document_screening", payload=screening_payload)

        await update_batch_progress(redis, batch_id, processed_incr=1)

        is_last = await batch_mark_done(redis, batch_id, candidate_id)
        if is_last:
            await complete_batch(redis, batch_id)

        logger.info("screen_single_candidate: candidate %s completed.", candidate_id)
        return {"candidate_id": candidate_id, "status": "COMPLETED"}

    except Exception as exc:
        logger.exception("screen_single_candidate: candidate %s failed.", candidate_id)
        await update_batch_progress(redis, batch_id, failed_incr=1)
        is_last = await batch_mark_done(redis, batch_id, candidate_id)
        if is_last:
            await complete_batch(redis, batch_id, status="PARTIAL")
        raise

    finally:
        await redis.aclose()


async def call_single_candidate(
    ctx: dict[str, Any],
    *,
    campaign_id: str,
    candidate_id: str,
    batch_id: str,
) -> dict[str, Any]:
    """Initiate an outbound call for exactly one candidate.

    Rate-limiting via arq.Retry (non-blocking)  worker slot freed immediately
    when at capacity.  Pause state is read exclusively from Postgres.
    """
    redis = await get_redis_client()
    active_calls_key = f"campaign_active_calls:{campaign_id}"

    try:
        async with AsyncSessionLocal() as db:
            campaign_result = await db.execute(
                select(Campaign).where(Campaign.id == UUID(campaign_id))
            )
            campaign = campaign_result.scalar_one_or_none()
            if not campaign:
                logger.error("call_single_candidate: campaign %s not found.", campaign_id)
                await update_batch_progress(redis, batch_id, failed_incr=1)
                is_last = await batch_mark_done(redis, batch_id, candidate_id)
                if is_last:
                    await complete_batch(redis, batch_id, status="PARTIAL")
                return {"candidate_id": candidate_id, "status": "FAILED"}

            candidate = await db.get(Candidate, UUID(candidate_id))
            if not candidate or not candidate.phone:
                logger.warning(
                    "call_single_candidate: candidate %s missing or has no phone.", candidate_id
                )
                await update_batch_progress(redis, batch_id, failed_incr=1)
                is_last = await batch_mark_done(redis, batch_id, candidate_id)
                if is_last:
                    await complete_batch(redis, batch_id, status="PARTIAL")
                return {"candidate_id": candidate_id, "status": "SKIPPED"}

            #  Pause guard (pure DB state — no Redis flag)
            if candidate.step_status == WorkflowStepStatus.PAUSED:
                logger.info(
                    "call_single_candidate: candidate %s is PAUSED, exiting.", candidate_id
                )
                # Must still remove from pending Set so the batch can complete
                # when all other candidates finish or are also paused.
                is_last = await batch_mark_done(redis, batch_id, candidate_id)
                if is_last:
                    await complete_batch(redis, batch_id, status="PARTIAL")
                return {"candidate_id": candidate_id, "status": "PAUSED"}

            # Idempotency guard
            if candidate.step_status in (
                WorkflowStepStatus.COMPLETED,
                WorkflowStepStatus.IN_PROGRESS,
            ):
                is_last = await batch_mark_done(redis, batch_id, candidate_id)
                if is_last:
                    await complete_batch(redis, batch_id)
                return {"candidate_id": candidate_id, "status": "SKIPPED"}

            #  Concurrency rate-limiting via arq.Retry 
            active_calls: int = await redis.scard(active_calls_key)  # type: ignore
            if active_calls >= _MAX_CONCURRENT_CALLS:
                logger.debug(
                    "call_single_candidate: campaign %s at capacity (%d/%d), deferring.",
                    campaign_id, active_calls, _MAX_CONCURRENT_CALLS,
                )
                # Non-blocking: release this worker slot and retry after delay
                raise Retry(defer=_CALL_RETRY_DEFER_SECS)

            #  Reserve concurrency slot atomically 
            await redis.sadd(active_calls_key, candidate_id)  # type: ignore
            await redis.expire(active_calls_key, _ACTIVE_CALLS_TTL)  # type: ignore

            candidate.step_status = WorkflowStepStatus.IN_PROGRESS
            candidate.workflow_step = "outbound_call"
            await db.commit()

            required_fields = campaign.required_fields or {}
            raw_text = campaign.raw_text
            candidate_phone = candidate.phone

        #  Initiate call (DB session closed before network call) 
        request = CallInitiationRequest(
            candidate_id=UUID(candidate_id),
            campaign_id=UUID(campaign_id),
            candidate_phone=candidate_phone,
            required_fields=required_fields,
            raw_text=raw_text,
        )
        await initiate_outbound_call(request)

        await update_batch_progress(redis, batch_id, processed_incr=1)

        # Call completion (SREM from active_calls_key) happens in telephony webhook.
        is_last = await batch_mark_done(redis, batch_id, candidate_id)
        if is_last:
            await complete_batch(redis, batch_id)

        logger.info("call_single_candidate: candidate %s call initiated.", candidate_id)
        return {"candidate_id": candidate_id, "status": "INITIATED"}

    except Retry:
        raise  # Let ARQ handle the deferred retry transparently

    except Exception as exc:
        logger.exception("call_single_candidate: candidate %s failed.", candidate_id)
        await redis.srem(active_calls_key, candidate_id)  # type: ignore
        await update_batch_progress(redis, batch_id, failed_incr=1)

        # Reset candidate back to PENDING so ARQ retries can actually attempt
        # the call again (idempotency guard would skip IN_PROGRESS candidates).
        job_try: int = ctx.get("job_try", 1)
        is_terminal = job_try >= settings.WORKER_MAX_TRIES
        async with AsyncSessionLocal() as db:
            candidate = await db.get(Candidate, UUID(candidate_id))
            if candidate and candidate.step_status == WorkflowStepStatus.IN_PROGRESS:
                candidate.step_status = (
                    WorkflowStepStatus.FAILED if is_terminal else WorkflowStepStatus.PENDING
                )
                await db.commit()

        is_last = await batch_mark_done(redis, batch_id, candidate_id)
        if is_last:
            await complete_batch(redis, batch_id, status="PARTIAL")

        if not is_terminal:
            raise

    finally:
        await redis.aclose()


async def process_call_completion_job(
    ctx: dict[str, Any],
    *,
    payload: dict[str, Any],
) -> dict[str, str]:
    """Run post-call extraction and screening as a retryable ARQ job."""
    webhook = CallWebhookPayload.model_validate(payload)
    await process_call_webhook(webhook)
    return {"call_id": webhook.call_id, "status": "COMPLETED"}


# ===========================================================================
# MAINTENANCE CRON JOB  (Phase 5)
# ===========================================================================

async def reconcile_zombie_tasks(ctx: dict[str, Any]) -> dict[str, Any]:
    """Re-queue candidates stuck in IN_PROGRESS for longer than the zombie threshold.

    Zombies arise when a worker process crashes or is OOM-killed mid-task before
    it can update Postgres.  This cron resets such candidates to PENDING and fans
    out fresh atomic tasks.  Runs every 10 minutes  configured in WorkerSettings.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=_ZOMBIE_THRESHOLD_HOURS)
    requeued = 0

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Candidate).where(
                Candidate.step_status == WorkflowStepStatus.IN_PROGRESS,
                Candidate.updated_at < cutoff,
            )
        )
        zombies = result.scalars().all()

        if not zombies:
            logger.debug("reconcile_zombie_tasks: no zombies found.")
            return {"requeued": 0}

        pool = await _get_arq_pool()
        try:
            for candidate in zombies:
                logger.warning(
                    "reconcile_zombie_tasks: re-queuing zombie candidate %s "
                    "(step=%s, last_updated=%s)",
                    candidate.id,
                    candidate.workflow_step,
                    candidate.updated_at,
                )
                candidate.step_status = WorkflowStepStatus.PENDING

                task_name: str | None = None
                if candidate.workflow_step == "outbound_call":
                    task_name = "call_single_candidate"
                elif candidate.workflow_step == "document_screening":
                    task_name = "screen_single_candidate"

                if task_name:
                    zombie_batch_id = f"zombie_{uuid7()}"
                    await pool.enqueue_job(
                        task_name,
                        campaign_id=str(candidate.campaign_id),
                        candidate_id=str(candidate.id),
                        batch_id=zombie_batch_id,
                    )
                    requeued += 1

            await db.commit()
        finally:
            await pool.aclose()

    logger.info("reconcile_zombie_tasks: re-queued %d zombie candidates.", requeued)
    return {"requeued": requeued}


# ===========================================================================
# INTERNAL FINALISERS
# ===========================================================================

async def _finalise_ingestion_batch(redis: Any, batch_id: str) -> None:
    """Mark IngestionBatch COMPLETED/PARTIAL in Postgres and update the Redis tracker."""
    try:
        ingestion_batch_id = UUID(batch_id.removeprefix("batch_"))
    except ValueError:
        logger.warning("_finalise_ingestion_batch: cannot parse batch_id '%s'.", batch_id)
        await complete_batch(redis, batch_id)
        return

    async with AsyncSessionLocal() as db:
        batch = await db.get(IngestionBatch, ingestion_batch_id)
        if batch:
            item_result = await db.execute(
                select(IngestionItem).where(
                    IngestionItem.batch_id == ingestion_batch_id
                )
            )
            items = item_result.scalars().all()
            chunk_result = await db.execute(
                select(IngestionChunk).where(
                    IngestionChunk.ingestion_item_id.in_(
                        [item.id for item in items]
                    )
                )
            ) if items else None
            chunks_by_item: dict[UUID, list[IngestionChunk]] = {}
            if chunk_result is not None:
                for chunk in chunk_result.scalars().all():
                    chunks_by_item.setdefault(chunk.ingestion_item_id, []).append(chunk)

            for item in items:
                item_chunks = chunks_by_item.get(item.id, [])
                if not item_chunks:
                    continue
                if all(chunk.status == "COMPLETED" for chunk in item_chunks):
                    item.status = IngestionItemStatus.COMPLETED
                    item.current_stage = None
                    item.last_error = None
                elif any(chunk.status == "FAILED" for chunk in item_chunks):
                    item.status = IngestionItemStatus.FAILED
                    item.last_error = next(
                        (
                            chunk.last_error
                            for chunk in item_chunks
                            if chunk.status == "FAILED" and chunk.last_error
                        ),
                        "One or more CSV chunks failed.",
                    )

            result = await db.execute(
                select(IngestionItem.status).where(IngestionItem.batch_id == ingestion_batch_id)
            )
            statuses = result.scalars().all()
            batch_status = (
                "COMPLETED"
                if statuses and all(s == IngestionItemStatus.COMPLETED for s in statuses)
                else "PARTIAL"
            )
            batch.status = batch_status
            await db.commit()
        else:
            batch_status = "PARTIAL"

    await complete_batch(redis, batch_id, status=batch_status)
    logger.info("_finalise_ingestion_batch: batch %s  %s.", batch_id, batch_status)
