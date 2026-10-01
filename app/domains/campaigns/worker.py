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
from app.domains.campaigns.models import (
    Campaign,
    Candidate,
    DocumentScreening,
    WorkflowStepStatus,
)
from app.domains.campaigns.orchestrator import on_step_completed
from app.domains.campaigns.service import (
    batch_add_pending,
    batch_mark_done,
    complete_batch,
    extract_candidates_from_csv_llm,
    extract_document_fields_llm,
    fail_batch,
    persist_candidate_from_ingestion,
    persist_csv_candidate,
    screen_document_llm,
    update_batch_progress,
)
from app.domains.ingestion.files import collect_processable_files
from app.domains.ingestion.models import IngestionBatch, IngestionItem, IngestionItemStatus
from app.domains.ingestion.pipeline import run_csv_pipeline, run_document_pipeline
from app.domains.ingestion.text import csv_rows_to_text, extract_csv_rows
from app.domains.telephony.schemas import CallInitiationRequest
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

        logger.info("dispatch_ingestion_batch %s: dispatched %d tasks.", batch_id, len(items))
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

    Removes the item from the Redis pending Set on definitive success.
    On terminal failure (after max_tries) the Set entry is also removed so
    the batch can still reach completion.  A private on_job_end hook in
    worker_entry.py handles the terminal-failure removal.
    """
    redis = await get_redis_client()
    tmp_dir: Path | None = None

    try:
        tmp_dir = Path(tempfile.mkdtemp(prefix=f"ingest_{ingestion_item_id}_"))
        tmp_file = tmp_dir / display_name

        await asyncio.to_thread(download_s3_object, source_key, tmp_file)

        async with AsyncSessionLocal() as db:
            item = await db.get(IngestionItem, UUID(ingestion_item_id))
            if item is None:
                raise RuntimeError(f"IngestionItem {ingestion_item_id} not found.")
            if item.status == IngestionItemStatus.COMPLETED:
                # Idempotent  already done (e.g. duplicate delivery)
                is_last = await batch_mark_done(redis, batch_id, ingestion_item_id)
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
            if ext == ".csv":
                csv_bytes = tmp_file.read_bytes()
                headers, rows = extract_csv_rows(csv_bytes)
                chunk_size = getattr(settings, "CSV_CHUNK_SIZE", 50)

                if len(rows) > chunk_size:
                    # Multi-chunk CSV: fan out atomic tasks into ARQ
                    chunk_slices = [
                        rows[i : i + chunk_size]
                        for i in range(0, len(rows), chunk_size)
                    ]
                    num_chunks = len(chunk_slices)
                    chunk_task_ids = [
                        f"csv_chunk_{ingestion_item_id}_{idx}"
                        for idx in range(num_chunks)
                    ]

                    # Register all chunk tasks into Redis pending Set
                    await batch_add_pending(redis, batch_id, chunk_task_ids)
                    # Expand the total candidates count by (num_chunks - 1)
                    await update_batch_progress(redis, batch_id, total_candidates=None)

                    pool = await _get_arq_pool()
                    try:
                        for idx, chunk_rows in enumerate(chunk_slices):
                            chunk_text = csv_rows_to_text(headers, chunk_rows)
                            await pool.enqueue_job(
                                "process_csv_chunk",
                                batch_id=batch_id,
                                campaign_id=campaign_id,
                                ingestion_item_id=ingestion_item_id,
                                chunk_task_id=chunk_task_ids[idx],
                                source_key=source_key,
                                display_name=display_name,
                                chunk_text=chunk_text,
                                is_last_chunk=(idx == num_chunks - 1),
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

                # Single chunk (<= chunk_size): process inline
                async def process_csv_candidate_cb(
                    cand_fields: dict[str, Any],
                    _db: AsyncSession = db,
                ) -> None:
                    candidate = await persist_csv_candidate(
                        _db,
                        campaign_id=UUID(campaign_id),
                        ingestion_item_id=UUID(ingestion_item_id),
                        source_url=f"s3://{settings.AWS_BUCKET_NAME}/{source_key}",
                        extracted_fields=cand_fields,
                    )
                    await on_step_completed(
                        _db,
                        candidate.id,
                        "document_extraction",
                        payload={"file": display_name, "extracted_fields": cand_fields},
                    )

                count = await run_csv_pipeline(
                    csv_bytes,
                    extract_candidates=extract_candidates_from_csv_llm,
                    process_candidate=process_csv_candidate_cb,
                    chunk_size=chunk_size,
                )
                logger.info("ingest_single_file: processed %d candidates from CSV %s inline", count, display_name)
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

        await update_batch_progress(redis, batch_id, processed_incr=1)

        is_last = await batch_mark_done(redis, batch_id, ingestion_item_id)
        if is_last:
            await _finalise_ingestion_batch(redis, batch_id)

        logger.info("ingest_single_file: item %s completed.", ingestion_item_id)
        return {"ingestion_item_id": ingestion_item_id, "status": "COMPLETED"}

    except Exception as exc:
        logger.exception("ingest_single_file: item %s failed.", ingestion_item_id)
        async with AsyncSessionLocal() as db:
            item = await db.get(IngestionItem, UUID(ingestion_item_id))
            if item:
                item.status = IngestionItemStatus.RETRYABLE
                item.last_error = str(exc)[:1000]
                await db.commit()
        await update_batch_progress(redis, batch_id, failed_incr=1)
        raise  # ARQ will retry up to max_tries; worker_entry on_job_end handles terminal removal

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
    is_last_chunk: bool = False,
) -> dict[str, Any]:
    """Atomic worker: extracts and persists candidates for one CSV chunk.

    Removes chunk_task_id from the Redis pending Set upon completion.
    If it is the last item in the Set, finalises the batch.
    """
    redis = await get_redis_client()
    try:
        # Extract candidates from this chunk using LLM
        candidates_data = await extract_candidates_from_csv_llm(chunk_text)

        async with AsyncSessionLocal() as db:
            for cand_fields in candidates_data:
                candidate = await persist_csv_candidate(
                    db,
                    campaign_id=UUID(campaign_id),
                    ingestion_item_id=UUID(ingestion_item_id),
                    source_url=f"s3://{settings.AWS_BUCKET_NAME}/{source_key}",
                    extracted_fields=cand_fields,
                )
                await on_step_completed(
                    db,
                    candidate.id,
                    "document_extraction",
                    payload={"file": display_name, "extracted_fields": cand_fields},
                )
            await db.commit()

        # If this is the last chunk, mark the parent IngestionItem as completed
        if is_last_chunk:
            async with AsyncSessionLocal() as db:
                item = await db.get(IngestionItem, UUID(ingestion_item_id))
                if item:
                    item.status = IngestionItemStatus.COMPLETED
                    item.current_stage = None
                    item.last_error = None
                    await db.commit()

        await update_batch_progress(redis, batch_id, processed_incr=1)

        is_last = await batch_mark_done(redis, batch_id, chunk_task_id)
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
        await update_batch_progress(redis, batch_id, failed_incr=1)
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

            # Idempotency guard
            if candidate.step_status == WorkflowStepStatus.COMPLETED:
                is_last = await batch_mark_done(redis, batch_id, candidate_id)
                if is_last:
                    await complete_batch(redis, batch_id)
                return {"candidate_id": candidate_id, "status": "ALREADY_DONE"}

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

            #  Pause guard (pure DB state  no Redis flag) 
            if candidate.step_status == WorkflowStepStatus.PAUSED:
                logger.info(
                    "call_single_candidate: candidate %s is PAUSED, exiting.", candidate_id
                )
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
        is_last = await batch_mark_done(redis, batch_id, candidate_id)
        if is_last:
            await complete_batch(redis, batch_id, status="PARTIAL")
        raise

    finally:
        await redis.aclose()


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
