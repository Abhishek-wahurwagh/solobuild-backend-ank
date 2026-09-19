"""
ARQ background worker for document batch processing.
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.core.redis import get_redis_pool
from app.core.s3 import download_s3_prefix, upload_bytes_to_s3
from app.domains.campaigns.models import (
    Candidate,
    WorkflowStepStatus,
    Campaign,
    DocumentScreening,
)
from app.domains.users.models import User
from app.domains.campaigns.service import (
    complete_batch,
    fail_batch,
    update_batch_progress,
    extract_document_fields_llm,
    persist_candidate_from_ingestion,
    screen_document_llm,
)
from app.domains.campaigns.orchestrator import on_step_completed
from app.domains.ingestion.files import collect_processable_files
from app.domains.ingestion.models import IngestionBatch, IngestionItem, IngestionItemStatus
from app.domains.ingestion.pipeline import run_document_pipeline

logger = logging.getLogger("arq.worker.document")


async def process_document_upload_batch(
    ctx: dict[str, Any],
    *,
    batch_id: str,
    campaign_id: str,
    s3_prefix: str,
    source_type: str,
) -> dict[str, Any]:
    redis = await get_redis_pool()
    tmp_dir: Path | None = None

    try:
        await update_batch_progress(redis, batch_id, status="PROCESSING")
        tmp_dir = Path(tempfile.mkdtemp(prefix=f"document_batch_{batch_id}_"))
        logger.info("Batch %s: downloading from s3://%s/%s", batch_id, settings.AWS_BUCKET_NAME, s3_prefix)

        await asyncio.to_thread(download_s3_prefix, s3_prefix, tmp_dir)
        files = collect_processable_files(tmp_dir)
        if not files:
            await fail_batch(redis, batch_id, "No processable files found after download.")
            return {"batch_id": batch_id, "status": "FAILED", "reason": "no files"}

        await update_batch_progress(redis, batch_id, total_files=len(files))

        ingestion_batch_id = UUID(batch_id.removeprefix("batch_"))

        # Build lookup of known ingestion items and mark batch as processing
        async with AsyncSessionLocal() as db:
            item_result = await db.execute(
                select(IngestionItem).where(IngestionItem.batch_id == ingestion_batch_id)
            )
            ingestion_items = {item.source_key: item for item in item_result.scalars().all()}
            batch = await db.get(IngestionBatch, ingestion_batch_id)
            if batch:
                batch.status = "PROCESSING"
            await db.commit()

        # Register expanded zip members as ingestion items
        for fp in files:
            relative_name = fp.relative_to(tmp_dir).as_posix()
            if relative_name.startswith("__expanded__/"):
                member_path = relative_name.removeprefix("__expanded__/")
                expanded_key = f"{s3_prefix}/expanded/{member_path}"
                await asyncio.to_thread(upload_bytes_to_s3, expanded_key, fp.read_bytes())
                async with AsyncSessionLocal() as db:
                    existing = await db.execute(
                        select(IngestionItem).where(
                            IngestionItem.batch_id == ingestion_batch_id,
                            IngestionItem.source_key == expanded_key,
                        )
                    )
                    item = existing.scalar_one_or_none()
                    if item is None:
                        item = IngestionItem(
                            batch_id=ingestion_batch_id,
                            source_key=expanded_key,
                            display_name=member_path,
                            member_path=member_path,
                        )
                        db.add(item)
                        await db.commit()
                    ingestion_items[expanded_key] = item

        # Mark original zip items as completed (they were just containers)
        async with AsyncSessionLocal() as db:
            zip_items = await db.execute(
                select(IngestionItem).where(
                    IngestionItem.batch_id == ingestion_batch_id,
                    IngestionItem.source_key.endswith(".zip"),
                )
            )
            for item in zip_items.scalars().all():
                item.status = IngestionItemStatus.COMPLETED
                item.current_stage = None
            await db.commit()

        # Process each file — one session per file to avoid long-held transactions
        for fp in files:
            relative_name = fp.relative_to(tmp_dir).as_posix()
            if relative_name.startswith("__expanded__/"):
                display_name = relative_name.removeprefix("__expanded__/")
                source_key = f"{s3_prefix}/expanded/{display_name}"
            else:
                display_name = relative_name
                source_key = f"{s3_prefix}/{relative_name}"

            try:
                ingestion_item_ref = ingestion_items.get(source_key)
                if ingestion_item_ref is None:
                    raise RuntimeError(f"No ingestion item registered for source {relative_name}")

                async with AsyncSessionLocal() as db:
                    item = await db.get(IngestionItem, ingestion_item_ref.id)
                    if item is None:
                        raise RuntimeError(f"Ingestion item disappeared for source {relative_name}")
                    if item.status == IngestionItemStatus.COMPLETED:
                        continue

                    item.status = IngestionItemStatus.PROCESSING
                    item.attempt_count += 1
                    item.current_stage = "validation"
                    await db.commit()

                async with AsyncSessionLocal() as db:
                    item = await db.get(IngestionItem, ingestion_item_ref.id)
                    if item is None:
                        raise RuntimeError(f"Ingestion item disappeared for source {relative_name}")
                    item.current_stage = "llm_extraction"
                    await db.commit()

                async with AsyncSessionLocal() as db:
                    item = await db.get(IngestionItem, ingestion_item_ref.id)
                    if item is None:
                        raise RuntimeError(f"Ingestion item disappeared for source {relative_name}")

                    # Use a closure to capture the current db session for the callback
                    async def process_fields(extracted_fields: dict[str, Any], _db: AsyncSession = db, _item: IngestionItem = item) -> None:
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
                                payload={"file": fp.name, "extracted_fields": extracted_fields},
                            )

                    # run_document_pipeline handles its own retry logic; no wrapper needed
                    await run_document_pipeline(
                        fp.read_bytes(),
                        fp.suffix.lower(),
                        extract_fields=extract_document_fields_llm,
                        process_fields=process_fields,
                    )
                    item.status = IngestionItemStatus.COMPLETED
                    item.current_stage = None
                    item.last_error = None
                    await db.commit()

                await update_batch_progress(redis, batch_id, processed_incr=1)

            except Exception as exc:
                logger.exception("Batch %s: failed to process file %s", batch_id, fp.name)
                ingestion_item_ref = ingestion_items.get(source_key)
                if ingestion_item_ref:
                    async with AsyncSessionLocal() as db:
                        item = await db.get(IngestionItem, ingestion_item_ref.id)
                        if item:
                            item.status = IngestionItemStatus.RETRYABLE
                            item.last_error = str(exc)[:1000]
                            await db.commit()
                await update_batch_progress(redis, batch_id, failed_incr=1)

        # Determine final batch status
        async with AsyncSessionLocal() as db:
            batch = await db.get(IngestionBatch, ingestion_batch_id)
            if batch:
                result = await db.execute(
                    select(IngestionItem.status).where(IngestionItem.batch_id == ingestion_batch_id)
                )
                statuses = result.scalars().all()
                batch_status = "COMPLETED" if statuses and all(
                    s == IngestionItemStatus.COMPLETED for s in statuses
                ) else "PARTIAL"
                batch.status = batch_status
                await db.commit()
            else:
                batch_status = "PARTIAL"

        await complete_batch(redis, batch_id, status=batch_status)
        return {"batch_id": batch_id, "status": "COMPLETED", "total_files": len(files)}

    except Exception as exc:
        logger.exception("Batch %s: unhandled error", batch_id)
        await fail_batch(redis, batch_id, str(exc))
        raise
    finally:
        if tmp_dir and tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)
        await redis.aclose()


async def screen_campaign_candidates(
    ctx: dict[str, Any],
    *,
    batch_id: str,
    campaign_id: str,
    candidate_ids: list[str] | None = None,
) -> dict[str, Any]:
    redis = await get_redis_pool()

    try:
        await update_batch_progress(redis, batch_id, status="PROCESSING")

        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Campaign).where(Campaign.id == UUID(campaign_id)))
            campaign = result.scalar_one_or_none()

            if not campaign:
                await fail_batch(redis, batch_id, "Campaign not found.")
                return {"batch_id": batch_id, "status": "FAILED"}

            campaign_fields = campaign.required_fields or {}

            query = select(Candidate).where(Candidate.campaign_id == UUID(campaign_id))
            if candidate_ids:
                query = query.where(Candidate.id.in_([UUID(cid) for cid in candidate_ids]))

            candidates_result = await db.execute(query)
            candidates = candidates_result.scalars().all()

            if not candidates:
                await fail_batch(redis, batch_id, "No eligible candidates found.")
                return {"batch_id": batch_id, "status": "FAILED"}

            await update_batch_progress(redis, batch_id, total_files=len(candidates))

            screened_count = 0
            for candidate in candidates:
                try:
                    candidate_fields = candidate.extracted_fields or {}

                    screening_payload = await screen_document_llm(
                        candidate_fields=candidate_fields,
                        campaign_fields=campaign_fields,
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

                    screened_count += 1
                    await update_batch_progress(redis, batch_id, processed_incr=1)
                except Exception:
                    logger.exception("Batch %s: screening error for candidate %s", batch_id, candidate.id)
                    await update_batch_progress(redis, batch_id, failed_incr=1)

        await complete_batch(redis, batch_id)
        return {"batch_id": batch_id, "status": "COMPLETED", "screened_count": screened_count}

    except Exception as exc:
        logger.exception("Batch %s: screening unhandled error", batch_id)
        await fail_batch(redis, batch_id, str(exc))
        return {"batch_id": batch_id, "status": "FAILED", "reason": str(exc)}
    finally:
        await redis.aclose()


async def task_initiate_outbound_call(
    ctx: dict[str, Any],
    candidate_id: UUID,
) -> dict[str, Any]:
    from app.integrations.voice.factory import VoiceFactory

    redis = await get_redis_pool()
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Candidate).where(Candidate.id == candidate_id))
            candidate = result.scalar_one_or_none()
            if not candidate:
                logger.error(f"Candidate {candidate_id} not found.")
                return {"status": "FAILED", "reason": "Candidate not found"}

            result = await db.execute(select(Campaign).where(Campaign.id == candidate.campaign_id))
            campaign = result.scalar_one_or_none()
            if not campaign:
                return {"status": "FAILED", "reason": "Campaign not found"}

            # Concurrency check: limit simultaneous outbound calls per campaign
            from sqlalchemy import func
            count_result = await db.execute(
                select(func.count(Candidate.id)).where(
                    Candidate.campaign_id == campaign.id,
                    Candidate.step_status == WorkflowStepStatus.IN_PROGRESS,
                    Candidate.workflow_step == "outbound_call"
                )
            )
            active_calls = count_result.scalar_one_or_none() or 0

            max_concurrent = 3
            if active_calls >= max_concurrent:
                logger.info(
                    "Campaign %s reached max concurrent calls (%s/%s). Delaying candidate %s.",
                    campaign.id, active_calls, max_concurrent, candidate_id,
                )
                from arq import Retry
                raise Retry(defer=60)

            candidate.step_status = WorkflowStepStatus.IN_PROGRESS
            candidate.workflow_step = "outbound_call"
            await db.commit()

            provider = VoiceFactory.get_provider()

            system_prompt = f"Objective: Conduct an interview.\nRequirements: {campaign.required_fields}\nContext: {campaign.raw_text}"
            phone = candidate.phone or ""

            if not phone:
                candidate.step_status = WorkflowStepStatus.FAILED
                await db.commit()
                return {"status": "FAILED", "reason": "Candidate has no phone number"}

            call_id = await provider.initiate_call(
                candidate_phone=phone,
                candidate_id=candidate.id,
                campaign_id=campaign.id,
                system_prompt=system_prompt,
            )

            return {"status": "SUCCESS", "call_id": call_id}

    except Exception as exc:
        # Let ARQ's Retry exception propagate unchanged
        if type(exc).__name__ == "Retry":
            raise

        logger.exception("Error initiating outbound call for candidate %s", candidate_id)
        return {"status": "FAILED", "reason": str(exc)}
    finally:
        await redis.aclose()
