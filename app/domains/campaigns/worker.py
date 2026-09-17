"""
ARQ background worker for document batch processing.
"""
from __future__ import annotations

import asyncio
import io
import logging
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.core.redis import get_redis_pool
from app.core.s3 import download_s3_prefix
from app.domains.campaigns.models import (
    Candidate,
    WorkflowStepStatus,
    Campaign,
    DocumentScreening,
)
from app.domains.campaigns.service import (
    complete_batch,
    fail_batch,
    update_batch_progress,
    validate_magic_bytes,
    extract_document_text,
    extract_document_fields_llm,
    screen_document_llm,
)
from app.domains.campaigns.orchestrator import on_step_completed

logger = logging.getLogger("arq.worker.document")

_PROCESSABLE_EXTS = {".pdf", ".docx", ".txt"}
_JUNK_NAMES = {"__MACOSX", ".DS_Store", "Thumbs.db", "desktop.ini"}

def _safe_extract_zip(zip_bytes: bytes, dest: Path) -> list[Path]:
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    extracted: list[Path] = []
    for info in zf.infolist():
        if info.is_dir(): continue
        name = info.filename
        if ".." in name or name.startswith("/") or name.startswith("\\"): continue
        if (info.external_attr >> 16) & 0o120000 == 0o120000: continue
        basename = Path(name).name
        if basename.startswith(".") or basename in _JUNK_NAMES or "__MACOSX" in name: continue
        ext = Path(name).suffix.lower()
        if ext not in _PROCESSABLE_EXTS: continue
        if ext == ".zip": continue
        
        target = dest / basename
        counter = 1
        while target.exists():
            stem = Path(basename).stem
            target = dest / f"{stem}_{counter}{ext}"
            counter += 1
            
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(zf.read(name))
        extracted.append(target)
    return extracted

def _collect_files(directory: Path) -> list[Path]:
    all_files: list[Path] = []
    zip_files: list[Path] = []
    for fp in directory.iterdir():
        if not fp.is_file(): continue
        if fp.name in _JUNK_NAMES or fp.name.startswith("."): continue
        ext = fp.suffix.lower()
        if ext == ".zip":
            zip_files.append(fp)
        elif ext in _PROCESSABLE_EXTS:
            all_files.append(fp)
            
    for zp in zip_files:
        try:
            extracted = _safe_extract_zip(zp.read_bytes(), directory)
            all_files.extend(extracted)
        except Exception:
            logger.exception("Failed to unpack zip: %s", zp.name)
            
    return all_files

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
        
        downloaded = await asyncio.to_thread(download_s3_prefix, s3_prefix, tmp_dir)
        files = _collect_files(tmp_dir)
        if not files:
            await fail_batch(redis, batch_id, "No processable files found after download.")
            return {"batch_id": batch_id, "status": "FAILED", "reason": "no files"}
            
        await update_batch_progress(redis, batch_id, total_files=len(files))
        
        for fp in files:
            ext = fp.suffix.lower()
            raw = fp.read_bytes()
            if not validate_magic_bytes(raw, ext):
                await update_batch_progress(redis, batch_id, failed_incr=1)
                continue
                
            text = extract_document_text(raw, ext)
            if not text or not text.strip():
                await update_batch_progress(redis, batch_id, failed_incr=1)
                continue
                
            extracted_fields = await extract_document_fields_llm(text)
            
            async with AsyncSessionLocal() as db:
                candidate = Candidate(
                    campaign_id=UUID(campaign_id),
                    name=extracted_fields.get("name", "Unknown"),
                    email=extracted_fields.get("email"),
                    phone=extracted_fields.get("phone"),
                    extracted_fields=extracted_fields,
                    workflow_step="document_extraction",
                    step_status=WorkflowStepStatus.COMPLETED,
                )
                db.add(candidate)
                await db.commit()
                await db.refresh(candidate)

                await on_step_completed(db, candidate.id, "document_extraction", payload={"file": fp.name, "extracted_fields": extracted_fields})
                
            await update_batch_progress(redis, batch_id, processed_incr=1)
            
        await complete_batch(redis, batch_id)
        return {"batch_id": batch_id, "status": "COMPLETED", "total_files": len(files)}
        
    except Exception as exc:
        logger.exception("Batch %s: unhandled error", batch_id)
        await fail_batch(redis, batch_id, str(exc))
        return {"batch_id": batch_id, "status": "FAILED", "reason": str(exc)}
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
