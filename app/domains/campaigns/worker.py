"""
ARQ background worker for resume batch processing.

Flow:
  1.  Download files from S3 into a temp directory
  2.  Unpack zip archives safely
  3.  Validate magic bytes
  4.  Extract text → deterministic profile parse
  5.  Create Candidate rows
  6.  Run deterministic pre-screen gate
  7.  Compute match scores → threshold-based LLM screening
  8.  Create ResumeScreening rows for eligible candidates
  9.  Update Redis batch tracker throughout
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
    CandidateStatus,
    HiringCampaign,
    ResumeScreening,
)
from app.domains.campaigns.service import (
    build_resume_screening_result,
    complete_batch,
    compute_match_score,
    extract_resume_text,
    fail_batch,
    filter_resume_against_jd,
    normalize_jd_text,
    parse_resume_profile,
    update_batch_progress,
    validate_magic_bytes,
)
from app.integrations.ai.factory import StructuredExtractionProviderFactory

logger = logging.getLogger("arq.worker.resume")

# File extensions we process inside zip/download
_PROCESSABLE_EXTS = {".pdf", ".docx", ".txt"}

# OS junk to skip
_JUNK_NAMES = {"__MACOSX", ".DS_Store", "Thumbs.db", "desktop.ini"}


# -----------------------------------------------------------------------
# Helper: safe zip extraction
# -----------------------------------------------------------------------

def _safe_extract_zip(zip_bytes: bytes, dest: Path) -> list[Path]:
    """Extract a zip archive with full safety checks.

    Returns a list of extracted file paths.
    """
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    extracted: list[Path] = []

    for info in zf.infolist():
        if info.is_dir():
            continue

        name = info.filename

        # Reject dangerous entries
        if ".." in name or name.startswith("/") or name.startswith("\\"):
            logger.warning("Skipping path-traversal entry: %s", name)
            continue
        if (info.external_attr >> 16) & 0o120000 == 0o120000:
            logger.warning("Skipping symlink entry: %s", name)
            continue

        basename = Path(name).name
        if basename.startswith(".") or basename in _JUNK_NAMES or "__MACOSX" in name:
            continue

        ext = Path(name).suffix.lower()
        if ext not in _PROCESSABLE_EXTS:
            continue

        # Nested zip check
        if ext == ".zip":
            logger.warning("Skipping nested zip: %s", name)
            continue

        # Write the file
        target = dest / basename
        # Avoid overwrites by appending a counter
        counter = 1
        while target.exists():
            stem = Path(basename).stem
            target = dest / f"{stem}_{counter}{ext}"
            counter += 1

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(zf.read(name))
        extracted.append(target)

    return extracted


# -----------------------------------------------------------------------
# Helper: collect processable files from a directory
# -----------------------------------------------------------------------

def _collect_files(directory: Path) -> list[Path]:
    """Return all resume files from *directory*, unpacking any zips."""
    all_files: list[Path] = []
    zip_files: list[Path] = []

    for fp in directory.iterdir():
        if not fp.is_file():
            continue
        if fp.name in _JUNK_NAMES or fp.name.startswith("."):
            continue
        ext = fp.suffix.lower()
        if ext == ".zip":
            zip_files.append(fp)
        elif ext in _PROCESSABLE_EXTS:
            all_files.append(fp)

    # Unpack zips into the same directory
    for zp in zip_files:
        try:
            extracted = _safe_extract_zip(zp.read_bytes(), directory)
            all_files.extend(extracted)
        except Exception:
            logger.exception("Failed to unpack zip: %s", zp.name)

    return all_files


# -----------------------------------------------------------------------
# Helper: LLM screening for a single candidate
# -----------------------------------------------------------------------

async def _llm_screen_candidate(
    candidate: Candidate,
    jd_text: str,
    jd_profile: dict[str, Any],
    match_score: float,
) -> ResumeScreening:
    """Run the LLM screening pass on one candidate.

    This is the ONLY place in the pipeline where an LLM is called per resume.
    """
    provider = StructuredExtractionProviderFactory.build()

    prompt = f"""You are a hiring screening assistant.

Job Description:
{jd_text}

JD Requirements:
- Experience required: {jd_profile.get('experience_required', 'not specified')}
- Skills required: {', '.join(jd_profile.get('skills_required', []))}

Candidate Resume Text:
{(candidate.resume_text or '')[:4000]}

Candidate Parsed Profile:
- Experience years: {candidate.experience_years}
- Skills: {', '.join(candidate.skills or [])}

Respond with a JSON object:
{{
  "match_score": <float 0-100>,
  "one_line_summary": "<one line summary of candidate fit>",
  "matched_skills": [<skills the candidate has that match the JD>],
  "missing_skills": [<required skills the candidate is missing>]
}}
Output valid JSON only."""

    try:
        result = await provider.screen_candidate(prompt)
    except Exception:
        logger.exception("LLM screening failed for candidate %s", candidate.id)
        result = {}

    return ResumeScreening(
        candidate_id=candidate.id,
        match_score=result.get("match_score", match_score * 100),
        one_line_summary=result.get("one_line_summary", ""),
        matched_skills=result.get("matched_skills", []),
        missing_skills=result.get("missing_skills", []),
    )


# -----------------------------------------------------------------------
# Main worker task
# -----------------------------------------------------------------------

async def process_resume_upload_batch(
    ctx: dict[str, Any],
    *,
    batch_id: str,
    campaign_id: str,
    s3_prefix: str,
    source_type: str,
) -> dict[str, Any]:
    """ARQ task: process a batch of uploaded resumes to extract and parse profiles."""
    redis = await get_redis_pool()
    tmp_dir: Path | None = None

    try:
        # -- Mark as PROCESSING --
        await update_batch_progress(redis, batch_id, status="PROCESSING")

        # ===== 1. Download from S3 =====
        tmp_dir = Path(tempfile.mkdtemp(prefix=f"resume_batch_{batch_id}_"))
        logger.info("Batch %s: downloading from s3://%s/%s", batch_id, settings.AWS_BUCKET_NAME, s3_prefix)

        downloaded = await asyncio.to_thread(
            download_s3_prefix, s3_prefix, tmp_dir
        )
        logger.info("Batch %s: downloaded %d files", batch_id, len(downloaded))

        # ===== 2. Collect & unpack =====
        files = _collect_files(tmp_dir)
        if not files:
            await fail_batch(redis, batch_id, "No processable files found after download.")
            return {"batch_id": batch_id, "status": "FAILED", "reason": "no files"}

        await update_batch_progress(redis, batch_id, total_files=len(files))

        # ===== 3. Parse each file → Candidate rows =====
        for fp in files:
            ext = fp.suffix.lower()

            # 3a. Magic byte validation
            raw = fp.read_bytes()
            if not validate_magic_bytes(raw, ext):
                logger.warning("Batch %s: magic-byte mismatch for %s", batch_id, fp.name)
                await update_batch_progress(redis, batch_id, failed_incr=1)
                continue

            # 3b. Extract text
            text = extract_resume_text(raw, ext)
            if not text or not text.strip():
                logger.warning("Batch %s: empty text for %s", batch_id, fp.name)
                await update_batch_progress(redis, batch_id, failed_incr=1)
                continue

            # 3c. Parse profile
            profile = parse_resume_profile(text)

            # 3d. Create Candidate row
            s3_key = f"{s3_prefix}/{fp.name}"
            async with AsyncSessionLocal() as db:
                candidate = Candidate(
                    campaign_id=UUID(campaign_id),
                    name=profile.get("name"),
                    email=profile.get("email"),
                    phone=profile.get("phone"),
                    location=profile.get("location"),
                    experience_years=profile.get("experience_years"),
                    skills=profile.get("skills", []),
                    raw_resume_url=s3_key,
                    resume_text=text[:50_000],  # cap storage
                    status=CandidateStatus.RESUME_PARSED,
                )
                db.add(candidate)
                await db.commit()
                await db.refresh(candidate)

            await update_batch_progress(redis, batch_id, processed_incr=1)

        # ===== 4. Complete =====
        await complete_batch(redis, batch_id)
        logger.info("Batch %s (Upload Phase): COMPLETED", batch_id)

        return {
            "batch_id": batch_id,
            "status": "COMPLETED",
            "total_files": len(files),
        }

    except Exception as exc:
        logger.exception("Batch %s: unhandled error", batch_id)
        await fail_batch(redis, batch_id, str(exc))
        return {"batch_id": batch_id, "status": "FAILED", "reason": str(exc)}

    finally:
        # Cleanup temp directory
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
    """ARQ task: screen extracted candidates against the JD.

    Keep all database reads/writes in one AsyncSessionLocal context so detached
    ORM candidates do not get re-added to a second session and cause the
    duplicate-insert / rollback pattern shown by the worker logs.
    """
    redis = await get_redis_pool()

    try:
        await update_batch_progress(redis, batch_id, status="PROCESSING")

        async with AsyncSessionLocal() as db:
            # 1. Load Campaign & JD
            result = await db.execute(
                select(HiringCampaign).where(HiringCampaign.id == UUID(campaign_id))
            )
            campaign = result.scalar_one_or_none()

            if not campaign:
                await fail_batch(redis, batch_id, "Campaign not found.")
                return {"batch_id": batch_id, "status": "FAILED"}

            jd_profile: dict[str, Any] = campaign.jd_extracted_data or {}
            jd_text: str = campaign.job_description_raw or ""
            threshold = settings.PRE_SCREEN_MATCH_THRESHOLD

            # 2. Load Candidates from the same session
            #    Allow re-screening: include RESUME_PARSED, RESUME_SCREENED, and REJECTED.
            eligible_statuses = [
                CandidateStatus.RESUME_PARSED,
                CandidateStatus.RESUME_SCREENED,
                CandidateStatus.REJECTED,
            ]
            query = select(Candidate).where(
                Candidate.campaign_id == UUID(campaign_id),
                Candidate.status.in_(eligible_statuses),
            )
            if candidate_ids:
                query = query.where(Candidate.id.in_([UUID(cid) for cid in candidate_ids]))

            candidates_result = await db.execute(query)
            candidates = candidates_result.scalars().all()

            if not candidates:
                await fail_batch(redis, batch_id, "No eligible candidates found.")
                return {"batch_id": batch_id, "status": "FAILED"}

            # Delete any existing screening rows for these candidates so
            # re-screening doesn't violate constraints or leave stale data.
            existing_screening_ids = [c.id for c in candidates]
            from sqlalchemy import delete
            await db.execute(
                delete(ResumeScreening)
                .where(ResumeScreening.candidate_id.in_(existing_screening_ids))
            )
            await db.commit()

            await update_batch_progress(redis, batch_id, total_files=len(candidates))

            screened_count = 0

            # 3. Screen Each Candidate while preserving ORM identity
            for candidate in candidates:
                try:
                    profile = {
                        "name": candidate.name,
                        "email": candidate.email,
                        "phone": candidate.phone,
                        "location": candidate.location,
                        "experience_years": candidate.experience_years,
                        "skills": candidate.skills or [],
                    }

                    # Always compute a deterministic match score before any gate.
                    score = compute_match_score(profile, jd_profile)
                    screening_payload = build_resume_screening_result(
                        profile,
                        jd_profile,
                        score,
                    )

                    # Deterministic gate rejection must still store a resume_screening row.
                    if not filter_resume_against_jd(profile, jd_profile):
                        candidate.status = CandidateStatus.REJECTED
                        db.add(ResumeScreening(
                            candidate_id=candidate.id,
                            match_score=screening_payload["match_score"],
                            one_line_summary=screening_payload["one_line_summary"],
                            matched_skills=screening_payload["matched_skills"],
                            missing_skills=screening_payload["missing_skills"],
                        ))
                        await db.commit()
                        await update_batch_progress(redis, batch_id, processed_incr=1)
                        continue

                    # Threshold gate also needs a persisted screening artifact.
                    if score < threshold:
                        candidate.status = CandidateStatus.REJECTED
                        db.add(ResumeScreening(
                            candidate_id=candidate.id,
                            match_score=screening_payload["match_score"],
                            one_line_summary=screening_payload["one_line_summary"],
                            matched_skills=screening_payload["matched_skills"],
                            missing_skills=screening_payload["missing_skills"],
                        ))
                        await db.commit()
                        await update_batch_progress(redis, batch_id, processed_incr=1)
                        continue

                    # LLM screening path
                    screening = await _llm_screen_candidate(candidate, jd_text, jd_profile, score)

                    # Persist LLM screening results and update candidate status in same session.
                    db.add(screening)
                    llm_score = screening.match_score or 0
                    if llm_score >= 50:
                        candidate.status = CandidateStatus.RESUME_SCREENED
                    else:
                        candidate.status = CandidateStatus.REJECTED
                    db.add(candidate)
                    await db.commit()

                    screened_count += 1
                    await update_batch_progress(redis, batch_id, processed_incr=1)

                except Exception:
                    logger.exception("Batch %s: screening error for candidate %s", batch_id, candidate.id)
                    await update_batch_progress(redis, batch_id, failed_incr=1)

        await complete_batch(redis, batch_id)
        logger.info("Batch %s (Screening Phase): COMPLETED", batch_id)

        return {
            "batch_id": batch_id,
            "status": "COMPLETED",
            "screened_count": screened_count,
        }

    except Exception as exc:
        logger.exception("Batch %s: screening unhandled error", batch_id)
        await fail_batch(redis, batch_id, str(exc))
        return {"batch_id": batch_id, "status": "FAILED", "reason": str(exc)}
    finally:
        await redis.aclose()
