"""
Campaign service layer.

Contains:
- JD file validation / text extraction / normalization  (existing)
- JD structured field extraction                          (existing)
- Resume upload validation (extension, MIME, zip safety)  (new)
- S3 staging                                              (new)
- Redis batch tracker CRUD                                (new)
- Deterministic resume profile parser                     (enhanced)
- Deterministic pre-screen gate                           (existing, refined)
- Campaign CRUD                                           (existing)
"""

from __future__ import annotations

import asyncio
import io
import os
import re
import unicodedata
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import HTTPException, UploadFile, status
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession
from uuid6 import uuid7

from app.core.config import settings
from app.core.s3 import upload_bytes_to_s3, upload_fileobj_to_s3
from app.domains.campaigns.models import (
    CampaignStatus,
    Candidate,
    CandidateStatus,
    EmploymentType,
    HiringCampaign,
    ResumeScreening,
)
from app.integrations.ai.factory import StructuredExtractionProviderFactory
from app.integrations.ai.providers.base import StructuredExtractionProvider

# ---------------------------------------------------------------------------
# Skill vocabulary — deterministic matching set
# ---------------------------------------------------------------------------

COMMON_SKILLS: set[str] = {
    "python",
    "fastapi",
    "sql",
    "postgresql",
    "postgres",
    "mysql",
    "redis",
    "docker",
    "kubernetes",
    "k8s",
    "aws",
    "gcp",
    "azure",
    "javascript",
    "typescript",
    "react",
    "angular",
    "vue",
    "node",
    "nodejs",
    "django",
    "flask",
    "spring",
    "java",
    "c++",
    "c#",
    "go",
    "golang",
    "rust",
    "ruby",
    "php",
    "swift",
    "kotlin",
    "elasticsearch",
    "machine learning",
    "deep learning",
    "ml",
    "nlp",
    "data science",
    "data engineering",
    "pandas",
    "numpy",
    "spark",
    "kafka",
    "rabbitmq",
    "terraform",
    "ansible",
    "jenkins",
    "ci/cd",
    "linux",
    "bash",
    "git",
    "html",
    "css",
    "graphql",
    "rest",
    "mongodb",
    "dynamodb",
    "cassandra",
    "airflow",
    "hadoop",
    "tableau",
    "power bi",
    "excel",
    "figma",
    "jira",
}

# ---------------------------------------------------------------------------
# JD file validation / extraction  (carried from original)
# ---------------------------------------------------------------------------

ALLOWED_JD_EXT_TO_MIME: dict[str, set[str]] = {
    ".pdf": {"application/pdf"},
    ".docx": {
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/zip",
    },
    ".txt": {"text/plain", "application/octet-stream"},
    ".md": {"text/markdown", "text/plain", "application/octet-stream"},
}


def validate_jd_file(upload: UploadFile) -> None:
    if not upload.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="JD file must include a valid filename",
        )
    ext = Path(upload.filename).suffix.lower()
    if ext not in ALLOWED_JD_EXT_TO_MIME:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported JD file type: {upload.filename}. Allowed: pdf, docx, txt, md",
        )
    expected_mimes = ALLOWED_JD_EXT_TO_MIME[ext]
    actual_mime = upload.content_type or ""
    if actual_mime and actual_mime not in expected_mimes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File MIME mismatch for JD file: {upload.filename}",
        )


async def extract_jd_from_upload(upload: UploadFile) -> str:
    validate_jd_file(upload)
    ext = Path(upload.filename).suffix.lower()  # type: ignore[union-attr]
    raw = await upload.read()

    if ext == ".pdf":
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(raw))
            return "\n".join(page.extract_text() or "" for page in reader.pages)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unable to read PDF JD file: {exc}",
            )

    if ext == ".docx":
        try:
            from docx import Document
            doc = Document(io.BytesIO(raw))
            return "\n".join(p.text for p in doc.paragraphs)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unable to read DOCX JD file: {exc}",
            )

    if ext in {".txt", ".md"}:
        try:
            return raw.decode("utf-8")
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unable to decode JD text file: {exc}",
            )

    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported JD file type")


# ---------------------------------------------------------------------------
# Text normalization (shared between JD and resume flows)
# ---------------------------------------------------------------------------

def normalize_jd_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[\x00-\x08\x0b-\x1f\x7f]", "", text)
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ---------------------------------------------------------------------------
# JD structured extraction
# ---------------------------------------------------------------------------

async def extract_jd_structured_fields(
    jd_text: str,
    extraction_provider: StructuredExtractionProvider | None = None,
) -> dict[str, Any]:
    normalized_text = normalize_jd_text(jd_text or "")
    if not normalized_text.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="JD text cannot be empty for structured extraction.",
        )
    if extraction_provider is None:
        extraction_provider = StructuredExtractionProviderFactory.build()

    extracted = await extraction_provider.extract(normalized_text)

    # Lightweight JD contract for the first-pass resume gate:
    # keep only the fields that directly drive experience filtering,
    # and strict skill minimum matching. Avoid a broad extraction pass.
    return {
        "experience_required": extracted.get("experience_required"),
        "skills_required": extracted.get("skills_required") or [],
    }


# =========================================================================
# RESUME UPLOAD VALIDATION
# =========================================================================

ALLOWED_RESUME_EXTENSIONS: set[str] = {".pdf", ".docx", ".txt", ".zip"}
BLOCKED_EXTENSIONS: set[str] = {".exe", ".js", ".html", ".csv", ".bat", ".sh", ".cmd", ".msi", ".dll"}

RESUME_EXT_TO_MIME: dict[str, set[str]] = {
    ".pdf": {"application/pdf"},
    ".docx": {
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/zip",
    },
    ".txt": {"text/plain", "application/octet-stream"},
    ".zip": {"application/zip", "application/x-zip-compressed", "application/octet-stream"},
}

# Maximum zip bomb thresholds
_ZIP_MAX_UNCOMPRESSED_BYTES = 500 * 1024 * 1024  # 500 MB
_ZIP_MAX_RATIO = 50  # compression ratio


def validate_resume_uploads(files: list[UploadFile]) -> list[UploadFile]:
    """Validate a list of uploaded resume files.

    Returns the validated list (unchanged) or raises 400.
    """
    if not files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one file is required.",
        )

    for f in files:
        if not f.filename:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Every uploaded file must have a filename.",
            )
        ext = Path(f.filename).suffix.lower()

        if ext in BLOCKED_EXTENSIONS:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Blocked file type: {f.filename}",
            )
        if ext not in ALLOWED_RESUME_EXTENSIONS:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported file type: {f.filename}. Allowed: pdf, docx, txt, zip",
            )

        # MIME check
        expected = RESUME_EXT_TO_MIME.get(ext, set())
        actual = f.content_type or ""
        if actual and expected and actual not in expected:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"MIME mismatch for {f.filename}: got {actual}",
            )

    return files


async def validate_zip_safety(data: bytes) -> list[str]:
    """Validate a zip archive for safety.

    Returns the list of **safe** member filenames inside the archive.
    Raises 400 on any dangerous condition.
    """
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded zip file is corrupt or not a valid zip.",
        )

    infos = zf.infolist()

    # File count check
    max_count = settings.RESUME_MAX_FILE_COUNT
    if len(infos) > max_count:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Zip contains {len(infos)} entries (max {max_count}).",
        )

    # Zip bomb — total uncompressed size
    total_uncompressed = sum(i.file_size for i in infos)
    compressed_size = len(data)
    if total_uncompressed > _ZIP_MAX_UNCOMPRESSED_BYTES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Zip file exceeds maximum uncompressed size (potential zip bomb).",
        )
    if compressed_size > 0 and (total_uncompressed / compressed_size) > _ZIP_MAX_RATIO:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Zip compression ratio is suspiciously high (potential zip bomb).",
        )

    safe_names: list[str] = []
    for info in infos:
        name = info.filename

        # Reject directories
        if info.is_dir():
            continue

        # Reject symlinks  (external_attr bit 29 set means symlink on Unix)
        if (info.external_attr >> 16) & 0o120000 == 0o120000:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Zip contains a symlink: {name}",
            )

        # Reject absolute paths
        if name.startswith("/") or name.startswith("\\"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Zip contains an absolute path: {name}",
            )

        # Reject zip-slip (path traversal)
        if ".." in name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Zip contains a path-traversal entry: {name}",
            )

        # Skip OS junk
        basename = Path(name).name
        if basename.startswith(".") or basename in {"__MACOSX", "Thumbs.db", "desktop.ini"}:
            continue
        # __MACOSX is often a directory, but also skip files under it
        if "__MACOSX" in name:
            continue

        # Reject nested zip
        if Path(name).suffix.lower() == ".zip":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Nested zip files are not allowed: {name}",
            )

        # Extension check
        ext = Path(name).suffix.lower()
        if ext not in {".pdf", ".docx", ".txt"}:
            continue  # skip unsupported, don't reject the whole zip

        safe_names.append(name)

    if not safe_names:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Zip contains no supported resume files (pdf, docx, txt).",
        )

    return safe_names


# =========================================================================
# S3 STAGING
# =========================================================================

async def stage_files_to_s3(
    batch_id: str,
    files: list[UploadFile],
) -> tuple[str, str]:
    """Upload validated files to S3.

    Returns ``(s3_prefix, source_type)`` where source_type is one of
    ``"zip"``, ``"single"``, ``"multi"``.
    """
    s3_prefix = f"resume-batches/{batch_id}/original"

    source_type = "single"
    if len(files) > 1:
        source_type = "multi"

    for f in files:
        ext = Path(f.filename).suffix.lower()  # type: ignore[union-attr]
        if ext == ".zip":
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

async def create_batch_tracker(
    redis: Redis,
    batch_id: str,
    file_count: int,
) -> None:
    """Create the initial Redis hash for a resume-processing batch."""
    now = datetime.now(timezone.utc).isoformat()
    await redis.hset(  # type: ignore
        f"job:{batch_id}",
        mapping={
            "status": "QUEUED",
            "total_files": str(file_count),
            "processed": "0",
            "failed": "0",
            "created_at": now,
            "updated_at": now,
        },
    )
    # Auto-expire after 7 days to avoid stale keys
    await redis.expire(f"job:{batch_id}", 7 * 86400)


async def update_batch_progress(
    redis: Redis,
    batch_id: str,
    *,
    total_files: int | None = None,
    processed_incr: int = 0,
    failed_incr: int = 0,
    status: str | None = None,
) -> None:
    """Atomically update progress counters on the batch tracker."""
    pipe = redis.pipeline(transaction=True)
    if total_files is not None:
        pipe.hset(f"job:{batch_id}", "total_files", str(total_files))
    if processed_incr:
        pipe.hincrby(f"job:{batch_id}", "processed", processed_incr)
    if failed_incr:
        pipe.hincrby(f"job:{batch_id}", "failed", failed_incr)
    if status:
        pipe.hset(f"job:{batch_id}", "status", status)
    pipe.hset(f"job:{batch_id}", "updated_at", datetime.now(timezone.utc).isoformat())
    await pipe.execute()


async def complete_batch(redis: Redis, batch_id: str) -> None:
    """Mark a batch as COMPLETED."""
    now = datetime.now(timezone.utc).isoformat()
    await redis.hset(  # type: ignore
        f"job:{batch_id}",
        mapping={
            "status": "COMPLETED",
            "finished_at": now,
            "updated_at": now,
        },
    )


async def fail_batch(redis: Redis, batch_id: str, reason: str = "") -> None:
    """Mark a batch as FAILED."""
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
    """Read the full batch tracker hash.  Returns ``None`` if missing."""
    data = await redis.hgetall(f"job:{batch_id}")  # type: ignore
    return data or None


# =========================================================================
# ARQ JOB ENQUEUE
# =========================================================================

async def enqueue_resume_upload_batch(
    redis: Redis,
    *,
    batch_id: str,
    campaign_id: str,
    s3_prefix: str,
    source_type: str,
) -> None:
    """Push a resume-upload-processing job onto the ARQ queue."""
    from arq import create_pool
    from arq.connections import RedisSettings

    # Parse the REDIS_URL into arq-compatible settings
    pool = await create_pool(RedisSettings.from_dsn(settings.REDIS_URL))
    await pool.enqueue_job(
        "process_resume_upload_batch",
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
    """Push a campaign screening job onto the ARQ queue."""
    from arq import create_pool
    from arq.connections import RedisSettings

    pool = await create_pool(RedisSettings.from_dsn(settings.REDIS_URL))
    await pool.enqueue_job(
        "screen_campaign_candidates",
        batch_id=batch_id,
        campaign_id=campaign_id,
        candidate_ids=candidate_ids,
    )
    await pool.aclose()


# =========================================================================
# RESUME TEXT EXTRACTION
# =========================================================================

def extract_text_from_pdf(data: bytes) -> str:
    """Extract visible text from a PDF byte string."""
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(data))
    pages = [page.extract_text() or "" for page in reader.pages]
    return "\n".join(pages)


def extract_text_from_docx(data: bytes) -> str:
    """Extract paragraph + table text from a DOCX byte string."""
    from docx import Document

    doc = Document(io.BytesIO(data))
    parts: list[str] = [p.text for p in doc.paragraphs]
    for table in doc.tables:
        for row in table.rows:
            parts.append(" | ".join(cell.text for cell in row.cells))
    return "\n".join(parts)


def extract_text_from_txt(data: bytes) -> str:
    """Decode a plain-text file with fallback encodings."""
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return data.decode(enc)
        except (UnicodeDecodeError, ValueError):
            continue
    return data.decode("utf-8", errors="replace")


def extract_resume_text(data: bytes, ext: str) -> str | None:
    """Route to the correct extractor based on file extension.

    Returns ``None`` when extraction fails (caller should skip the file).
    """
    try:
        if ext == ".pdf":
            return extract_text_from_pdf(data)
        if ext == ".docx":
            return extract_text_from_docx(data)
        if ext == ".txt":
            return extract_text_from_txt(data)
    except Exception:
        return None
    return None


# =========================================================================
# MAGIC BYTE VALIDATION
# =========================================================================

def validate_magic_bytes(data: bytes, ext: str) -> bool:
    """Check that *data* starts with the expected magic bytes for *ext*."""
    if ext == ".pdf":
        return data[:5] == b"%PDF-"
    if ext == ".docx":
        # DOCX is a zip archive — starts with PK\x03\x04
        return data[:4] == b"PK\x03\x04"
    if ext == ".txt":
        # Heuristic: reject if the first 512 bytes look binary
        sample = data[:512]
        # Allow high-bit UTF-8, reject control chars except \t \n \r
        control = sum(1 for b in sample if b < 0x09 or (0x0E <= b <= 0x1F) or b == 0x7F)
        return control <= 2
    return False


# =========================================================================
# DETERMINISTIC RESUME PROFILE PARSER
# =========================================================================

# --- Contact extraction patterns ---
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
_PHONE_RE = re.compile(
    r"(?:\+?\d{1,3}[\s\-.]?)?"         # country code
    r"(?:\(?\d{2,4}\)?[\s\-.]?)?"       # area code
    r"\d{3,4}[\s\-.]?\d{3,4}"           # subscriber
)

# --- Experience patterns (sorted most-specific first) ---
_EXP_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"(\d+(?:\.\d+)?)\s*\+?\s*years?\s+of\s+experience", re.I),
    re.compile(r"(\d+(?:\.\d+)?)\s*\+?\s*years?\s+experience", re.I),
    re.compile(r"(\d+(?:\.\d+)?)\s*\+?\s*(?:yrs?|years?)", re.I),
    re.compile(r"experience\s*:?\s*(\d+(?:\.\d+)?)\s*\+?\s*(?:yrs?|years?)", re.I),
    re.compile(r"(?:worked|working)\s+(?:for|in|as)\s+.*?(\d+(?:\.\d+)?)\s*\+?\s*years?", re.I),
]

# Section headings used to locate "experience" blocks
_EXPERIENCE_HEADINGS = re.compile(
    r"^(?:professional\s+)?(?:experience|work\s+(?:experience|history)|employment\s+history|projects?)\s*$",
    re.I | re.M,
)


def parse_resume_profile(resume_text: str) -> dict[str, Any]:
    """Deterministic, non-LLM candidate profile parser.

    Returns a dictionary with:
    - name, email, phone, location
    - experience_years
    - skills
    - raw_text
    - profile_status  ("PARSED" or "INCOMPLETE")
    """
    if not resume_text or not resume_text.strip():
        return {
            "name": None,
            "email": None,
            "phone": None,
            "location": None,
            "experience_years": None,
            "skills": [],
            "raw_text": "",
            "profile_status": "INCOMPLETE",
        }

    text = normalize_jd_text(resume_text)
    low = text.lower()
    lines = text.split("\n")

    # ------ Name (first non-blank line that looks like a name) ------
    name: str | None = None
    for line in lines[:10]:
        stripped = line.strip()
        if not stripped:
            continue
        # Skip lines that are obviously not names
        if "@" in stripped or stripped.startswith("http"):
            continue
        if _PHONE_RE.fullmatch(stripped.replace(" ", "")):
            continue
        # Likely name: short, title-cased or all-caps, no digits
        if len(stripped.split()) <= 5 and not any(ch.isdigit() for ch in stripped):
            name = stripped
            break

    # ------ Email ------
    email_match = _EMAIL_RE.search(text)
    email = email_match.group(0) if email_match else None

    # ------ Phone ------
    phone: str | None = None
    phone_match = _PHONE_RE.search(text)
    if phone_match:
        candidate_phone = phone_match.group(0).strip()
        # Only accept if the matched string has enough digit content
        digits = re.sub(r"\D", "", candidate_phone)
        if len(digits) >= 7:
            phone = candidate_phone

    # ------ Location (city/state/country heuristic) ------
    location: str | None = None
    _loc_re = re.compile(
        r"(?:location|address|city|based\s+in)\s*:?\s*(.+)",
        re.I,
    )
    loc_match = _loc_re.search(text)
    if loc_match:
        location = loc_match.group(1).strip().split("\n")[0].strip()

    # ------ Experience years ------
    experience_years: int | None = None
    found_years: list[float] = []
    for pat in _EXP_PATTERNS:
        for m in pat.finditer(low):
            try:
                found_years.append(float(m.group(1)))
            except ValueError:
                continue
    if found_years:
        experience_years = int(max(found_years))

    # ------ Skills ------
    found_skills: set[str] = set()
    for skill in COMMON_SKILLS:
        # Use word-boundary matching for short skills to avoid false positives
        if len(skill) <= 3:
            if re.search(rf"\b{re.escape(skill)}\b", low):
                found_skills.add(skill)
        else:
            if skill in low:
                found_skills.add(skill)

    skills = sorted(found_skills)

    profile_status = "PARSED" if (experience_years is not None or skills) else "INCOMPLETE"

    return {
        "name": name,
        "email": email,
        "phone": phone,
        "location": location,
        "experience_years": experience_years,
        "skills": skills,
        "raw_text": text,
        "profile_status": profile_status,
    }


# =========================================================================
# DETERMINISTIC PRE-SCREEN GATE
# =========================================================================

def parse_experience_requirement(raw: Any) -> int | None:
    """Extract a numeric year value from a JD experience requirement string."""
    if raw is None:
        return None
    match = re.search(r"(\d+(?:\.\d+)?)\s*\+?\s*years?", str(raw).lower())
    if match:
        return int(float(match.group(1)))
    # Bare number
    try:
        return int(float(str(raw)))
    except (ValueError, TypeError):
        return None


def build_resume_screening_result(
    candidate_profile: dict[str, Any],
    jd_profile: dict[str, Any],
    score: float,
    *,
    one_line_summary: str = "",
    matched_skills: list[str] | None = None,
    missing_skills: list[str] | None = None,
) -> dict[str, Any]:
    """Return the deterministic screening payload that belongs in the ResumeScreening table.

    The payload is intentionally independent of the LLM response so the worker can
    persist a score and the skill comparison list for deterministic rejections and
    threshold failures as well as successful LLM screeners.
    """
    required_skills = jd_profile.get("skills_required") or []
    required_skills_norm = {str(s).lower() for s in required_skills}
    candidate_skills = {str(s).lower() for s in (candidate_profile.get("skills") or [])}

    if matched_skills is None:
        matched_skills = sorted(required_skills_norm.intersection(candidate_skills))
    if missing_skills is None:
        missing_skills = sorted(required_skills_norm.difference(candidate_skills))

    return {
        "match_score": round(score * 100, 2),
        "one_line_summary": one_line_summary,
        "matched_skills": matched_skills,
        "missing_skills": missing_skills,
    }


def compute_skill_overlap(
    candidate_skills: list[str],
    required_skills: list[str],
) -> float:
    """Return the fraction of required skills that the candidate has.

    Returns 1.0 when there are no required skills (vacuously true).
    """
    if not required_skills:
        return 1.0
    cset = {s.lower() for s in candidate_skills}
    rset = {s.lower() for s in required_skills}
    overlap = cset & rset
    return len(overlap) / len(rset)


def filter_resume_against_jd(
    candidate_profile: dict[str, Any],
    jd_profile: dict[str, Any],
) -> bool:
    """Deterministic filter for the cheap gate.

    Returns ``True`` if the candidate should proceed past the gate.
    """
    # --- Experience gate ---
    required_min = parse_experience_requirement(jd_profile.get("experience_required"))
    if required_min is not None:
        candidate_years = candidate_profile.get("experience_years")
        if candidate_years is not None and candidate_years < required_min:
            return False

    # --- Skills gate (at least one overlap) ---
    required_skills = jd_profile.get("skills_required") or []
    required_skills_norm = {s.lower() for s in required_skills}
    candidate_skills = {s.lower() for s in (candidate_profile.get("skills") or [])}

    if required_skills_norm and not required_skills_norm.intersection(candidate_skills):
        return False

    return True


def compute_match_score(
    candidate_profile: dict[str, Any],
    jd_profile: dict[str, Any],
) -> float:
    """Compute a simple 0..1 match score for threshold-based LLM eligibility.

    Factors:
    - skill overlap ratio   (weight 0.7)
    - experience sufficiency (weight 0.3)
    """
    required_skills = jd_profile.get("skills_required") or []
    skill_score = compute_skill_overlap(
        candidate_profile.get("skills") or [],
        required_skills,
    )

    exp_score = 1.0
    required_min = parse_experience_requirement(jd_profile.get("experience_required"))
    if required_min and required_min > 0:
        candidate_years = candidate_profile.get("experience_years")
        if candidate_years is not None:
            exp_score = min(candidate_years / required_min, 1.0)
        else:
            exp_score = 0.5  # unknown → neutral

    return round(skill_score * 0.7 + exp_score * 0.3, 4)


# =========================================================================
# CAMPAIGN CRUD
# =========================================================================

class CampaignService:
    @staticmethod
    async def create_campaign(
        db: AsyncSession,
        *,
        job_title: str,
        location: str | None,
        employment_type: EmploymentType,
        role_summary: str | None,
        jd_text: str,
        user_id,
        extraction_provider: StructuredExtractionProvider | None = None,
    ) -> HiringCampaign:
        if not jd_text or not jd_text.strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="JD text cannot be empty.",
            )

        structured_data = await extract_jd_structured_fields(
            jd_text,
            extraction_provider=extraction_provider,
        )

        campaign = HiringCampaign(
            user_id=user_id,
            job_title=job_title,
            location=location,
            employment_type=employment_type,
            role_summary=role_summary,
            status=CampaignStatus.DRAFT,
            job_description_raw=jd_text,
            jd_extracted_data=structured_data,
        )

        db.add(campaign)
        await db.commit()
        await db.refresh(campaign)
        return campaign