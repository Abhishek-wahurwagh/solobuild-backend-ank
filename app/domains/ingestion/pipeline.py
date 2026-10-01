from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from app.core.config import settings
from app.domains.ingestion.text import (
    csv_rows_to_text,
    extract_csv_rows,
    extract_document_text,
    validate_magic_bytes,
)


class IngestionPipelineError(RuntimeError):
    """A reusable ingestion stage failed."""


FieldExtractor = Callable[[str], Awaitable[dict[str, Any]]]
DocumentProcessor = Callable[[dict[str, Any]], Awaitable[None]]


async def run_document_pipeline(
    data: bytes,
    extension: str,
    *,
    extract_fields: FieldExtractor,
    process_fields: DocumentProcessor,
) -> str:
    """Run generic file validation, text extraction, field extraction, and processing.
    process_fields - function to further process the extracted fields"""
    if not validate_magic_bytes(data, extension):
        raise IngestionPipelineError("File content does not match its extension.")

    text = extract_document_text(data, extension)
    if not text or not text.strip():
        raise IngestionPipelineError("No usable text could be extracted from the file.")

    attempts = settings.LLM_MAX_RETRIES + 1
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            fields = await extract_fields(text)
            break
        except Exception as exc:  # pragma: no cover - exercised via tests and actual LLM errors
            last_error = exc
            if attempt >= attempts:
                raise IngestionPipelineError(
                    f"Field extraction failed after {attempts} attempts: {exc}"
                ) from exc

            delay = settings.LLM_RETRY_DELAY_SECONDS * attempt
            await asyncio.sleep(delay)
    else:
        raise IngestionPipelineError(
            "Field extraction failed without any usable result."
        )

    await process_fields(fields)
    return text


# ---------------------------------------------------------------------------
# CSV pipeline  (one IngestionItem → multiple candidates)
# ---------------------------------------------------------------------------

CandidateExtractor = Callable[[str], Awaitable[list[dict[str, Any]]]]
CandidateProcessor = Callable[[dict[str, Any]], Awaitable[None]]


async def run_csv_pipeline(
    data: bytes,
    *,
    extract_candidates: CandidateExtractor,
    process_candidate: CandidateProcessor,
    chunk_size: int | None = None,
) -> int:
    """Parse a CSV file and persist each row as a separate candidate.

    Steps:
    1. Decode and parse the CSV into rows.
    2. Split rows into chunks of ``chunk_size`` to avoid LLM token limits.
    3. For each chunk call ``extract_candidates`` (an LLM-backed function that
       maps arbitrary CSV text to a list of candidate dicts).
    4. For every candidate dict returned, call ``process_candidate``.

    Returns the total number of candidates processed.
    """
    if not data:
        raise IngestionPipelineError("CSV file is empty.")

    headers, rows = extract_csv_rows(data)
    if not rows:
        raise IngestionPipelineError("CSV contains no data rows.")

    effective_chunk_size = chunk_size or getattr(settings, "CSV_CHUNK_SIZE", 50)
    total_processed = 0
    attempts = settings.LLM_MAX_RETRIES + 1

    for chunk_start in range(0, len(rows), effective_chunk_size):
        chunk = rows[chunk_start : chunk_start + effective_chunk_size]
        chunk_text = csv_rows_to_text(headers, chunk)

        last_error: Exception | None = None
        candidates: list[dict[str, Any]] = []

        for attempt in range(1, attempts + 1):
            try:
                candidates = await extract_candidates(chunk_text)
                break
            except Exception as exc:  # pragma: no cover
                last_error = exc
                if attempt >= attempts:
                    raise IngestionPipelineError(
                        f"CSV candidate extraction failed after {attempts} attempts "
                        f"(chunk starting at row {chunk_start + 1}): {exc}"
                    ) from exc
                delay = settings.LLM_RETRY_DELAY_SECONDS * attempt
                await asyncio.sleep(delay)

        for candidate_fields in candidates:
            await process_candidate(candidate_fields)
            total_processed += 1

    return total_processed
