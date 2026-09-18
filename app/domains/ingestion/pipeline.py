from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from app.core.config import settings
from app.domains.ingestion.text import extract_document_text, validate_magic_bytes


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
    """Run generic file validation, text extraction, field extraction, and processing."""
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
