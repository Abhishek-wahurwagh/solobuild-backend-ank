import asyncio

import pytest

from app.domains.ingestion.pipeline import IngestionPipelineError, run_document_pipeline


def test_pipeline_passes_extracted_fields_to_domain_processor():
    processed = []

    async def extract_fields(text):
        return {"text": text}

    async def process_fields(fields):
        processed.append(fields)

    result = asyncio.run(
        run_document_pipeline(
            b"plain text",
            ".txt",
            extract_fields=extract_fields,
            process_fields=process_fields,
        )
    )

    assert result == "plain text"
    assert processed == [{"text": "plain text"}]


def test_run_document_pipeline_retries_transient_field_extraction_failures():
    attempts = 0

    async def extract_fields(text: str):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("transient extraction failure")
        return {"name": "Ada Lovelace"}

    captured = {}

    async def process_fields(fields: dict):
        captured["fields"] = fields

    result = asyncio.run(
        run_document_pipeline(
            b"Ada Lovelace\nAnalyst\n",
            ".txt",
            extract_fields=extract_fields,
            process_fields=process_fields,
        )
    )

    assert result == "Ada Lovelace\nAnalyst"
    assert attempts == 3
    assert captured["fields"] == {"name": "Ada Lovelace"}


def test_run_document_pipeline_raises_pipeline_error_after_retries_exhausted():
    async def extract_fields(text: str):
        raise RuntimeError("persistent failure")

    with pytest.raises(IngestionPipelineError, match="Field extraction failed"):
        asyncio.run(
            run_document_pipeline(
                b"Some document text\n",
                ".txt",
                extract_fields=extract_fields,
                process_fields=lambda fields: None,
            )
        )
