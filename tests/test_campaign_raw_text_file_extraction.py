import asyncio

from app.domains.campaigns.service import (
    build_campaign_raw_text,
    extract_campaign_requirements_llm,
)


class DummyUploadFile:
    filename = "resume.txt"

    async def read(self):
        return b"hello from uploaded file"


class DummyExtractionProvider:
    async def extract(self, document_text, existing_fields=None):
        return {"skills": ["Python"], "source": document_text}


def test_build_campaign_raw_text_extracts_uploaded_file_text():
    result = asyncio.run(build_campaign_raw_text(None, DummyUploadFile()))

    assert result == "hello from uploaded file"


def test_build_campaign_raw_text_rejects_text_and_file_together():
    try:
        asyncio.run(build_campaign_raw_text("manual text", DummyUploadFile()))
    except Exception as exc:
        assert "either raw_text or a requirement file" in str(exc)
    else:
        raise AssertionError("Expected text and file to be mutually exclusive")


def test_extract_campaign_requirements_uses_structured_extraction_provider():
    result = asyncio.run(
        extract_campaign_requirements_llm(
            "Python backend engineer",
            extraction_provider=DummyExtractionProvider(),
        )
    )

    assert result == {
        "skills": ["Python"],
        "source": "Python backend engineer",
    }
