import asyncio

import pytest

from app.core.config import settings
from app.domains.campaigns.service import (
    build_resume_screening_result,
    extract_jd_structured_fields,
    filter_resume_against_jd,
    parse_resume_profile,
)
from app.integrations.ai.factory import EmbeddingProviderFactory
from app.integrations.ai.providers.external_embed import GeminiEmbeddingProvider


class DummyStructuredExtractor:
    async def extract(self, jd_text: str):
        return {
            "experience_required": "2+ years",
            "skills_required": ["Python", "FastAPI"],
            "location": "Remote",
            "employment_type": "FULL_TIME",
            "job_title": "Backend Engineer",
            "must_have": ["APIs"],
            "nice_to_have": ["Docker"],
        }


def test_extract_jd_structured_fields_returns_lightweight_payload_only():
    async def run_check():
        payload = await extract_jd_structured_fields(
            "Backend engineer with Python and FastAPI experience.",
            extraction_provider=DummyStructuredExtractor(),
        )

        assert set(payload.keys()) == {"experience_required", "skills_required"}
        assert payload["experience_required"] == "2+ years"
        assert payload["skills_required"] == ["Python", "FastAPI"]

    asyncio.run(run_check())


def test_parse_resume_profile_and_filter_against_jd_are_deterministic():
    resume_text = """
    Python developer with 4 years of experience in FastAPI, SQL, and PostgreSQL.
    Worked with Docker and AWS.
    """

    profile = parse_resume_profile(resume_text)
    assert profile["experience_years"] == 4
    assert "python" in profile["skills"]
    assert "fastapi" in profile["skills"]
    assert "sql" in profile["skills"]

    jd_profile = {
        "experience_required": "2+ years",
        "skills_required": ["Python", "FastAPI"],
    }

    assert filter_resume_against_jd(profile, jd_profile) is True


def test_build_resume_screening_result_is_available_for_rejection_and_threshold_failures():
    profile = {
        "skills": ["Python", "FastAPI"],
        "experience_years": 2,
    }
    jd_profile = {
        "experience_required": "2+ years",
        "skills_required": ["Python", "FastAPI", "Docker"],
    }

    result = build_resume_screening_result(profile, jd_profile, score=0.4)

    assert result["match_score"] == 40.0
    assert result["matched_skills"] == ["fastapi", "python"]
    assert result["missing_skills"] == ["docker"]
    assert result["one_line_summary"] == ""


def test_embedding_provider_factory_returns_gemini_provider_for_gemini_settings(monkeypatch):
    monkeypatch.setattr(settings, "AI_EMBEDDING_PROVIDER", "gemini", raising=False)
    monkeypatch.setattr(settings, "GEMINI_API_KEY", "test-key", raising=False)
    monkeypatch.setattr(settings, "GEMINI_EMBEDDING_MODEL", "models/embedding-001", raising=False)

    provider = EmbeddingProviderFactory.build()

    assert isinstance(provider, GeminiEmbeddingProvider)
