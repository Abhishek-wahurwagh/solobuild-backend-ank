from __future__ import annotations

from typing import Any

from app.core.config import settings


class GeminiPipecatLLM:
    """Deferred construction point for the Pipecat Gemini service."""

    @staticmethod
    def build(*, system_prompt: str) -> Any:
        try:
            from pipecat.services.google.llm import GoogleLLMService
        except ImportError as exc:
            raise RuntimeError(
                "Pipecat Gemini support requires pipecat-ai and its Google extra."
            ) from exc

        if not settings.GEMINI_API_KEY:
            raise RuntimeError("GEMINI_API_KEY is required for the Pipecat Gemini LLM.")

        return GoogleLLMService(
            api_key=settings.GEMINI_API_KEY,
            model=settings.PIPECAT_LLM_MODEL or settings.GEMINI_EXTRACTION_MODEL,
            system_instruction=system_prompt,
        )
