from __future__ import annotations

from typing import Any

from app.core.config import settings


class PipecatLLMFactory:
    """Build the configured Pipecat LLM independently of telephony."""

    @staticmethod
    def build(*, system_prompt: str) -> Any:
        provider = settings.PIPECAT_LLM_PROVIDER.lower()
        if provider == "gemini":
            from app.integrations.pipecat.llm.gemini import GeminiPipecatLLM

            return GeminiPipecatLLM.build(system_prompt=system_prompt)
        raise ValueError(f"Unsupported Pipecat LLM provider: {provider}")
