from __future__ import annotations

from typing import Any

from app.core.config import settings


class PipecatSTTFactory:
    """Build the configured Pipecat STT independently of telephony."""

    @staticmethod
    def build() -> Any:
        provider = settings.PIPECAT_STT_PROVIDER.lower()
        if provider == "gemini" or provider == "google":
            try:
                from pipecat.services.google import GoogleSTTService
            except ImportError as exc:
                raise RuntimeError(
                    "Pipecat Google support requires pipecat-ai and its Google extra."
                ) from exc

            # Note: GoogleSTTService normally requires standard GCP credentials
            # Pipecat will fall back to application default credentials if not passed explicitly.
            return GoogleSTTService()
        elif provider == "openai":
            try:
                from pipecat.services.openai import OpenAISTTService
            except ImportError:
                # older pipecat package structure
                from pipecat.services.openai.stt import OpenAISTTService # type: ignore
                
            if not settings.PIPECAT_OPENAI_API_KEY:
                raise RuntimeError("PIPECAT_OPENAI_API_KEY is required for OpenAI STT.")
            return OpenAISTTService(
                api_key=settings.PIPECAT_OPENAI_API_KEY,
                model=settings.PIPECAT_STT_MODEL,
            )
        raise ValueError(f"Unsupported Pipecat STT provider: {provider}")
