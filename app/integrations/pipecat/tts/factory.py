from __future__ import annotations

from typing import Any

from app.core.config import settings


class PipecatTTSFactory:
    """Build the configured Pipecat TTS independently of telephony."""

    @staticmethod
    def build() -> Any:
        provider = settings.PIPECAT_TTS_PROVIDER.lower()
        if provider == "gemini" or provider == "google":
            try:
                from pipecat.services.google import GoogleTTSService
            except ImportError as exc:
                raise RuntimeError(
                    "Pipecat Google support requires pipecat-ai and its Google extra."
                ) from exc

            # Note: GoogleTTSService normally requires standard GCP credentials
            # Pipecat will fall back to application default credentials if not passed explicitly.
            return GoogleTTSService()
        elif provider == "openai":
            try:
                from pipecat.services.openai import OpenAITTSService
            except ImportError:
                # older pipecat package structure
                from pipecat.services.openai.tts import OpenAITTSService # type: ignore
                
            if not settings.PIPECAT_OPENAI_API_KEY:
                raise RuntimeError("PIPECAT_OPENAI_API_KEY is required for OpenAI TTS.")
            return OpenAITTSService(
                api_key=settings.PIPECAT_OPENAI_API_KEY,
                model=settings.PIPECAT_TTS_MODEL,
                voice=settings.PIPECAT_TTS_VOICE,
            )
        raise ValueError(f"Unsupported Pipecat TTS provider: {provider}")
