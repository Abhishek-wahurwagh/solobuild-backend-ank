from app.core.config import settings
from app.integrations.ai.providers.base import (
    EmbeddingProvider,
    StructuredExtractionProvider,
)
from app.integrations.ai.providers.external_embed import (
    GeminiEmbeddingProvider,
    GeminiStructuredExtractor,
    OpenAIEmbeddingProvider,
)


class EmbeddingProviderFactory:
    @staticmethod
    def build() -> EmbeddingProvider:
        provider = (settings.AI_EMBEDDING_PROVIDER).lower()

        if provider == "openai":
            return OpenAIEmbeddingProvider(
                api_key=settings.OPENAI_API_KEY,
                model=getattr(settings, "OPENAI_EMBEDDING_MODEL", "text-embedding-3-small"),
            )

        if provider == "gemini":
            return GeminiEmbeddingProvider(
                api_key=settings.GEMINI_API_KEY,
                model=settings.GEMINI_EMBEDDING_MODEL,
            )

        raise RuntimeError(f"Unsupported embedding provider: {settings.AI_EMBEDDING_PROVIDER}")


class StructuredExtractionProviderFactory:
    @staticmethod
    def build() -> StructuredExtractionProvider:
        provider = (settings.AI_EMBEDDING_PROVIDER or "gemini").lower()

        if provider == "gemini":
            return GeminiStructuredExtractor(
                api_key=settings.GEMINI_API_KEY,
                model=getattr(settings, "GEMINI_EXTRACTION_MODEL", "gemini-2.0-flash-lite"),
            )

        if provider == "openai":
            # openai extraction can be layered later using the same method contract
            raise RuntimeError("OpenAI structured extraction is not yet configured in this repository version.")

        raise RuntimeError(f"Unsupported structured extraction provider: {settings.AI_EMBEDDING_PROVIDER}")