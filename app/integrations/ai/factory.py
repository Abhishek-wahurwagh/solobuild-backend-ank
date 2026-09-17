from app.core.config import settings
from app.integrations.ai.providers.base import StructuredExtractionProvider
from app.integrations.ai.providers.gemini import GeminiStructuredExtractor


class StructuredExtractionProviderFactory:
    @staticmethod
    def build() -> StructuredExtractionProvider:
        return GeminiStructuredExtractor(
            api_key=settings.GEMINI_API_KEY,
            model=getattr(settings, "GEMINI_EXTRACTION_MODEL", "gemini-2.0-flash-lite"),
        )