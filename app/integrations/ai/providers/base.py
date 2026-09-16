from abc import ABC, abstractmethod
from typing import Any


class EmbeddingProvider(ABC):
    @abstractmethod
    async def embed(self, text: str) -> list[float]:
        """
        Return a vector of length 384, or a provider-specific embedding vector.
        """
        raise NotImplementedError


class StructuredExtractionProvider(ABC):
    @abstractmethod
    async def extract(self, jd_text: str) -> dict[str, Any]:
        """
        Return a lightweight metadata payload for a JD such as
        {experience_required, skills_required}.

        Keep the extraction contract intentionally narrow because the
        point of this pass is only to prefilter bootstrapping data cheaply.
        """
        raise NotImplementedError

    @abstractmethod
    async def screen_candidate(self, prompt: str) -> dict[str, Any]:
        """
        Return candidate screening payload such as
        {match_score, one_line_summary, matched_skills, missing_skills}.
        """
        raise NotImplementedError