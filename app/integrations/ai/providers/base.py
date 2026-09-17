from abc import ABC, abstractmethod
from typing import Any


class StructuredExtractionProvider(ABC):
    @abstractmethod
    async def extract(self, document_text: str, existing_fields: dict | None = None) -> dict[str, Any]:
        """
        Extract fields from a generic document.
        If existing_fields is provided, retain them and append new fields extracted from the document.
        """
        raise NotImplementedError

    @abstractmethod
    async def screen_candidate(
        self, 
        candidate_text: str, 
        candidate_fields: dict, 
        campaign_text: str, 
        campaign_fields: dict
    ) -> dict[str, Any]:
        """
        Return candidate screening payload based on generic fields and text:
        {match_score, one_line_summary, matched_fields, unmatched_fields}.
        """
        raise NotImplementedError