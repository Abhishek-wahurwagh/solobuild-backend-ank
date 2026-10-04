from __future__ import annotations

from abc import ABC, abstractmethod
from uuid import UUID


class BaseTelephonyCarrier(ABC):
    """Provider-neutral interface for PSTN/SIP call control."""

    @abstractmethod
    def build_answer_url(self, *, candidate_id: UUID, campaign_id: UUID) -> str:
        """Build the carrier callback URL that connects an answered call."""
        raise NotImplementedError

    @abstractmethod
    async def initiate_call(
        self,
        *,
        candidate_phone: str,
        candidate_id: UUID,
        campaign_id: UUID,
        answer_url: str,
    ) -> str:
        """Start a call and return the carrier call identifier."""
        raise NotImplementedError
