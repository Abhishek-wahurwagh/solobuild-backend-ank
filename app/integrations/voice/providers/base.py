import abc
from uuid import UUID

class BaseVoiceProvider(abc.ABC):
    """
    Base interface for all Voice Calling Providers.
    """

    @abc.abstractmethod
    async def initiate_call(
        self, 
        candidate_phone: str, 
        candidate_id: UUID, 
        campaign_id: UUID,
        system_prompt: str,
    ) -> str:
        """
        Initiates an outbound call to the given phone number.
        Returns a provider-specific call ID.
        """
        pass
