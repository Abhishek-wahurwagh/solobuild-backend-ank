import logging
from uuid import UUID
from app.core.config import settings
from app.integrations.voice.providers.base import BaseVoiceProvider

logger = logging.getLogger("solo.voice.mock")

class MockVoiceProvider(BaseVoiceProvider):
    async def initiate_call(
        self, 
        candidate_phone: str, 
        candidate_id: UUID, 
        campaign_id: UUID,
        system_prompt: str,
    ) -> str:
        """
        Mocks an outbound call. In a real scenario, this would make an API call to Vobiz.
        Here we just return a fake call ID. The webhook will be manually simulated for testing.
        """
        logger.info(f"Mocking outbound call to {candidate_phone} for candidate {candidate_id}")
        return "mock_call_12345"
