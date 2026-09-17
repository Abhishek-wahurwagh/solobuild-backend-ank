import logging
from uuid import UUID
import httpx
from app.core.config import settings
from app.integrations.voice.providers.base import BaseVoiceProvider

logger = logging.getLogger("solo.voice.vobiz")

class VobizVoiceProvider(BaseVoiceProvider):
    async def initiate_call(
        self, 
        candidate_phone: str, 
        candidate_id: UUID, 
        campaign_id: UUID,
        system_prompt: str,
    ) -> str:
        """
        Initiates an outbound call via Vobiz REST API.
        """
        api_key = getattr(settings, "VOBIZ_API_KEY", None)
        if not api_key:
            logger.warning("VOBIZ_API_KEY not configured. Falling back to mock behavior.")
            return "mock_vobiz_call"
            
        url = "https://api.vobiz.com/v1/calls"
        payload = {
            "to": candidate_phone,
            "system_prompt": system_prompt,
            "metadata": {
                "candidate_id": str(candidate_id),
                "campaign_id": str(campaign_id),
            }
        }
        
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
                return data.get("call_id", "unknown_call_id")
            except Exception as e:
                logger.error(f"Failed to initiate Vobiz call: {e}")
                raise e
