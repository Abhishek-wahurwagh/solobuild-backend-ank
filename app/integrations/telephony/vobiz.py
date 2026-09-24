from __future__ import annotations

import logging
from uuid import UUID

import httpx

from app.core.config import settings
from app.integrations.telephony.base import BaseTelephonyCarrier

logger = logging.getLogger("solo.telephony.vobiz")


class VobizTelephonyCarrier(BaseTelephonyCarrier):
    """Vobiz REST call-control adapter."""

    async def initiate_call(
        self,
        *,
        candidate_phone: str,
        candidate_id: UUID,
        campaign_id: UUID,
        answer_url: str,
    ) -> str:
        auth_id = settings.VOBIZ_AUTH_ID
        auth_token = settings.VOBIZ_AUTH_TOKEN
        if not auth_id or not auth_token or not settings.VOBIZ_PHONE_NUMBER:
            raise RuntimeError(
                "VOBIZ_AUTH_ID, VOBIZ_AUTH_TOKEN, and VOBIZ_PHONE_NUMBER are required."
            )

        url = f"{settings.VOBIZ_API_BASE_URL.rstrip('/')}/Account/{auth_id}/Call/"
        payload = {
            "from": settings.VOBIZ_PHONE_NUMBER,
            "to": candidate_phone,
            "answer_url": answer_url,
            "answer_method": "POST",
            "metadata": {
                "candidate_id": str(candidate_id),
                "campaign_id": str(campaign_id),
            },
        }
        headers = {
            "X-Auth-ID": auth_id,
            "X-Auth-Token": auth_token,
            "Content-Type": "application/json",
        }

        async with httpx.AsyncClient(timeout=settings.VOBIZ_REQUEST_TIMEOUT_SECONDS) as client:
            response = await client.post(url, json=payload, headers=headers)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError:
                logger.error("Vobiz call creation failed with status %s", response.status_code)
                raise

        data = response.json()
        call_id = data.get("call_uuid") or data.get("request_uuid") or data.get("call_id")
        if not call_id:
            raise RuntimeError("Vobiz response did not contain a call identifier.")
        return str(call_id)
