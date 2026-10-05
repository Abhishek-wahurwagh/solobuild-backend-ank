"""
from __future__ import annotations

import logging
import re
from uuid import UUID

import httpx

from app.core.config import settings
from app.integrations.telephony.base import BaseTelephonyCarrier

logger = logging.getLogger("solo.telephony.twilio")


def _normalize_e164(phone: str) -> str:
    Normalize a phone number to E.164 (a leading '+' followed by digits only).

    #Twilio rejects numbers containing spaces, dashes, or parentheses and, on
    #trial accounts, can only match a `To` number against Verified Caller IDs
    #when it is in strict E.164 form.  Candidate phones pulled from resumes/CSVs
    #frequently carry formatting like '+91 95295 12911', so we strip everything
    #except digits and the leading plus before sending.
    
    if not phone:
        return phone
    cleaned = re.sub(r"[^\d+]", "", phone)
    # Collapse any stray internal '+' and keep only a leading one.
    if cleaned.startswith("+"):
        return "+" + cleaned[1:].lstrip("+") or "+"
    return "+" + cleaned


class TwilioTelephonyCarrier(BaseTelephonyCarrier):
    #Twilio REST call-control adapter

    def build_answer_url(self, *, candidate_id: UUID, campaign_id: UUID) -> str:
        from urllib.parse import urlencode

        base_url = settings.APP_BASE_URL.rstrip("/")
        answer_path = settings.TWILIO_ANSWER_PATH
        return (
            f"{base_url}{answer_path}?"
            f"{urlencode({'candidate_id': str(candidate_id), 'campaign_id': str(campaign_id)})}"
        )

    async def initiate_call(
        self,
        *,
        candidate_phone: str,
        candidate_id: UUID,
        campaign_id: UUID,
        answer_url: str,
    ) -> str:
        account_sid = settings.TWILIO_ACCOUNT_SID
        auth_token = settings.TWILIO_AUTH_TOKEN
        from_number = settings.TWILIO_PHONE_NUMBER

        if not account_sid or not auth_token or not from_number:
            raise RuntimeError(
                "TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, and TWILIO_PHONE_NUMBER are required."
            )

        # Twilio requires the `To` number in strict E.164.  Candidate phones
        # ingested from resumes/CSVs often contain spaces or dashes, which
        # prevent Twilio (especially on trial accounts) from matching the
        # number against Verified Caller IDs.
        to_number = _normalize_e164(candidate_phone)
        if not to_number or to_number == "+":
            raise RuntimeError(f"Candidate phone is not a valid phone number: {candidate_phone!r}")

        # Twilio REST API: POST /2010-04-01/Accounts/{AccountSid}/Calls.json
        url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Calls.json"
        payload = {
            "From": from_number,
            "To": to_number,
            "Url": answer_url,
            "Method": "POST",
        }

        async with httpx.AsyncClient(timeout=settings.TWILIO_REQUEST_TIMEOUT_SECONDS) as client:
            response = await client.post(url, data=payload, auth=(account_sid, auth_token))
            if response.status_code >= 400:
                # Surface Twilio's error message directly for easier debugging
                try:
                    twilio_error = response.json()
                    message = twilio_error.get("message", response.text)
                except Exception:
                    message = response.text
                raise RuntimeError(
                    f"Twilio call creation failed (HTTP {response.status_code}): {message}"
                )

        data = response.json()
        call_sid = data.get("sid")
        if not call_sid:
            raise RuntimeError("Twilio response did not contain a call SID.")

        logger.info(
            "Twilio call initiated: sid=%s to=%s campaign=%s candidate=%s",
            call_sid,
            candidate_phone,
            campaign_id,
            candidate_id,
        )
        return str(call_sid)
"""