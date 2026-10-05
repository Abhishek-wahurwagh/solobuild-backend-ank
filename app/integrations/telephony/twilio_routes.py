"""
from __future__ import annotations

import logging
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response, WebSocket, status

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.domains.campaigns.models import Campaign
from app.domains.telephony.schemas import CallCompletionWebhook
from app.domains.telephony.service import (
    build_system_prompt,
    get_call_status,
    handle_call_completion,
    set_call_status,
    verify_webhook_signature,
)
from app.domains.telephony.xml import build_twilio_answer_twiml
from app.integrations.pipecat.conversation import run_conversation
from app.integrations.telephony.twilio_media import create_twilio_audio_session

router = APIRouter(prefix="/twilio")
logger = logging.getLogger("solo.telephony.twilio")


def _public_url(path: str, *, websocket: bool = False) -> str:
    base_url = settings.APP_BASE_URL.rstrip("/")
    if websocket:
        base_url = base_url.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    return f"{base_url}{path}"


def build_twilio_media_url(call_sid: str) -> str:
    return _public_url(f"{settings.TWILIO_MEDIA_PATH}/{call_sid}", websocket=True)


def build_twilio_recording_url() -> str:
    return _public_url(settings.TWILIO_RECORDING_PATH)


@router.post("/answer", response_class=Response, status_code=status.HTTP_200_OK)
async def twilio_answer_endpoint(request: Request) -> Response:
    #Return TwiML that connects the answered call to our media WebSocket.

    #Twilio POSTs form-encoded data when a call is answered.  The call SID is
    #in the ``CallSid`` field; candidate/campaign context comes from the query
    #string we embedded in the answer URL.
    
    form = await request.form()
    query = request.query_params

    call_sid = str(form.get("CallSid") or query.get("call_sid") or "")
    if not call_sid:
        raise HTTPException(status_code=400, detail="Twilio CallSid is missing.")

    # Populate Redis state if it isn't already there (race-free path: the
    # initiate_call service writes it before Twilio dials, so this is a fallback).
    call_data = await get_call_status(call_sid)
    if not (call_data and call_data.get("system_prompt")):
        campaign_id_str = query.get("campaign_id")
        if campaign_id_str:
            try:
                campaign_uuid = UUID(campaign_id_str)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="Invalid campaign_id.") from exc

            async with AsyncSessionLocal() as db:
                campaign = await db.get(Campaign, campaign_uuid)
            if campaign:
                await set_call_status(
                    call_sid,
                    status="INITIATED",
                    candidate_id=query.get("candidate_id"),
                    campaign_id=campaign_id_str,
                    system_prompt=build_system_prompt(
                        required_fields=campaign.required_fields,
                        raw_text=campaign.raw_text,
                    ),
                )

    twiml = build_twilio_answer_twiml(
        media_url=build_twilio_media_url(call_sid),
        recording_url=build_twilio_recording_url(),
    )
    return Response(content=twiml, media_type="application/xml")


@router.websocket("/media/{call_sid}")
async def twilio_media_endpoint(websocket: WebSocket, call_sid: str) -> None:
    #Bidirectional audio WebSocket used by Twilio Media Streams
    call_data = await get_call_status(call_sid)
    if not call_data or not call_data.get("system_prompt"):
        await websocket.close(code=4404, reason="Call session not found.")
        return

    await websocket.accept()

    candidate_id = call_data.get("candidate_id")
    campaign_id = call_data.get("campaign_id")

    try:
        session = await create_twilio_audio_session(websocket, call_sid=call_sid)
        transcript = await run_conversation(
            session,
            system_prompt=call_data["you are a ai calling agent and you need to perform an interview of the candidate on call"],
            opening_message=(
                "Begin the candidate screening interview. Greet the candidate, "
                "introduce yourself, and ask the first relevant question."
            ),
        )
    except Exception:
        logger.exception("Pipecat conversation failed for Twilio call %s.", call_sid)
        if candidate_id and campaign_id:
            try:
                await handle_call_completion(
                    CallCompletionWebhook(
                        call_id=call_sid,
                        candidate_id=UUID(candidate_id),
                        campaign_id=UUID(campaign_id),
                        status="failed",
                    )
                )
            except Exception:
                logger.exception(
                    "Failed to enqueue cleanup for Twilio call %s.", call_sid
                )
        raise

    if candidate_id and campaign_id:
        await handle_call_completion(
            CallCompletionWebhook(
                call_id=call_sid,
                candidate_id=UUID(candidate_id),
                campaign_id=UUID(campaign_id),
                status="completed",
                transcript=transcript,
            )
        )


@router.post("/recording-ready", status_code=status.HTTP_200_OK)
async def twilio_recording_ready_endpoint(request: Request) -> dict[str, str]:
    #Webhook called by Twilio when a recording is available
    body = await request.body()
    # Twilio signs webhooks with X-Twilio-Signature; we delegate to the shared
    # verify_webhook_signature helper which reads TELEPHONY_WEBHOOK_SECRET.
    signature_header = (
        request.headers.get("X-Twilio-Signature")
        or request.headers.get(settings.TELEPHONY_WEBHOOK_HEADER)
    )
    if not verify_webhook_signature(body, signature_header):
        raise HTTPException(status_code=401, detail="Invalid webhook signature.")
    return {"status": "accepted"}
"""