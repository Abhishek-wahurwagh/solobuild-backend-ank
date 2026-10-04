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
from app.domains.telephony.xml import build_vobiz_answer_xml
from app.integrations.pipecat.conversation import run_conversation
from app.integrations.telephony.vobiz_media import create_vobiz_audio_session

router = APIRouter(prefix="/vobiz")
logger = logging.getLogger("solo.telephony.vobiz")


def _public_url(path: str, *, websocket: bool = False) -> str:
    base_url = settings.APP_BASE_URL.rstrip("/")
    if websocket:
        base_url = base_url.replace("https://", "wss://", 1).replace(
            "http://", "ws://", 1
        )
    return f"{base_url}{path}"


def build_vobiz_recording_url() -> str:
    return _public_url(settings.VOBIZ_RECORDING_PATH)


def build_vobiz_media_url(call_id: str) -> str:
    return _public_url(f"{settings.VOBIZ_MEDIA_PATH}/{call_id}", websocket=True)


@router.post("/answer", response_class=Response, status_code=status.HTTP_200_OK)
async def vobiz_answer_endpoint(request: Request) -> Response:
    """Return Vobiz instructions that connect the call to the media adapter."""
    form = await request.form()
    query = request.query_params
    call_id = form.get("CallUUID") or form.get("RequestUUID") or query.get("call_id")
    if not call_id:
        raise HTTPException(status_code=400, detail="Vobiz call ID is missing.")

    call_data = await get_call_status(str(call_id))
    if not (call_data and call_data.get("system_prompt")):
        campaign_id = query.get("campaign_id")
        if campaign_id:
            try:
                campaign_uuid = UUID(campaign_id)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="Invalid campaign ID.") from exc

            async with AsyncSessionLocal() as db:
                campaign = await db.get(Campaign, campaign_uuid)
            if campaign:
                await set_call_status(
                    str(call_id),
                    status="INITIATED",
                    candidate_id=query.get("candidate_id"),
                    campaign_id=campaign_id,
                    system_prompt=build_system_prompt(
                        required_fields=campaign.required_fields,
                        raw_text=campaign.raw_text,
                    ),
                )

    return Response(
        content=build_vobiz_answer_xml(
            media_url=build_vobiz_media_url(str(call_id)),
            recording_url=build_vobiz_recording_url(),
        ),
        media_type="application/xml",
    )


@router.websocket("/media/{call_id}")
async def vobiz_media_endpoint(websocket: WebSocket, call_id: str) -> None:
    call_data = await get_call_status(call_id)
    if not call_data or not call_data.get("system_prompt"):
        await websocket.close(code=4404, reason="Call session not found.")
        return

    await websocket.accept()
    candidate_id = call_data.get("candidate_id")
    campaign_id = call_data.get("campaign_id")
    try:
        session = await create_vobiz_audio_session(websocket, call_id=call_id)
        transcript = await run_conversation(
            session,
            system_prompt=call_data["system_prompt"],
            opening_message=(
                "Begin the candidate screening interview. Greet the candidate, "
                "introduce yourself, and ask the first relevant question."
            ),
        )
    except Exception:
        logger.exception("Pipecat conversation failed for call %s.", call_id)
        if candidate_id and campaign_id:
            try:
                await handle_call_completion(
                    CallCompletionWebhook(
                        call_id=call_id,
                        candidate_id=UUID(candidate_id),
                        campaign_id=UUID(campaign_id),
                        status="failed",
                    )
                )
            except Exception:
                logger.exception(
                    "Failed to enqueue cleanup for Pipecat call %s.", call_id
                )
        raise

    if candidate_id and campaign_id:
        await handle_call_completion(
            CallCompletionWebhook(
                call_id=call_id,
                candidate_id=UUID(candidate_id),
                campaign_id=UUID(campaign_id),
                status="completed",
                transcript=transcript,
            )
        )


@router.post("/recording-ready", status_code=status.HTTP_200_OK)
async def vobiz_recording_ready_endpoint(request: Request) -> dict[str, str]:
    body = await request.body()
    signature_header = (
        request.headers.get(settings.TELEPHONY_WEBHOOK_HEADER)
        or request.headers.get("X-Webhook-Signature")
        or request.headers.get("X-Vobiz-Signature")
    )
    if not verify_webhook_signature(body, signature_header):
        raise HTTPException(status_code=401, detail="Invalid webhook signature.")
    return {"status": "accepted"}
