from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response, WebSocket, status

from app.core.database import AsyncSessionLocal
from app.core.config import settings
from app.domains.campaigns.models import Campaign
from app.domains.telephony.schemas import CallCompletionWebhook, CallInitiationRequest, CallInitiationResponse, CallStatusResponse
from app.domains.telephony.service import (
    build_system_prompt,
    build_vobiz_media_url,
    build_vobiz_recording_url,
    get_call_status,
    handle_call_completion,
    initiate_outbound_call,
    set_call_status,
    verify_webhook_signature,
)
from app.domains.telephony.xml import build_vobiz_answer_xml
from app.integrations.pipecat.runtime import run_vobiz_session

router = APIRouter(prefix="/telephony")


@router.post("/calls/initiate", response_model=CallInitiationResponse, status_code=status.HTTP_200_OK)
async def initiate_call_endpoint(request: CallInitiationRequest):
    return await initiate_outbound_call(request)


@router.get("/calls/{call_id}/status", response_model=CallStatusResponse, status_code=status.HTTP_200_OK)
async def call_status_endpoint(call_id: str):
    data = await get_call_status(call_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Call status not found.")
    return CallStatusResponse(**data)


@router.post("/vobiz/answer", response_class=Response, status_code=status.HTTP_200_OK)
async def vobiz_answer_endpoint(request: Request):
    """Return Vobiz XML that starts recording and the Pipecat media stream."""
    form = await request.form()
    query = request.query_params
    call_id = (
        form.get("CallUUID")
        or form.get("RequestUUID")
        or query.get("call_id")
    )
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
                system_prompt = build_system_prompt(
                    required_fields=campaign.required_fields,
                    raw_text=campaign.raw_text,
                )
                await set_call_status(
                    str(call_id),
                    status="INITIATED",
                    candidate_id=query.get("candidate_id"),
                    campaign_id=campaign_id,
                    system_prompt=system_prompt,
                )

    xml = build_vobiz_answer_xml(
        media_url=build_vobiz_media_url(str(call_id)),
        recording_url=build_vobiz_recording_url(),
    )
    return Response(content=xml, media_type="application/xml")


@router.websocket("/vobiz/media/{call_id}")
async def vobiz_media_endpoint(websocket: WebSocket, call_id: str):
    call_data = await get_call_status(call_id)
    system_prompt = call_data.get("system_prompt") if call_data else None
    if not system_prompt:
        system_prompt = "Conduct a structured candidate screening interview."

    await websocket.accept()
    try:
        await run_vobiz_session(
            websocket,
            system_prompt=system_prompt,
        )
    finally:
        await websocket.close()


@router.post("/vobiz/recording-ready", status_code=status.HTTP_200_OK)
async def vobiz_recording_ready_endpoint(request: Request):
    """Accept Vobiz recording metadata for asynchronous post-call processing."""
    body = await request.body()
    signature_header = (
        request.headers.get(getattr(settings, "TELEPHONY_WEBHOOK_HEADER", "X-Telephony-Signature"))
        or request.headers.get("X-Webhook-Signature")
        or request.headers.get("X-Vobiz-Signature")
    )
    if not verify_webhook_signature(body, signature_header):
        raise HTTPException(status_code=401, detail="Invalid webhook signature.")
    return {"status": "accepted"}


@router.post("/webhooks/call-completed", status_code=status.HTTP_200_OK)
async def call_completed_webhook_endpoint(request: Request):
    body = await request.body()
    signature_header = (
        request.headers.get(getattr(settings, "TELEPHONY_WEBHOOK_HEADER", "X-Telephony-Signature"))
        or request.headers.get("X-Webhook-Signature")
        or request.headers.get("X-Vobiz-Signature")
    )

    if not verify_webhook_signature(body, signature_header):
        raise HTTPException(status_code=401, detail="Invalid webhook signature.")

    payload = CallCompletionWebhook.model_validate_json(body.decode("utf-8"))
    return await handle_call_completion(payload)
