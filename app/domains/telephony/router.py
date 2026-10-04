from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.core.config import settings
from app.domains.auth.dependencies import get_current_user
from app.domains.users.models import User
from app.domains.telephony.schemas import (
    CallCompletionWebhook,
    CallInitiationRequest,
    CallInitiationResponse,
    CallStatusResponse,
)
from app.domains.telephony.service import (
    get_call_status,
    handle_call_completion,
    initiate_outbound_call,
    verify_webhook_signature,
)
from app.integrations.telephony.vobiz_routes import (
    build_vobiz_media_url,
    build_vobiz_recording_url,
    router as vobiz_router,
    vobiz_answer_endpoint,
)

router = APIRouter(prefix="/telephony")
router.include_router(vobiz_router)


@router.post("/calls/initiate", response_model=CallInitiationResponse, status_code=status.HTTP_200_OK)
async def initiate_call_endpoint(
    request: CallInitiationRequest,
    current_user: User = Depends(get_current_user),
):
    return await initiate_outbound_call(request)


@router.get("/calls/{call_id}/status", response_model=CallStatusResponse, status_code=status.HTTP_200_OK)
async def call_status_endpoint(
    call_id: str,
    current_user: User = Depends(get_current_user),
):
    data = await get_call_status(call_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Call status not found.")
    return CallStatusResponse(**data)


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
