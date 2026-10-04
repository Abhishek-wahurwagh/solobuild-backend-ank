from __future__ import annotations

import hashlib
import hmac
from typing import Any

from app.core.config import settings
from app.core.redis import get_redis_client
from app.domains.telephony.schemas import CallCompletionWebhook, CallInitiationRequest, CallInitiationResponse
from app.integrations.telephony.factory import TelephonyCarrierFactory


def _public_url(path: str, *, websocket: bool = False) -> str:
    base_url = settings.APP_BASE_URL.rstrip("/")
    if websocket:
        base_url = base_url.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    return f"{base_url}{path}"


def build_vobiz_answer_url(candidate_id: str, campaign_id: str) -> str:
    return (
        f"{_public_url(settings.VOBIZ_ANSWER_PATH)}"
        f"?candidate_id={candidate_id}&campaign_id={campaign_id}"
    )


def build_vobiz_recording_url() -> str:
    return _public_url(settings.VOBIZ_RECORDING_PATH)


def build_vobiz_media_url(call_id: str) -> str:
    return _public_url(f"{settings.VOBIZ_MEDIA_PATH}/{call_id}", websocket=True)


def build_system_prompt(required_fields: dict[str, Any] | None = None, raw_text: str | None = None) -> str:
    requirements = required_fields or {}
    fields_summary = "\n".join(f"- {key}: {value}" for key, value in requirements.items()) if requirements else "- No explicit requirements provided."
    context = raw_text or "No additional campaign context provided."
    return (
        "Objective: Conduct an interview with the candidate.\n"
        "Requirements:\n"
        f"{fields_summary}\n"
        "Context:\n"
        f"{context}"
    )


def _get_webhook_secret() -> str:
    return (
        getattr(settings, "TELEPHONY_WEBHOOK_SECRET", "")
        or getattr(settings, "VOBIZ_WEBHOOK_SECRET", "")
        or ""
    )


def verify_webhook_signature(payload: bytes, signature: str | None) -> bool:
    """Fail closed when a shared secret is not configured."""
    secret = _get_webhook_secret()
    if not secret:
        return False
    if not signature:
        return False

    expected = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.strip())


async def set_call_status(
    call_id: str,
    *,
    status: str,
    candidate_id: str | None = None,
    campaign_id: str | None = None,
    transcript: str | None = None,
    recording_url: str | None = None,
    system_prompt: str | None = None,
) -> dict[str, Any]:
    redis = await get_redis_client()
    try:
        mapping: dict[str, Any] = {
            "call_id": call_id,
            "status": status,
        }
        if candidate_id is not None:
            mapping["candidate_id"] = candidate_id
        if campaign_id is not None:
            mapping["campaign_id"] = campaign_id
        if transcript is not None:
            mapping["transcript"] = transcript
        if recording_url is not None:
            mapping["recording_url"] = recording_url
        if system_prompt is not None:
            mapping["system_prompt"] = system_prompt

        await redis.hset(f"call_status:{call_id}", mapping=mapping)  # type:ignore
        await redis.expire(f"call_status:{call_id}", 7 * 24 * 60 * 60)
        return mapping
    finally:
        await redis.aclose()


async def get_call_status(call_id: str) -> dict[str, Any] | None:
    redis = await get_redis_client()
    try:
        data = await redis.hgetall(f"call_status:{call_id}") # type: ignore
        if not data:
            return None
        return dict(data)
    finally:
        await redis.aclose()


async def initiate_outbound_call(request: CallInitiationRequest) -> CallInitiationResponse:
    carrier = TelephonyCarrierFactory.build()
    system_prompt = request.system_prompt or build_system_prompt(
        required_fields=request.required_fields,
        raw_text=request.raw_text,
    )

    call_id = await carrier.initiate_call(
        candidate_phone=request.candidate_phone,
        candidate_id=request.candidate_id,
        campaign_id=request.campaign_id,
        answer_url=build_vobiz_answer_url(str(request.candidate_id), str(request.campaign_id)),
    )

    await set_call_status(
        call_id,
        status="INITIATED",
        candidate_id=str(request.candidate_id),
        campaign_id=str(request.campaign_id),
        system_prompt=system_prompt,
    )

    return CallInitiationResponse(
        call_id=call_id,
        status="SUCCESS",
        carrier=carrier.__class__.__name__,
    )


async def handle_call_completion(webhook: CallCompletionWebhook) -> dict[str, Any]:
    """Persist callback state and enqueue durable post-call processing."""
    result = {
        "call_id": webhook.call_id,
        "candidate_id": str(webhook.candidate_id),
        "campaign_id": str(webhook.campaign_id),
        "status": webhook.status,
        "transcript": webhook.transcript,
        "recording_url": webhook.recording_url,
    }
    await set_call_status(
        webhook.call_id,
        status=webhook.status,
        candidate_id=str(webhook.candidate_id),
        campaign_id=str(webhook.campaign_id),
        transcript=webhook.transcript,
        recording_url=webhook.recording_url,
    )

    from arq import create_pool
    from arq.connections import RedisSettings

    pool = await create_pool(RedisSettings.from_dsn(settings.REDIS_URL))
    try:
        await pool.enqueue_job(
            "process_call_completion_job",
            payload=result,
            _job_id=f"call-completion:{webhook.call_id}",
        )
    finally:
        await pool.aclose()

    return result
