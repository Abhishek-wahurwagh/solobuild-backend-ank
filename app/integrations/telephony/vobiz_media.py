from __future__ import annotations

from typing import Any

from app.integrations.pipecat.contracts import AudioSession

GEMINI_AUDIO_INPUT_RATE = 16000
GEMINI_AUDIO_OUTPUT_RATE = 24000


async def create_vobiz_audio_session(websocket: Any, *, call_id: str) -> AudioSession:
    """Adapt Vobiz's WebSocket protocol to the carrier-neutral Pipecat session."""
    from app.core.config import settings

    try:
        from pipecat.serializers.vobiz import VobizFrameSerializer, parse_vobiz_start
        from pipecat.transports.websocket.fastapi import (
            FastAPIWebsocketParams,
            FastAPIWebsocketTransport,
        )
    except ImportError as exc:
        raise RuntimeError("The Pipecat Vobiz serializer is required.") from exc

    start = await parse_vobiz_start(websocket)
    if not start.get("stream_id"):
        raise ValueError("Vobiz media start event is missing stream_id.")

    wire_sample_rate = start.get("sample_rate") or 8000
    encoding = start.get("encoding") or "audio/x-mulaw"
    serializer = VobizFrameSerializer(
        stream_id=start["stream_id"],
        call_id=start.get("call_id") or call_id,
        auth_id=settings.VOBIZ_AUTH_ID,
        auth_token=settings.VOBIZ_AUTH_TOKEN,
        params=VobizFrameSerializer.InputParams(
            encoding=encoding,
            vobiz_sample_rate=wire_sample_rate,
            sample_rate=GEMINI_AUDIO_INPUT_RATE,
            hangup_method="ws_stop",
        ),
    )
    # Transport operates at the Vobiz wire rate (8000 Hz μ-law).
    # The serializer handles conversion between wire rate and Gemini's internal rates.
    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_in_sample_rate=wire_sample_rate,
            audio_out_sample_rate=wire_sample_rate,
            add_wav_header=False,
            serializer=serializer,
        ),
    )
    return AudioSession(
        session_id=str(start.get("call_id") or call_id),
        transport=transport,
        input_sample_rate=GEMINI_AUDIO_INPUT_RATE,
        output_sample_rate=GEMINI_AUDIO_OUTPUT_RATE,
    )
