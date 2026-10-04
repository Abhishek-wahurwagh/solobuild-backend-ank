from __future__ import annotations

from typing import Any


async def run_vobiz_session(websocket: Any, *, system_prompt: str) -> None:
    """Run one Vobiz media WebSocket through the Pipecat pipeline."""
    from app.core.config import settings

    try:
        from pipecat.audio.vad.silero import SileroVADAnalyzer
        from pipecat.pipeline.pipeline import Pipeline
        from pipecat.pipeline.task import PipelineParams, PipelineTask
        from pipecat.processors.aggregators.llm_context import LLMContext
        from pipecat.processors.aggregators.llm_response_universal import (
            LLMContextAggregatorPair,
            LLMUserAggregatorParams,
        )
        from pipecat.pipeline.runner import PipelineRunner
        from pipecat.serializers.vobiz import VobizFrameSerializer, parse_vobiz_start
        from pipecat.transports.websocket.fastapi import (
            FastAPIWebsocketParams,
            FastAPIWebsocketTransport,
        )
    except ImportError as exc:
        raise RuntimeError(
            "Pipecat runtime dependencies are required for Vobiz media sessions."
        ) from exc

    start = await parse_vobiz_start(websocket)
    encoding = start.get("encoding") or "audio/x-mulaw"
    sample_rate = start.get("sample_rate") or 8000
    serializer = VobizFrameSerializer(
        stream_id=start.get("stream_id", ""),
        call_id=start.get("call_id"),
        auth_id=settings.VOBIZ_AUTH_ID,
        auth_token=settings.VOBIZ_AUTH_TOKEN,
        params=VobizFrameSerializer.InputParams(
            encoding=encoding,
            vobiz_sample_rate=sample_rate,
        ),
    )
    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            add_wav_header=False,
            serializer=serializer,
        ),
    )

    from app.integrations.pipecat.llm.factory import PipecatLLMFactory
    from app.integrations.pipecat.stt.factory import PipecatSTTFactory
    from app.integrations.pipecat.tts.factory import PipecatTTSFactory

    stt = PipecatSTTFactory.build()
    llm = PipecatLLMFactory.build(system_prompt=system_prompt)
    tts = PipecatTTSFactory.build()
    context = LLMContext(messages=[{"role": "system", "content": system_prompt}])
    context_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(vad_analyzer=SileroVADAnalyzer()),
    )
    pipeline = Pipeline([
        transport.input(),
        stt,
        context_aggregator.user(),
        llm,
        tts,
        transport.output(),
        context_aggregator.assistant(),
    ])
    task = PipelineTask(
        pipeline,
        params=PipelineParams(audio_in_sample_rate=sample_rate, audio_out_sample_rate=sample_rate),
    )
    await PipelineRunner().run(task)
