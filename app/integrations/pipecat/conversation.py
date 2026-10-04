from __future__ import annotations

from typing import Any

from app.core.config import settings
from app.integrations.pipecat.contracts import AudioSession


def build_realtime_service(*, system_prompt: str) -> Any:
    provider = settings.PIPECAT_VOICE_PROVIDER.strip().lower()
    if provider != "gemini_live":
        raise ValueError(f"Unsupported Pipecat voice provider: {provider}")
    if not settings.PIPECAT_GEMINI_API_KEY:
        raise RuntimeError("PIPECAT_GEMINI_API_KEY is required for Pipecat Gemini Live.")

    try:
        from pipecat.services.google.gemini_live.llm import GeminiLiveLLMService
    except ImportError as exc:
        raise RuntimeError(
            "Gemini Live requires the Pipecat Google extra and google-genai."
        ) from exc

    return GeminiLiveLLMService(
        api_key=settings.PIPECAT_GEMINI_API_KEY,
        settings=GeminiLiveLLMService.Settings(
            model=settings.PIPECAT_GEMINI_LIVE_MODEL,
            system_instruction=system_prompt,
            voice=settings.PIPECAT_GEMINI_LIVE_VOICE,
            language=settings.PIPECAT_GEMINI_LIVE_LANGUAGE,
        ),
    )


async def run_conversation(
    session: AudioSession,
    *,
    system_prompt: str,
    opening_message: str,
) -> str:
    """Run a Gemini Live conversation over a carrier-provided Pipecat transport."""
    try:
        from pipecat.frames.frames import LLMRunFrame
        from pipecat.pipeline.pipeline import Pipeline
        from pipecat.pipeline.worker import PipelineParams, PipelineWorker
        from pipecat.processors.aggregators.llm_context import LLMContext
        from pipecat.processors.aggregators.llm_response_universal import (
            AssistantTurnStoppedMessage,
            LLMContextAggregatorPair,
            UserTurnMessageAddedMessage,
        )
        from pipecat.workers.runner import WorkerRunner
    except ImportError as exc:
        raise RuntimeError("Pipecat runtime dependencies are required.") from exc

    service = build_realtime_service(system_prompt=system_prompt)
    context = LLMContext()
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(context)
    transcript: list[str] = []
    pipeline = Pipeline(
        [
            session.transport.input(),
            user_aggregator,
            service,
            session.transport.output(),
            assistant_aggregator,
        ]
    )
    worker = PipelineWorker(
        pipeline,
        params=PipelineParams(
            audio_in_sample_rate=session.input_sample_rate,
            audio_out_sample_rate=session.output_sample_rate,
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
        idle_timeout_secs=None,
    )
    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)

    @session.transport.event_handler("on_client_connected")
    async def on_client_connected(transport: Any, client: Any) -> None:
        context.add_message({"role": "developer", "content": opening_message})
        await worker.queue_frames([LLMRunFrame()])

    @session.transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport: Any, client: Any) -> None:
        await runner.cancel()

    @user_aggregator.event_handler("on_user_turn_message_added")
    async def on_user_turn_message_added(
        aggregator: Any,
        message: UserTurnMessageAddedMessage,
    ) -> None:
        transcript.append(f"candidate: {message.content}")

    @assistant_aggregator.event_handler("on_assistant_turn_stopped")
    async def on_assistant_turn_stopped(
        aggregator: Any,
        message: AssistantTurnStoppedMessage,
    ) -> None:
        transcript.append(f"assistant: {message.content}")

    await runner.run()
    return "\n".join(transcript)
