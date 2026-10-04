from __future__ import annotations

from dataclasses import dataclass

from pipecat.transports.base_transport import BaseTransport


@dataclass(frozen=True, slots=True)
class AudioSession:
    """Carrier-neutral audio transport and normalized PCM rates for a session."""

    session_id: str
    transport: BaseTransport
    input_sample_rate: int = 16000
    output_sample_rate: int = 24000
