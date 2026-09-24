from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class PipecatLLMFactory(ABC):
    """Factory contract for constructing the LLM service used by Pipecat."""

    @abstractmethod
    def build(self, *, system_prompt: str) -> Any:
        raise NotImplementedError
