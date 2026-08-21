from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from .agent import AgentContext


@dataclass(frozen=True)
class ModelResponse:
    text: str
    tool_calls: tuple[dict[str, Any], ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


class ModelProvider(Protocol):
    name: str

    async def generate(self, context: AgentContext, prompt: str, *, system: str = "") -> ModelResponse:
        """Generate a model response. Implementations must be provider-agnostic to callers."""
        ...


class MockModelProvider:
    name = "mock"

    def __init__(self, response: str = "MOCK_MODEL_RESPONSE") -> None:
        self.response = response

    async def generate(self, context: AgentContext, prompt: str, *, system: str = "") -> ModelResponse:
        return ModelResponse(text=self.response, metadata={"round": context.round.value})
