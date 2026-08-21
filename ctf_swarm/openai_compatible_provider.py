from __future__ import annotations

import os
from typing import Any

import httpx

from .agent import AgentContext
from .model_provider import ModelResponse


class OpenAICompatibleProvider:
    """Provider for vLLM/SGLang/other OpenAI-compatible inference servers."""

    def __init__(
        self,
        model: str,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: float = 300.0,
    ) -> None:
        self.name = model
        self.model = model
        self.base_url = (base_url or os.getenv("MODEL_BASE_URL", "http://127.0.0.1:8001/v1")).rstrip("/")
        self.api_key = api_key or os.getenv("MODEL_API_KEY", "EMPTY")
        self.timeout = timeout

    @staticmethod
    def _system_context(context: AgentContext) -> str:
        dead = "\n".join(f"- {x}" for x in sorted(context.dead_ends)) or "- none"
        evidence = "\n".join(
            f"- [{item.agent_id}] {item.claim} (confidence={item.confidence:.2f})"
            for item in context.evidence
        ) or "- none"
        return (
            f"You are a CTF security research agent. Current round: {context.round.value}.\n"
            "Do not repeat known dead ends. Prefer reproducible evidence over speculation.\n\n"
            f"Known dead ends:\n{dead}\n\nShared evidence:\n{evidence}"
        )

    async def generate(self, context: AgentContext, prompt: str, *, system: str = "") -> ModelResponse:
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        body: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system or self._system_context(context)},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.2,
        }
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(f"{self.base_url}/chat/completions", json=body, headers=headers)
            response.raise_for_status()
            data = response.json()

        choice = data["choices"][0]
        message = choice.get("message", {})
        return ModelResponse(
            text=str(message.get("content", "")),
            tool_calls=tuple(message.get("tool_calls") or ()),
            metadata={"model": self.model, "usage": data.get("usage", {})},
        )
