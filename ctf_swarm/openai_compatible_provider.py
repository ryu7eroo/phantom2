from __future__ import annotations

import os
from typing import Any

import httpx

from .agent import AgentContext
from .model_provider import ModelResponse


class OpenAICompatibleProvider:
    """Provider for vLLM/SGLang/llama.cpp OpenAI-compatible servers."""

    def __init__(
        self,
        model: str,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: float = 300.0,
        max_tokens: int = 4096,
    ) -> None:
        self.name = model
        self.model = model
        self.base_url = (base_url or os.getenv("MODEL_BASE_URL", "http://127.0.0.1:8001/v1")).rstrip("/")
        self.api_key = api_key or os.getenv("MODEL_API_KEY", "EMPTY")
        self.timeout = timeout
        self.max_tokens = max_tokens
        self.thinking_policy = os.getenv("MODEL_THINKING_POLICY", "round")

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

    def _thinking_directive(self, context: AgentContext) -> str:
        if self.thinking_policy == "always":
            return "/think"
        if self.thinking_policy == "never":
            return "/no_think"
        if context.round.value in {"divergent", "adversarial"}:
            return "/think"
        return "/no_think"

    async def generate(self, context: AgentContext, prompt: str, *, system: str = "") -> ModelResponse:
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        directive = self._thinking_directive(context)
        user_prompt = f"{prompt}\n\n{directive}"
        body: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system or self._system_context(context)},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.6 if directive == "/think" else 0.2,
            "top_p": 0.95 if directive == "/think" else 0.9,
            "max_tokens": self.max_tokens,
        }
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(f"{self.base_url}/chat/completions", json=body, headers=headers)
            response.raise_for_status()
            data = response.json()

        choice = data["choices"][0]
        message = choice.get("message", {})
        content = str(message.get("content") or "")
        reasoning = str(message.get("reasoning_content") or "")
        return ModelResponse(
            text=content,
            tool_calls=tuple(message.get("tool_calls") or ()),
            metadata={
                "model": self.model,
                "usage": data.get("usage", {}),
                "reasoning_content": reasoning,
                "finish_reason": choice.get("finish_reason"),
                "thinking_directive": directive,
            },
        )
