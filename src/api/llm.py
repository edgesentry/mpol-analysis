"""Shared LLM client — provider abstraction reused by C6 (analyst chat).

Supports any OpenAI-compatible local runtime (MLX LM, LM Studio, Ollama, Jan,
llamafile, Gemini) and Anthropic Claude via a thin httpx wrapper.

Provider selection via environment variables:

    LLM_PROVIDER   mlx | lmstudio | ollama | gemini | anthropic  (default: mlx)
    LLM_BASE_URL   base URL for OpenAI-compat providers
    LLM_API_KEY    API key (use "local" for local runtimes)
    LLM_MODEL      model name or ID
    ANTHROPIC_API_KEY  required when LLM_PROVIDER=anthropic
"""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from typing import Protocol, runtime_checkable


@runtime_checkable
class LLMClient(Protocol):
    def chat(self, system: str, user: str) -> AsyncIterator[str]:
        """Stream response tokens for a system + user message pair."""
        ...

    def stream_messages(self, system: str, messages: list[dict]) -> AsyncIterator[str]:
        """Stream response tokens for a multi-turn conversation."""
        ...


class OpenAICompatClient:
    """OpenAI-compatible /v1/chat/completions — MLX LM, LM Studio, Ollama, Jan, llamafile, Gemini."""

    def __init__(self, base_url: str, api_key: str, model: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._model = model

    async def chat(self, system: str, user: str) -> AsyncIterator[str]:
        async for token in self.stream_messages(system, [{"role": "user", "content": user}]):
            yield token

    async def stream_messages(self, system: str, messages: list[dict]) -> AsyncIterator[str]:
        import httpx

        url = f"{self._base_url}/chat/completions"
        payload = {
            "model": self._model,
            "stream": True,
            "messages": [{"role": "system", "content": system}] + messages,
        }
        async with httpx.AsyncClient(timeout=120) as client:
            async with client.stream(
                "POST",
                url,
                json=payload,
                headers={"Authorization": f"Bearer {self._api_key}"},
            ) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    chunk = line[6:].strip()
                    if chunk == "[DONE]":
                        return
                    try:
                        data = json.loads(chunk)
                        delta = data["choices"][0]["delta"].get("content") or ""
                        if delta:
                            yield delta
                    except Exception:
                        continue


class AnthropicClient:
    """Thin httpx wrapper around Anthropic /v1/messages with streaming."""

    def __init__(self, api_key: str, model: str) -> None:
        self._api_key = api_key
        self._model = model

    async def chat(self, system: str, user: str) -> AsyncIterator[str]:
        async for token in self.stream_messages(system, [{"role": "user", "content": user}]):
            yield token

    async def stream_messages(self, system: str, messages: list[dict]) -> AsyncIterator[str]:
        import httpx

        payload = {
            "model": self._model,
            "max_tokens": 1024,
            "system": system,
            "messages": messages,
            "stream": True,
        }
        async with httpx.AsyncClient(timeout=120) as client:
            async with client.stream(
                "POST",
                "https://api.anthropic.com/v1/messages",
                json=payload,
                headers={
                    "x-api-key": self._api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
            ) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    chunk = line[6:].strip()
                    try:
                        data = json.loads(chunk)
                        if data.get("type") == "content_block_delta":
                            text = data.get("delta", {}).get("text") or ""
                            if text:
                                yield text
                    except Exception:
                        continue


def get_llm_client() -> LLMClient:
    """Construct the appropriate LLMClient from environment variables."""
    provider = os.getenv("LLM_PROVIDER", "mlx")
    if provider == "anthropic":
        return AnthropicClient(
            api_key=os.getenv("LLM_API_KEY", os.getenv("ANTHROPIC_API_KEY", "")),
            model=os.getenv("LLM_MODEL", "claude-haiku-4-5-20251001"),
        )
    return OpenAICompatClient(
        base_url=os.getenv("LLM_BASE_URL", "http://localhost:8080/v1"),
        api_key=os.getenv("LLM_API_KEY", "local"),
        model=os.getenv("LLM_MODEL", "mlx-community/Llama-3.2-3B-Instruct-4bit"),
    )
