"""Shared LLM client — provider abstraction reused by C6 (analyst chat).

Three providers:

    llamacpp   — local GGUF inference via llama-cpp-python, no server required
    anthropic  — Anthropic Claude API
    openai     — any OpenAI-compatible remote API (OpenAI, Ollama, MLX, LM Studio, …)

Provider selection via environment variables:

    LLM_PROVIDER        llamacpp | anthropic | openai  (default: llamacpp)
    LLM_BASE_URL        base URL for openai provider  (default: http://localhost:8080/v1)
    LLM_API_KEY         API key — use "local" for self-hosted runtimes
    LLM_MODEL           model name / ID
    ANTHROPIC_API_KEY   required when LLM_PROVIDER=anthropic
    LLAMACPP_MODEL_PATH path to a local GGUF file  (takes priority)
    LLAMACPP_MODEL_REPO HuggingFace repo ID to download from (e.g. unsloth/gemma-4-E4B-it-GGUF)
    LLAMACPP_MODEL_FILE filename / glob within the repo   (default: *Q4_K_M*)
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
    """OpenAI-compatible /v1/chat/completions — works with OpenAI, Ollama, MLX LM, LM Studio, and any other compatible endpoint."""

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


class LlamaCppClient:
    """Local GGUF inference via llama-cpp-python.

    Model resolution (first match wins):
      1. LLAMACPP_MODEL_PATH  — path to a local .gguf file
      2. LLAMACPP_MODEL_REPO  — HuggingFace repo ID; downloads on first use
                                e.g. unsloth/gemma-4-E4B-it-GGUF
         LLAMACPP_MODEL_FILE  — filename within the repo (glob ok, e.g. *Q4_K_M*)

    Falls back gracefully if no model is configured or the package is not installed.
    """

    _instance: object = None  # lazy singleton — loaded once on first call

    def _get_model(self) -> object | None:
        if LlamaCppClient._instance is not None:
            return LlamaCppClient._instance
        try:
            from llama_cpp import Llama  # noqa: PLC0415
        except ImportError:
            return None

        model_path = os.getenv("LLAMACPP_MODEL_PATH", "")
        repo_id = os.getenv("LLAMACPP_MODEL_REPO", "")

        try:
            if model_path and os.path.exists(model_path):
                LlamaCppClient._instance = Llama(
                    model_path=model_path,
                    n_ctx=4096,
                    n_threads=os.cpu_count() or 4,
                    verbose=False,
                )
            elif repo_id:
                filename = os.getenv("LLAMACPP_MODEL_FILE", "*Q4_K_M*")
                LlamaCppClient._instance = Llama.from_pretrained(
                    repo_id=repo_id,
                    filename=filename,
                    n_ctx=4096,
                    n_threads=os.cpu_count() or 4,
                    verbose=False,
                )
            else:
                return None
        except Exception:
            return None

        return LlamaCppClient._instance

    async def chat(self, system: str, user: str) -> AsyncIterator[str]:
        async for token in self.stream_messages(system, [{"role": "user", "content": user}]):
            yield token

    async def stream_messages(self, system: str, messages: list[dict]) -> AsyncIterator[str]:
        import asyncio

        model = self._get_model()
        if model is None:
            yield "LLM not configured — set LLAMACPP_MODEL_PATH to a valid GGUF file."
            return

        full_messages = [{"role": "system", "content": system}] + messages

        # llama-cpp-python is synchronous; run in thread to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        queue: asyncio.Queue[str | None] = asyncio.Queue()

        def _run() -> None:
            try:
                response = model.create_chat_completion(  # type: ignore[attr-defined]
                    messages=full_messages,
                    stream=True,
                    max_tokens=512,
                    temperature=0.2,
                )
                for chunk in response:
                    delta = chunk["choices"][0]["delta"].get("content") or ""
                    if delta:
                        loop.call_soon_threadsafe(queue.put_nowait, delta)
            except Exception as exc:
                loop.call_soon_threadsafe(queue.put_nowait, f"[error: {exc}]")
            finally:
                loop.call_soon_threadsafe(queue.put_nowait, None)

        import threading

        threading.Thread(target=_run, daemon=True).start()

        while True:
            token = await queue.get()
            if token is None:
                break
            yield token


def get_llm_client() -> LLMClient:
    """Construct the appropriate LLMClient from environment variables."""
    provider = os.getenv("LLM_PROVIDER", "llamacpp")
    if provider == "llamacpp":
        return LlamaCppClient()
    if provider == "anthropic":
        return AnthropicClient(
            api_key=os.getenv("LLM_API_KEY", os.getenv("ANTHROPIC_API_KEY", "")),
            model=os.getenv("LLM_MODEL", "claude-haiku-4-5-20251001"),
        )
    # "openai" or any unrecognised value — OpenAI-compatible remote API
    return OpenAICompatClient(
        base_url=os.getenv("LLM_BASE_URL", "http://localhost:8080/v1"),
        api_key=os.getenv("LLM_API_KEY", "local"),
        model=os.getenv("LLM_MODEL", "gpt-4o-mini"),
    )
