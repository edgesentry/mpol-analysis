"""Tests for src/api/llm.py — LLM client provider abstraction."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.api.llm import AnthropicClient, OpenAICompatClient, get_llm_client


# ── helpers ────────────────────────────────────────────────────────────────

async def _async_iter(items):
    for item in items:
        yield item


def _make_openai_lines(tokens: list[str]) -> list[str]:
    lines = []
    for t in tokens:
        lines.append(f"data: {json.dumps({'choices': [{'delta': {'content': t}}]})}")
    lines.append("data: [DONE]")
    return lines


def _make_anthropic_lines(tokens: list[str]) -> list[str]:
    lines = []
    for t in tokens:
        lines.append(
            f"data: {json.dumps({'type': 'content_block_delta', 'delta': {'text': t}})}"
        )
    lines.append(f"data: {json.dumps({'type': 'message_stop'})}")
    return lines


def _mock_httpx_stream(lines: list[str]):
    """Build the nested AsyncMock that mimics httpx.AsyncClient().stream().__aenter__()."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.aiter_lines = MagicMock(return_value=_async_iter(lines))

    mock_stream_cm = AsyncMock()
    mock_stream_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_stream_cm.__aexit__ = AsyncMock(return_value=None)

    mock_client = MagicMock()
    mock_client.stream = MagicMock(return_value=mock_stream_cm)

    mock_client_cm = AsyncMock()
    mock_client_cm.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client_cm.__aexit__ = AsyncMock(return_value=None)

    return mock_client_cm


# ── OpenAICompatClient ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_openai_compat_streams_tokens():
    lines = _make_openai_lines(["Hello", " world"])
    mock_cm = _mock_httpx_stream(lines)

    with patch("httpx.AsyncClient", return_value=mock_cm):
        client = OpenAICompatClient("http://localhost:8080/v1", "local", "test-model")
        tokens = [t async for t in client.chat("sys", "usr")]

    assert tokens == ["Hello", " world"]


@pytest.mark.asyncio
async def test_openai_compat_skips_non_data_lines():
    lines = [
        ": heartbeat",
        "",
        _make_openai_lines(["token"])[0],
        "data: [DONE]",
    ]
    mock_cm = _mock_httpx_stream(lines)

    with patch("httpx.AsyncClient", return_value=mock_cm):
        client = OpenAICompatClient("http://localhost:8080/v1", "local", "m")
        tokens = [t async for t in client.chat("s", "u")]

    assert tokens == ["token"]


@pytest.mark.asyncio
async def test_openai_compat_skips_empty_delta():
    lines = [
        f"data: {json.dumps({'choices': [{'delta': {}}]})}",
        "data: [DONE]",
    ]
    mock_cm = _mock_httpx_stream(lines)

    with patch("httpx.AsyncClient", return_value=mock_cm):
        client = OpenAICompatClient("http://localhost:8080/v1", "local", "m")
        tokens = [t async for t in client.chat("s", "u")]

    assert tokens == []


# ── AnthropicClient ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_anthropic_client_streams_tokens():
    lines = _make_anthropic_lines(["Hello", " world"])
    mock_cm = _mock_httpx_stream(lines)

    with patch("httpx.AsyncClient", return_value=mock_cm):
        client = AnthropicClient("sk-test", "claude-haiku-4-5")
        tokens = [t async for t in client.chat("sys", "usr")]

    assert tokens == ["Hello", " world"]


@pytest.mark.asyncio
async def test_anthropic_client_ignores_non_delta_events():
    lines = [
        f"data: {json.dumps({'type': 'message_start', 'message': {}})}",
        f"data: {json.dumps({'type': 'content_block_start'})}",
        f"data: {json.dumps({'type': 'content_block_delta', 'delta': {'text': 'Hi'}})}",
        f"data: {json.dumps({'type': 'message_stop'})}",
    ]
    mock_cm = _mock_httpx_stream(lines)

    with patch("httpx.AsyncClient", return_value=mock_cm):
        client = AnthropicClient("sk-test", "claude-haiku-4-5")
        tokens = [t async for t in client.chat("s", "u")]

    assert tokens == ["Hi"]


# ── get_llm_client factory ─────────────────────────────────────────────────

def test_get_llm_client_default_is_openai_compat(monkeypatch):
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    client = get_llm_client()
    assert isinstance(client, OpenAICompatClient)


def test_get_llm_client_anthropic(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    monkeypatch.setenv("LLM_MODEL", "claude-haiku-4-5")
    client = get_llm_client()
    assert isinstance(client, AnthropicClient)


def test_get_llm_client_explicit_openai_compat(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "ollama")
    monkeypatch.setenv("LLM_BASE_URL", "http://localhost:11434/v1")
    monkeypatch.setenv("LLM_MODEL", "llama3.2:3b")
    client = get_llm_client()
    assert isinstance(client, OpenAICompatClient)
