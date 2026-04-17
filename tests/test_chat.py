"""Tests for POST /api/chat — interactive analyst chat with caching."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fastapi.testclient import TestClient

# ── fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture
def watchlist_parquet(tmp_path):
    df = pl.DataFrame(
        {
            "mmsi": ["123456789", "987654321"],
            "vessel_name": ["OCEAN GLORY", "DARK STAR"],
            "vessel_type": ["Tanker", "Bulk Carrier"],
            "flag": ["KH", "PW"],
            "imo": ["IMO1234567", "IMO9876543"],
            "confidence": [0.83, 0.45],
            "last_lat": [1.15, 1.30],
            "last_lon": [103.6, 104.0],
            "last_seen": ["2026-04-01T06:00:00", "2026-04-01T12:00:00"],
            "top_signals": [
                json.dumps([{"feature": "ais_gap_count_30d", "value": 12, "contribution": 0.34}]),
                json.dumps([{"feature": "sanctions_distance", "value": 2, "contribution": 0.20}]),
            ],
        }
    )
    path = str(tmp_path / "candidate_watchlist.parquet")
    df.write_parquet(path)
    return path


async def _fake_stream(system: str, messages: list[dict]):
    for token in ["Analysis ", "complete."]:
        yield token


@pytest.fixture
def client(watchlist_parquet, tmp_db, monkeypatch):
    monkeypatch.setenv("WATCHLIST_OUTPUT_PATH", watchlist_parquet)
    monkeypatch.setenv("DB_PATH", tmp_db)

    import importlib

    import pipeline.src.api.routes.alerts as alerts_mod
    import pipeline.src.api.routes.briefs as briefs_mod
    import pipeline.src.api.routes.chat as chat_mod
    import pipeline.src.api.routes.vessels as vessels_mod

    importlib.reload(vessels_mod)
    importlib.reload(alerts_mod)
    importlib.reload(briefs_mod)
    importlib.reload(chat_mod)

    from pipeline.src.api.main import create_app

    return TestClient(create_app())


# ── POST /api/chat — vessel-specific ──────────────────────────────────────


def test_chat_vessel_streams_tokens(client):
    mock_llm = MagicMock()
    mock_llm.stream_messages = _fake_stream

    with (
        patch("pipeline.src.api.routes.chat.get_llm_client", return_value=mock_llm),
        patch("pipeline.src.api.routes.chat.query_gdelt_context", return_value=[]),
        patch("pipeline.src.api.routes.chat._query_graph_ownership", return_value="No graph."),
    ):
        resp = client.post(
            "/api/chat", json={"message": "Why is this vessel flagged?", "mmsi": "123456789"}
        )

    assert resp.status_code == 200
    assert "text/event-stream" in resp.headers["content-type"]
    lines = [l for l in resp.text.split("\n") if l.startswith("data: ")]
    tokens = [l[6:] for l in lines if l[6:] != "[DONE]"]
    assert "".join(tokens) == "Analysis complete."
    assert any(l == "data: [DONE]" for l in lines)


def test_chat_vessel_context_in_system_prompt(client):
    captured: list[str] = []

    async def _capturing_stream(system: str, messages: list[dict]):
        captured.append(system)
        yield "ok."

    mock_llm = MagicMock()
    mock_llm.stream_messages = _capturing_stream

    gdelt_events = [
        {
            "event_date": "20260401",
            "actor1_name": "Cambodia",
            "actor2_name": "Iran",
            "action_geo": "SCS",
            "source_url": "http://x.com",
        }
    ]

    with (
        patch("pipeline.src.api.routes.chat.get_llm_client", return_value=mock_llm),
        patch("pipeline.src.api.routes.chat.query_gdelt_context", return_value=gdelt_events),
        patch(
            "pipeline.src.api.routes.chat._query_graph_ownership",
            return_value="Owner Corp (Panama)",
        ),
    ):
        client.post("/api/chat", json={"message": "Explain risk.", "mmsi": "123456789"})

    assert captured, "stream_messages never called"
    system = captured[0]
    assert "OCEAN GLORY" in system
    assert "123456789" in system
    assert "ais_gap_count_30d" in system
    assert "Cambodia" in system
    assert "Owner Corp" in system


# ── POST /api/chat — cross-vessel (no mmsi) ───────────────────────────────


def test_chat_cross_vessel_no_mmsi(client):
    mock_llm = MagicMock()
    mock_llm.stream_messages = _fake_stream

    with (
        patch("pipeline.src.api.routes.chat.get_llm_client", return_value=mock_llm),
        patch("pipeline.src.api.routes.chat.query_gdelt_context", return_value=[]),
    ):
        resp = client.post(
            "/api/chat", json={"message": "Which vessels share the same owner network?"}
        )

    assert resp.status_code == 200
    lines = [l for l in resp.text.split("\n") if l.startswith("data: ")]
    assert any(l != "data: [DONE]" for l in lines)


def test_chat_cross_vessel_fleet_overview_in_prompt(client):
    captured: list[str] = []

    async def _cap(system: str, messages: list[dict]):
        captured.append(system)
        yield "ok."

    mock_llm = MagicMock()
    mock_llm.stream_messages = _cap

    with (
        patch("pipeline.src.api.routes.chat.get_llm_client", return_value=mock_llm),
        patch("pipeline.src.api.routes.chat.query_gdelt_context", return_value=[]),
    ):
        client.post("/api/chat", json={"message": "Cross vessel question"})

    assert captured
    assert "OCEAN GLORY" in captured[0]
    assert "DARK STAR" in captured[0]


# ── response caching ──────────────────────────────────────────────────────


def test_chat_response_cached_after_first_call(client, tmp_db):
    call_count = 0

    async def _counting_stream(system: str, messages: list[dict]):
        nonlocal call_count
        call_count += 1
        yield "cached-answer."

    mock_llm = MagicMock()
    mock_llm.stream_messages = _counting_stream

    with (
        patch("pipeline.src.api.routes.chat.get_llm_client", return_value=mock_llm),
        patch("pipeline.src.api.routes.chat.query_gdelt_context", return_value=[]),
        patch("pipeline.src.api.routes.chat._query_graph_ownership", return_value="none"),
    ):
        client.post("/api/chat", json={"message": "Why flagged?", "mmsi": "123456789"})
        client.post("/api/chat", json={"message": "Why flagged?", "mmsi": "123456789"})

    assert call_count == 1, "LLM called more than once; caching is broken"


def test_chat_different_questions_not_shared_cache(client):
    call_count = 0

    async def _counting_stream(system: str, messages: list[dict]):
        nonlocal call_count
        call_count += 1
        yield f"answer-{call_count}."

    mock_llm = MagicMock()
    mock_llm.stream_messages = _counting_stream

    with (
        patch("pipeline.src.api.routes.chat.get_llm_client", return_value=mock_llm),
        patch("pipeline.src.api.routes.chat.query_gdelt_context", return_value=[]),
        patch("pipeline.src.api.routes.chat._query_graph_ownership", return_value="none"),
    ):
        client.post("/api/chat", json={"message": "Question A", "mmsi": "123456789"})
        client.post("/api/chat", json={"message": "Question B", "mmsi": "123456789"})

    assert call_count == 2


# ── unknown vessel ────────────────────────────────────────────────────────


def test_chat_unknown_mmsi_falls_back_to_fleet_context(client):
    captured: list[str] = []

    async def _cap(system: str, messages: list[dict]):
        captured.append(system)
        yield "ok."

    mock_llm = MagicMock()
    mock_llm.stream_messages = _cap

    with (
        patch("pipeline.src.api.routes.chat.get_llm_client", return_value=mock_llm),
        patch("pipeline.src.api.routes.chat.query_gdelt_context", return_value=[]),
    ):
        resp = client.post("/api/chat", json={"message": "Any info?", "mmsi": "000000000"})

    assert resp.status_code == 200
    # Unknown MMSI → no vessel section, but fleet overview still present
    assert captured
    assert "OCEAN GLORY" in captured[0]
    assert "000000000" not in captured[0]


# ── multi-turn history ────────────────────────────────────────────────────


def test_chat_history_forwarded_to_llm(client):
    forwarded_messages: list[list[dict]] = []

    async def _cap(system: str, messages: list[dict]):
        forwarded_messages.append(messages)
        yield "ok."

    mock_llm = MagicMock()
    mock_llm.stream_messages = _cap

    history = [
        {"role": "user", "content": "First question"},
        {"role": "assistant", "content": "First answer"},
    ]

    with (
        patch("pipeline.src.api.routes.chat.get_llm_client", return_value=mock_llm),
        patch("pipeline.src.api.routes.chat.query_gdelt_context", return_value=[]),
        patch("pipeline.src.api.routes.chat._query_graph_ownership", return_value="none"),
    ):
        client.post(
            "/api/chat",
            json={
                "message": "Follow-up",
                "mmsi": "123456789",
                "history": history,
            },
        )

    assert forwarded_messages
    msgs = forwarded_messages[0]
    roles = [m["role"] for m in msgs]
    assert "user" in roles
    assert "assistant" in roles
    assert msgs[-1]["content"] == "Follow-up"
