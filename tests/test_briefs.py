"""Tests for /api/briefs endpoints and brief caching."""

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
            "mmsi": [
                "123456789",
                "987654321",
                "273456782",
                "613115678",
                "352123456",
                "538009876",
            ],
            "vessel_name": [
                "OCEAN GLORY",
                "DARK STAR",
                # Realistic shadow fleet candidates for LLM brief testing
                "PETROVSKY ZVEZDA",
                "SARI NOUR",
                "OCEAN VOYAGER",
                "VERA SUNSET",
            ],
            "vessel_type": ["Tanker", "Bulk Carrier", "Tanker", "Tanker", "Tanker", "Tanker"],
            "flag": ["KH", "PW", "RU", "CM", "PA", "MH"],
            "imo": [
                "IMO1234567",
                "IMO9876543",
                "IMO9234567",
                "IMO9345612",
                "IMO9456781",
                "IMO9678901",
            ],
            "confidence": [0.83, 0.45, 0.91, 0.87, 0.79, 0.72],
            "last_lat": [1.15, 1.30, 26.50, 29.10, 35.90, 25.10],
            "last_lon": [103.6, 104.0, 55.50, 50.30, -5.50, 56.40],
            "last_seen": [
                "2026-04-01T06:00:00",
                "2026-04-01T12:00:00",
                "2026-03-15T00:00:00",
                "2026-03-20T00:00:00",
                "2026-03-10T00:00:00",
                "2026-03-25T00:00:00",
            ],
            "top_signals": [
                json.dumps([{"feature": "ais_gap_count_30d", "value": 12, "contribution": 0.34}]),
                json.dumps([{"feature": "sanctions_distance", "value": 2, "contribution": 0.20}]),
                # PETROVSKY ZVEZDA: AIS dark ops in Hormuz, 1 hop from OFAC entity, reflagged twice
                json.dumps(
                    [
                        {"feature": "ais_gap_count_30d", "value": 14, "contribution": 0.38},
                        {"feature": "sanctions_distance", "value": 1, "contribution": 0.28},
                        {"feature": "flag_changes_2y", "value": 2, "contribution": 0.15},
                    ]
                ),
                # SARI NOUR: trades Kharg Island crude with no Comtrade record, 3 GPS spoofing jumps, IR→CM reflag
                json.dumps(
                    [
                        {"feature": "route_cargo_mismatch", "value": 1.0, "contribution": 0.42},
                        {"feature": "position_jump_count", "value": 3, "contribution": 0.25},
                        {"feature": "high_risk_flag_ratio", "value": 0.85, "contribution": 0.18},
                    ]
                ),
                # OCEAN VOYAGER: 6 STS partners off Ceuta, shared Piraeus address with 5 vessels (40% OFAC-listed)
                json.dumps(
                    [
                        {"feature": "sts_hub_degree", "value": 6, "contribution": 0.30},
                        {"feature": "shared_address_centrality", "value": 5, "contribution": 0.22},
                        {"feature": "cluster_sanctions_ratio", "value": 0.40, "contribution": 0.18},
                    ]
                ),
                # VERA SUNSET: 5-layer ownership chain, beneficial owner 2 hops from designated entity, renamed once
                json.dumps(
                    [
                        {"feature": "ownership_depth", "value": 5, "contribution": 0.28},
                        {"feature": "sanctions_distance", "value": 2, "contribution": 0.24},
                        {"feature": "name_changes_2y", "value": 1, "contribution": 0.12},
                    ]
                ),
            ],
        }
    )
    path = str(tmp_path / "candidate_watchlist.parquet")
    df.write_parquet(path)
    return path


async def _fake_chat(system: str, user: str):
    for token in ["Test ", "brief ", "text."]:
        yield token


@pytest.fixture
def client(watchlist_parquet, tmp_db, monkeypatch):
    monkeypatch.setenv("WATCHLIST_OUTPUT_PATH", watchlist_parquet)
    monkeypatch.setenv("DB_PATH", tmp_db)

    import importlib

    import pipeline.src.api.routes.alerts as alerts_mod
    import pipeline.src.api.routes.briefs as briefs_mod
    import pipeline.src.api.routes.vessels as vessels_mod

    importlib.reload(vessels_mod)
    importlib.reload(alerts_mod)
    importlib.reload(briefs_mod)

    from pipeline.src.api.main import create_app

    return TestClient(create_app())


# ── /api/briefs/{mmsi} ─────────────────────────────────────────────────────


def test_brief_streams_tokens(client, monkeypatch):
    mock_llm = MagicMock()
    mock_llm.chat = _fake_chat

    with (
        patch("pipeline.src.api.routes.briefs.get_llm_client", return_value=mock_llm),
        patch("pipeline.src.api.routes.briefs.query_gdelt_context", return_value=[]),
    ):
        resp = client.get("/api/briefs/123456789")

    assert resp.status_code == 200
    assert "text/event-stream" in resp.headers["content-type"]

    lines = [l for l in resp.text.split("\n") if l.startswith("data: ")]
    tokens = [l[6:] for l in lines if l[6:] != "[DONE]"]
    assert "".join(tokens) == "Test brief text."
    assert any(l == "data: [DONE]" for l in lines)


def test_brief_unknown_vessel_returns_not_found(client):
    with patch("pipeline.src.api.routes.briefs.query_gdelt_context", return_value=[]):
        resp = client.get("/api/briefs/000000000")

    assert resp.status_code == 200
    assert "not found" in resp.text.lower()


def test_brief_includes_gdelt_events_in_prompt(client, monkeypatch):
    gdelt_events = [
        {
            "event_date": "20260401",
            "actor1_name": "Cambodia",
            "actor2_name": "Iran",
            "action_geo": "South China Sea",
            "source_url": "http://example.com/news",
        }
    ]
    captured_system = []

    async def _capture_chat(system: str, user: str):
        captured_system.append(system)
        yield "brief."

    mock_llm = MagicMock()
    mock_llm.chat = _capture_chat

    with (
        patch("pipeline.src.api.routes.briefs.get_llm_client", return_value=mock_llm),
        patch("pipeline.src.api.routes.briefs.query_gdelt_context", return_value=gdelt_events),
    ):
        client.get("/api/briefs/123456789")

    assert captured_system, "chat() was never called"
    assert "Cambodia" in captured_system[0]
    assert "South China Sea" in captured_system[0]


# ── /api/briefs/{mmsi}/cached ──────────────────────────────────────────────


def test_cached_brief_not_available_initially(client):
    resp = client.get("/api/briefs/123456789/cached")
    assert resp.status_code == 200
    assert resp.json()["available"] is False


def test_cached_brief_available_after_streaming(client, monkeypatch):
    mock_llm = MagicMock()
    mock_llm.chat = _fake_chat

    with (
        patch("pipeline.src.api.routes.briefs.get_llm_client", return_value=mock_llm),
        patch("pipeline.src.api.routes.briefs.query_gdelt_context", return_value=[]),
    ):
        client.get("/api/briefs/123456789")  # generate and cache

    resp = client.get("/api/briefs/123456789/cached")
    body = resp.json()
    assert body["available"] is True
    assert body["brief"] == "Test brief text."


def test_cached_brief_served_on_second_request(client, monkeypatch):
    mock_llm = MagicMock()
    mock_llm.chat = _fake_chat
    call_count = 0

    async def _counting_chat(system: str, user: str):
        nonlocal call_count
        call_count += 1
        yield "cached."

    mock_llm.chat = _counting_chat

    with (
        patch("pipeline.src.api.routes.briefs.get_llm_client", return_value=mock_llm),
        patch("pipeline.src.api.routes.briefs.query_gdelt_context", return_value=[]),
    ):
        client.get("/api/briefs/123456789")  # first — calls LLM
        client.get("/api/briefs/123456789")  # second — should use cache

    assert call_count == 1, "LLM called more than once; caching is broken"


# ── GDELT context formatting ───────────────────────────────────────────────


def test_brief_with_no_gdelt_still_generates(client):
    mock_llm = MagicMock()
    mock_llm.chat = _fake_chat

    with (
        patch("pipeline.src.api.routes.briefs.get_llm_client", return_value=mock_llm),
        patch("pipeline.src.api.routes.briefs.query_gdelt_context", return_value=[]),
    ):
        resp = client.get("/api/briefs/123456789")

    assert resp.status_code == 200
    lines = [l for l in resp.text.split("\n") if l.startswith("data: ")]
    assert any(l != "data: [DONE]" for l in lines)


# ── /api/briefs/{mmsi}/dispatch ────────────────────────────────────────────


def test_dispatch_brief_streams_and_caches(client):
    mock_llm = MagicMock()
    mock_llm.chat = _fake_chat

    with patch("pipeline.src.api.routes.briefs.get_llm_client", return_value=mock_llm):
        # 1. Initially not cached
        resp = client.get("/api/briefs/123456789/dispatch/cached")
        assert resp.json()["available"] is False

        # 2. Stream and cache
        resp = client.get("/api/briefs/123456789/dispatch")
        assert resp.status_code == 200
        # Check for tokens in SSE stream
        assert "data: Test " in resp.text
        assert "data: brief " in resp.text
        assert "data: text." in resp.text

        # 3. Now it should be cached
        resp = client.get("/api/briefs/123456789/dispatch/cached")
        body = resp.json()
        assert body["available"] is True
        assert body["brief"] == "Test brief text."


def test_dispatch_brief_served_from_cache(client):
    mock_llm = MagicMock()
    call_count = 0

    async def _counting_chat(system: str, user: str):
        nonlocal call_count
        call_count += 1
        yield "dispatch cached."

    mock_llm.chat = _counting_chat

    with patch("pipeline.src.api.routes.briefs.get_llm_client", return_value=mock_llm):
        client.get("/api/briefs/123456789/dispatch")  # call 1: LLM
        client.get("/api/briefs/123456789/dispatch")  # call 2: Cache

    assert call_count == 1
