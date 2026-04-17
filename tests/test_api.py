"""Tests for the FastAPI dashboard endpoints."""

from __future__ import annotations

import json

import polars as pl
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def watchlist_parquet(tmp_path):
    """Write a minimal candidate_watchlist.parquet and return its path."""
    df = pl.DataFrame(
        {
            "mmsi": ["123456789", "987654321"],
            "vessel_name": ["OCEAN GLORY", "DARK STAR"],
            "vessel_type": ["Tanker", "Bulk Carrier"],
            "flag": ["KH", "PW"],
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


@pytest.fixture
def validation_json(tmp_path):
    path = tmp_path / "validation_metrics.json"
    path.write_text(json.dumps({"precision_at_50": 0.72, "recall_at_200": 0.55, "auroc": 0.88}))
    return str(path)


@pytest.fixture
def client(watchlist_parquet, validation_json, monkeypatch, tmp_path):
    monkeypatch.setenv("WATCHLIST_OUTPUT_PATH", watchlist_parquet)
    monkeypatch.setenv("VALIDATION_METRICS_PATH", validation_json)
    monkeypatch.setenv("CAUSAL_EFFECTS_PATH", str(tmp_path / "nonexistent_causal.parquet"))

    # Re-import after env vars are set so module-level constants pick them up
    import importlib

    import pipeline.src.api.routes.alerts as alerts_mod
    import pipeline.src.api.routes.vessels as vessels_mod

    importlib.reload(vessels_mod)
    importlib.reload(alerts_mod)

    from pipeline.src.api.main import create_app

    return TestClient(create_app())


def test_index_returns_html(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    assert "MPOL" in r.text


def test_geojson_all(client):
    r = client.get("/api/vessels/geojson")
    assert r.status_code == 200
    body = r.json()
    assert body["type"] == "FeatureCollection"
    assert len(body["features"]) == 2


def test_geojson_min_confidence(client):
    r = client.get("/api/vessels/geojson?min_confidence=0.75")
    body = r.json()
    assert len(body["features"]) == 1
    assert body["features"][0]["properties"]["mmsi"] == "123456789"


def test_geojson_vessel_type_filter(client):
    r = client.get("/api/vessels/geojson?vessel_types=Tanker")
    body = r.json()
    assert len(body["features"]) == 1
    assert body["features"][0]["properties"]["vessel_type"] == "Tanker"


def test_watchlist_top_html(client):
    r = client.get("/api/watchlist/top?min_confidence=0.0&top_n=10")
    assert r.status_code == 200
    assert "watchlist-row" in r.text
    assert "OCEAN GLORY" in r.text
    assert "review-tier" in r.text
    assert "review-handoff" in r.text
    assert "Review</button>" in r.text


def test_watchlist_top_filtered(client):
    r = client.get("/api/watchlist/top?min_confidence=0.75&top_n=10")
    assert "OCEAN GLORY" in r.text
    assert "DARK STAR" not in r.text


def test_metrics_endpoint(client):
    r = client.get("/api/metrics")
    assert r.status_code == 200
    body = r.json()
    assert body["available"] is True
    assert abs(body["precision_at_50"] - 0.72) < 0.001
    assert abs(body["auroc"] - 0.88) < 0.001


def test_vessel_types_endpoint(client):
    r = client.get("/api/vessel-types")
    assert r.status_code == 200
    types = r.json()
    assert "Tanker" in types
    assert "Bulk Carrier" in types


def test_no_watchlist_returns_empty(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHLIST_OUTPUT_PATH", str(tmp_path / "nonexistent.parquet"))
    monkeypatch.setenv("VALIDATION_METRICS_PATH", str(tmp_path / "nonexistent.json"))

    import importlib

    import pipeline.src.api.routes.vessels as vessels_mod

    importlib.reload(vessels_mod)

    from pipeline.src.api.main import create_app

    c = TestClient(create_app())

    r = c.get("/api/vessels/geojson")
    assert r.json()["features"] == []

    r = c.get("/api/metrics")
    assert r.json()["available"] is False


# ── /api/causal-effects ────────────────────────────────────────────────────


@pytest.fixture
def causal_effects_parquet(tmp_path):
    """Write a minimal causal_effects.parquet and return its path."""
    df = pl.DataFrame(
        {
            "regime": ["OFAC Iran", "OFAC Russia", "UN DPRK"],
            "n_treated": [18, 32, 11],
            "n_control": [142, 180, 95],
            "att_estimate": [0.42, 0.15, -0.05],
            "att_ci_lower": [0.31, -0.02, -0.18],
            "att_ci_upper": [0.53, 0.32, 0.08],
            "p_value": [0.0003, 0.09, 0.45],
            "is_significant": [True, False, False],
            "calibrated_weight": [0.55, 0.40, 0.40],
        }
    )
    path = str(tmp_path / "causal_effects.parquet")
    df.write_parquet(path)
    return path


@pytest.fixture
def client_with_causal(watchlist_parquet, validation_json, causal_effects_parquet, monkeypatch):
    monkeypatch.setenv("WATCHLIST_OUTPUT_PATH", watchlist_parquet)
    monkeypatch.setenv("VALIDATION_METRICS_PATH", validation_json)
    monkeypatch.setenv("CAUSAL_EFFECTS_PATH", causal_effects_parquet)

    import importlib

    import pipeline.src.api.routes.alerts as alerts_mod
    import pipeline.src.api.routes.vessels as vessels_mod

    importlib.reload(vessels_mod)
    importlib.reload(alerts_mod)

    from pipeline.src.api.main import create_app

    return TestClient(create_app())


def test_causal_effects_available(client_with_causal):
    r = client_with_causal.get("/api/causal-effects")
    assert r.status_code == 200
    body = r.json()
    assert body["available"] is True
    assert len(body["regimes"]) == 3
    regimes = {re["regime"]: re for re in body["regimes"]}
    assert regimes["OFAC Iran"]["is_significant"] is True
    assert abs(regimes["OFAC Iran"]["att_estimate"] - 0.42) < 0.001
    assert abs(regimes["OFAC Iran"]["att_ci_lower"] - 0.31) < 0.001
    assert abs(regimes["OFAC Iran"]["att_ci_upper"] - 0.53) < 0.001
    assert regimes["OFAC Russia"]["is_significant"] is False
    assert regimes["UN DPRK"]["is_significant"] is False


def test_causal_effects_unavailable(client):
    # client fixture has no CAUSAL_EFFECTS_PATH set → file won't exist
    r = client.get("/api/causal-effects")
    assert r.status_code == 200
    body = r.json()
    assert body["available"] is False
    assert body["regimes"] == []


# ── /api/vessels/{mmsi}/dispatch-brief ────────────────────────────────────


def test_dispatch_brief_returns_identity(client):
    r = client.get("/api/vessels/123456789/dispatch-brief")
    assert r.status_code == 200
    body = r.json()
    assert body["mmsi"] == "123456789"
    assert body["identity"]["vessel_name"] == "OCEAN GLORY"
    assert body["identity"]["flag"] == "KH"
    assert body["identity"]["confidence_tier"] == "High"
    assert body["identity"]["confidence"] > 0.8
    assert isinstance(body["signals"], list)
    assert isinstance(body["ais_history"], list)
    assert isinstance(body["ownership_chain"], list)
    assert "generated_at" in body


def test_dispatch_brief_causal_from_effects(client_with_causal):
    r = client_with_causal.get("/api/vessels/123456789/dispatch-brief")
    assert r.status_code == 200
    body = r.json()
    # Should pick OFAC Iran (only significant regime)
    assert body["causal"] is not None
    assert body["causal"]["regime"] == "OFAC Iran"
    assert body["causal"]["is_significant"] is True


def test_dispatch_brief_not_found(client):
    r = client.get("/api/vessels/000000000/dispatch-brief")
    assert r.status_code == 404
