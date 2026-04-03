"""Tests for the FastAPI dashboard endpoints."""

from __future__ import annotations

import json
import os

import polars as pl
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def watchlist_parquet(tmp_path):
    """Write a minimal candidate_watchlist.parquet and return its path."""
    df = pl.DataFrame({
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
    })
    path = str(tmp_path / "candidate_watchlist.parquet")
    df.write_parquet(path)
    return path


@pytest.fixture
def validation_json(tmp_path):
    path = tmp_path / "validation_metrics.json"
    path.write_text(json.dumps({"precision_at_50": 0.72, "recall_at_200": 0.55, "auroc": 0.88}))
    return str(path)


@pytest.fixture
def client(watchlist_parquet, validation_json, monkeypatch):
    monkeypatch.setenv("WATCHLIST_OUTPUT_PATH", watchlist_parquet)
    monkeypatch.setenv("VALIDATION_METRICS_PATH", validation_json)

    # Re-import after env vars are set so module-level constants pick them up
    import importlib
    import src.api.routes.vessels as vessels_mod
    import src.api.routes.alerts as alerts_mod
    importlib.reload(vessels_mod)
    importlib.reload(alerts_mod)

    from src.api.main import create_app
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
    import src.api.routes.vessels as vessels_mod
    importlib.reload(vessels_mod)

    from src.api.main import create_app
    c = TestClient(create_app())

    r = c.get("/api/vessels/geojson")
    assert r.json()["features"] == []

    r = c.get("/api/metrics")
    assert r.json()["available"] is False
