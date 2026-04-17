from __future__ import annotations

import importlib

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def review_client(tmp_path, monkeypatch):
    db_path = tmp_path / "reviews.duckdb"
    monkeypatch.setenv("DB_PATH", str(db_path))

    import pipeline.src.api.main as main_mod
    import pipeline.src.api.routes.reviews as reviews_mod

    importlib.reload(reviews_mod)
    importlib.reload(main_mod)

    with TestClient(main_mod.create_app()) as client:
        yield client


def test_create_and_get_review(review_client: TestClient) -> None:
    payload = {
        "mmsi": "123456789",
        "review_tier": "Probable",
        "handoff_state": "handoff_recommended",
        "rationale": "Repeated AIS gaps near known STS corridor.",
        "reviewed_by": "analyst-1",
        "evidence_refs": [
            {
                "source": "ofac_notice",
                "url": "https://example.test/ofac/1",
                "published_at": "2026-03-01",
            }
        ],
    }

    r = review_client.post("/api/reviews", json=payload)
    assert r.status_code == 200
    body = r.json()
    assert body["mmsi"] == "123456789"
    assert body["review_tier"] == "probable"
    assert body["handoff_state"] == "handoff_recommended"
    assert body["reviewed_by"] == "analyst-1"
    assert len(body["evidence_refs"]) == 1

    g = review_client.get("/api/reviews/123456789")
    assert g.status_code == 200
    got = g.json()
    assert got["review_tier"] == "probable"
    assert got["rationale"].startswith("Repeated AIS gaps")


def test_export_latest_reviews(review_client: TestClient) -> None:
    review_client.post(
        "/api/reviews",
        json={
            "mmsi": "111111111",
            "review_tier": "suspect",
            "handoff_state": "in_review",
            "rationale": "Initial triage.",
        },
    )
    review_client.post(
        "/api/reviews",
        json={
            "mmsi": "111111111",
            "review_tier": "cleared",
            "handoff_state": "closed",
            "rationale": "No corroborating evidence.",
        },
    )

    r = review_client.get("/api/reviews/export")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] >= 1
    target = [x for x in body["items"] if x["mmsi"] == "111111111"]
    assert len(target) == 1
    assert target[0]["review_tier"] == "cleared"


def test_invalid_tier_rejected(review_client: TestClient) -> None:
    r = review_client.post(
        "/api/reviews",
        json={
            "mmsi": "222222222",
            "review_tier": "high-risk",
            "handoff_state": "in_review",
            "rationale": "bad tier",
        },
    )
    assert r.status_code == 400
    assert "Invalid review_tier" in r.json()["detail"]


def test_get_missing_review_returns_404(review_client: TestClient) -> None:
    r = review_client.get("/api/reviews/999999999")
    assert r.status_code == 404


def test_review_history_returns_all_records(review_client: TestClient) -> None:
    review_client.post(
        "/api/reviews",
        json={
            "mmsi": "555555555",
            "review_tier": "suspect",
            "handoff_state": "in_review",
            "rationale": "Initial suspicion.",
            "reviewed_by": "analyst-a",
        },
    )
    review_client.post(
        "/api/reviews",
        json={
            "mmsi": "555555555",
            "review_tier": "confirmed",
            "handoff_state": "handoff_recommended",
            "rationale": "Escalated with corroborating signals.",
            "reviewed_by": "analyst-b",
        },
    )

    r = review_client.get("/api/reviews/555555555/history?limit=10")
    assert r.status_code == 200
    body = r.json()
    assert body["mmsi"] == "555555555"
    assert body["count"] == 2
    assert len(body["items"]) == 2
    assert body["items"][0]["review_tier"] == "confirmed"
    assert body["items"][1]["review_tier"] == "suspect"
