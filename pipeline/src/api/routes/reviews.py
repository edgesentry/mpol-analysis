"""Review decision endpoints for tiered human-in-the-loop workflow."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from pipeline.src.api.db import get_conn

router = APIRouter()

ALLOWED_TIERS = {"confirmed", "probable", "suspect", "cleared", "inconclusive"}
ALLOWED_HANDOFF_STATES = {
    "queued_review",
    "in_review",
    "handoff_recommended",
    "handoff_accepted",
    "handoff_completed",
    "closed",
}


class EvidenceRef(BaseModel):
    source: str = Field(min_length=1)
    url: str = Field(default="")
    published_at: str = Field(default="")


class ReviewUpsertRequest(BaseModel):
    mmsi: str = Field(min_length=1)
    review_tier: str = Field(min_length=1)
    handoff_state: str = Field(default="queued_review")
    rationale: str = Field(default="")
    reviewed_by: str = Field(default="")
    evidence_refs: list[EvidenceRef] = Field(default_factory=list)


class ReviewResponse(BaseModel):
    mmsi: str
    review_tier: str
    handoff_state: str
    rationale: str
    reviewed_by: str
    reviewed_at: str
    evidence_refs: list[EvidenceRef]


def _normalize_tier(tier: str) -> str:
    value = tier.strip().lower()
    if value not in ALLOWED_TIERS:
        raise HTTPException(status_code=400, detail=f"Invalid review_tier: {tier}")
    return value


def _normalize_handoff_state(state: str) -> str:
    value = state.strip().lower()
    if value not in ALLOWED_HANDOFF_STATES:
        raise HTTPException(status_code=400, detail=f"Invalid handoff_state: {state}")
    return value


def _row_to_response(row: dict[str, Any]) -> ReviewResponse:
    refs_raw = row.get("evidence_refs_json") or "[]"
    try:
        refs = json.loads(refs_raw)
    except json.JSONDecodeError:
        refs = []

    return ReviewResponse(
        mmsi=str(row.get("mmsi", "")),
        review_tier=str(row.get("review_tier", "")),
        handoff_state=str(row.get("handoff_state", "")),
        rationale=str(row.get("rationale") or ""),
        reviewed_by=str(row.get("reviewed_by") or ""),
        reviewed_at=str(row.get("reviewed_at") or ""),
        evidence_refs=[EvidenceRef(**r) for r in refs if isinstance(r, dict)],
    )


@router.post("/api/reviews", response_model=ReviewResponse)
def create_review(payload: ReviewUpsertRequest) -> ReviewResponse:
    tier = _normalize_tier(payload.review_tier)
    handoff_state = _normalize_handoff_state(payload.handoff_state)
    evidence_json = json.dumps([r.model_dump() for r in payload.evidence_refs])

    with get_conn() as con:
        if con is None:
            raise HTTPException(status_code=503, detail="Database unavailable")
        con.execute(
            """
            INSERT INTO vessel_reviews (mmsi, review_tier, handoff_state, rationale, evidence_refs_json, reviewed_by)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                payload.mmsi.strip(),
                tier,
                handoff_state,
                payload.rationale,
                evidence_json,
                payload.reviewed_by,
            ],
        )

        row = (
            con.execute(
                """
            SELECT mmsi, review_tier, handoff_state, rationale, evidence_refs_json, reviewed_by, reviewed_at
            FROM vessel_reviews
            WHERE mmsi = ?
            ORDER BY reviewed_at DESC
            LIMIT 1
            """,
                [payload.mmsi.strip()],
            )
            .fetchdf()
            .to_dict("records")[0]
        )
        return _row_to_response(row)


@router.get("/api/reviews/export")
def export_latest_reviews() -> dict[str, Any]:
    with get_conn() as con:
        if con is None:
            return {"generated_at_utc": datetime.now(UTC).isoformat(), "count": 0, "items": []}
        rows = (
            con.execute(
                """
            SELECT mmsi, review_tier, handoff_state, rationale, evidence_refs_json, reviewed_by, reviewed_at
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY reviewed_at DESC) AS rn
                FROM vessel_reviews
            )
            WHERE rn = 1
            ORDER BY reviewed_at DESC
            """
            )
            .fetchdf()
            .to_dict("records")
        )
    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "count": len(rows),
        "items": [_row_to_response(r).model_dump() for r in rows],
    }


@router.get("/api/reviews/{mmsi}", response_model=ReviewResponse)
def get_latest_review(mmsi: str) -> ReviewResponse:
    with get_conn() as con:
        if con is None:
            raise HTTPException(status_code=503, detail="Database unavailable")
        rows = (
            con.execute(
                """
            SELECT mmsi, review_tier, handoff_state, rationale, evidence_refs_json, reviewed_by, reviewed_at
            FROM vessel_reviews
            WHERE mmsi = ?
            ORDER BY reviewed_at DESC
            LIMIT 1
            """,
                [mmsi.strip()],
            )
            .fetchdf()
            .to_dict("records")
        )
    if not rows:
        raise HTTPException(status_code=404, detail="No review found for MMSI")
    return _row_to_response(rows[0])


@router.get("/api/reviews/{mmsi}/history")
def get_review_history(mmsi: str, limit: int = 20) -> dict[str, Any]:
    safe_limit = max(1, min(int(limit), 200))
    with get_conn() as con:
        if con is None:
            return {"mmsi": mmsi.strip(), "count": 0, "items": []}
        rows = (
            con.execute(
                """
            SELECT mmsi, review_tier, handoff_state, rationale, evidence_refs_json, reviewed_by, reviewed_at
            FROM vessel_reviews
            WHERE mmsi = ?
            ORDER BY reviewed_at DESC
            LIMIT ?
            """,
                [mmsi.strip(), safe_limit],
            )
            .fetchdf()
            .to_dict("records")
        )
    return {
        "mmsi": mmsi.strip(),
        "count": len(rows),
        "items": [_row_to_response(r).model_dump() for r in rows],
    }
