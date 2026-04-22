"""Tests for scripts/validate_lead_time_ofac.py (#253)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from scripts.validate_lead_time_ofac import (
    CONFIDENCE_THRESHOLD,
    UU_CONFIDENCE_THRESHOLD,
    UU_SANCTIONS_DISTANCE,
    _load_designation_dates,
    _prospective,
    _retrospective,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_jsonl(tmp_path: Path, entries: list[dict]) -> Path:
    p = tmp_path / "opensanctions_entities.jsonl"
    with p.open("w") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")
    return p


def _entity(mmsi: str, first_seen: str, name: str = "VESSEL") -> dict:
    return {
        "id": f"ofac-{mmsi}",
        "caption": name,
        "first_seen": first_seen,
        "properties": {"mmsi": [mmsi], "name": [name]},
    }


def _watchlist_row(
    mmsi: str,
    confidence: float,
    sanctions_distance: int = 99,
    ais_gap_count: int = 5,
    last_seen: str | None = None,
    vessel_name: str = "VESSEL",
    flag: str = "PA",
) -> dict:
    return {
        "mmsi": mmsi,
        "vessel_name": vessel_name,
        "flag": flag,
        "confidence": confidence,
        "sanctions_distance": sanctions_distance,
        "ais_gap_count_30d": ais_gap_count,
        "last_seen": last_seen,
    }


# ---------------------------------------------------------------------------
# _load_designation_dates
# ---------------------------------------------------------------------------


def test_load_designation_dates_basic(tmp_path):
    jsonl = _make_jsonl(
        tmp_path,
        [_entity("123456789", "2024-06-01T00:00:00Z")],
    )
    dates = _load_designation_dates(jsonl)
    assert "123456789" in dates
    assert dates["123456789"] == datetime(2024, 6, 1, tzinfo=UTC)


def test_load_designation_dates_keeps_earliest(tmp_path):
    jsonl = _make_jsonl(
        tmp_path,
        [
            _entity("123456789", "2024-06-01T00:00:00Z"),
            _entity("123456789", "2024-01-15T00:00:00Z"),  # earlier
        ],
    )
    dates = _load_designation_dates(jsonl)
    assert dates["123456789"] == datetime(2024, 1, 15, tzinfo=UTC)


def test_load_designation_dates_missing_file(tmp_path):
    dates = _load_designation_dates(tmp_path / "nonexistent.jsonl")
    assert dates == {}


def test_load_designation_dates_skips_malformed_lines(tmp_path):
    p = tmp_path / "opensanctions_entities.jsonl"
    p.write_text('not-json\n{"id":"x","first_seen":"bad-date","properties":{"mmsi":["999"]}}\n')
    dates = _load_designation_dates(p)
    assert "999" not in dates


def test_load_designation_dates_multiple_mmsis_per_entity(tmp_path):
    entry = {
        "id": "ofac-multi",
        "first_seen": "2024-03-10T00:00:00Z",
        "properties": {"mmsi": ["111111111", "222222222"]},
    }
    jsonl = _make_jsonl(tmp_path, [entry])
    dates = _load_designation_dates(jsonl)
    assert "111111111" in dates
    assert "222222222" in dates


def test_load_designation_dates_entity_without_mmsi(tmp_path):
    entry = {"id": "ofac-person", "first_seen": "2024-01-01T00:00:00Z", "properties": {}}
    jsonl = _make_jsonl(tmp_path, [entry])
    assert _load_designation_dates(jsonl) == {}


# ---------------------------------------------------------------------------
# _retrospective
# ---------------------------------------------------------------------------


def test_retrospective_pre_designation_detection():
    # Vessel designated 90 days from now; detection window starts 30 days ago → +60 day lead
    now = datetime.now(UTC)
    designation_date = now + timedelta(days=60)

    wl = pl.DataFrame([_watchlist_row("111111111", confidence=0.50, last_seen=None)])
    dates = {"111111111": designation_date}
    rows = _retrospective(wl, dates, reference_date=now)

    assert len(rows) == 1
    assert rows[0]["pre_designation"] is True
    assert rows[0]["lead_days"] > 0


def test_retrospective_post_designation_flagged_after():
    # Designated 60 days ago; detection window starts 30 days ago → negative lead
    now = datetime.now(UTC)
    designation_date = now - timedelta(days=60)

    wl = pl.DataFrame([_watchlist_row("222222222", confidence=0.50, last_seen=None)])
    dates = {"222222222": designation_date}
    rows = _retrospective(wl, dates, reference_date=now)

    assert len(rows) == 1
    assert rows[0]["pre_designation"] is False
    assert rows[0]["lead_days"] < 0


def test_retrospective_skips_low_confidence():
    now = datetime.now(UTC)
    wl = pl.DataFrame([_watchlist_row("333333333", confidence=CONFIDENCE_THRESHOLD - 0.01)])
    dates = {"333333333": now + timedelta(days=30)}
    rows = _retrospective(wl, dates, reference_date=now)
    assert rows == []


def test_retrospective_skips_vessels_not_in_designation_dates():
    now = datetime.now(UTC)
    wl = pl.DataFrame([_watchlist_row("444444444", confidence=0.60)])
    rows = _retrospective(wl, {}, reference_date=now)
    assert rows == []


def test_retrospective_sorted_by_lead_days_descending():
    now = datetime.now(UTC)
    dates = {
        "111111111": now + timedelta(days=120),  # large lead
        "222222222": now + timedelta(days=10),  # small lead
    }
    wl = pl.DataFrame(
        [
            _watchlist_row("111111111", confidence=0.60),
            _watchlist_row("222222222", confidence=0.60),
        ]
    )
    rows = _retrospective(wl, dates, reference_date=now)
    assert len(rows) == 2
    assert rows[0]["lead_days"] >= rows[1]["lead_days"]


# ---------------------------------------------------------------------------
# _prospective
# ---------------------------------------------------------------------------


def test_prospective_returns_unsanctioned_high_confidence():
    wl = pl.DataFrame(
        [
            _watchlist_row(
                "555555555",
                confidence=UU_CONFIDENCE_THRESHOLD + 0.1,
                sanctions_distance=UU_SANCTIONS_DISTANCE,
            ),
        ]
    )
    rows = _prospective(wl, {})
    assert len(rows) == 1
    assert rows[0]["mmsi"] == "555555555"


def test_prospective_excludes_already_designated():
    now = datetime.now(UTC)
    wl = pl.DataFrame(
        [
            _watchlist_row("666666666", confidence=0.80, sanctions_distance=UU_SANCTIONS_DISTANCE),
        ]
    )
    dates = {"666666666": now - timedelta(days=10)}
    rows = _prospective(wl, dates)
    assert rows == []


def test_prospective_excludes_low_confidence():
    wl = pl.DataFrame(
        [
            _watchlist_row(
                "777777777",
                confidence=UU_CONFIDENCE_THRESHOLD - 0.01,
                sanctions_distance=UU_SANCTIONS_DISTANCE,
            ),
        ]
    )
    rows = _prospective(wl, {})
    assert rows == []


def test_prospective_excludes_vessels_with_sanctions_link():
    wl = pl.DataFrame(
        [
            _watchlist_row("888888888", confidence=0.80, sanctions_distance=2),
        ]
    )
    rows = _prospective(wl, {})
    assert rows == []


def test_prospective_capped_at_50():
    rows_input = [
        _watchlist_row(str(i).zfill(9), confidence=0.80, sanctions_distance=UU_SANCTIONS_DISTANCE)
        for i in range(100)
    ]
    wl = pl.DataFrame(rows_input)
    rows = _prospective(wl, {})
    assert len(rows) <= 50


def test_prospective_sorted_by_confidence_descending():
    wl = pl.DataFrame(
        [
            _watchlist_row("100000001", confidence=0.50, sanctions_distance=UU_SANCTIONS_DISTANCE),
            _watchlist_row("100000002", confidence=0.90, sanctions_distance=UU_SANCTIONS_DISTANCE),
            _watchlist_row("100000003", confidence=0.70, sanctions_distance=UU_SANCTIONS_DISTANCE),
        ]
    )
    rows = _prospective(wl, {})
    confidences = [r["confidence"] for r in rows]
    assert confidences == sorted(confidences, reverse=True)


# ---------------------------------------------------------------------------
# p25 / median / p75 computation (inline — mirrors script logic)
# ---------------------------------------------------------------------------


def test_percentile_values_correct():
    lead_days = sorted([10, 20, 30, 40, 50, 60, 70, 80])
    n = len(lead_days)
    p25 = lead_days[n // 4]  # index 2 → 30
    median = lead_days[n // 2]  # index 4 → 50
    p75 = lead_days[(3 * n) // 4]  # index 6 → 70
    assert p25 == 30
    assert median == 50
    assert p75 == 70


def test_percentiles_single_element():
    lead_days = sorted([42])
    n = len(lead_days)
    assert lead_days[n // 4] == 42
    assert lead_days[n // 2] == 42
    assert lead_days[(3 * n) // 4] == 42
