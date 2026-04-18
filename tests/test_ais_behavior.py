"""Unit tests for pipeline/src/features/ais_behavior.py.

All tests use in-memory Polars DataFrames — no DuckDB fixture required.
"""

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from pipeline.src.features.ais_behavior import (
    GAP_THRESHOLD_H,
    LOITER_SPEED_KNOTS,
    PORT_MOORED_STATUS,
    _haversine_km,
    compute_ais_features,
    compute_gap_features,
    compute_loitering,
    compute_port_call_ratio,
    compute_position_jumps,
    compute_sts_candidates,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_BASE_TS = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)


def _make_df(records: list[dict]) -> pl.DataFrame:
    """Build a minimal AIS DataFrame matching load_ais_window output."""
    rows = []
    for r in records:
        rows.append(
            {
                "mmsi": r["mmsi"],
                "timestamp": r["timestamp"],
                "lat": float(r.get("lat", 1.0)),
                "lon": float(r.get("lon", 103.0)),
                "sog": float(r.get("sog", 5.0)),
                "nav_status": int(r.get("nav_status", 0)),
            }
        )
    schema = {
        "mmsi": pl.Utf8,
        "timestamp": pl.Datetime("us", "UTC"),
        "lat": pl.Float64,
        "lon": pl.Float64,
        "sog": pl.Float64,
        "nav_status": pl.Int64,
    }
    return pl.DataFrame(rows, schema=schema)


# ---------------------------------------------------------------------------
# _haversine_km
# ---------------------------------------------------------------------------


def test_haversine_same_point_is_zero():
    assert _haversine_km(1.3, 103.8, 1.3, 103.8) == pytest.approx(0.0)


def test_haversine_known_distance():
    # Singapore → Kuala Lumpur: ~309 km
    dist = _haversine_km(1.3521, 103.8198, 3.1390, 101.6869)
    assert 305 < dist < 315


def test_haversine_one_degree_latitude():
    # 1° latitude ≈ 111.195 km
    dist = _haversine_km(0.0, 0.0, 1.0, 0.0)
    assert dist == pytest.approx(111.195, abs=0.1)


# ---------------------------------------------------------------------------
# compute_gap_features
# ---------------------------------------------------------------------------


def test_gap_below_threshold_not_counted():
    """9-hour gap is below the 10-hour threshold — should not be counted."""
    t0 = _BASE_TS
    df = _make_df(
        [
            {"mmsi": "111111111", "timestamp": t0},
            {"mmsi": "111111111", "timestamp": t0 + timedelta(hours=9)},
        ]
    )
    result = compute_gap_features(df, gap_threshold_h=GAP_THRESHOLD_H)
    assert result.is_empty() or result.filter(pl.col("mmsi") == "111111111").is_empty()


def test_gap_at_threshold_not_counted():
    """Exactly 10-hour gap — filter is strict (>), so not counted."""
    t0 = _BASE_TS
    df = _make_df(
        [
            {"mmsi": "111111111", "timestamp": t0},
            {"mmsi": "111111111", "timestamp": t0 + timedelta(hours=GAP_THRESHOLD_H)},
        ]
    )
    result = compute_gap_features(df, gap_threshold_h=GAP_THRESHOLD_H)
    assert result.is_empty() or result.filter(pl.col("mmsi") == "111111111").is_empty()


def test_gap_above_threshold_counted():
    """11-hour gap exceeds the 10-hour threshold — must be counted."""
    t0 = _BASE_TS
    df = _make_df(
        [
            {"mmsi": "222222222", "timestamp": t0},
            {"mmsi": "222222222", "timestamp": t0 + timedelta(hours=11)},
        ]
    )
    result = compute_gap_features(df, gap_threshold_h=GAP_THRESHOLD_H)
    row = result.filter(pl.col("mmsi") == "222222222")
    assert not row.is_empty()
    assert row["ais_gap_count_30d"][0] == 1
    assert row["ais_gap_max_hours"][0] == pytest.approx(11.0, abs=0.01)


def test_gap_multiple_gaps_counted():
    """Two qualifying gaps → count=2, max reflects the larger one."""
    t0 = _BASE_TS
    df = _make_df(
        [
            {"mmsi": "333333333", "timestamp": t0},
            {"mmsi": "333333333", "timestamp": t0 + timedelta(hours=12)},
            {"mmsi": "333333333", "timestamp": t0 + timedelta(hours=12, minutes=30)},
            {"mmsi": "333333333", "timestamp": t0 + timedelta(hours=36)},
        ]
    )
    result = compute_gap_features(df, gap_threshold_h=GAP_THRESHOLD_H)
    row = result.filter(pl.col("mmsi") == "333333333")
    assert not row.is_empty()
    assert row["ais_gap_count_30d"][0] == 2
    assert row["ais_gap_max_hours"][0] == pytest.approx(23.5, abs=0.01)


def test_gap_multiple_vessels_isolated():
    """Gaps for different MMSIs are counted independently."""
    t0 = _BASE_TS
    df = _make_df(
        [
            {"mmsi": "AAA000001", "timestamp": t0},
            {"mmsi": "AAA000001", "timestamp": t0 + timedelta(hours=11)},
            {"mmsi": "BBB000002", "timestamp": t0},
            {"mmsi": "BBB000002", "timestamp": t0 + timedelta(hours=5)},  # below threshold
        ]
    )
    result = compute_gap_features(df, gap_threshold_h=GAP_THRESHOLD_H)
    assert result.filter(pl.col("mmsi") == "AAA000001")["ais_gap_count_30d"][0] == 1
    assert result.filter(pl.col("mmsi") == "BBB000002").is_empty()


def test_gap_empty_dataframe_returns_empty():
    df = pl.DataFrame(
        schema={
            "mmsi": pl.Utf8,
            "timestamp": pl.Datetime("us", "UTC"),
            "lat": pl.Float64,
            "lon": pl.Float64,
            "sog": pl.Float64,
            "nav_status": pl.Int64,
        }
    )
    result = compute_gap_features(df)
    assert result.is_empty()


# ---------------------------------------------------------------------------
# compute_position_jumps
# ---------------------------------------------------------------------------


def test_position_jump_detected():
    """Two positions 51 km apart in 30 min → implied speed ~55 knots (> 50 threshold)."""
    t0 = _BASE_TS
    # 0.46° latitude ≈ 51.15 km ≈ 27.6 nmi; in 0.5h → 55.2 knots
    df = _make_df(
        [
            {"mmsi": "111111111", "timestamp": t0, "lat": 1.0, "lon": 103.0},
            {
                "mmsi": "111111111",
                "timestamp": t0 + timedelta(minutes=30),
                "lat": 1.46,
                "lon": 103.0,
            },
        ]
    )
    result = compute_position_jumps(df)
    row = result.filter(pl.col("mmsi") == "111111111")
    assert not row.is_empty()
    assert row["position_jump_count"][0] == 1


def test_position_jump_not_detected_slow_vessel():
    """Two positions 5.6 km apart in 1h → ~3 knots, well below 50-knot threshold."""
    t0 = _BASE_TS
    df = _make_df(
        [
            {"mmsi": "222222222", "timestamp": t0, "lat": 1.0, "lon": 103.0},
            {"mmsi": "222222222", "timestamp": t0 + timedelta(hours=1), "lat": 1.05, "lon": 103.0},
        ]
    )
    result = compute_position_jumps(df)
    assert result.is_empty() or result.filter(pl.col("mmsi") == "222222222").is_empty()


def test_position_jump_gap_exceeds_1h_excluded():
    """Consecutive pings > 1h apart are excluded from the jump window."""
    t0 = _BASE_TS
    # Same 51-km distance but spread over 2 hours → ~28 knots, below threshold
    df = _make_df(
        [
            {"mmsi": "333333333", "timestamp": t0, "lat": 1.0, "lon": 103.0},
            {
                "mmsi": "333333333",
                "timestamp": t0 + timedelta(hours=2),
                "lat": 1.46,
                "lon": 103.0,
            },
        ]
    )
    result = compute_position_jumps(df)
    assert result.is_empty() or result.filter(pl.col("mmsi") == "333333333").is_empty()


def test_position_jump_empty_dataframe():
    df = pl.DataFrame(
        schema={
            "mmsi": pl.Utf8,
            "timestamp": pl.Datetime("us", "UTC"),
            "lat": pl.Float64,
            "lon": pl.Float64,
            "sog": pl.Float64,
            "nav_status": pl.Int64,
        }
    )
    result = compute_position_jumps(df)
    assert result.is_empty()
    assert "position_jump_count" in result.columns


# ---------------------------------------------------------------------------
# compute_sts_candidates
# ---------------------------------------------------------------------------


def test_sts_two_vessels_same_cell_same_bucket():
    """Two vessels ~100 m apart in the same 30-min bucket → both are STS candidates."""
    t0 = _BASE_TS.replace(minute=0, second=0)  # clean bucket boundary
    # Both positions map to the same H3-8 cell (~700 m edge → 100 m apart is the same cell)
    df = _make_df(
        [
            {
                "mmsi": "ALPHA0001",
                "timestamp": t0 + timedelta(minutes=5),
                "lat": 1.3,
                "lon": 103.8,
                "nav_status": 1,  # at anchor
            },
            {
                "mmsi": "BETA00001",
                "timestamp": t0 + timedelta(minutes=10),
                "lat": 1.3001,
                "lon": 103.8001,
                "nav_status": 1,
            },
        ]
    )
    result = compute_sts_candidates(df)
    mmsis = result["mmsi"].to_list()
    assert "ALPHA0001" in mmsis
    assert "BETA00001" in mmsis
    # Each vessel was co-located in 1 bucket
    for m in ["ALPHA0001", "BETA00001"]:
        assert result.filter(pl.col("mmsi") == m)["sts_candidate_count"][0] >= 1


def test_sts_single_vessel_not_candidate():
    """A single vessel has no co-location partners."""
    t0 = _BASE_TS
    df = _make_df(
        [{"mmsi": "SOLO00001", "timestamp": t0, "lat": 1.3, "lon": 103.8, "nav_status": 1}]
    )
    result = compute_sts_candidates(df)
    assert result.is_empty() or result.filter(pl.col("mmsi") == "SOLO00001").is_empty()


def test_sts_vessels_in_different_cells_not_candidates():
    """Two vessels far apart (different H3 cells) are not STS candidates."""
    t0 = _BASE_TS
    df = _make_df(
        [
            {"mmsi": "FAR00001", "timestamp": t0, "lat": 1.3, "lon": 103.8, "nav_status": 1},
            {"mmsi": "FAR00002", "timestamp": t0, "lat": 5.0, "lon": 100.0, "nav_status": 1},
        ]
    )
    result = compute_sts_candidates(df)
    for m in ["FAR00001", "FAR00002"]:
        assert result.is_empty() or result.filter(pl.col("mmsi") == m).is_empty()


def test_sts_underway_vessels_excluded():
    """Vessels not in STOPPED_STATUSES (e.g., nav_status=8 sailing) are excluded."""
    t0 = _BASE_TS
    df = _make_df(
        [
            {
                "mmsi": "SAIL00001",
                "timestamp": t0,
                "lat": 1.3,
                "lon": 103.8,
                "nav_status": 8,  # under way using engine — not in STOPPED_STATUSES
            },
            {
                "mmsi": "SAIL00002",
                "timestamp": t0,
                "lat": 1.3001,
                "lon": 103.8001,
                "nav_status": 8,
            },
        ]
    )
    result = compute_sts_candidates(df)
    assert result.is_empty()


def test_sts_empty_returns_correct_schema():
    df = pl.DataFrame(
        schema={
            "mmsi": pl.Utf8,
            "timestamp": pl.Datetime("us", "UTC"),
            "lat": pl.Float64,
            "lon": pl.Float64,
            "sog": pl.Float64,
            "nav_status": pl.Int64,
        }
    )
    result = compute_sts_candidates(df)
    assert result.is_empty()
    assert "sts_candidate_count" in result.columns


# ---------------------------------------------------------------------------
# compute_loitering
# ---------------------------------------------------------------------------


def test_loitering_slow_non_moored_counted():
    """SOG < 2 knots and nav_status != 5 accumulates loitering hours."""
    t0 = _BASE_TS
    df = _make_df(
        [
            {
                "mmsi": "LOIT00001",
                "timestamp": t0,
                "sog": 0.5,
                "nav_status": 0,
            },
            {
                "mmsi": "LOIT00001",
                "timestamp": t0 + timedelta(minutes=30),
                "sog": 0.5,
                "nav_status": 0,
            },
        ]
    )
    result = compute_loitering(df)
    row = result.filter(pl.col("mmsi") == "LOIT00001")
    assert not row.is_empty()
    assert row["loitering_hours_30d"][0] == pytest.approx(0.5, abs=0.01)


def test_loitering_moored_vessel_excluded():
    """nav_status == 5 (moored) is not counted as loitering."""
    t0 = _BASE_TS
    df = _make_df(
        [
            {
                "mmsi": "MOOR00001",
                "timestamp": t0,
                "sog": 0.0,
                "nav_status": PORT_MOORED_STATUS,
            },
            {
                "mmsi": "MOOR00001",
                "timestamp": t0 + timedelta(minutes=30),
                "sog": 0.0,
                "nav_status": PORT_MOORED_STATUS,
            },
        ]
    )
    result = compute_loitering(df)
    assert result.is_empty() or result.filter(pl.col("mmsi") == "MOOR00001").is_empty()


def test_loitering_fast_vessel_excluded():
    """SOG >= 2 knots is not loitering regardless of nav_status."""
    t0 = _BASE_TS
    df = _make_df(
        [
            {"mmsi": "FAST00001", "timestamp": t0, "sog": LOITER_SPEED_KNOTS, "nav_status": 0},
            {
                "mmsi": "FAST00001",
                "timestamp": t0 + timedelta(minutes=30),
                "sog": LOITER_SPEED_KNOTS,
                "nav_status": 0,
            },
        ]
    )
    result = compute_loitering(df)
    assert result.is_empty() or result.filter(pl.col("mmsi") == "FAST00001").is_empty()


def test_loitering_large_gap_excluded():
    """Consecutive pings > 60 min apart are excluded (implies data gap, not loitering)."""
    t0 = _BASE_TS
    df = _make_df(
        [
            {"mmsi": "GAP000001", "timestamp": t0, "sog": 0.5, "nav_status": 0},
            {
                "mmsi": "GAP000001",
                "timestamp": t0 + timedelta(minutes=90),
                "sog": 0.5,
                "nav_status": 0,
            },
        ]
    )
    result = compute_loitering(df)
    assert result.is_empty() or result.filter(pl.col("mmsi") == "GAP000001").is_empty()


# ---------------------------------------------------------------------------
# compute_port_call_ratio
# ---------------------------------------------------------------------------


def test_port_call_ratio_all_moored():
    t0 = _BASE_TS
    df = _make_df(
        [
            {"mmsi": "PORT00001", "timestamp": t0, "nav_status": 5},
            {"mmsi": "PORT00001", "timestamp": t0 + timedelta(hours=1), "nav_status": 5},
        ]
    )
    result = compute_port_call_ratio(df)
    row = result.filter(pl.col("mmsi") == "PORT00001")
    assert not row.is_empty()
    assert row["port_call_ratio"][0] == pytest.approx(1.0)


def test_port_call_ratio_none_moored():
    t0 = _BASE_TS
    df = _make_df(
        [
            {"mmsi": "ANCH00001", "timestamp": t0, "nav_status": 1},  # at anchor, not moored
            {"mmsi": "ANCH00001", "timestamp": t0 + timedelta(hours=1), "nav_status": 1},
        ]
    )
    result = compute_port_call_ratio(df)
    row = result.filter(pl.col("mmsi") == "ANCH00001")
    assert not row.is_empty()
    assert row["port_call_ratio"][0] == pytest.approx(0.0)


def test_port_call_ratio_half_moored():
    t0 = _BASE_TS
    df = _make_df(
        [
            {"mmsi": "HALF00001", "timestamp": t0, "nav_status": 5},
            {"mmsi": "HALF00001", "timestamp": t0 + timedelta(hours=1), "nav_status": 1},
        ]
    )
    result = compute_port_call_ratio(df)
    row = result.filter(pl.col("mmsi") == "HALF00001")
    assert not row.is_empty()
    assert row["port_call_ratio"][0] == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# compute_ais_features (orchestrator)
# ---------------------------------------------------------------------------


def test_compute_ais_features_empty_db_returns_correct_schema(tmp_db):
    result = compute_ais_features(tmp_db, window_days=30)
    assert result.is_empty()
    expected_cols = {
        "mmsi",
        "ais_gap_count_30d",
        "ais_gap_max_hours",
        "position_jump_count",
        "sts_candidate_count",
        "port_call_ratio",
        "loitering_hours_30d",
    }
    assert expected_cols == set(result.columns)


def test_compute_ais_features_fill_null_defaults(tmp_db):
    """Vessels with no matching events get fill_null defaults (0 for counts, 0.5 for ratio)."""
    import duckdb

    con = duckdb.connect(tmp_db)
    try:
        # Insert a vessel with just one position — no gaps, no jumps, no STS
        con.execute(
            """
            INSERT INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status, ship_type)
            VALUES ('999999999', now(), 1.0, 103.0, 8.0, 0, 70)
            """
        )
    finally:
        con.close()

    result = compute_ais_features(tmp_db, window_days=30)
    row = result.filter(pl.col("mmsi") == "999999999")
    assert not row.is_empty()
    assert row["ais_gap_count_30d"][0] == 0
    assert row["position_jump_count"][0] == 0
    assert row["sts_candidate_count"][0] == 0
    assert row["loitering_hours_30d"][0] == pytest.approx(0.0)
    assert row["port_call_ratio"][0] == pytest.approx(0.5)  # fill_null default
