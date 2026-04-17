"""Tests for EO-AIS fusion feature engineering."""

from datetime import UTC, datetime, timedelta

import duckdb

from pipeline.src.features.eo_fusion import compute_eo_features
from pipeline.src.ingest.eo_gfw import ingest_eo_records


def _now() -> datetime:
    return datetime.now(UTC)


def _seed_ais(db_path: str, records: list[dict]) -> None:
    con = duckdb.connect(db_path)
    try:
        for r in records:
            con.execute(
                """
                INSERT INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status, ship_type)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["mmsi"],
                    r["timestamp"],
                    r["lat"],
                    r["lon"],
                    r.get("sog", 5.0),
                    r.get("nav_status", 0),
                    r.get("ship_type", 70),
                ],
            )
    finally:
        con.close()


# ---------------------------------------------------------------------------
# No EO data
# ---------------------------------------------------------------------------


def test_empty_eo_table_returns_empty(tmp_db):
    _seed_ais(
        tmp_db,
        [{"mmsi": "111111111", "timestamp": _now() - timedelta(days=1), "lat": 1.0, "lon": 103.0}],
    )
    result = compute_eo_features(tmp_db, window_days=30)
    assert result.is_empty()


# ---------------------------------------------------------------------------
# --skip-eo flag
# ---------------------------------------------------------------------------


def test_skip_eo_returns_empty(tmp_db):
    ingest_eo_records(
        [{"detected_at": _now() - timedelta(days=1), "lat": 1.0, "lon": 103.0}],
        db_path=tmp_db,
    )
    result = compute_eo_features(tmp_db, skip_eo=True)
    assert result.is_empty()


# ---------------------------------------------------------------------------
# EO matched by AIS — dark_count=0, ratio=0
# ---------------------------------------------------------------------------


def test_matched_eo_gives_zero_dark_count(tmp_db):
    t = _now() - timedelta(days=5)
    _seed_ais(
        tmp_db,
        [
            {"mmsi": "111111111", "timestamp": t - timedelta(minutes=30), "lat": 1.0, "lon": 103.0},
            {"mmsi": "111111111", "timestamp": t + timedelta(minutes=30), "lat": 1.0, "lon": 103.0},
        ],
    )
    # EO detection within 0.05° and 60 min of the AIS ping → matched
    ingest_eo_records(
        [{"detected_at": t, "lat": 1.02, "lon": 103.02, "source": "gfw"}],
        db_path=tmp_db,
    )
    result = compute_eo_features(tmp_db, window_days=30, match_radius_deg=0.1)
    if not result.is_empty():
        row = result.filter(result["mmsi"] == "111111111")
        assert row.is_empty() or row["eo_dark_count_30d"][0] == 0


# ---------------------------------------------------------------------------
# Unmatched EO during AIS gap → dark_count=1, ratio=1.0
# ---------------------------------------------------------------------------


def test_unmatched_eo_during_gap_counts(tmp_db):
    t = _now() - timedelta(days=5)
    gap_start = t
    gap_end = t + timedelta(hours=12)

    _seed_ais(
        tmp_db,
        [
            {
                "mmsi": "222222222",
                "timestamp": gap_start - timedelta(minutes=30),
                "lat": 2.0,
                "lon": 104.0,
            },
            {
                "mmsi": "222222222",
                "timestamp": gap_end + timedelta(minutes=30),
                "lat": 2.0,
                "lon": 104.0,
            },
        ],
    )
    ingest_eo_records(
        [
            {
                "detected_at": gap_start + timedelta(hours=5),
                "lat": 2.1,
                "lon": 104.1,
                "source": "gfw",
            }
        ],
        db_path=tmp_db,
    )

    result = compute_eo_features(
        tmp_db,
        window_days=30,
        match_radius_deg=0.1,
        match_window_minutes=120,
        gap_threshold_h=6.0,
        attribution_radius_deg=0.5,
    )

    assert not result.is_empty()
    row = result.filter(result["mmsi"] == "222222222")
    assert not row.is_empty()
    assert row["eo_dark_count_30d"][0] == 1
    assert abs(row["eo_ais_mismatch_ratio"][0] - 1.0) < 1e-5


# ---------------------------------------------------------------------------
# Mixed: 1 matched + 1 unmatched → dark_count=1, ratio=0.5
# ---------------------------------------------------------------------------


def test_mixed_matched_and_unmatched(tmp_db):
    t = _now() - timedelta(days=5)
    gap_start = t + timedelta(hours=2)
    gap_end = gap_start + timedelta(hours=10)

    _seed_ais(
        tmp_db,
        [
            # ping before gap — matched detection will be near this
            {"mmsi": "333333333", "timestamp": t, "lat": 3.0, "lon": 105.0},
            # gap_start to gap_end — no AIS
            {
                "mmsi": "333333333",
                "timestamp": gap_end + timedelta(hours=1),
                "lat": 3.0,
                "lon": 105.0,
            },
        ],
    )
    ingest_eo_records(
        [
            # matched: near AIS ping at time t
            {
                "detection_id": "m1",
                "detected_at": t + timedelta(minutes=30),
                "lat": 3.02,
                "lon": 105.02,
                "source": "gfw",
            },
            # unmatched: during gap
            {
                "detection_id": "u1",
                "detected_at": gap_start + timedelta(hours=4),
                "lat": 3.1,
                "lon": 105.1,
                "source": "gfw",
            },
        ],
        db_path=tmp_db,
    )

    result = compute_eo_features(
        tmp_db,
        window_days=30,
        match_radius_deg=0.1,
        match_window_minutes=120,
        gap_threshold_h=6.0,
        attribution_radius_deg=0.5,
    )

    row = result.filter(result["mmsi"] == "333333333")
    assert not row.is_empty()
    assert row["eo_dark_count_30d"][0] == 1
    assert abs(row["eo_ais_mismatch_ratio"][0] - 0.5) < 1e-4


# ---------------------------------------------------------------------------
# EO outside window → not counted
# ---------------------------------------------------------------------------


def test_eo_outside_window_not_counted(tmp_db):
    t = _now() - timedelta(days=45)

    _seed_ais(
        tmp_db,
        [
            {"mmsi": "444444444", "timestamp": t - timedelta(hours=1), "lat": 1.0, "lon": 103.0},
            {"mmsi": "444444444", "timestamp": t + timedelta(hours=13), "lat": 1.0, "lon": 103.0},
        ],
    )
    ingest_eo_records(
        [{"detected_at": t + timedelta(hours=5), "lat": 1.1, "lon": 103.1}],
        db_path=tmp_db,
    )

    result = compute_eo_features(tmp_db, window_days=30)
    if not result.is_empty():
        row = result.filter(result["mmsi"] == "444444444")
        assert row.is_empty() or row["eo_dark_count_30d"][0] == 0
