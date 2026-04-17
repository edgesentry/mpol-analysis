"""Tests for SAR vessel detection feature engineering."""

from datetime import UTC, datetime, timedelta

import duckdb

from pipeline.src.features.sar_detections import compute_unmatched_sar_detections
from pipeline.src.ingest.sar import ingest_sar_records


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
# No SAR data
# ---------------------------------------------------------------------------


def test_empty_sar_table_returns_empty(tmp_db):
    _seed_ais(
        tmp_db,
        [
            {
                "mmsi": "111111111",
                "timestamp": _now() - timedelta(days=1),
                "lat": 1.0,
                "lon": 103.0,
            },
        ],
    )
    result = compute_unmatched_sar_detections(tmp_db, window_days=30)
    assert result.is_empty()


# ---------------------------------------------------------------------------
# SAR detection matched by AIS → should NOT count
# ---------------------------------------------------------------------------


def test_matched_sar_detection_not_counted(tmp_db):
    t = _now() - timedelta(days=5)

    # AIS position at same location at nearly the same time
    _seed_ais(
        tmp_db,
        [
            {"mmsi": "111111111", "timestamp": t - timedelta(minutes=10), "lat": 1.0, "lon": 103.0},
            {"mmsi": "111111111", "timestamp": t + timedelta(minutes=10), "lat": 1.0, "lon": 103.0},
        ],
    )

    # SAR detection right next to the AIS position
    ingest_sar_records(
        [{"detection_id": "d001", "detected_at": t, "lat": 1.0001, "lon": 103.0001}],
        db_path=tmp_db,
    )

    result = compute_unmatched_sar_detections(tmp_db, window_days=30, match_radius_km=5.0)
    assert result.is_empty() or result["unmatched_sar_detections_30d"].sum() == 0


# ---------------------------------------------------------------------------
# Unmatched SAR during AIS gap, vessel nearby → should count
# ---------------------------------------------------------------------------


def test_unmatched_sar_during_gap_counts(tmp_db):
    t = _now() - timedelta(days=5)
    gap_start = t
    gap_end = t + timedelta(hours=12)  # 12-hour gap (> 6h threshold)

    # Two AIS pings: one before gap, one after — vessel near lat=2.0, lon=104.0
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

    # SAR detection during the gap, within 50 km of last known position
    # 0.1° ≈ 11 km — well within 50 km attribution radius
    detection_time = gap_start + timedelta(hours=5)
    ingest_sar_records(
        [{"detection_id": "d002", "detected_at": detection_time, "lat": 2.1, "lon": 104.1}],
        db_path=tmp_db,
    )

    result = compute_unmatched_sar_detections(
        tmp_db,
        window_days=30,
        match_radius_km=5.0,
        match_window_minutes=60,
        gap_threshold_h=6.0,
        attribution_radius_km=50.0,
    )

    assert not result.is_empty()
    row = result.filter(result["mmsi"] == "222222222")
    assert not row.is_empty()
    assert row["unmatched_sar_detections_30d"][0] == 1


# ---------------------------------------------------------------------------
# SAR detection far from vessel's last position → should NOT be attributed
# ---------------------------------------------------------------------------


def test_unmatched_sar_far_from_vessel_not_counted(tmp_db):
    t = _now() - timedelta(days=5)
    gap_start = t
    gap_end = t + timedelta(hours=12)

    # Vessel near lat=2.0, lon=104.0
    _seed_ais(
        tmp_db,
        [
            {
                "mmsi": "333333333",
                "timestamp": gap_start - timedelta(minutes=30),
                "lat": 2.0,
                "lon": 104.0,
            },
            {
                "mmsi": "333333333",
                "timestamp": gap_end + timedelta(minutes=30),
                "lat": 2.0,
                "lon": 104.0,
            },
        ],
    )

    # SAR detection during gap but 500+ km away (5° ~ 555 km)
    detection_time = gap_start + timedelta(hours=5)
    ingest_sar_records(
        [{"detection_id": "d003", "detected_at": detection_time, "lat": 7.0, "lon": 104.0}],
        db_path=tmp_db,
    )

    result = compute_unmatched_sar_detections(
        tmp_db,
        window_days=30,
        match_radius_km=5.0,
        attribution_radius_km=50.0,
    )

    # This vessel should not appear (too far away)
    if not result.is_empty():
        row = result.filter(result["mmsi"] == "333333333")
        assert row.is_empty() or row["unmatched_sar_detections_30d"][0] == 0


# ---------------------------------------------------------------------------
# Multiple detections during gap → counts all
# ---------------------------------------------------------------------------


def test_multiple_unmatched_detections_counted(tmp_db):
    t = _now() - timedelta(days=5)
    gap_start = t
    gap_end = t + timedelta(hours=24)

    _seed_ais(
        tmp_db,
        [
            {
                "mmsi": "444444444",
                "timestamp": gap_start - timedelta(hours=1),
                "lat": 5.0,
                "lon": 100.0,
            },
            {
                "mmsi": "444444444",
                "timestamp": gap_end + timedelta(hours=1),
                "lat": 5.0,
                "lon": 100.0,
            },
        ],
    )

    detections = [
        {
            "detection_id": "d004",
            "detected_at": gap_start + timedelta(hours=4),
            "lat": 5.05,
            "lon": 100.05,
        },
        {
            "detection_id": "d005",
            "detected_at": gap_start + timedelta(hours=12),
            "lat": 5.1,
            "lon": 100.1,
        },
        {
            "detection_id": "d006",
            "detected_at": gap_start + timedelta(hours=20),
            "lat": 5.05,
            "lon": 100.0,
        },
    ]
    ingest_sar_records(detections, db_path=tmp_db)

    result = compute_unmatched_sar_detections(
        tmp_db,
        window_days=30,
        match_radius_km=5.0,
        attribution_radius_km=50.0,
    )

    row = result.filter(result["mmsi"] == "444444444")
    assert not row.is_empty()
    assert row["unmatched_sar_detections_30d"][0] == 3


# ---------------------------------------------------------------------------
# SAR outside 30-day window → not counted
# ---------------------------------------------------------------------------


def test_sar_outside_window_not_counted(tmp_db):
    t = _now() - timedelta(days=45)  # outside 30-day window

    _seed_ais(
        tmp_db,
        [
            {"mmsi": "555555555", "timestamp": t - timedelta(hours=1), "lat": 1.0, "lon": 103.0},
            {"mmsi": "555555555", "timestamp": t + timedelta(hours=13), "lat": 1.0, "lon": 103.0},
        ],
    )

    ingest_sar_records(
        [
            {
                "detection_id": "d007",
                "detected_at": t + timedelta(hours=5),
                "lat": 1.05,
                "lon": 103.05,
            }
        ],
        db_path=tmp_db,
    )

    result = compute_unmatched_sar_detections(tmp_db, window_days=30)
    if not result.is_empty():
        row = result.filter(result["mmsi"] == "555555555")
        assert row.is_empty() or row["unmatched_sar_detections_30d"][0] == 0
