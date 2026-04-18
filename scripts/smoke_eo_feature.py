"""Smoke test for eo_dark_count_30d and eo_ais_mismatch_ratio features.

Seeds a fresh DuckDB with one vessel (AIS gap) + 2 dark and 1 matched EO
detections, runs the feature computation, and asserts the result.

Usage:
    uv run python scripts/smoke_eo_feature.py
    uv run python scripts/smoke_eo_feature.py --db data/processed/mpol.duckdb
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime, timedelta

import duckdb

from pipeline.src.features.eo_fusion import compute_eo_features
from pipeline.src.ingest.eo_gfw import ingest_eo_records
from pipeline.src.ingest.schema import init_schema

DEFAULT_DB = "data/processed/mpol.duckdb"
TARGET_MMSI = "123456789"


def main(db: str, gap_hours: float, vessel_lat: float, vessel_lon: float) -> None:
    if os.path.exists(db):
        os.remove(db)
    init_schema(db)

    now = datetime.now(UTC)
    gap_start = now - timedelta(days=3)
    gap_end = gap_start + timedelta(hours=gap_hours)

    con = duckdb.connect(db)
    con.execute(
        """
        INSERT INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status, ship_type)
        VALUES ('123456789', ?, ?, ?, 8.0, 0, 70),
               ('123456789', ?, ?, ?, 7.0, 0, 70)
        """,
        [
            gap_start - timedelta(hours=1),
            vessel_lat,
            vessel_lon,
            gap_end + timedelta(hours=1),
            vessel_lat,
            vessel_lon,
        ],
    )
    for mmsi, lat, lon, stype in [
        ("200000001", 1.3, 103.8, 70),
        ("200000002", 1.4, 104.0, 70),
        ("200000003", 1.2, 103.5, 80),
        ("200000004", 1.5, 103.9, 70),
        ("200000005", 1.1, 104.1, 80),
    ]:
        for h in range(0, 72, 3):
            con.execute(
                "INSERT INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status, ship_type) "
                "VALUES (?, ?, ?, ?, 8.0, 0, ?)",
                [mmsi, now - timedelta(hours=72 - h), lat, lon, stype],
            )
    con.execute(
        """
        INSERT INTO vessel_meta (mmsi, flag, ship_type) VALUES
          ('123456789','IR',70),('200000001','SG',70),('200000002','SG',70),
          ('200000003','SG',80),('200000004','SG',70),('200000005','SG',80)
        """
    )
    con.close()
    print(f"Seeded vessel {TARGET_MMSI} with {gap_hours}h AIS gap at ({vessel_lat}, {vessel_lon})")
    print("Seeded 5 normal background vessels")

    ingest_eo_records(
        [
            {
                "detection_id": "eo-dark-1",
                "detected_at": gap_start + timedelta(hours=3),
                "lat": vessel_lat + 0.05,
                "lon": vessel_lon + 0.05,
                "source": "gfw",
            },
            {
                "detection_id": "eo-dark-2",
                "detected_at": gap_start + timedelta(hours=gap_hours / 2),
                "lat": vessel_lat + 0.08,
                "lon": vessel_lon,
                "source": "gfw",
            },
            {
                "detection_id": "eo-matched",
                "detected_at": gap_start - timedelta(minutes=30),
                "lat": vessel_lat + 0.02,
                "lon": vessel_lon + 0.02,
                "source": "gfw",
            },
        ],
        db_path=db,
    )
    print("Seeded 2 dark + 1 matched EO detections")

    result = compute_eo_features(
        db,
        window_days=30,
        match_radius_deg=0.1,
        match_window_minutes=120,
        gap_threshold_h=6.0,
        attribution_radius_deg=0.5,
    )
    if result.is_empty():
        print("FAILED — no EO features computed")
        raise SystemExit(1)

    row = result.filter(result["mmsi"] == TARGET_MMSI)
    if row.is_empty():
        print(f"FAILED — vessel {TARGET_MMSI} not in output")
        raise SystemExit(1)

    dark = row["eo_dark_count_30d"][0]
    ratio = row["eo_ais_mismatch_ratio"][0]
    print(
        f"SUCCESS — mmsi={TARGET_MMSI}  eo_dark_count_30d={dark}  eo_ais_mismatch_ratio={ratio:.2f}"
    )
    print(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EO feature smoke test")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--gap-hours", type=float, default=12.0)
    parser.add_argument("--lat", type=float, default=1.0)
    parser.add_argument("--lon", type=float, default=103.0)
    args = parser.parse_args()
    main(args.db, args.gap_hours, args.lat, args.lon)
