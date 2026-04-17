"""Smoke test for unmatched_sar_detections_30d feature.

Seeds a fresh DuckDB with one vessel (AIS gap) + three unmatched SAR
detections nearby, runs the feature computation, and asserts the result.

Usage:
    uv run python scripts/smoke_sar_feature.py
    uv run python scripts/smoke_sar_feature.py --db data/processed/mpol.duckdb
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime, timedelta

import duckdb

from pipeline.src.features.sar_detections import compute_unmatched_sar_detections
from pipeline.src.ingest.sar import ingest_sar_records
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

    ingest_sar_records(
        [
            {
                "detection_id": "smoke-d1",
                "detected_at": gap_start + timedelta(hours=2),
                "lat": vessel_lat + 0.1,
                "lon": vessel_lon,
                "source_scene": "S1A_IW_GRDH_smoke_1",
            },
            {
                "detection_id": "smoke-d2",
                "detected_at": gap_start + timedelta(hours=gap_hours / 2),
                "lat": vessel_lat + 0.1,
                "lon": vessel_lon + 0.1,
                "source_scene": "S1A_IW_GRDH_smoke_2",
            },
            {
                "detection_id": "smoke-d3",
                "detected_at": gap_end - timedelta(hours=2),
                "lat": vessel_lat,
                "lon": vessel_lon + 0.1,
                "source_scene": "S1A_IW_GRDH_smoke_3",
            },
        ],
        db_path=db,
    )
    print("Seeded 3 unmatched SAR detections during the gap")

    result = compute_unmatched_sar_detections(db, window_days=30)
    if result.is_empty():
        print("FAILED — no unmatched detections attributed")
        raise SystemExit(1)

    row = result.filter(result["mmsi"] == TARGET_MMSI)
    if row.is_empty():
        print(f"FAILED — vessel {TARGET_MMSI} not in output")
        raise SystemExit(1)

    count = row["unmatched_sar_detections_30d"][0]
    print(f"SUCCESS — mmsi={TARGET_MMSI}  unmatched_sar_detections_30d={count}")
    print(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SAR feature smoke test")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--gap-hours", type=float, default=12.0)
    parser.add_argument("--lat", type=float, default=1.0)
    parser.add_argument("--lon", type=float, default=103.0)
    args = parser.parse_args()
    main(args.db, args.gap_hours, args.lat, args.lon)
