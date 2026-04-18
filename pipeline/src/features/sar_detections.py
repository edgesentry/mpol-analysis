"""
SAR-based feature engineering: unmatched_sar_detections_30d.

For each vessel (MMSI), counts how many Sentinel-1 SAR detections in the
last 30 days occurred during an AIS gap and were spatially close to the
vessel's last known position — i.e., the vessel was "seen" by radar but was
not broadcasting AIS.

Algorithm
---------
1. Load SAR detections and AIS positions for the rolling window.
2. Label each SAR detection as *matched* if any AIS broadcast exists within
   ``match_radius_km`` and ``match_window_minutes``.
3. For *unmatched* SAR detections, derive per-vessel AIS gaps
   (consecutive AIS records separated by > ``gap_threshold_h`` hours).
4. Attribute an unmatched detection to a vessel when:
   - the detection timestamp falls inside the vessel's gap interval, AND
   - the vessel's last AIS position before the gap is within
     ``attribution_radius_km`` of the SAR detection.
5. ``unmatched_sar_detections_30d`` = count of attributed unmatched
   detections per MMSI.

Output columns
--------------
    mmsi, unmatched_sar_detections_30d

Usage
-----
    uv run python src/features/sar_detections.py
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime, timedelta

import duckdb
import polars as pl
from dotenv import load_dotenv

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")

# SAR-AIS match parameters
MATCH_RADIUS_KM: float = 5.0  # AIS broadcast within this radius = matched
MATCH_WINDOW_MINUTES: int = 60  # and within this time window

# Gap & attribution parameters
GAP_THRESHOLD_H: float = 10.0  # gaps longer than this (hours) are considered dark periods
# Raised from 6h → 10h: matches ais_behavior.py; Singapore anchorage waits are 8-12h normal.
ATTRIBUTION_RADIUS_KM: float = 50.0  # last known position must be within this of detection


def load_sar_window(db_path: str, window_days: int = 30) -> pl.DataFrame:
    """Load SAR detections from the last *window_days*."""
    cutoff = datetime.now(UTC) - timedelta(days=window_days)
    con = duckdb.connect(db_path, read_only=True)
    try:
        return con.execute(
            """
            SELECT detection_id, detected_at, lat, lon
            FROM sar_detections
            WHERE detected_at >= ?
            """,
            [cutoff],
        ).pl()
    finally:
        con.close()


def _load_ais_window(db_path: str, window_days: int) -> pl.DataFrame:
    cutoff = datetime.now(UTC) - timedelta(days=window_days)
    con = duckdb.connect(db_path, read_only=True)
    try:
        return con.execute(
            """
            SELECT mmsi, timestamp, lat, lon
            FROM ais_positions
            WHERE timestamp >= ?
            ORDER BY mmsi, timestamp
            """,
            [cutoff],
        ).pl()
    finally:
        con.close()


def compute_unmatched_sar_detections(
    db_path: str = DEFAULT_DB_PATH,
    window_days: int = 60,
    match_radius_km: float = MATCH_RADIUS_KM,
    match_window_minutes: int = MATCH_WINDOW_MINUTES,
    gap_threshold_h: float = GAP_THRESHOLD_H,
    attribution_radius_km: float = ATTRIBUTION_RADIUS_KM,
) -> pl.DataFrame:
    """Compute ``unmatched_sar_detections_30d`` per MMSI.

    Returns a DataFrame with columns [mmsi, unmatched_sar_detections_30d].
    Vessels with zero unmatched detections are omitted (filled downstream).
    """
    _empty = pl.DataFrame(schema={"mmsi": pl.Utf8, "unmatched_sar_detections_30d": pl.Int32})

    sar_df = load_sar_window(db_path, window_days)
    if sar_df.is_empty():
        return _empty

    ais_df = _load_ais_window(db_path, window_days)
    if ais_df.is_empty():
        return _empty

    # Degree-based bounding-box tolerances (conservative at ~45° lat)
    lat_match = match_radius_km / 111.0
    lon_match = match_radius_km / 78.0  # ≈ 111 * cos(45°)
    lat_attr = attribution_radius_km / 111.0
    lon_attr = attribution_radius_km / 78.0
    match_secs = match_window_minutes * 60
    gap_secs = gap_threshold_h * 3600

    con = duckdb.connect()
    con.register("sar", sar_df)
    con.register("ais", ais_df)

    result: pl.DataFrame = con.execute(
        f"""
        WITH matched_sar AS (
            -- SAR detections that have at least one nearby AIS broadcast
            SELECT DISTINCT s.detection_id
            FROM sar s
            JOIN ais a ON (
                ABS(a.lat - s.lat) <= {lat_match}
                AND ABS(a.lon - s.lon) <= {lon_match * 2}
                AND ABS(epoch(a.timestamp) - epoch(s.detected_at)) <= {match_secs}
                AND 2 * 6371 * ASIN(SQRT(
                    POWER(SIN(RADIANS(a.lat - s.lat) / 2), 2) +
                    COS(RADIANS(s.lat)) * COS(RADIANS(a.lat)) *
                    POWER(SIN(RADIANS(a.lon - s.lon) / 2), 2)
                )) <= {match_radius_km}
            )
        ),
        unmatched_sar AS (
            SELECT * FROM sar
            WHERE detection_id NOT IN (SELECT detection_id FROM matched_sar)
        ),
        ais_gaps AS (
            -- Per-vessel AIS gaps: last known position + gap interval
            SELECT
                mmsi,
                timestamp        AS gap_start,
                LEAD(timestamp) OVER (PARTITION BY mmsi ORDER BY timestamp) AS gap_end,
                lat              AS last_lat,
                lon              AS last_lon
            FROM ais
            QUALIFY epoch(gap_end) - epoch(gap_start) > {gap_secs}
        )
        SELECT
            g.mmsi,
            COUNT(DISTINCT u.detection_id) AS unmatched_sar_detections_30d
        FROM unmatched_sar u
        JOIN ais_gaps g ON (
            u.detected_at >= g.gap_start
            AND u.detected_at <= g.gap_end
            AND ABS(u.lat - g.last_lat) <= {lat_attr}
            AND ABS(u.lon - g.last_lon) <= {lon_attr * 2}
            AND 2 * 6371 * ASIN(SQRT(
                POWER(SIN(RADIANS(g.last_lat - u.lat) / 2), 2) +
                COS(RADIANS(u.lat)) * COS(RADIANS(g.last_lat)) *
                POWER(SIN(RADIANS(g.last_lon - u.lon) / 2), 2)
            )) <= {attribution_radius_km}
        )
        GROUP BY g.mmsi
        """
    ).pl()

    if result.is_empty():
        return _empty

    return result.with_columns(pl.col("unmatched_sar_detections_30d").cast(pl.Int32))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute SAR-based vessel features")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--window", type=int, default=60, help="Rolling window (days)")
    parser.add_argument("--match-radius-km", type=float, default=MATCH_RADIUS_KM)
    parser.add_argument("--match-window-minutes", type=int, default=MATCH_WINDOW_MINUTES)
    parser.add_argument("--gap-threshold-hours", type=float, default=GAP_THRESHOLD_H)
    parser.add_argument("--attribution-radius-km", type=float, default=ATTRIBUTION_RADIUS_KM)
    args = parser.parse_args()

    result = compute_unmatched_sar_detections(
        db_path=args.db,
        window_days=args.window,
        match_radius_km=args.match_radius_km,
        match_window_minutes=args.match_window_minutes,
        gap_threshold_h=args.gap_threshold_hours,
        attribution_radius_km=args.attribution_radius_km,
    )
    print(f"SAR features: {len(result)} vessels with unmatched detections")
    print(result.head())
