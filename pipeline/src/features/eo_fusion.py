"""
EO-AIS fusion feature engineering.

Computes two features per vessel (MMSI) from EO (Electro-Optical satellite
imagery) vessel detections cross-referenced against AIS position records:

eo_dark_count_30d
    Number of EO detections in the 30-day window that have no matching AIS
    broadcast within ``match_radius_deg`` degrees and ``match_window_minutes``
    minutes, yet are attributable to this vessel (i.e. they fall during an AIS
    gap and are spatially close to the vessel's last known position).

eo_ais_mismatch_ratio
    dark_count / total_attributed_count for the vessel.  Zero when the vessel
    has no attributed EO detections; 1.0 when every attributed detection was
    dark (no concurrent AIS).

Algorithm
---------
1. Load EO detections and AIS positions for the rolling window.
2. Label each EO detection as *matched* if any AIS broadcast is within
   ``match_radius_deg`` and ``match_window_minutes``.
3. Attribute both matched and unmatched EO detections to vessels:
   - *Matched*: the AIS broadcast identifies the vessel directly.
   - *Unmatched*: attributed when the detection falls inside a vessel's AIS gap
     and is within ``attribution_radius_deg`` of the vessel's last position.
4. Aggregate per MMSI → (eo_dark_count_30d, eo_ais_mismatch_ratio).

Output columns
--------------
    mmsi, eo_dark_count_30d, eo_ais_mismatch_ratio

Usage
-----
    uv run python src/features/eo_fusion.py
    uv run python src/features/eo_fusion.py --skip-eo   # returns empty frame
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

# EO-AIS match parameters (degrees ≈ ~11 km at 0.1°)
MATCH_RADIUS_DEG: float = 0.1
MATCH_WINDOW_MINUTES: int = 120

# Gap & attribution parameters
GAP_THRESHOLD_H: float = 6.0
ATTRIBUTION_RADIUS_DEG: float = 0.5  # ~55 km


def load_eo_window(db_path: str, window_days: int = 30) -> pl.DataFrame:
    """Load EO detections from the last *window_days*."""
    cutoff = datetime.now(UTC) - timedelta(days=window_days)
    con = duckdb.connect(db_path, read_only=True)
    try:
        return con.execute(
            """
            SELECT detection_id, detected_at, lat, lon
            FROM eo_detections
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


def compute_eo_features(
    db_path: str = DEFAULT_DB_PATH,
    window_days: int = 60,
    match_radius_deg: float = MATCH_RADIUS_DEG,
    match_window_minutes: int = MATCH_WINDOW_MINUTES,
    gap_threshold_h: float = GAP_THRESHOLD_H,
    attribution_radius_deg: float = ATTRIBUTION_RADIUS_DEG,
    skip_eo: bool = False,
) -> pl.DataFrame:
    """Compute ``eo_dark_count_30d`` and ``eo_ais_mismatch_ratio`` per MMSI.

    Returns DataFrame with columns [mmsi, eo_dark_count_30d, eo_ais_mismatch_ratio].
    Vessels with no attributed EO detections are omitted (filled downstream).
    """
    _empty = pl.DataFrame(
        schema={
            "mmsi": pl.Utf8,
            "eo_dark_count_30d": pl.Int32,
            "eo_ais_mismatch_ratio": pl.Float32,
        }
    )

    if skip_eo:
        return _empty

    eo_df = load_eo_window(db_path, window_days)
    if eo_df.is_empty():
        return _empty

    ais_df = _load_ais_window(db_path, window_days)
    if ais_df.is_empty():
        return _empty

    match_secs = match_window_minutes * 60
    gap_secs = gap_threshold_h * 3600

    con = duckdb.connect()
    con.register("eo", eo_df)
    con.register("ais", ais_df)

    result: pl.DataFrame = con.execute(
        f"""
        WITH matched_eo AS (
            -- EO detections that have a nearby AIS broadcast (vessel was visible on AIS)
            SELECT DISTINCT e.detection_id, a.mmsi
            FROM eo e
            JOIN ais a ON (
                ABS(a.lat - e.lat) <= {match_radius_deg}
                AND ABS(a.lon - e.lon) <= {match_radius_deg}
                AND ABS(epoch(a.timestamp) - epoch(e.detected_at)) <= {match_secs}
            )
        ),
        unmatched_eo AS (
            SELECT * FROM eo
            WHERE detection_id NOT IN (SELECT detection_id FROM matched_eo)
        ),
        ais_gaps AS (
            -- Per-vessel AIS gaps with last known position
            SELECT
                mmsi,
                timestamp        AS gap_start,
                LEAD(timestamp) OVER (PARTITION BY mmsi ORDER BY timestamp) AS gap_end,
                lat              AS last_lat,
                lon              AS last_lon
            FROM ais
            QUALIFY epoch(gap_end) - epoch(gap_start) > {gap_secs}
        ),
        attributed_dark AS (
            -- Unmatched EO detections attributed to a vessel via AIS gap + proximity
            SELECT DISTINCT g.mmsi, u.detection_id
            FROM unmatched_eo u
            JOIN ais_gaps g ON (
                u.detected_at >= g.gap_start
                AND u.detected_at <= g.gap_end
                AND ABS(u.lat - g.last_lat) <= {attribution_radius_deg}
                AND ABS(u.lon - g.last_lon) <= {attribution_radius_deg}
            )
        ),
        attributed_matched AS (
            -- Matched EO detections attributed directly via AIS
            SELECT DISTINCT mmsi, detection_id FROM matched_eo
        ),
        all_attributed AS (
            SELECT mmsi, detection_id, 1 AS is_dark FROM attributed_dark
            UNION
            SELECT mmsi, detection_id, 0 AS is_dark FROM attributed_matched
            WHERE (mmsi, detection_id) NOT IN (SELECT mmsi, detection_id FROM attributed_dark)
        )
        SELECT
            mmsi,
            SUM(is_dark)::INTEGER                                           AS eo_dark_count_30d,
            (SUM(is_dark)::FLOAT / COUNT(*))::FLOAT                        AS eo_ais_mismatch_ratio
        FROM all_attributed
        GROUP BY mmsi
        """
    ).pl()

    if result.is_empty():
        return _empty

    return result.with_columns(
        [
            pl.col("eo_dark_count_30d").cast(pl.Int32),
            pl.col("eo_ais_mismatch_ratio").cast(pl.Float32),
        ]
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute EO fusion features")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--window", type=int, default=60)
    parser.add_argument("--match-radius-deg", type=float, default=MATCH_RADIUS_DEG)
    parser.add_argument("--match-window-minutes", type=int, default=MATCH_WINDOW_MINUTES)
    parser.add_argument("--gap-threshold-hours", type=float, default=GAP_THRESHOLD_H)
    parser.add_argument("--attribution-radius-deg", type=float, default=ATTRIBUTION_RADIUS_DEG)
    parser.add_argument(
        "--skip-eo", action="store_true", help="Skip EO computation (returns empty frame)"
    )
    args = parser.parse_args()

    result = compute_eo_features(
        db_path=args.db,
        window_days=args.window,
        match_radius_deg=args.match_radius_deg,
        match_window_minutes=args.match_window_minutes,
        gap_threshold_h=args.gap_threshold_hours,
        attribution_radius_deg=args.attribution_radius_deg,
        skip_eo=args.skip_eo,
    )
    print(f"EO features: {len(result)} vessels")
    print(result.head())
