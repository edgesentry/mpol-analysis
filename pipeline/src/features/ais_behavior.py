"""
AIS behavioral feature engineering.

Reads ais_positions from DuckDB, computes all behavioral features using the
Polars lazy API, and returns a DataFrame with one row per MMSI.

Output columns:
    mmsi, ais_gap_count_30d, ais_gap_max_hours, position_jump_count,
    sts_candidate_count, port_call_ratio, loitering_hours_30d

Usage:
    uv run python src/features/ais_behavior.py
"""

import math
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import polars as pl
from dotenv import load_dotenv

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")

GAP_THRESHOLD_H = 10  # gaps longer than this (hours) are anomalous
# Raised from 6h → 10h: Singapore/Malacca anchorage wait times are 8-12h
# (normal commercial behaviour).  Genuine shadow-fleet dark periods are
# 12-48h (STS transfer duration) so real evasion is still captured.
JUMP_SPEED_KNOTS = 50.0  # implied speed above this = GPS spoofing
LOITER_SPEED_KNOTS = 2.0  # SOG below this (outside port) = loitering
H3_RESOLUTION = 8  # ~0.7 km cell edge ≈ 0.5 nm for STS detection
STOPPED_STATUSES = [0, 1, 3, 5]  # underway, at anchor, restricted, moored
PORT_MOORED_STATUS = 5


def _geo_to_h3(lat: float, lon: float, res: int) -> str:
    """H3 cell index — compatible with h3-py 3.x and 4.x."""
    import h3

    try:
        return h3.latlng_to_cell(lat, lon, res)  # h3-py >= 4
    except AttributeError:
        return h3.geo_to_h3(lat, lon, res)  # h3-py < 4 fallback


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return 2 * R * math.asin(math.sqrt(max(a, 0.0)))


def load_ais_window(db_path: str, window_days: int = 30) -> pl.DataFrame:
    """Load AIS positions from the last *window_days* from DuckDB."""
    cutoff = datetime.now(UTC) - timedelta(days=window_days)
    con = duckdb.connect(db_path, read_only=True)
    try:
        return con.execute(
            """
            SELECT mmsi, timestamp, lat, lon,
                   COALESCE(sog, 0.0)        AS sog,
                   COALESCE(nav_status, 0)   AS nav_status
            FROM ais_positions
            WHERE timestamp >= ?
            ORDER BY mmsi, timestamp
        """,
            [cutoff],
        ).pl()
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Individual feature computations
# ---------------------------------------------------------------------------


def compute_gap_features(
    df: pl.DataFrame, gap_threshold_h: float = GAP_THRESHOLD_H
) -> pl.DataFrame:
    """ais_gap_count_30d and ais_gap_max_hours per MMSI."""
    return (
        df.lazy()
        .sort(["mmsi", "timestamp"])
        .with_columns(pl.col("timestamp").diff().over("mmsi").dt.total_minutes().alias("gap_min"))
        .filter(pl.col("gap_min") > gap_threshold_h * 60)
        .group_by("mmsi")
        .agg(
            [
                pl.len().alias("ais_gap_count_30d"),
                (pl.col("gap_min").max() / 60.0).alias("ais_gap_max_hours"),
            ]
        )
        .collect()
    )


def compute_position_jumps(df: pl.DataFrame) -> pl.DataFrame:
    """position_jump_count per MMSI (implied speed > 50 knots within 1-hour windows)."""
    sorted_df = (
        df.sort(["mmsi", "timestamp"])
        .with_columns(
            [
                pl.col("lat").shift(1).over("mmsi").alias("prev_lat"),
                pl.col("lon").shift(1).over("mmsi").alias("prev_lon"),
                pl.col("timestamp").shift(1).over("mmsi").alias("prev_ts"),
            ]
        )
        .drop_nulls(subset=["prev_lat", "prev_lon", "prev_ts"])
    )

    with_delta = sorted_df.with_columns(
        (
            (pl.col("timestamp").cast(pl.Int64) - pl.col("prev_ts").cast(pl.Int64))
            / 1_000_000  # µs → seconds (Polars Datetime casts to µs since epoch)
            / 3600.0
        ).alias("delta_h")
    ).filter((pl.col("delta_h") > 0) & (pl.col("delta_h") <= 1.0))

    if with_delta.is_empty():
        return pl.DataFrame(
            {"mmsi": [], "position_jump_count": []},
            schema={"mmsi": pl.Utf8, "position_jump_count": pl.Int32},
        )

    with_speed = with_delta.with_columns(
        pl.struct(["lat", "lon", "prev_lat", "prev_lon"])
        .map_elements(
            lambda s: _haversine_km(s["lat"], s["lon"], s["prev_lat"], s["prev_lon"]) / 1.852,
            return_dtype=pl.Float64,
        )
        .truediv(pl.col("delta_h"))
        .alias("implied_knots")
    ).filter(pl.col("implied_knots") > JUMP_SPEED_KNOTS)

    return with_speed.group_by("mmsi").agg(pl.len().alias("position_jump_count"))


def compute_sts_candidates(
    df: pl.DataFrame,
    deep_cells: frozenset[str] | None = None,
) -> pl.DataFrame:
    """sts_candidate_count per MMSI via H3 co-location (≥2 vessels, same cell, same 30-min bucket).

    When ``deep_cells`` is provided (a frozenset of H3 resolution-8 cell IDs where
    GEBCO depth ≤ -200 m), co-locations in shallow water are excluded.  This
    removes false positives from port anchorages and shallow straits (e.g.
    Malacca Strait narrows to ~25 m in some sections).
    """
    stopped = df.filter(pl.col("nav_status").is_in(STOPPED_STATUSES))
    if stopped.is_empty():
        return pl.DataFrame(
            {"mmsi": [], "sts_candidate_count": []},
            schema={"mmsi": pl.Utf8, "sts_candidate_count": pl.Int32},
        )

    stopped = stopped.with_columns(
        [
            pl.struct(["lat", "lon"])
            .map_elements(
                lambda s: _geo_to_h3(s["lat"], s["lon"], H3_RESOLUTION),
                return_dtype=pl.Utf8,
            )
            .alias("h3_cell"),
            pl.col("timestamp").dt.truncate("30m").alias("ts_bucket"),
        ]
    )

    if deep_cells is not None:
        stopped = stopped.filter(pl.col("h3_cell").is_in(deep_cells))
        if stopped.is_empty():
            return pl.DataFrame(
                {"mmsi": [], "sts_candidate_count": []},
                schema={"mmsi": pl.Utf8, "sts_candidate_count": pl.Int32},
            )

    multi = (
        stopped.group_by(["h3_cell", "ts_bucket"])
        .agg(
            [
                pl.col("mmsi").n_unique().alias("vessel_count"),
                pl.col("mmsi").alias("mmsi_list"),
            ]
        )
        .filter(pl.col("vessel_count") >= 2)
    )

    if multi.is_empty():
        return pl.DataFrame(
            {"mmsi": [], "sts_candidate_count": []},
            schema={"mmsi": pl.Utf8, "sts_candidate_count": pl.Int32},
        )

    return (
        multi.explode("mmsi_list")
        .group_by("mmsi_list")
        .agg(pl.len().alias("sts_candidate_count"))
        .rename({"mmsi_list": "mmsi"})
    )


def compute_loitering(df: pl.DataFrame) -> pl.DataFrame:
    """loitering_hours_30d: total hours at SOG < 2 knots, not declared as moored."""
    return (
        df.lazy()
        .filter((pl.col("sog") < LOITER_SPEED_KNOTS) & (pl.col("nav_status") != PORT_MOORED_STATUS))
        .sort(["mmsi", "timestamp"])
        .with_columns(pl.col("timestamp").diff().over("mmsi").dt.total_minutes().alias("delta_min"))
        .filter(pl.col("delta_min").is_not_null() & (pl.col("delta_min") <= 60))
        .group_by("mmsi")
        .agg((pl.col("delta_min").sum() / 60.0).alias("loitering_hours_30d"))
        .collect()
    )


def compute_port_call_ratio(df: pl.DataFrame) -> pl.DataFrame:
    """port_call_ratio = fraction of stopped time declared as moored (nav_status=5)."""
    return (
        df.lazy()
        .filter(pl.col("nav_status").is_in([1, 5]))
        .group_by("mmsi")
        .agg(
            [
                (pl.col("nav_status") == PORT_MOORED_STATUS).sum().cast(pl.Float32).alias("moored"),
                pl.len().cast(pl.Float32).alias("total_stopped"),
            ]
        )
        .with_columns((pl.col("moored") / pl.col("total_stopped")).alias("port_call_ratio"))
        .select(["mmsi", "port_call_ratio"])
        .collect()
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def _load_deep_cells(db_path: str) -> frozenset[str] | None:
    """Load GEBCO deep-cell mask for the region inferred from db_path.

    Looks for ``{stem}_deep_cells.parquet`` alongside the DB file.
    Returns None (no filter) if the mask is not found.
    """
    p = Path(db_path)
    mask_path = p.parent / f"{p.stem}_deep_cells.parquet"
    if not mask_path.exists():
        return None
    cells = pl.read_parquet(mask_path)["h3_cell"].to_list()
    return frozenset(cells)


def compute_ais_features(
    db_path: str = DEFAULT_DB_PATH,
    window_days: int = 60,
    gap_threshold_h: float = GAP_THRESHOLD_H,
) -> pl.DataFrame:
    """Compute all AIS behavioral features. Returns DataFrame one row per MMSI."""
    df = load_ais_window(db_path, window_days)

    _empty = pl.DataFrame(
        schema={
            "mmsi": pl.Utf8,
            "ais_gap_count_30d": pl.Int32,
            "ais_gap_max_hours": pl.Float32,
            "position_jump_count": pl.Int32,
            "sts_candidate_count": pl.Int32,
            "port_call_ratio": pl.Float32,
            "loitering_hours_30d": pl.Float32,
        }
    )
    if df.is_empty():
        return _empty

    deep_cells = _load_deep_cells(db_path)
    all_mmsi = df.select("mmsi").unique()
    gaps = compute_gap_features(df, gap_threshold_h)
    jumps = compute_position_jumps(df)
    sts = compute_sts_candidates(df, deep_cells=deep_cells)
    loiter = compute_loitering(df)
    port = compute_port_call_ratio(df)

    return (
        all_mmsi.lazy()
        .join(gaps.lazy(), on="mmsi", how="left")
        .join(jumps.lazy(), on="mmsi", how="left")
        .join(sts.lazy(), on="mmsi", how="left")
        .join(loiter.lazy(), on="mmsi", how="left")
        .join(port.lazy(), on="mmsi", how="left")
        .with_columns(
            [
                pl.col("ais_gap_count_30d").fill_null(0).cast(pl.Int32),
                pl.col("ais_gap_max_hours").fill_null(0.0).cast(pl.Float32),
                pl.col("position_jump_count").fill_null(0).cast(pl.Int32),
                pl.col("sts_candidate_count").fill_null(0).cast(pl.Int32),
                pl.col("port_call_ratio").fill_null(0.5).cast(pl.Float32),
                pl.col("loitering_hours_30d").fill_null(0.0).cast(pl.Float32),
            ]
        )
        .collect()
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Compute AIS behavioral features")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--window", type=int, default=60, help="Rolling window (days)")
    parser.add_argument(
        "--gap-threshold-hours",
        type=float,
        default=GAP_THRESHOLD_H,
        help="AIS gap threshold in hours (default: 6; use 12 for DPRK/Iran analysis)",
    )
    args = parser.parse_args()

    result = compute_ais_features(args.db, args.window, args.gap_threshold_hours)
    print(f"AIS features: {len(result)} vessels")
    print(result.head())
