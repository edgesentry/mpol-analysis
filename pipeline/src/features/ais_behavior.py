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

# Chokepoint exit reference points (lat, lon) — positions ~5–10 nm outside each strait exit
# bearing away from the chokepoint into open water.  A gap onset within 50 nm of any of
# these points on an outbound heading is the "AIS compliance weaponization" signature.
_CHOKEPOINT_EXITS: list[tuple[float, float, str]] = [
    (1.16, 104.4, "singapore_east"),  # Singapore Strait eastern exit → South China Sea
    (1.28, 103.5, "singapore_west"),  # Singapore Strait western exit → Malacca
    (6.5, 99.8, "malacca_north"),  # Malacca Strait northern exit → Andaman Sea
    (26.5, 57.2, "hormuz_east"),  # Strait of Hormuz eastern exit → Gulf of Oman
    (12.4, 43.6, "babelMandeb_south"),  # Bab-el-Mandeb southern exit → Gulf of Aden
    (29.9, 32.6, "suez_south"),  # Suez Canal southern exit → Red Sea
    (36.2, 28.0, "aegean_south"),  # Turkish Straits / Aegean exit (Dardanelles)
    (-34.0, 18.4, "capetown"),  # Cape of Good Hope (no canal, used as diversion)
]
_CHOKEPOINT_EXIT_RADIUS_KM = 50.0 * 1.852  # 50 nautical miles in km
PRE_GAP_WINDOW_H = 6.0  # hours of transmission history to analyse before each gap


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


def _near_chokepoint_exit(lat: float, lon: float) -> bool:
    return any(
        _haversine_km(lat, lon, clat, clon) <= _CHOKEPOINT_EXIT_RADIUS_KM
        for clat, clon, _ in _CHOKEPOINT_EXITS
    )


def compute_chokepoint_gap_features(
    df: pl.DataFrame,
    gap_threshold_h: float = GAP_THRESHOLD_H,
    pre_gap_window_h: float = PRE_GAP_WINDOW_H,
) -> pl.DataFrame:
    """chokepoint_exit_gap_count and ais_pre_gap_regularity per MMSI.

    chokepoint_exit_gap_count — number of AIS gap onsets whose last known position
    is within 50 nm of a major chokepoint exit.  Near-zero false-positive rate for
    legitimate commercial traffic; high specificity for evasion.

    ais_pre_gap_regularity — mean coefficient of variation (std/mean) of AIS
    transmission intervals in the ``pre_gap_window_h`` hours before each gap onset,
    averaged across all qualifying gaps per MMSI.  Suspiciously low CV (machine-like
    regular transmissions) preceding a long dark period is the "AIS compliance
    weaponization" signature identified in the Al Jazeera 2026-04-30 investigation.
    Returns NaN (→ filled with 1.0 = noisy baseline) when no qualifying gap exists.
    """
    sorted_df = (
        df.lazy()
        .sort(["mmsi", "timestamp"])
        .with_columns(pl.col("timestamp").diff().over("mmsi").dt.total_minutes().alias("gap_min"))
        .collect()
    )

    gap_rows = sorted_df.filter(pl.col("gap_min") > gap_threshold_h * 60)
    if gap_rows.is_empty():
        return pl.DataFrame(
            {
                "mmsi": [],
                "chokepoint_exit_gap_count": [],
                "ais_pre_gap_regularity": [],
            },
            schema={
                "mmsi": pl.Utf8,
                "chokepoint_exit_gap_count": pl.Int32,
                "ais_pre_gap_regularity": pl.Float32,
            },
        )

    # --- chokepoint_exit_gap_count ---
    # Each row in gap_rows is the *first* position after a gap; the gap-onset position
    # is the row immediately before (previous lat/lon for that MMSI).  We already have
    # the previous timestamp via diff, but need the previous lat/lon.  Recompute with
    # shift so each gap row carries its onset coordinates.
    with_onset = (
        sorted_df.lazy()
        .sort(["mmsi", "timestamp"])
        .with_columns(
            [
                pl.col("lat").shift(1).over("mmsi").alias("onset_lat"),
                pl.col("lon").shift(1).over("mmsi").alias("onset_lon"),
            ]
        )
        .filter(pl.col("gap_min") > gap_threshold_h * 60)
        .drop_nulls(subset=["onset_lat", "onset_lon"])
        .collect()
    )

    with_choke = with_onset.with_columns(
        pl.struct(["onset_lat", "onset_lon"])
        .map_elements(
            lambda s: _near_chokepoint_exit(s["onset_lat"], s["onset_lon"]),
            return_dtype=pl.Boolean,
        )
        .alias("near_exit")
    )

    choke_counts = with_choke.group_by("mmsi").agg(
        pl.col("near_exit").sum().cast(pl.Int32).alias("chokepoint_exit_gap_count")
    )

    # --- ais_pre_gap_regularity ---
    # For each gap onset, collect all inter-message intervals in the preceding
    # pre_gap_window_h hours and compute CV.  We work in plain Python here because
    # variable-window group ops in Polars are verbose; the number of gap events is small.
    pre_gap_window_us = int(pre_gap_window_h * 3600 * 1_000_000)

    mmsi_ts = sorted_df.select(["mmsi", "timestamp"]).sort(["mmsi", "timestamp"])
    ts_by_mmsi: dict[str, list[int]] = {}
    for row in mmsi_ts.iter_rows(named=True):
        ts_by_mmsi.setdefault(row["mmsi"], []).append(
            row["timestamp"].timestamp() * 1_000_000
            if hasattr(row["timestamp"], "timestamp")
            else int(row["timestamp"])
        )

    cv_records: list[dict] = []
    for row in with_onset.iter_rows(named=True):
        mmsi = row["mmsi"]
        # onset timestamp: re-derive from gap row timestamp minus gap_min.
        gap_start_us = (
            int(row["timestamp"].timestamp() * 1_000_000)
            if hasattr(row["timestamp"], "timestamp")
            else int(row["timestamp"])
        ) - int(row["gap_min"] * 60 * 1_000_000)

        window_start_us = gap_start_us - pre_gap_window_us
        ts_list = ts_by_mmsi.get(mmsi, [])
        window_ts = [t for t in ts_list if window_start_us <= t <= gap_start_us]
        if len(window_ts) < 3:
            continue
        intervals = [window_ts[i + 1] - window_ts[i] for i in range(len(window_ts) - 1)]
        mean_iv = sum(intervals) / len(intervals)
        if mean_iv == 0:
            continue
        std_iv = (sum((x - mean_iv) ** 2 for x in intervals) / len(intervals)) ** 0.5
        cv_records.append({"mmsi": mmsi, "cv": std_iv / mean_iv})

    if cv_records:
        cv_df = pl.DataFrame(cv_records, schema={"mmsi": pl.Utf8, "cv": pl.Float64})
        regularity = cv_df.group_by("mmsi").agg(
            pl.col("cv").mean().cast(pl.Float32).alias("ais_pre_gap_regularity")
        )
    else:
        regularity = pl.DataFrame(
            {"mmsi": [], "ais_pre_gap_regularity": []},
            schema={"mmsi": pl.Utf8, "ais_pre_gap_regularity": pl.Float32},
        )

    return choke_counts.join(regularity, on="mmsi", how="full", coalesce=True)


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
            "chokepoint_exit_gap_count": pl.Int32,
            "ais_pre_gap_regularity": pl.Float32,
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
    choke = compute_chokepoint_gap_features(df, gap_threshold_h)

    return (
        all_mmsi.lazy()
        .join(gaps.lazy(), on="mmsi", how="left")
        .join(jumps.lazy(), on="mmsi", how="left")
        .join(sts.lazy(), on="mmsi", how="left")
        .join(loiter.lazy(), on="mmsi", how="left")
        .join(port.lazy(), on="mmsi", how="left")
        .join(choke.lazy(), on="mmsi", how="left")
        .with_columns(
            [
                pl.col("ais_gap_count_30d").fill_null(0).cast(pl.Int32),
                pl.col("ais_gap_max_hours").fill_null(0.0).cast(pl.Float32),
                pl.col("position_jump_count").fill_null(0).cast(pl.Int32),
                pl.col("sts_candidate_count").fill_null(0).cast(pl.Int32),
                pl.col("port_call_ratio").fill_null(0.5).cast(pl.Float32),
                pl.col("loitering_hours_30d").fill_null(0.0).cast(pl.Float32),
                pl.col("chokepoint_exit_gap_count").fill_null(0).cast(pl.Int32),
                # 1.0 = noisy/normal baseline; low CV = suspiciously machine-like
                pl.col("ais_pre_gap_regularity").fill_null(1.0).cast(pl.Float32),
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
