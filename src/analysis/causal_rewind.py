"""
Causal rewind analysis for newly confirmed shadow-fleet vessels.

For each confirmed vessel, reconstructs behavioral feature snapshots over the
trailing 12 months before the confirmation timestamp and identifies precursor
signals: features elevated in the 0–90-day pre-confirmation window vs the
90–365-day baseline window.

Output JSON report per vessel:
  mmsi, confirmed_at, ais_records_scanned, rewind_days,
  precursor_signals[], monthly_snapshots[]

Usage:
    uv run python src/analysis/causal_rewind.py --db data/processed/mpol.duckdb
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb
import polars as pl
from dotenv import load_dotenv

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")
DEFAULT_OUTPUT_PATH = os.getenv(
    "CAUSAL_REWIND_OUTPUT_PATH", "data/processed/causal_rewind.json"
)

REWIND_DAYS = 365
PRECURSOR_WINDOW_DAYS = 90
SNAPSHOT_INTERVAL_DAYS = 30
GAP_THRESHOLD_H = 6.0
PRECURSOR_UPLIFT_THRESHOLD = 1.5   # 50 % uplift triggers a signal

FEATURE_KEYS = ["ais_gap_count", "sts_candidate_proxy", "low_sog_fraction"]


def _load_ais_for_vessel(
    db_path: str, mmsi: str, start_dt: datetime, end_dt: datetime
) -> pl.DataFrame:
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute(
            """
            SELECT mmsi, timestamp, lat, lon,
                   COALESCE(sog, 0.0)      AS sog,
                   COALESCE(nav_status, 0) AS nav_status
            FROM ais_positions
            WHERE mmsi = ?
              AND timestamp >= ?
              AND timestamp < ?
            ORDER BY timestamp
            """,
            [mmsi, start_dt, end_dt],
        ).pl()
    finally:
        con.close()
    # Normalise to UTC so downstream comparisons with tz-aware Python datetimes work.
    if df["timestamp"].dtype != pl.Datetime("us", "UTC"):
        df = df.with_columns(pl.col("timestamp").dt.convert_time_zone("UTC"))
    return df


def _count_ais_gaps(df: pl.DataFrame) -> int:
    if len(df) < 2:
        return 0
    gaps = (
        df.sort("timestamp")
        .with_columns(pl.col("timestamp").diff().dt.total_minutes().alias("gap_min"))
        .filter(pl.col("gap_min") > GAP_THRESHOLD_H * 60)
    )
    return len(gaps)


def _sts_candidate_proxy(df: pl.DataFrame) -> int:
    """Count stopped records (anchor/underway) that are not moored — proxy for STS activity."""
    STOPPED_STATUSES = [0, 1, 3]  # underway, at-anchor, restricted (not PORT_MOORED=5)
    if df.is_empty():
        return 0
    return df.filter(pl.col("nav_status").is_in(STOPPED_STATUSES)).height


def _compute_snapshot_features(df: pl.DataFrame) -> dict[str, float]:
    if df.is_empty():
        return {k: 0.0 for k in FEATURE_KEYS} | {"record_count": 0.0}
    n = len(df)
    return {
        "ais_gap_count": float(_count_ais_gaps(df)),
        "sts_candidate_proxy": float(_sts_candidate_proxy(df)),
        "low_sog_fraction": float((df["sog"] < 2.0).sum()) / max(n, 1),
        "record_count": float(n),
    }


def compute_monthly_snapshots(
    ais_df: pl.DataFrame,
    confirmed_at: datetime,
    rewind_days: int = REWIND_DAYS,
    interval_days: int = SNAPSHOT_INTERVAL_DAYS,
) -> list[dict[str, Any]]:
    """Compute feature snapshots over rolling windows leading up to confirmed_at."""
    snapshots: list[dict[str, Any]] = []
    start = confirmed_at - timedelta(days=rewind_days)
    cursor = start
    while cursor < confirmed_at:
        window_end = min(cursor + timedelta(days=interval_days), confirmed_at)
        window_df = ais_df.filter(
            (pl.col("timestamp") >= cursor) & (pl.col("timestamp") < window_end)
        )
        features = _compute_snapshot_features(window_df)
        snapshots.append(
            {
                "window_start": cursor.isoformat(),
                "window_end": window_end.isoformat(),
                "days_before_confirmation": int(
                    (confirmed_at - cursor).total_seconds() / 86400
                ),
                **features,
            }
        )
        cursor = window_end
    return snapshots


def detect_precursor_signals(
    snapshots: list[dict[str, Any]],
    precursor_window_days: int = PRECURSOR_WINDOW_DAYS,
) -> list[dict[str, Any]]:
    """Compare feature averages in recent vs baseline windows.

    A feature is flagged as a precursor signal when its recent average exceeds
    the baseline average by more than PRECURSOR_UPLIFT_THRESHOLD.
    """
    precursor = [s for s in snapshots if s["days_before_confirmation"] <= precursor_window_days]
    baseline = [s for s in snapshots if s["days_before_confirmation"] > precursor_window_days]

    if not precursor or not baseline:
        return []

    signals = []
    for feature in FEATURE_KEYS:
        recent_avg = sum(s[feature] for s in precursor) / len(precursor)
        baseline_avg = sum(s[feature] for s in baseline) / len(baseline)
        uplift = recent_avg / (baseline_avg + 1e-9)
        if uplift > PRECURSOR_UPLIFT_THRESHOLD:
            signals.append(
                {
                    "feature": feature,
                    "recent_value": round(recent_avg, 4),
                    "baseline_value": round(baseline_avg, 4),
                    "uplift_ratio": round(uplift, 4),
                }
            )

    return sorted(signals, key=lambda x: x["uplift_ratio"], reverse=True)


def rewind_vessel(
    db_path: str,
    mmsi: str,
    confirmed_at: datetime,
    rewind_days: int = REWIND_DAYS,
) -> dict[str, Any]:
    """Run causal rewind for a single confirmed vessel."""
    if confirmed_at.tzinfo is None:
        confirmed_at = confirmed_at.replace(tzinfo=timezone.utc)
    start_dt = confirmed_at - timedelta(days=rewind_days)
    ais_df = _load_ais_for_vessel(db_path, mmsi, start_dt, confirmed_at)
    snapshots = compute_monthly_snapshots(ais_df, confirmed_at, rewind_days)
    precursor_signals = detect_precursor_signals(snapshots)
    return {
        "mmsi": mmsi,
        "confirmed_at": confirmed_at.isoformat(),
        "ais_records_scanned": len(ais_df),
        "rewind_days": rewind_days,
        "precursor_signals": precursor_signals,
        "monthly_snapshots": snapshots,
    }


def run_causal_rewind(
    db_path: str,
    output_path: str,
    mmsis: list[str] | None = None,
    as_of_utc: str | None = None,
    rewind_days: int = REWIND_DAYS,
) -> dict[str, Any]:
    """Run causal rewind for all confirmed vessels (or a specific list)."""
    cutoff = as_of_utc or datetime.now(timezone.utc).isoformat()
    con = duckdb.connect(db_path, read_only=True)
    try:
        seeds_df = con.execute(
            """
            SELECT mmsi, MAX(reviewed_at) AS confirmed_at
            FROM vessel_reviews
            WHERE review_tier = 'confirmed'
              AND reviewed_at <= ?
            GROUP BY mmsi
            """,
            [cutoff],
        ).pl()
    finally:
        con.close()

    if mmsis is not None:
        seeds_df = seeds_df.filter(pl.col("mmsi").is_in(mmsis))

    if not seeds_df.is_empty() and seeds_df["confirmed_at"].dtype != pl.Datetime("us", "UTC"):
        seeds_df = seeds_df.with_columns(
            pl.col("confirmed_at").dt.convert_time_zone("UTC")
        )

    vessel_results = []
    for row in seeds_df.iter_rows(named=True):
        confirmed_dt = row["confirmed_at"]
        if isinstance(confirmed_dt, str):
            confirmed_dt = datetime.fromisoformat(confirmed_dt)
        if confirmed_dt.tzinfo is None:
            confirmed_dt = confirmed_dt.replace(tzinfo=timezone.utc)
        vessel_results.append(rewind_vessel(db_path, row["mmsi"], confirmed_dt, rewind_days))

    report: dict[str, Any] = {
        "as_of_utc": cutoff,
        "rewind_days": rewind_days,
        "vessel_count": len(vessel_results),
        "vessels": vessel_results,
    }
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Causal rewind analysis for confirmed vessels"
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument(
        "--mmsis",
        default=None,
        help="Comma-separated MMSI list (default: all confirmed)",
    )
    parser.add_argument("--as-of-utc", default=None)
    parser.add_argument("--rewind-days", type=int, default=REWIND_DAYS)
    args = parser.parse_args()

    mmsis = [m.strip() for m in args.mmsis.split(",")] if args.mmsis else None
    report = run_causal_rewind(
        args.db, args.output, mmsis, args.as_of_utc, args.rewind_days
    )
    print(f"Vessel count: {report['vessel_count']}")
    print(f"Artifact: {args.output}")


if __name__ == "__main__":
    main()
