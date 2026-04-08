"""Inject SAR demo signals into the processed watchlist for the Cap Vista screenshot.

Modifies SARI NOUR (MMSI 613115678) in data/processed/candidate_watchlist.parquet
to include unmatched_sar_detections_30d as the top SHAP signal.  Run this after
use_demo_watchlist.py and before capture_screenshots.py.

Usage:
    uv run python scripts/seed_demo_sar.py
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

WATCHLIST_PATH = Path("data/processed/candidate_watchlist.parquet")
SAR_MMSI = "613115678"

SAR_SIGNALS = [
    {"feature": "unmatched_sar_detections_30d", "value": 3, "contribution": 0.24},
    {"feature": "ais_gap_count_30d", "value": 5, "contribution": 0.18},
    {"feature": "loitering_hours_30d", "value": 19.0, "contribution": 0.11},
    {"feature": "position_jump_count", "value": 1, "contribution": 0.06},
    {"feature": "sanctions_distance", "value": 0.31, "contribution": 0.04},
]


def main() -> None:
    if not WATCHLIST_PATH.exists():
        raise SystemExit(
            f"Watchlist not found at {WATCHLIST_PATH}. "
            "Run: uv run python scripts/use_demo_watchlist.py --backup"
        )

    df = pl.read_parquet(WATCHLIST_PATH)

    if SAR_MMSI not in df["mmsi"].to_list():
        raise SystemExit(f"MMSI {SAR_MMSI} not found in watchlist.")

    updated = df.with_columns(
        pl.when(pl.col("mmsi") == SAR_MMSI)
        .then(pl.lit(json.dumps(SAR_SIGNALS)))
        .otherwise(pl.col("top_signals"))
        .alias("top_signals")
    )

    updated.write_parquet(WATCHLIST_PATH)
    print(f"Injected SAR signals for MMSI {SAR_MMSI} into {WATCHLIST_PATH}")
    print(f"  Top signal: unmatched_sar_detections_30d = 3 (contribution 0.24)")


if __name__ == "__main__":
    main()
