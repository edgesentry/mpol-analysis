"""Inject SAR demo signals into the processed watchlist for the Cap Vista screenshot.

Modifies SARI NOUR (MMSI 613115678) in the active dashboard watchlist
(watchlist_uri() — singapore_watchlist.parquet by default) to include
unmatched_sar_detections_30d as the top SHAP signal and boosts confidence
to 0.85 so the vessel clears the dashboard min_confidence=0.4 filter.

Run this after use_demo_watchlist.py and before capture_screenshots.py.

Usage:
    uv run python scripts/seed_demo_sar.py
"""

from __future__ import annotations

import json

import polars as pl

from pipeline.src.storage.config import read_parquet, watchlist_uri, write_parquet

WATCHLIST_URI = watchlist_uri()
SAR_MMSI = "613115678"

SAR_SIGNALS = [
    {"feature": "unmatched_sar_detections_30d", "value": 3, "contribution": 0.24},
    {"feature": "ais_gap_count_30d", "value": 5, "contribution": 0.18},
    {"feature": "loitering_hours_30d", "value": 19.0, "contribution": 0.11},
    {"feature": "position_jump_count", "value": 1, "contribution": 0.06},
    {"feature": "sanctions_distance", "value": 0.31, "contribution": 0.04},
]


def main() -> None:
    df = read_parquet(WATCHLIST_URI)
    if df is None:
        raise SystemExit(
            f"Watchlist not found at {WATCHLIST_URI}. "
            "Run: uv run python scripts/use_demo_watchlist.py --backup\n"
            "or: uv run python scripts/sync_r2.py pull-demo"
        )

    if SAR_MMSI not in df["mmsi"].to_list():
        raise SystemExit(f"MMSI {SAR_MMSI} not found in watchlist.")

    # Also raise confidence so the vessel clears the dashboard min_confidence=0.4 filter.
    # Then sort by confidence descending so the vessel appears in head(top_n) — the
    # watchlist/top endpoint uses head() without a sort, so file order is the rank order.
    updated = df.with_columns(
        pl.when(pl.col("mmsi") == SAR_MMSI)
        .then(pl.lit(json.dumps(SAR_SIGNALS)))
        .otherwise(pl.col("top_signals"))
        .alias("top_signals"),
        pl.when(pl.col("mmsi") == SAR_MMSI)
        .then(pl.lit(0.85).cast(pl.Float64))
        .otherwise(pl.col("confidence"))
        .alias("confidence"),
    ).sort("confidence", descending=True)

    write_parquet(updated, WATCHLIST_URI)
    print(f"Injected SAR signals for MMSI {SAR_MMSI} into {WATCHLIST_URI}")
    print("  Top signal: unmatched_sar_detections_30d = 3 (contribution 0.24)")
    print("  confidence boosted to 0.85 (sorted to rank 1 so it appears in watchlist/top)")


if __name__ == "__main__":
    main()
