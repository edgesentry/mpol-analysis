"""Edge gateway re-score benchmark.

Seeds a temporary DuckDB with synthetic AIS data for N vessels (default 5,000),
then times the incremental re-score pipeline:

    build_matrix.py  (feature engineering)
    composite.py     (HDBSCAN + Isolation Forest + SHAP + composite score)
    watchlist.py     (Parquet output)

This is the pipeline that runs on every live-streaming batch.  The Cap Vista
proposal claims this completes in under 30 seconds on a 4-core / 4 GB edge
gateway (Raspberry Pi 4 / NVIDIA Jetson Nano class).

To reproduce with hardware constraints (Docker):

    docker run --rm --cpus 4 --memory 4g \\
        -v $(pwd):/app -w /app \\
        ghcr.io/edgesentry/mpol-dashboard:latest \\
        uv run python scripts/benchmark_rescore.py --vessels 5000

Usage:
    uv run python scripts/benchmark_rescore.py
    uv run python scripts/benchmark_rescore.py --vessels 5000 --seed 42
    uv run python scripts/benchmark_rescore.py --keep   # keep the temp DB after run
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import duckdb  # noqa: E402

from pipeline.src.features.build_matrix import (  # noqa: E402
    build_feature_matrix,
    write_vessel_features,
)
from pipeline.src.ingest.schema import init_schema  # noqa: E402
from pipeline.src.score.composite import compute_composite_scores  # noqa: E402
from pipeline.src.score.watchlist import write_candidate_watchlist  # noqa: E402


def _seed_vessels(db_path: str, n_vessels: int, rng: random.Random) -> None:
    """Populate ais_positions and vessel_meta with n_vessels synthetic records."""
    init_schema(db_path)
    con = duckdb.connect(db_path)

    # Singapore Strait bounding box — realistic operational area
    LAT_MIN, LAT_MAX = 1.0, 1.5
    LON_MIN, LON_MAX = 103.5, 104.2

    # Generate MMSIs
    mmsis = [str(200_000_000 + i) for i in range(n_vessels)]

    # AIS positions: 10 position fixes per vessel over the last 30 days
    base_ts = datetime(2026, 3, 10, 0, 0, 0, tzinfo=UTC)
    positions = []
    for mmsi in mmsis:
        lat = rng.uniform(LAT_MIN, LAT_MAX)
        lon = rng.uniform(LON_MIN, LON_MAX)
        ship_type = rng.choice([70, 71, 72, 80, 81, 82])
        for j in range(10):
            ts = base_ts + timedelta(days=j * 3, hours=rng.randint(0, 23))
            positions.append(
                (
                    mmsi,
                    ts,
                    round(lat + rng.uniform(-0.05, 0.05), 6),
                    round(lon + rng.uniform(-0.05, 0.05), 6),
                    round(rng.uniform(0, 15), 1),  # sog
                    round(rng.uniform(0, 360), 1),  # cog
                    rng.randint(0, 8),  # nav_status
                    ship_type,
                )
            )

    con.executemany(
        "INSERT OR IGNORE INTO ais_positions VALUES (?,?,?,?,?,?,?,?)",
        positions,
    )

    # vessel_meta: one row per vessel
    flags = ["SG", "PA", "LR", "MH", "BS", "IR", "RU", ""]
    meta = [
        (
            mmsi,
            f"IMO{9_000_000 + i}",
            f"VESSEL {i:05d}",
            rng.choice(flags),
            rng.choice([70, 80]),
            float(rng.randint(5_000, 150_000)),
        )
        for i, mmsi in enumerate(mmsis)
    ]
    con.executemany(
        "INSERT OR IGNORE INTO vessel_meta VALUES (?,?,?,?,?,?)",
        meta,
    )
    con.close()
    print(f"  Seeded {n_vessels} vessels ({n_vessels * 10} AIS fixes)")


def _fmt(seconds: float) -> str:
    return f"{seconds:.2f}s"


def main() -> None:
    parser = argparse.ArgumentParser(description="Edge gateway re-score benchmark")
    parser.add_argument("--vessels", type=int, default=5_000, help="Number of vessels to seed")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--keep", action="store_true", help="Keep temp DB after run")
    parser.add_argument("--output", default="/tmp/benchmark_watchlist.parquet")
    args = parser.parse_args()

    rng = random.Random(args.seed)

    db_path = str(Path(tempfile.mkdtemp()) / "benchmark.duckdb")

    try:
        print(f"\n{'=' * 60}")
        print("  arktrace edge gateway re-score benchmark")
        print(f"  Vessels: {args.vessels:,}  |  seed: {args.seed}")
        print(f"  DB: {db_path}")
        print(f"{'=' * 60}\n")

        # ── Seed ──────────────────────────────────────────────────────────
        print("[1/4] Seeding synthetic AIS data ...")
        t0 = time.perf_counter()
        _seed_vessels(db_path, args.vessels, rng)
        t_seed = time.perf_counter() - t0
        print(f"  Done in {_fmt(t_seed)}\n")

        # ── Feature matrix ────────────────────────────────────────────────
        print("[2/4] Building feature matrix (build_matrix.py) ...")
        t0 = time.perf_counter()
        feature_df = build_feature_matrix(db_path, skip_graph=True, skip_eo=True)
        n_features = write_vessel_features(db_path, feature_df)
        t_features = time.perf_counter() - t0
        print(f"  {n_features} vessel rows written in {_fmt(t_features)}\n")

        # ── Composite scoring (HDBSCAN + IF + SHAP) ───────────────────────
        print("[3/4] Composite scoring (composite.py) ...")
        t0 = time.perf_counter()
        scored_df = compute_composite_scores(db_path)
        t_score = time.perf_counter() - t0
        print(f"  {scored_df.height} vessels scored in {_fmt(t_score)}\n")

        # ── Watchlist output ──────────────────────────────────────────────
        print("[4/4] Writing watchlist parquet (watchlist.py) ...")
        t0 = time.perf_counter()
        write_candidate_watchlist(scored_df, args.output)
        t_watchlist = time.perf_counter() - t0
        print(f"  Written to {args.output} in {_fmt(t_watchlist)}\n")

        # ── Summary ───────────────────────────────────────────────────────
        t_pipeline = t_features + t_score + t_watchlist
        t_total = t_seed + t_pipeline

        print(f"{'=' * 60}")
        print(f"  RESULTS  ({args.vessels:,} vessels)")
        print(f"{'=' * 60}")
        print(f"  Seed (excluded from pipeline time)  {_fmt(t_seed):>10}")
        print(f"  Feature matrix (build_matrix)        {_fmt(t_features):>10}")
        print(f"  Composite score (HDBSCAN + IF + SHAP){_fmt(t_score):>10}")
        print(f"  Watchlist output                     {_fmt(t_watchlist):>10}")
        print(f"  ── Pipeline total ──────────────────  {_fmt(t_pipeline):>10}")
        print(f"  Total wall-clock (incl. seed)        {_fmt(t_total):>10}")
        print(f"{'=' * 60}")

        target = 30.0
        if t_pipeline <= target:
            print(f"  ✓ PASS — pipeline completes in {_fmt(t_pipeline)} (target: <{target:.0f}s)")
        else:
            print(f"  ✗ FAIL — pipeline takes {_fmt(t_pipeline)} (target: <{target:.0f}s)")
        print()

        # Machine info for documentation
        import platform

        cpu_count = os.cpu_count() or 1
        print(
            f"  Host: {platform.node()}  |  CPUs: {cpu_count}  |  Python {platform.python_version()}"
        )
        print()

    finally:
        if args.keep:
            print(f"DB retained at {db_path}")
        else:
            Path(db_path).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
