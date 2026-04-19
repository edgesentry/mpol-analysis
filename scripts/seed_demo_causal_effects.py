"""Write a demo causal_effects.parquet with per-vessel ATT estimates for the dashboard.

Reads MMSIs from candidate_watchlist.parquet in the data dir, then assigns each
vessel a regime and fixed ATT estimate so the causal-effect panel in VesselDetail
renders with realistic data.

Detects MinIO at localhost:9000 automatically (same logic as ops shell).

Usage:
    uv run python scripts/seed_demo_causal_effects.py
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import polars as pl

from pipeline.src.storage.config import output_uri
from pipeline.src.storage.config import write_parquet as write_parquet_uri

_REGIMES = ["OFAC Iran", "OFAC Russia", "UN DPRK"]
_DEFAULT_DATA_DIR = Path.home() / ".arktrace" / "data"


def _load_mmsis(data_dir: Path) -> list[str]:
    watchlist_path = data_dir / "candidate_watchlist.parquet"
    if not watchlist_path.exists():
        print(
            f"[warn] candidate_watchlist.parquet not found at {watchlist_path}. "
            "Using a fixed fallback set of 10 demo MMSIs.",
            file=sys.stderr,
        )
        return [str(i) for i in range(312171000, 312171010)]
    df = pl.read_parquet(watchlist_path, columns=["mmsi"])
    return df["mmsi"].to_list()


def make_causal_effects(mmsis: list[str], seed: int = 42) -> pl.DataFrame:
    """Assign each vessel a regime + deterministic ATT estimate (~90% coverage)."""
    rng = random.Random(seed)
    rows = []
    for mmsi in mmsis:
        if rng.random() > 0.1:  # 90% of vessels get an estimate
            att = round(rng.uniform(-0.1, 0.65), 3)
            half_width = round(rng.uniform(0.05, 0.15), 3)
            p = round(rng.uniform(0.001, 0.5), 4)
            rows.append(
                {
                    "mmsi": mmsi,
                    "regime": rng.choice(_REGIMES),
                    "att_estimate": att,
                    "att_ci_lower": round(att - half_width, 3),
                    "att_ci_upper": round(att + half_width, 3),
                    "p_value": p,
                    "is_significant": p < 0.05,
                }
            )
    return pl.DataFrame(rows)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        default=str(_DEFAULT_DATA_DIR),
        metavar="DIR",
        help=f"Directory containing candidate_watchlist.parquet (default: {_DEFAULT_DATA_DIR})",
    )
    args = parser.parse_args()
    data_dir = Path(args.data_dir).expanduser()

    mmsis = _load_mmsis(data_dir)
    df = make_causal_effects(mmsis)
    print(f"Generated causal_effects with {len(df)} per-vessel rows ({len(mmsis)} input MMSIs)")

    uri = output_uri("causal_effects.parquet")
    write_parquet_uri(df, uri)
    print(f"Artifact: {uri}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
