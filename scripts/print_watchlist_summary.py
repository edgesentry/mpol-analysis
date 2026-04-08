"""Print a brief summary of a candidate watchlist parquet file.

Usage:
    uv run python scripts/print_watchlist_summary.py <watchlist_path>
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

path = Path(sys.argv[1]).resolve()
if not path.exists():
    print(f"Result: watchlist not found at {path}")
    raise SystemExit(0)

df = pl.read_parquet(path)
print(f"Result: watchlist rows = {df.height}")
if df.height > 0 and {"mmsi", "confidence"}.issubset(set(df.columns)):
    row = df.sort("confidence", descending=True).head(1).to_dicts()[0]
    print(f"Top candidate: mmsi={row.get('mmsi')} confidence={row.get('confidence')}")
print(f"Artifact: {path}")
