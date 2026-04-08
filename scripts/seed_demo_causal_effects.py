"""Write a demo causal_effects.parquet with fixed ATT estimates for the dashboard.

Detects MinIO at localhost:9000 automatically (same logic as ops shell).

Usage:
    uv run python scripts/seed_demo_causal_effects.py
"""

from __future__ import annotations

import polars as pl

from src.storage.config import output_uri
from src.storage.config import write_parquet as write_parquet_uri

df = pl.DataFrame(
    {
        "regime": ["OFAC Iran", "OFAC Russia", "UN DPRK"],
        "n_treated": [18, 32, 11],
        "n_control": [142, 180, 95],
        "att_estimate": [0.42, 0.15, -0.05],
        "att_ci_lower": [0.31, -0.02, -0.18],
        "att_ci_upper": [0.53, 0.32, 0.08],
        "p_value": [0.0003, 0.09, 0.45],
        "is_significant": [True, False, False],
        "calibrated_weight": [0.55, 0.40, 0.40],
    }
)

uri = output_uri("causal_effects.parquet")
write_parquet_uri(df, uri)
print(f"Artifact: {uri}")
