#!/usr/bin/env python3
"""CLI entry point for analyst pre-label holdout evaluation.

Evaluates the watchlist ranking against the analyst-curated pre-label holdout
set and produces a JSON report with leading-indicator metrics and disagreement
analysis.

Usage (CSV mode):
    uv run python scripts/run_prelabel_evaluation.py \\
      --watchlist data/processed/candidate_watchlist.parquet \\
      --prelabels-csv data/demo/analyst_prelabels_demo.csv \\
      --output data/processed/prelabel_evaluation.json \\
      --end-date 2025-11-15 \\
      --min-confidence-tier medium

Usage (DB mode):
    uv run python scripts/run_prelabel_evaluation.py \\
      --watchlist data/processed/candidate_watchlist.parquet \\
      --db data/processed/mpol.duckdb \\
      --output data/processed/prelabel_evaluation.json \\
      --end-date 2025-11-15 \\
      --region singapore

See docs/prelabel-governance.md for full policy documentation.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.src.score.prelabel_evaluation import main

if __name__ == "__main__":
    main()
