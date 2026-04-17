"""CLI entry point for the delayed-label intelligence loop (Issue #67).

Wraps src/analysis/backtracking_runner.py.

Usage:
    uv run python scripts/run_backtracking.py --db data/processed/mpol.duckdb
    uv run python scripts/run_backtracking.py --since 2026-01-01T00:00:00Z
"""

from pipeline.src.analysis.backtracking_runner import main

if __name__ == "__main__":
    main()
