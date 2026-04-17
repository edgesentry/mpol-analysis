"""Run the unknown-unknown causal reasoner and print top candidates.

Usage:
    uv run python scripts/run_causal_reasoner.py --db <path> [--top-n N]
"""

from __future__ import annotations

import argparse

from pipeline.src.analysis.causal import score_unknown_unknowns
from pipeline.src.score.causal_sanction import run_causal_model

parser = argparse.ArgumentParser(description="Unknown-unknown causal reasoner")
parser.add_argument("--db", required=True, help="DuckDB path")
parser.add_argument("--top-n", type=int, default=5, help="Number of candidates to show")
args = parser.parse_args()

try:
    effects = run_causal_model(args.db)
    sig = sum(1 for e in effects if e.is_significant)
    print(f"C3 causal effects: {len(effects)} regimes, {sig} significant")
except Exception as exc:
    print(f"C3 model unavailable ({exc}), running without causal evidence")
    effects = []

candidates = score_unknown_unknowns(db_path=args.db, causal_effects=effects or None)
print(f"Unknown-unknown candidates: {len(candidates)}")
if not candidates:
    print("  (no vessels meet the minimum signal threshold)")
else:
    for c in candidates[: args.top_n]:
        signals = ", ".join(s.feature for s in c.matching_signals)
        print(f"  mmsi={c.mmsi}  score={c.causal_score:.3f}  signals=[{signals}]")
    print()
    print("Sample prompt context for top candidate:")
    print(candidates[0].prompt_context())
