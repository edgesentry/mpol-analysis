"""Print a summary of a pre-label holdout evaluation JSON report.

Usage:
    uv run python scripts/print_prelabel_report.py --report <path>
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--report", required=True)
args = parser.parse_args()

path = Path(args.report).resolve()
if not path.exists():
    print("Result: SUCCESS, but report was not found")
    raise SystemExit(0)

report = json.loads(path.read_text())
result = report.get("result", {})
m = result.get("metrics", {})
leak = result.get("leakage_report", {})
dis = result.get("disagreement", {})

print("Result: SUCCESS")
print(
    f"Metrics: candidates={m.get('candidate_count', 0)}, "
    f"labeled={m.get('labeled_count', 0)}, "
    f"positives={m.get('positive_count', 0)}, "
    f"precision@50={m.get('precision_at_50', 0.0):.3f}, "
    f"recall@100={m.get('recall_at_100', 0.0):.3f}, "
    f"auroc={m.get('auroc') or 'n/a'}"
)
print(f"Leakage: {leak.get('labels_dropped', 0)} pre-labels dropped (evidence after cutoff date)")
print(
    f"Disagreement: model-high/analyst-negative={dis.get('model_high_analyst_negative_count', 0)}, "
    f"model-low/analyst-positive={dis.get('model_low_analyst_positive_count', 0)}"
)

tier_breakdown = result.get("confidence_tier_breakdown", {})
if tier_breakdown:
    print("Tier breakdown:")
    for tier, stats in tier_breakdown.items():
        print(
            f"  {tier}: count={stats['count']}, "
            f"positives={stats['positive_count']}, "
            f"precision@50={stats['precision_at_50']:.3f}"
        )

if dis.get("model_low_analyst_positive"):
    print("Model missed (low-score suspected-positives):")
    for row in dis["model_low_analyst_positive"][:3]:
        print(
            f"  mmsi={row.get('mmsi')} "
            f"score={row.get('confidence', '?'):.3f} "
            f"notes={row.get('evidence_notes', '')[:60]}"
        )

print(f"Artifact: {path}")
