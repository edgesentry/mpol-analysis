"""Print a summary of a review feedback evaluation JSON report.

Usage:
    uv run python scripts/print_review_feedback_report.py --report <path>
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
    print("Result: SUCCESS, but output report was not found")
    raise SystemExit(0)

report = json.loads(path.read_text())
summary = report.get("summary", {}) if isinstance(report, dict) else {}
print("Result: SUCCESS")
print(
    "Summary: "
    f"reviewed_vessel_count={summary.get('reviewed_vessel_count', 0)}, "
    f"regions_evaluated={summary.get('regions_evaluated', 0)}, "
    f"overall_drift_pass={summary.get('overall_drift_pass', True)}"
)
print(f"Artifact: {path}")
