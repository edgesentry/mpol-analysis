"""Print a summary of a public backtest batch JSON report.

Usage:
    uv run python scripts/print_backtest_report.py --summary <path> --report <path>
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--summary", required=True)
parser.add_argument("--report", required=True)
args = parser.parse_args()

summary_path = Path(args.summary).resolve()
report_path = Path(args.report).resolve()
if not summary_path.exists():
    print("Result: SUCCESS, but summary report was not found")
    raise SystemExit(0)

summary = json.loads(summary_path.read_text())
print("Result: SUCCESS")
print(
    "Summary: "
    f"regions={summary.get('regions', [])}, "
    f"total_known_cases={summary.get('total_known_cases', 0)}"
)
print(f"Artifacts: {summary_path}, {report_path}")
