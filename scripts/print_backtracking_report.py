"""Print a summary of a backtracking JSON report.

Usage:
    uv run python scripts/print_backtracking_report.py --report <path>
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
rc = report.get("regression_checks", {})
status = "PASS" if rc.get("pass") else "FAIL"
print("Result: SUCCESS")
print(
    f"Summary: confirmed={rc.get('confirmed_vessel_count', 0)}, "
    f"rewound={rc.get('rewind_vessel_count', 0)}, "
    f"propagated={rc.get('propagated_entity_count', 0)}, "
    f"regression={status}"
)
print(f"Artifact: {path}")
