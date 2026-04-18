"""Read drift monitor JSON from stdin and print a human-readable summary.

Usage:
    uv run python src/analysis/monitor.py --db <db> --json | uv run python scripts/print_monitor_summary.py
"""

from __future__ import annotations

import json
import sys

data = json.load(sys.stdin)
s = data["summary"]
print(f"Result: ok={s['ok']}  warning={s['warning']}  critical={s['critical']}")
for a in data["alerts"]:
    icon = {"ok": "✓", "warning": "⚠", "critical": "✗"}.get(a["severity"], "?")
    print(f"  {icon} [{a['severity'].upper()}] {a['check_name']}: {a['message']}")
