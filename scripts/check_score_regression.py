"""Score regression gate for CI.

Reads the JSON artefacts produced by run_public_backtest_batch.py and exits
non-zero if any metric violates its floor or ceiling.

Both floor AND ceiling checks matter:
  - Floor breach  → scoring is broken (e.g. AUROC < 0.65 means worse than random)
  - Ceiling breach → data leakage or label inflation (e.g. P@50 = 1.0 on real data
    is implausibly perfect and signals a seeding bug like #229)

Seed-mode / all-positive dataset handling
------------------------------------------
When the pipeline runs with --seed-dummy the labeled set contains only positive
vessels (labeled_count == positive_count across all windows).  Three checks need
special treatment in that case:

  - P@50 ceiling: trivially 1.0 when there are no labeled negatives; skipped.
  - AUROC: cannot be computed without at least one negative label; None is treated
    as "not applicable" and skipped rather than reported as a violation.
  - false_negatives: the backtest uses a fixed 0.7 threshold which produces many
    "false negatives" in seed mode even for vessels scoring 0.45-0.69.  Only
    vessels scoring below NEAR_ZERO_THRESHOLD (0.1) are counted as real failures,
    consistent with the issue's intent ("near-zero is a bug").

Exit codes
----------
  0  all checks passed
  1  one or more metric violations
  2  required input files are missing (pipeline did not run)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Vessels scoring below this are genuinely "near-zero" (the issue's intent).
# Vessels scoring 0.45–0.69 are not near-zero — they just fall below the
# backtest's default 0.7 recommended_threshold.
NEAR_ZERO_THRESHOLD = 0.10

# ---------------------------------------------------------------------------
# Thresholds (mirrors the table in issue #237)
# ---------------------------------------------------------------------------
THRESHOLDS: list[dict] = [
    {
        "metric": "precision_at_50",
        "floor": 0.25,
        "ceiling": 0.95,
        "rationale": "< 0.25 → scoring broken; > 0.95 → label inflation",
    },
    {
        "metric": "auroc",
        "floor": 0.65,
        "ceiling": 0.99,
        "rationale": "< 0.65 → worse than random; > 0.99 → data leakage",
    },
    {
        "metric": "recall_at_200",
        "floor": 0.50,
        "ceiling": None,
        "rationale": "all positives must be reachable in top 200",
    },
    {
        "metric": "total_known_cases",
        "floor": 5,
        "ceiling": 500,
        "rationale": "< 5 → eval DB empty; > 500 → label inflation",
    },
]


def _load_json(path: Path) -> dict:
    if not path.exists():
        print(f"[error] required file not found: {path}", flush=True)
        sys.exit(2)
    with path.open() as f:
        return json.load(f)


def _is_all_positive_dataset(report: dict) -> bool:
    """Return True when every labeled vessel is positive (seed mode / no negatives)."""
    for window in report.get("windows", []):
        m = window.get("metrics", {})
        labeled = m.get("labeled_count", 0)
        positive = m.get("positive_count", 0)
        if labeled > 0 and labeled != positive:
            return False
    return True


def _collect_metrics(summary: dict, report: dict) -> dict[str, float | int]:
    """Flatten the key metrics from both JSON files into a single dict."""
    ms = summary.get("metrics_summary", {})

    def _mean(key: str) -> float | None:
        block = ms.get(key, {})
        return block.get("mean") if isinstance(block, dict) else None

    # Count only truly near-zero false negatives (confidence < NEAR_ZERO_THRESHOLD).
    # The backtest's error_analysis uses a 0.7 default threshold which produces many
    # spurious "false negatives" in seed mode for vessels scoring 0.45–0.69.
    near_zero_fn = 0
    for window in report.get("windows", []):
        for fn in window.get("error_analysis", {}).get("false_negatives", []):
            if fn.get("confidence", 1.0) < NEAR_ZERO_THRESHOLD:
                near_zero_fn += 1

    return {
        "precision_at_50": _mean("precision_at_50"),
        "auroc": _mean_auroc(report),
        "recall_at_200": _mean("recall_at_200"),
        "total_known_cases": summary.get("total_known_cases"),
        "false_negatives": near_zero_fn,
        "skipped_regions": summary.get("skipped_regions", []),
        "_all_positive": _is_all_positive_dataset(report),
    }


def _mean_auroc(report: dict) -> float | None:
    """Average AUROC across all windows that have a value."""
    values = [
        w["metrics"]["auroc"]
        for w in report.get("windows", [])
        if w.get("metrics", {}).get("auroc") is not None
    ]
    return sum(values) / len(values) if values else None


def _print_summary_table(metrics: dict, violations: list[str]) -> None:
    all_positive = metrics.get("_all_positive", False)

    print()
    print("┌─────────────────────┬──────────────┬────────────┬────────────┬────────┐")
    print("│ Metric              │ Value        │ Floor      │ Ceiling    │ Status │")
    print("├─────────────────────┼──────────────┼────────────┼────────────┼────────┤")

    def _row(name: str, value, floor, ceiling, skip: bool = False) -> str:
        val_str = (
            "n/a" if value is None else (f"{value:.4f}" if isinstance(value, float) else str(value))
        )
        floor_str = f"{floor}" if floor is not None else "—"
        ceil_str = f"{ceiling}" if ceiling is not None else "—"
        if skip:
            status = "skip"
        elif value is None:
            status = "n/a"
        else:
            ok = True
            if floor is not None and value < floor:
                ok = False
            if ceiling is not None and value > ceiling:
                ok = False
            status = "✓" if ok else "✗ FAIL"
        return f"│ {name:<19} │ {val_str:<12} │ {floor_str:<10} │ {ceil_str:<10} │ {status:<6} │"

    for t in THRESHOLDS:
        skip = (t["metric"] == "auroc" and metrics.get("auroc") is None) or (
            t["metric"] == "precision_at_50" and t["ceiling"] is not None and all_positive
        )
        print(_row(t["metric"], metrics.get(t["metric"]), t["floor"], t["ceiling"], skip=skip))

    # false_negatives (ceiling only = 0, near-zero only)
    fn = metrics.get("false_negatives", 0)
    fn_ok = fn == 0
    print(
        f"│ {'false_negatives':<19} │ {str(fn):<12} │ {'—':<10} │ {f'0 (<{NEAR_ZERO_THRESHOLD})':<10} │ {'✓' if fn_ok else '✗ FAIL':<6} │"
    )

    print("└─────────────────────┴──────────────┴────────────┴────────────┴────────┘")

    skipped = metrics.get("skipped_regions", [])
    if skipped:
        print(f"\n[warn] skipped regions: {skipped}")

    print()
    if violations:
        print(f"RESULT: {len(violations)} violation(s) detected")
        for v in violations:
            print(f"  • {v}")
    else:
        print("RESULT: all checks passed ✓")
    print()


def run_checks(summary_path: Path, report_path: Path) -> list[str]:
    summary = _load_json(summary_path)
    report = _load_json(report_path)
    metrics = _collect_metrics(summary, report)
    all_positive = metrics.get("_all_positive", False)

    if all_positive:
        print(
            "[info] all labeled vessels are positive (seed/no-negative dataset) — "
            "P@50 ceiling and AUROC checks are skipped",
            flush=True,
        )

    violations: list[str] = []

    for t in THRESHOLDS:
        value = metrics.get(t["metric"])

        # AUROC: skip when None — backtest cannot compute ROC with zero labeled negatives.
        if t["metric"] == "auroc" and value is None:
            print("[info] AUROC = None (no labeled negatives) — skipping AUROC check", flush=True)
            continue

        # P@50 ceiling: skip when dataset has no labeled negatives.  P@50 is trivially
        # 1.0 in that case and does not indicate the #229 inflation bug.
        if t["metric"] == "precision_at_50" and t["ceiling"] is not None and all_positive:
            # Still enforce the floor — a broken scorer can score positives near-zero
            # even in seed mode.
            if value is not None and t["floor"] is not None and value < t["floor"]:
                violations.append(
                    f"{t['metric']} = {value:.4f} is below floor {t['floor']} ({t['rationale']})"
                )
            continue

        if value is None:
            violations.append(f"{t['metric']}: no value found in output files")
            continue
        if t["floor"] is not None and value < t["floor"]:
            violations.append(
                f"{t['metric']} = {value:.4f} is below floor {t['floor']} ({t['rationale']})"
            )
        if t["ceiling"] is not None and value > t["ceiling"]:
            violations.append(
                f"{t['metric']} = {value:.4f} exceeds ceiling {t['ceiling']} ({t['rationale']})"
            )

    fn = metrics.get("false_negatives", 0)
    if fn > 0:
        violations.append(
            f"false_negatives = {fn}: confirmed OFAC vessel(s) scoring near-zero "
            f"(confidence < {NEAR_ZERO_THRESHOLD}) — check MMSI/IMO matching in the sanctions join"
        )

    _print_summary_table(metrics, violations)
    return violations


def main() -> None:
    parser = argparse.ArgumentParser(description="Score regression gate for CI")
    parser.add_argument(
        "--summary",
        default="data/processed/backtest_public_integration_summary.json",
        help="Path to backtest_public_integration_summary.json",
    )
    parser.add_argument(
        "--report",
        default="data/processed/backtest_report_public_integration.json",
        help="Path to backtest_report_public_integration.json",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    summary_path = (project_root / args.summary).resolve()
    report_path = (project_root / args.report).resolve()

    violations = run_checks(summary_path, report_path)
    sys.exit(1 if violations else 0)


if __name__ == "__main__":
    main()
