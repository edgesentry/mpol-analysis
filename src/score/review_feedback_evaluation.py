"""Periodic feedback-driven evaluation using reviewed outcomes.

This job consumes the latest vessel review decisions, joins them with regional
watchlists, and produces tier-aware / operations-aware metrics plus threshold
recommendations and regression checks.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

POSITIVE_TIERS = {"confirmed", "probable", "suspect"}
NEGATIVE_TIERS = {"cleared"}
DEFAULT_WATCHLISTS = {
    "singapore": "data/processed/singapore_watchlist.parquet",
    "japan": "data/processed/japansea_watchlist.parquet",
    "middleeast": "data/processed/middleeast_watchlist.parquet",
    "europe": "data/processed/europe_watchlist.parquet",
    "gulf": "data/processed/gulf_watchlist.parquet",
}


@dataclass(frozen=True)
class DriftTolerance:
    precision_drop: float = 0.05
    recall_drop: float = 0.05


def _coerce_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    return default


def _coerce_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return default


def _parse_as_of(as_of_utc: str | None) -> str:
    if not as_of_utc:
        return datetime.now(UTC).isoformat()
    parsed = datetime.fromisoformat(as_of_utc.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def _latest_reviews(db_path: str, as_of_utc: str) -> pl.DataFrame:
    con = duckdb.connect(db_path, read_only=True)
    try:
        pdf = con.execute(
            """
            SELECT mmsi, review_tier, handoff_state, reviewed_by, reviewed_at
            FROM (
                SELECT
                    mmsi,
                    lower(trim(review_tier)) AS review_tier,
                    lower(trim(handoff_state)) AS handoff_state,
                    reviewed_by,
                    reviewed_at,
                    ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY reviewed_at DESC) AS rn
                FROM vessel_reviews
                WHERE reviewed_at <= CAST(? AS TIMESTAMPTZ)
            )
            WHERE rn = 1
            """,
            [as_of_utc],
        ).fetchdf()
    finally:
        con.close()

    if pdf.empty:
        return pl.DataFrame(
            {
                "mmsi": pl.Series([], dtype=pl.Utf8),
                "review_tier": pl.Series([], dtype=pl.Utf8),
                "handoff_state": pl.Series([], dtype=pl.Utf8),
                "reviewed_by": pl.Series([], dtype=pl.Utf8),
                "reviewed_at": pl.Series([], dtype=pl.Utf8),
            }
        )

    df = pl.from_pandas(pdf).with_columns(pl.col("mmsi").cast(pl.Utf8).str.strip_chars())
    return df


def _tier_to_label_expr() -> pl.Expr:
    return (
        pl.when(pl.col("review_tier").is_in(sorted(POSITIVE_TIERS)))
        .then(pl.lit(1, dtype=pl.Int8))
        .when(pl.col("review_tier").is_in(sorted(NEGATIVE_TIERS)))
        .then(pl.lit(0, dtype=pl.Int8))
        .otherwise(pl.lit(None, dtype=pl.Int8))
    )


def _precision_at_k(df: pl.DataFrame, k: int) -> float:
    if df.is_empty() or k <= 0:
        return 0.0
    head = df.head(min(k, df.height))
    return _coerce_float(head["y_true"].cast(pl.Float64).mean())


def _recall_at_k(df: pl.DataFrame, k: int, positive_count: int) -> float:
    if df.is_empty() or k <= 0 or positive_count == 0:
        return 0.0
    hits = int(df.head(min(k, df.height))["y_true"].cast(pl.Int64).sum())
    return float(hits / positive_count)


def _best_f1_threshold(scores: list[float], labels: list[int]) -> tuple[float | None, float]:
    if not scores or not labels:
        return None, 0.0
    positives = sum(labels)
    negatives = len(labels) - positives
    if positives == 0 or negatives == 0:
        return None, 0.0

    best_thr: float | None = None
    best_f1 = -1.0
    for thr in sorted(set(scores), reverse=True):
        tp = fp = fn = 0
        for s, y in zip(scores, labels):
            pred = 1 if s >= thr else 0
            if pred == 1 and y == 1:
                tp += 1
            elif pred == 1 and y == 0:
                fp += 1
            elif pred == 0 and y == 1:
                fn += 1
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
        if f1 > best_f1:
            best_f1 = f1
            best_thr = thr

    if best_f1 < 0:
        best_f1 = 0.0
    return best_thr, best_f1


def _ops_thresholds(df: pl.DataFrame, capacities: list[int]) -> list[dict[str, float | int]]:
    out: list[dict[str, float | int]] = []
    for cap in capacities:
        if cap <= 0 or df.is_empty():
            out.append({"review_capacity": cap, "min_score": 1.0, "hit_rate": 0.0})
            continue
        top = df.head(min(cap, df.height))
        min_score = _coerce_float(top["confidence"].min())
        hit_rate = _coerce_float(top["y_true"].cast(pl.Float64).mean())
        out.append(
            {
                "review_capacity": cap,
                "min_score": round(min_score, 4),
                "hit_rate": round(hit_rate, 4),
            }
        )
    return out


def _tier_counts(df: pl.DataFrame) -> dict[str, int]:
    if df.is_empty() or "review_tier" not in df.columns:
        return {}
    counts = df.group_by("review_tier").len().rename({"len": "n"}).sort("review_tier")
    return {str(r["review_tier"]): int(r["n"]) for r in counts.iter_rows(named=True)}


def _top_k_tier_mix(df: pl.DataFrame, capacities: list[int]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for cap in capacities:
        top = df.head(min(max(cap, 0), df.height)) if not df.is_empty() else df
        out.append(
            {
                "review_capacity": cap,
                "count": int(top.height),
                "tiers": _tier_counts(
                    top.select(["review_tier"]) if "review_tier" in top.columns else top
                ),
            }
        )
    return out


def _evaluate_region(
    region: str,
    watchlist_path: str,
    latest_reviews: pl.DataFrame,
    capacities: list[int],
) -> dict[str, object]:
    path = Path(watchlist_path)
    if not path.exists():
        return {
            "region": region,
            "watchlist_path": str(path.resolve()),
            "status": "missing_watchlist",
            "error": "watchlist file not found",
        }

    watchlist = pl.read_parquet(path)
    required_cols = {"mmsi", "confidence"}
    if not required_cols.issubset(set(watchlist.columns)):
        return {
            "region": region,
            "watchlist_path": str(path.resolve()),
            "status": "invalid_watchlist",
            "error": "watchlist missing required columns: mmsi, confidence",
        }

    ranked = (
        watchlist.with_columns(pl.col("mmsi").cast(pl.Utf8).str.strip_chars())
        .join(latest_reviews, on="mmsi", how="left")
        .with_columns(_tier_to_label_expr().alias("y_true"))
        .sort("confidence", descending=True)
    )

    labeled = ranked.filter(pl.col("y_true").is_not_null())
    if labeled.is_empty():
        return {
            "region": region,
            "watchlist_path": str(path.resolve()),
            "status": "ok",
            "candidate_count": int(ranked.height),
            "labeled_count": 0,
            "positive_count": 0,
            "tier_aware": {
                "review_tier_counts_labeled": {},
                "top_k_tier_mix": [],
            },
            "ops_aware": {
                "ops_thresholds": _ops_thresholds(labeled, capacities),
                "primary_capacity": capacities[0] if capacities else 0,
                "precision_at_primary_capacity": 0.0,
                "recall_at_primary_capacity": 0.0,
            },
            "threshold_recommendation": {
                "recommended_threshold": None,
                "f1_at_recommended": 0.0,
                "support": {
                    "labeled_count": 0,
                    "positive_count": 0,
                },
            },
        }

    positive_count = int(labeled["y_true"].cast(pl.Int64).sum())
    primary_capacity = capacities[0] if capacities else 0

    scores = labeled["confidence"].cast(pl.Float64).to_list()
    labels = labeled["y_true"].cast(pl.Int8).to_list()
    best_threshold, best_f1 = _best_f1_threshold(scores, labels)

    return {
        "region": region,
        "watchlist_path": str(path.resolve()),
        "status": "ok",
        "candidate_count": int(ranked.height),
        "labeled_count": int(labeled.height),
        "positive_count": positive_count,
        "tier_aware": {
            "review_tier_counts_labeled": _tier_counts(labeled.select(["review_tier"])),
            "top_k_tier_mix": _top_k_tier_mix(labeled, capacities),
        },
        "ops_aware": {
            "ops_thresholds": _ops_thresholds(labeled, capacities),
            "primary_capacity": primary_capacity,
            "precision_at_primary_capacity": round(_precision_at_k(labeled, primary_capacity), 4),
            "recall_at_primary_capacity": round(
                _recall_at_k(labeled, primary_capacity, positive_count), 4
            ),
        },
        "threshold_recommendation": {
            "recommended_threshold": round(float(best_threshold), 4)
            if best_threshold is not None
            else None,
            "f1_at_recommended": round(best_f1, 4),
            "support": {
                "labeled_count": int(labeled.height),
                "positive_count": positive_count,
            },
        },
    }


def _load_baseline_report(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    raw = json.loads(p.read_text())
    if isinstance(raw, dict):
        return raw
    return None


def _region_index(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    regions = report.get("regions", [])
    if not isinstance(regions, list):
        return out
    for row in regions:
        if isinstance(row, dict) and row.get("status") == "ok":
            region = str(row.get("region", ""))
            if region:
                out[region] = row
    return out


def _drift_checks(
    current: dict[str, Any],
    baseline: dict[str, Any] | None,
    tolerance: DriftTolerance,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    if baseline is None:
        return {
            "baseline_used": False,
            "overall_pass": True,
            "checks": checks,
            "note": "No baseline report provided; drift checks skipped.",
        }

    current_idx = _region_index(current)
    baseline_idx = _region_index(baseline)

    for region, cur in current_idx.items():
        prev = baseline_idx.get(region)
        if prev is None:
            checks.append(
                {
                    "region": region,
                    "metric": "baseline_presence",
                    "passed": True,
                    "detail": "No baseline for region; check skipped.",
                }
            )
            continue

        cur_ops = cur.get("ops_aware", {}) if isinstance(cur.get("ops_aware"), dict) else {}
        prev_ops = prev.get("ops_aware", {}) if isinstance(prev.get("ops_aware"), dict) else {}

        cur_p = float(cur_ops.get("precision_at_primary_capacity", 0.0))
        prev_p = float(prev_ops.get("precision_at_primary_capacity", 0.0))
        cur_r = float(cur_ops.get("recall_at_primary_capacity", 0.0))
        prev_r = float(prev_ops.get("recall_at_primary_capacity", 0.0))

        p_pass = cur_p >= (prev_p - tolerance.precision_drop)
        r_pass = cur_r >= (prev_r - tolerance.recall_drop)

        checks.append(
            {
                "region": region,
                "metric": "precision_at_primary_capacity",
                "baseline": round(prev_p, 4),
                "current": round(cur_p, 4),
                "allowed_drop": tolerance.precision_drop,
                "passed": p_pass,
            }
        )
        checks.append(
            {
                "region": region,
                "metric": "recall_at_primary_capacity",
                "baseline": round(prev_r, 4),
                "current": round(cur_r, 4),
                "allowed_drop": tolerance.recall_drop,
                "passed": r_pass,
            }
        )

    overall_pass = all(bool(c.get("passed", False)) for c in checks) if checks else True
    return {
        "baseline_used": True,
        "overall_pass": overall_pass,
        "checks": checks,
    }


def run_review_feedback_evaluation(
    db_path: str,
    output_path: str,
    capacities: list[int],
    watchlists: dict[str, str] | None = None,
    as_of_utc: str | None = None,
    baseline_report_path: str | None = None,
    tolerance: DriftTolerance | None = None,
) -> dict[str, Any]:
    if not capacities:
        raise ValueError("At least one review capacity is required")

    resolved_as_of = _parse_as_of(as_of_utc)
    watchlist_map = watchlists or DEFAULT_WATCHLISTS
    tol = tolerance or DriftTolerance()

    latest_reviews = _latest_reviews(db_path, resolved_as_of)

    regions: list[dict[str, object]] = []
    for region in sorted(watchlist_map):
        regions.append(_evaluate_region(region, watchlist_map[region], latest_reviews, capacities))

    successful = [r for r in regions if isinstance(r, dict) and r.get("status") == "ok"]
    summary = {
        "region_count": len(successful),
        "snapshot_review_count": int(latest_reviews.height),
        "total_labeled_count": int(sum(_coerce_int(r.get("labeled_count", 0)) for r in successful)),
    }

    report: dict[str, Any] = {
        "schema_version": "1.0",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "reproducibility": {
            "as_of_utc": resolved_as_of,
            "db_path": str(Path(db_path).resolve()),
            "watchlists": {k: str(Path(v).resolve()) for k, v in watchlist_map.items()},
            "review_capacities": capacities,
            "tier_mapping": {
                "positive": sorted(POSITIVE_TIERS),
                "negative": sorted(NEGATIVE_TIERS),
                "ignored": ["inconclusive", "unknown"],
            },
        },
        "regions": regions,
        "summary": summary,
    }

    baseline = _load_baseline_report(baseline_report_path)
    report["drift_regression_checks"] = _drift_checks(report, baseline, tol)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")
    return report


def _parse_watchlist_args(pairs: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in pairs:
        if "=" not in raw:
            raise ValueError(f"Invalid watchlist mapping: {raw}")
        region, path = raw.split("=", 1)
        region = region.strip().lower()
        path = path.strip()
        if not region or not path:
            raise ValueError(f"Invalid watchlist mapping: {raw}")
        out[region] = path
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run periodic reviewed-outcome feedback evaluation"
    )
    parser.add_argument("--db", default="data/processed/mpol.duckdb", help="DuckDB path")
    parser.add_argument(
        "--output",
        default="data/processed/review_feedback_evaluation.json",
        help="Output report JSON path",
    )
    parser.add_argument(
        "--review-capacities",
        default="25,50,100",
        help="Comma-separated review capacities used for ops metrics",
    )
    parser.add_argument(
        "--as-of-utc",
        default=None,
        help="Freeze review snapshot at this UTC timestamp (ISO-8601)",
    )
    parser.add_argument(
        "--watchlist",
        action="append",
        default=[],
        help="Override region watchlist path as region=path (repeatable)",
    )
    parser.add_argument(
        "--baseline-report",
        default=None,
        help="Optional previous report path for regression checks",
    )
    parser.add_argument(
        "--precision-drop-tolerance",
        type=float,
        default=0.05,
        help="Maximum allowed precision drop versus baseline",
    )
    parser.add_argument(
        "--recall-drop-tolerance",
        type=float,
        default=0.05,
        help="Maximum allowed recall drop versus baseline",
    )
    parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="Exit non-zero when drift/regression checks fail",
    )
    args = parser.parse_args()

    capacities = [int(x.strip()) for x in args.review_capacities.split(",") if x.strip()]
    watchlists = _parse_watchlist_args(args.watchlist) if args.watchlist else None

    report = run_review_feedback_evaluation(
        db_path=args.db,
        output_path=args.output,
        capacities=capacities,
        watchlists=watchlists,
        as_of_utc=args.as_of_utc,
        baseline_report_path=args.baseline_report,
        tolerance=DriftTolerance(
            precision_drop=args.precision_drop_tolerance,
            recall_drop=args.recall_drop_tolerance,
        ),
    )
    print(json.dumps(report["summary"], indent=2))

    checks = report.get("drift_regression_checks", {})
    overall_pass = bool(checks.get("overall_pass", True)) if isinstance(checks, dict) else True
    if args.fail_on_regression and not overall_pass:
        raise SystemExit("Regression checks failed")


if __name__ == "__main__":
    main()
