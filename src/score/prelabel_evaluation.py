"""Analyst pre-label holdout evaluation.

Evaluates the watchlist ranking model against an analyst-curated pre-label
holdout set — vessels labelled *suspected-positive* / *uncertain* /
*analyst-negative* BEFORE public sanctions confirmation.

Pre-labels can be loaded from:
  - the ``analyst_prelabels`` DuckDB table (default)
  - a standalone CSV file (for backtest pipeline integration via --prelabels-path)

Key controls:
  - Leakage guard: pre-labels with ``evidence_timestamp > window_end_date``
    are silently dropped before evaluation.
  - Confidence filtering: ``--min-confidence-tier`` restricts to high/medium
    labels only.

Outputs (JSON):
  - Pre-label slice metrics: precision@K, recall@K, AUROC (when computable)
  - Disagreement analysis:
      * model_high_analyst_negative — high-scoring vessels the analyst cleared
      * model_low_analyst_positive — low-scoring vessels the analyst suspects
  - Leakage report: count of labels dropped for the window
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
from sklearn.metrics import average_precision_score, roc_auc_score

PRE_LABEL_POSITIVE = "suspected-positive"
PRE_LABEL_NEGATIVE = "analyst-negative"
PRE_LABEL_UNCERTAIN = "uncertain"

VALID_PRE_LABELS = {PRE_LABEL_POSITIVE, PRE_LABEL_NEGATIVE, PRE_LABEL_UNCERTAIN}
VALID_CONFIDENCE_TIERS = {"high", "medium", "weak"}
CONFIDENCE_TIER_RANK = {"high": 3, "medium": 2, "weak": 1}


@dataclass(frozen=True)
class PrelabelWindow:
    """Parameters for a single pre-label evaluation window."""

    window_id: str
    watchlist_path: str
    end_date: str | None = None
    region: str | None = None
    min_confidence_tier: str = "weak"


def _normalize(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _parse_end_date(end_date: str | None) -> datetime | None:
    if not end_date:
        return None
    parsed = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def load_prelabels_from_db(
    db_path: str,
    end_date: str | None = None,
    region: str | None = None,
    min_confidence_tier: str = "weak",
) -> tuple[pl.DataFrame, int]:
    """Load analyst pre-labels from DuckDB, applying leakage and confidence filters.

    Returns (filtered_df, n_dropped_for_leakage).
    """
    end_dt = _parse_end_date(end_date)

    con = duckdb.connect(db_path)
    try:
        pdf = con.execute(
            """
            SELECT mmsi, imo, pre_label, confidence_tier, region,
                   evidence_notes, source_urls_json, analyst_id, evidence_timestamp
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY evidence_timestamp DESC) AS rn
                FROM analyst_prelabels
            )
            WHERE rn = 1
            """
        ).fetchdf()
    finally:
        con.close()

    if pdf.empty:
        return pl.DataFrame(), 0

    df = pl.from_pandas(pdf).with_columns(
        pl.col("mmsi").cast(pl.Utf8).str.strip_chars(),
        pl.col("pre_label").cast(pl.Utf8).str.to_lowercase().str.strip_chars(),
        pl.col("confidence_tier").cast(pl.Utf8).str.to_lowercase().str.strip_chars(),
    )
    return _filter_prelabels(df, end_dt, region, min_confidence_tier)


def load_prelabels_from_csv(
    csv_path: str,
    end_date: str | None = None,
    region: str | None = None,
    min_confidence_tier: str = "weak",
) -> tuple[pl.DataFrame, int]:
    """Load analyst pre-labels from a CSV file."""
    df = pl.read_csv(csv_path).with_columns(
        pl.col("mmsi").cast(pl.Utf8).str.strip_chars(),
        pl.col("pre_label").cast(pl.Utf8).str.to_lowercase().str.strip_chars(),
        pl.col("confidence_tier").cast(pl.Utf8).str.to_lowercase().str.strip_chars(),
    )
    end_dt = _parse_end_date(end_date)
    return _filter_prelabels(df, end_dt, region, min_confidence_tier)


def _filter_prelabels(
    df: pl.DataFrame,
    end_dt: datetime | None,
    region: str | None,
    min_confidence_tier: str,
) -> tuple[pl.DataFrame, int]:
    """Apply leakage, region, and confidence filters; return (filtered, n_leaked)."""
    n_before = df.height

    if end_dt is not None and "evidence_timestamp" in df.columns:
        df = df.with_columns(
            pl.col("evidence_timestamp")
            .cast(pl.Utf8)
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
            .alias("evidence_timestamp")
        )
        cutoff = pl.lit(end_dt).dt.replace_time_zone("UTC")
        df = df.filter(pl.col("evidence_timestamp") <= cutoff)

    n_leaked = n_before - df.height

    if region and "region" in df.columns:
        df = df.filter(pl.col("region").str.to_lowercase() == region.lower())

    min_rank = CONFIDENCE_TIER_RANK.get(min_confidence_tier.lower(), 1)
    if "confidence_tier" in df.columns:
        df = df.filter(
            pl.col("confidence_tier").map_elements(
                lambda t: CONFIDENCE_TIER_RANK.get(str(t).lower(), 0) >= min_rank,
                return_dtype=pl.Boolean,
            )
        )

    return df, n_leaked


def _label_watchlist(watchlist: pl.DataFrame, prelabels: pl.DataFrame) -> pl.DataFrame:
    """Join watchlist with pre-labels; add ``y_true`` column (1 / 0 / None)."""
    if prelabels.is_empty():
        return watchlist.with_columns(
            pl.lit(None, dtype=pl.Int8).alias("y_true"),
            pl.lit(PRE_LABEL_UNCERTAIN).alias("pre_label"),
            pl.lit("unknown").alias("confidence_tier"),
        )

    label_cols = [
        c for c in ["mmsi", "pre_label", "confidence_tier", "analyst_id", "evidence_notes"]
        if c in prelabels.columns
    ]
    slim = prelabels.select(label_cols).with_columns(
        pl.col("mmsi").cast(pl.Utf8).str.strip_chars()
    )

    joined = (
        watchlist.with_columns(pl.col("mmsi").cast(pl.Utf8).str.strip_chars())
        .join(slim, on="mmsi", how="left")
        .with_columns(
            pl.col("pre_label").fill_null(PRE_LABEL_UNCERTAIN),
            pl.col("confidence_tier").fill_null("unknown"),
        )
        .with_columns(
            pl.when(pl.col("pre_label") == PRE_LABEL_POSITIVE)
            .then(pl.lit(1, dtype=pl.Int8))
            .when(pl.col("pre_label") == PRE_LABEL_NEGATIVE)
            .then(pl.lit(0, dtype=pl.Int8))
            .otherwise(pl.lit(None, dtype=pl.Int8))
            .alias("y_true")
        )
    )
    return joined


def _precision_at_k(df: pl.DataFrame, k: int) -> float:
    if df.is_empty() or k <= 0:
        return 0.0
    head = df.head(min(k, df.height))
    return float(head["y_true"].cast(pl.Float64).mean() or 0.0)


def _recall_at_k(df: pl.DataFrame, k: int, positive_count: int) -> float:
    if df.is_empty() or k <= 0 or positive_count == 0:
        return 0.0
    hits = int(df.head(min(k, df.height))["y_true"].cast(pl.Int64).sum())
    return float(hits / positive_count)


def _disagreement_report(
    labeled: pl.DataFrame,
    threshold: float,
    display_cols: list[str],
    max_examples: int = 20,
) -> dict[str, Any]:
    """Build disagreement report: model vs analyst label."""
    if labeled.is_empty():
        return {
            "threshold_used": threshold,
            "model_high_analyst_negative": [],
            "model_low_analyst_positive": [],
        }

    cols = [c for c in display_cols if c in labeled.columns]

    model_high_neg = (
        labeled.filter(
            (pl.col("confidence") >= threshold) & (pl.col("y_true") == 0)
        )
        .sort("confidence", descending=True)
        .head(max_examples)
        .select(cols)
        .to_dicts()
    )
    model_low_pos = (
        labeled.filter(
            (pl.col("confidence") < threshold) & (pl.col("y_true") == 1)
        )
        .sort("confidence", descending=False)
        .head(max_examples)
        .select(cols)
        .to_dicts()
    )

    return {
        "threshold_used": round(threshold, 4),
        "model_high_analyst_negative_count": len(model_high_neg),
        "model_low_analyst_positive_count": len(model_low_pos),
        "model_high_analyst_negative": model_high_neg,
        "model_low_analyst_positive": model_low_pos,
    }


def evaluate_prelabel_window(
    window: PrelabelWindow,
    prelabels: pl.DataFrame,
    n_leaked: int,
    capacities: list[int],
    disagreement_threshold: float | None = None,
) -> dict[str, Any]:
    """Evaluate one watchlist window against the pre-label holdout set."""
    from src.storage.config import read_parquet as read_parquet_uri

    watchlist = read_parquet_uri(window.watchlist_path)
    if watchlist is None:
        raise FileNotFoundError(f"watchlist not found: {window.watchlist_path}")
    if "confidence" not in watchlist.columns:
        raise ValueError(f"watchlist missing 'confidence' column: {window.watchlist_path}")

    labeled_all = _label_watchlist(watchlist, prelabels).sort("confidence", descending=True)
    labeled = labeled_all.filter(pl.col("y_true").is_not_null())

    display_cols = [
        c for c in ["mmsi", "imo", "vessel_name", "vessel_type", "confidence",
                    "pre_label", "confidence_tier", "analyst_id", "evidence_notes"]
        if c in labeled_all.columns
    ]

    if labeled.is_empty():
        return {
            "window_id": window.window_id,
            "end_date": window.end_date,
            "region": window.region,
            "leakage_report": {
                "labels_dropped": n_leaked,
                "reason": "evidence_timestamp > window end_date",
            },
            "metrics": {
                "candidate_count": watchlist.height,
                "labeled_count": 0,
                "positive_count": 0,
                "precision_at_50": 0.0,
                "precision_at_100": 0.0,
                "recall_at_50": 0.0,
                "recall_at_100": 0.0,
                "auroc": None,
                "pr_auc": None,
            },
            "ops_thresholds": _ops_thresholds(labeled_all, capacities),
            "disagreement": _disagreement_report(labeled, disagreement_threshold or 0.7, display_cols),
        }

    ranked = labeled.sort("confidence", descending=True)
    y_true = ranked["y_true"].cast(pl.Int8).to_list()
    scores = ranked["confidence"].to_list()
    positive_count = int(sum(y_true))

    auroc = pr_auc = None
    if positive_count and positive_count != len(y_true):
        auroc = round(float(roc_auc_score(y_true, scores)), 4)
        pr_auc = round(float(average_precision_score(y_true, scores)), 4)

    threshold = disagreement_threshold or _best_f1_threshold(scores, y_true) or 0.7

    return {
        "window_id": window.window_id,
        "end_date": window.end_date,
        "region": window.region,
        "leakage_report": {
            "labels_dropped": n_leaked,
            "reason": "evidence_timestamp > window end_date",
        },
        "metrics": {
            "candidate_count": watchlist.height,
            "labeled_count": ranked.height,
            "positive_count": positive_count,
            "precision_at_50": round(_precision_at_k(ranked, 50), 4),
            "precision_at_100": round(_precision_at_k(ranked, 100), 4),
            "recall_at_50": round(_recall_at_k(ranked, 50, positive_count), 4),
            "recall_at_100": round(_recall_at_k(ranked, 100, positive_count), 4),
            "auroc": auroc,
            "pr_auc": pr_auc,
        },
        "ops_thresholds": _ops_thresholds(labeled_all, capacities),
        "disagreement": _disagreement_report(ranked, threshold, display_cols),
        "confidence_tier_breakdown": _tier_breakdown(ranked),
    }


def _best_f1_threshold(scores: list[float], labels: list[int]) -> float | None:
    if not scores or sum(labels) == 0 or sum(labels) == len(labels):
        return None
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
        prec = tp / (tp + fp) if (tp + fp) else 0.0
        rec = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
        if f1 > best_f1:
            best_f1 = f1
            best_thr = thr
    return best_thr


def _ops_thresholds(df: pl.DataFrame, capacities: list[int]) -> list[dict[str, Any]]:
    out = []
    for cap in capacities:
        if cap <= 0 or df.is_empty():
            out.append({"review_capacity": cap, "min_score": 1.0, "hit_rate": 0.0})
            continue
        top = df.head(min(cap, df.height))
        min_score = float(top["confidence"].min() or 0.0)
        known = top.filter(pl.col("y_true").is_not_null())
        hit_rate = float(known["y_true"].cast(pl.Float64).mean() or 0.0) if not known.is_empty() else 0.0
        out.append({
            "review_capacity": cap,
            "min_score": round(min_score, 4),
            "hit_rate": round(hit_rate, 4),
        })
    return out


def _tier_breakdown(df: pl.DataFrame) -> dict[str, Any]:
    """Metrics split by analyst confidence tier."""
    if "confidence_tier" not in df.columns or df.is_empty():
        return {}
    out: dict[str, Any] = {}
    for tier in ["high", "medium", "weak"]:
        sub = df.filter(pl.col("confidence_tier") == tier)
        if sub.is_empty():
            continue
        y = sub["y_true"].cast(pl.Int8).to_list()
        total = len(y)
        positives = sum(y)
        out[tier] = {
            "count": total,
            "positive_count": positives,
            "precision_at_50": round(_precision_at_k(sub, 50), 4),
        }
    return out


def run_prelabel_evaluation(
    watchlist_path: str,
    output_path: str,
    capacities: list[int],
    db_path: str | None = None,
    prelabels_csv: str | None = None,
    end_date: str | None = None,
    region: str | None = None,
    min_confidence_tier: str = "weak",
    disagreement_threshold: float | None = None,
) -> dict[str, Any]:
    """Run pre-label holdout evaluation and write JSON report.

    Exactly one of ``db_path`` or ``prelabels_csv`` must be provided.
    """
    if db_path and prelabels_csv:
        raise ValueError("Provide either db_path or prelabels_csv, not both")
    if not db_path and not prelabels_csv:
        raise ValueError("One of db_path or prelabels_csv is required")

    if db_path:
        prelabels, n_leaked = load_prelabels_from_db(
            db_path, end_date=end_date, region=region, min_confidence_tier=min_confidence_tier
        )
    else:
        prelabels, n_leaked = load_prelabels_from_csv(
            prelabels_csv, end_date=end_date, region=region, min_confidence_tier=min_confidence_tier  # type: ignore[arg-type]
        )

    window = PrelabelWindow(
        window_id="prelabel_eval",
        watchlist_path=watchlist_path,
        end_date=end_date,
        region=region,
        min_confidence_tier=min_confidence_tier,
    )

    window_result = evaluate_prelabel_window(
        window, prelabels, n_leaked, capacities,
        disagreement_threshold=disagreement_threshold,
    )

    report: dict[str, Any] = {
        "schema_version": "1.0",
        "generated_at": datetime.now(UTC).isoformat(),
        "config": {
            "watchlist_path": str(Path(watchlist_path).resolve()),
            "end_date": end_date,
            "region": region,
            "min_confidence_tier": min_confidence_tier,
            "review_capacities": capacities,
            "label_taxonomy": {
                "positive": PRE_LABEL_POSITIVE,
                "negative": PRE_LABEL_NEGATIVE,
                "ignored": PRE_LABEL_UNCERTAIN,
            },
        },
        "result": window_result,
    }

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run analyst pre-label holdout evaluation against a watchlist"
    )
    parser.add_argument("--watchlist", required=True, help="Watchlist parquet path")
    parser.add_argument("--db", default=None, help="DuckDB path (analyst_prelabels table)")
    parser.add_argument("--prelabels-csv", default=None, help="Pre-labels CSV path (alternative to --db)")
    parser.add_argument("--output", default="data/processed/prelabel_evaluation.json",
                        help="Output report JSON path")
    parser.add_argument("--end-date", default=None,
                        help="Leakage cutoff: drop labels with evidence_timestamp after this date (ISO-8601)")
    parser.add_argument("--region", default=None, help="Filter pre-labels to this region")
    parser.add_argument("--min-confidence-tier", default="weak",
                        choices=["high", "medium", "weak"],
                        help="Minimum analyst confidence tier to include")
    parser.add_argument("--review-capacities", default="25,50,100",
                        help="Comma-separated review capacities")
    parser.add_argument("--disagreement-threshold", type=float, default=None,
                        help="Score threshold for disagreement analysis (default: best-F1)")
    args = parser.parse_args()

    capacities = [int(x.strip()) for x in args.review_capacities.split(",") if x.strip()]
    report = run_prelabel_evaluation(
        watchlist_path=args.watchlist,
        output_path=args.output,
        capacities=capacities,
        db_path=args.db,
        prelabels_csv=args.prelabels_csv,
        end_date=args.end_date,
        region=args.region,
        min_confidence_tier=args.min_confidence_tier,
        disagreement_threshold=args.disagreement_threshold,
    )
    print(json.dumps(report["result"]["metrics"], indent=2))


if __name__ == "__main__":
    main()
