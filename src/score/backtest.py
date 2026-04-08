"""Backtesting pipeline for historical shadow-fleet candidate evaluation.

This module evaluates ranked watchlists against a labeled historical corpus
defined in a versioned manifest.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from sklearn.metrics import average_precision_score, roc_auc_score

from src.storage.config import read_parquet as read_parquet_uri


@dataclass(frozen=True)
class BacktestWindow:
    window_id: str
    watchlist_path: str
    labels_path: str
    start_date: str | None = None
    end_date: str | None = None
    region: str | None = None


TRUE_LABELS = {"positive", "confirmed", "probable"}
FALSE_LABELS = {"negative", "cleared"}


def _normalize_id(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def load_manifest(manifest_path: str) -> tuple[str, list[BacktestWindow]]:
    raw = json.loads(Path(manifest_path).read_text())
    schema_version = str(raw.get("schema_version", "1"))
    windows_raw = raw.get("windows")
    if not isinstance(windows_raw, list) or not windows_raw:
        raise ValueError("Manifest must include non-empty 'windows' list")

    base = Path(manifest_path).resolve().parent
    windows: list[BacktestWindow] = []
    for i, row in enumerate(windows_raw):
        if not isinstance(row, dict):
            raise ValueError(f"Window at index {i} is not an object")
        window_id = str(row.get("window_id", f"window_{i + 1}"))
        watchlist_path = str(row["watchlist_path"])
        labels_path = str(row["labels_path"])
        if not Path(watchlist_path).is_absolute():
            watchlist_path = str((base / watchlist_path).resolve())
        if not Path(labels_path).is_absolute():
            labels_path = str((base / labels_path).resolve())
        windows.append(
            BacktestWindow(
                window_id=window_id,
                watchlist_path=watchlist_path,
                labels_path=labels_path,
                start_date=row.get("start_date"),
                end_date=row.get("end_date"),
                region=row.get("region"),
            )
        )
    return schema_version, windows


def _load_labels(labels_path: str) -> pl.DataFrame:
    labels = pl.read_csv(labels_path)
    if "label" not in labels.columns:
        raise ValueError(f"labels file missing 'label' column: {labels_path}")
    if "mmsi" not in labels.columns and "imo" not in labels.columns:
        raise ValueError(f"labels file must include 'mmsi' and/or 'imo': {labels_path}")

    cols = []
    if "mmsi" in labels.columns:
        cols.append(pl.col("mmsi").cast(pl.Utf8).str.strip_chars().alias("mmsi"))
    else:
        cols.append(pl.lit("").alias("mmsi"))
    if "imo" in labels.columns:
        cols.append(pl.col("imo").cast(pl.Utf8).str.strip_chars().alias("imo"))
    else:
        cols.append(pl.lit("").alias("imo"))

    conf_expr = (
        pl.col("label_confidence").cast(pl.Utf8).str.to_lowercase().str.strip_chars()
        if "label_confidence" in labels.columns
        else pl.lit("unknown")
    )
    source_expr = (
        pl.col("evidence_source").cast(pl.Utf8).str.strip_chars()
        if "evidence_source" in labels.columns
        else pl.lit("unknown")
    )
    url_expr = (
        pl.col("evidence_url").cast(pl.Utf8).str.strip_chars()
        if "evidence_url" in labels.columns
        else pl.lit("")
    )

    return labels.with_columns(
        *cols,
        pl.col("label").cast(pl.Utf8).str.to_lowercase().str.strip_chars().alias("label"),
        conf_expr.alias("label_confidence"),
        source_expr.alias("evidence_source"),
        url_expr.alias("evidence_url"),
    )


def _source_positive_coverage(
    ranked_all: pl.DataFrame,
    labels: pl.DataFrame,
    capacities: list[int],
) -> dict[str, object]:
    positives = labels.filter(pl.col("label").is_in(sorted(TRUE_LABELS)))
    if positives.is_empty():
        return {
            "source_positive_total": 0,
            "matched_total": 0,
            "missed_total": 0,
            "source_recall_in_watchlist": 0.0,
            "detected_in_top_k": [],
            "matched_examples": [],
            "missed_examples": [],
        }

    ranked_rows = ranked_all.select(
        [
            c
            for c in ["mmsi", "imo", "vessel_name", "vessel_type", "confidence"]
            if c in ranked_all.columns
        ]
    ).to_dicts()

    mmsi_idx: dict[str, tuple[int, dict[str, object]]] = {}
    imo_idx: dict[str, tuple[int, dict[str, object]]] = {}
    for idx, row in enumerate(ranked_rows, start=1):
        mmsi = _normalize_id(row.get("mmsi"))
        imo = _normalize_id(row.get("imo"))
        if mmsi and mmsi not in mmsi_idx:
            mmsi_idx[mmsi] = (idx, row)
        if imo and imo not in imo_idx:
            imo_idx[imo] = (idx, row)

    matched: list[dict[str, object]] = []
    missed: list[dict[str, object]] = []
    for row in positives.select(
        [
            c
            for c in ["mmsi", "imo", "label_confidence", "evidence_source", "evidence_url"]
            if c in positives.columns
        ]
    ).iter_rows(named=True):
        mmsi = _normalize_id(row.get("mmsi"))
        imo = _normalize_id(row.get("imo"))
        hit: tuple[int, dict[str, object]] | None = None
        if mmsi and mmsi in mmsi_idx:
            hit = mmsi_idx[mmsi]
        elif imo and imo in imo_idx:
            hit = imo_idx[imo]

        common = {
            "mmsi": mmsi,
            "imo": imo,
            "label_confidence": str(row.get("label_confidence") or "unknown"),
            "evidence_source": str(row.get("evidence_source") or "unknown"),
            "evidence_url": str(row.get("evidence_url") or ""),
        }

        if hit is None:
            missed.append(common)  # type: ignore[arg-type]
            continue

        rank, watch = hit
        matched.append(
            {
                **common,
                "rank": rank,
                "vessel_name": watch.get("vessel_name"),
                "vessel_type": watch.get("vessel_type"),
                "watchlist_confidence": watch.get("confidence"),
            }
        )

    detected_in_top_k: list[dict[str, int | float]] = []
    matched_ranks = [int(x["rank"]) for x in matched]  # type: ignore[call-overload]
    total = len(positives)
    for k in capacities:
        hits = sum(1 for r in matched_ranks if r <= k)
        detected_in_top_k.append(
            {
                "k": k,
                "hits": hits,
                "recall": round((hits / total) if total else 0.0, 4),
            }
        )

    return {
        "source_positive_total": total,
        "matched_total": len(matched),
        "missed_total": len(missed),
        "source_recall_in_watchlist": round((len(matched) / total) if total else 0.0, 4),
        "detected_in_top_k": detected_in_top_k,
        "matched_examples": matched[:20],
        "missed_examples": missed[:20],
    }


def _label_watchlist(watchlist: pl.DataFrame, labels: pl.DataFrame) -> pl.DataFrame:
    if watchlist.is_empty():
        return watchlist.with_columns(
            pl.lit(None, dtype=pl.Int8).alias("y_true"),
            pl.lit("unknown").alias("label_confidence"),
        )

    pos = labels.filter(pl.col("label").is_in(sorted(TRUE_LABELS)))
    neg = labels.filter(pl.col("label").is_in(sorted(FALSE_LABELS)))

    pos_mmsi = {_normalize_id(v) for v in pos["mmsi"].to_list()} if "mmsi" in pos.columns else set()
    pos_imo = {_normalize_id(v) for v in pos["imo"].to_list()} if "imo" in pos.columns else set()
    neg_mmsi = {_normalize_id(v) for v in neg["mmsi"].to_list()} if "mmsi" in neg.columns else set()
    neg_imo = {_normalize_id(v) for v in neg["imo"].to_list()} if "imo" in neg.columns else set()

    confidence_by_key: dict[tuple[str, str], str] = {}
    for row in labels.select(["mmsi", "imo", "label_confidence"]).iter_rows(named=True):
        mmsi = _normalize_id(row.get("mmsi"))
        imo = _normalize_id(row.get("imo"))
        conf = str(row.get("label_confidence") or "unknown")
        if mmsi:
            confidence_by_key[("mmsi", mmsi)] = conf
        if imo:
            confidence_by_key[("imo", imo)] = conf

    y_true: list[int | None] = []
    label_confidence: list[str] = []
    for row in watchlist.select(["mmsi", "imo"]).iter_rows(named=True):
        mmsi = _normalize_id(row.get("mmsi"))
        imo = _normalize_id(row.get("imo"))

        is_pos = (mmsi and mmsi in pos_mmsi) or (imo and imo in pos_imo)
        is_neg = (mmsi and mmsi in neg_mmsi) or (imo and imo in neg_imo)

        conf = "unknown"
        if mmsi and ("mmsi", mmsi) in confidence_by_key:
            conf = confidence_by_key[("mmsi", mmsi)]
        elif imo and ("imo", imo) in confidence_by_key:
            conf = confidence_by_key[("imo", imo)]

        if is_pos:
            y_true.append(1)
            label_confidence.append(conf)
        elif is_neg:
            y_true.append(0)
            label_confidence.append(conf)
        else:
            y_true.append(None)
            label_confidence.append("unknown")

    return watchlist.with_columns(
        pl.Series("y_true", y_true, dtype=pl.Int8),
        pl.Series("label_confidence", label_confidence, dtype=pl.Utf8),
    )


def _precision_at_k(df: pl.DataFrame, k: int) -> float:
    if df.is_empty():
        return 0.0
    head = df.head(min(k, df.height))
    return float(head["y_true"].cast(pl.Float64).mean() or 0.0)  # type: ignore[arg-type]


def _recall_at_k(df: pl.DataFrame, k: int, positive_count: int) -> float:
    if df.is_empty() or positive_count == 0:
        return 0.0
    hits = int(df.head(min(k, df.height))["y_true"].cast(pl.Int64).sum())
    return float(hits / positive_count)


def _ece(scores: list[float], labels: list[int], bins: int = 10) -> float:
    if not scores:
        return 0.0
    total = len(scores)
    acc = 0.0
    for b in range(bins):
        lo = b / bins
        hi = (b + 1) / bins
        idx = [
            i for i, s in enumerate(scores) if (s >= lo and s < hi) or (b == bins - 1 and s <= hi)
        ]
        if not idx:
            continue
        bin_scores = [scores[i] for i in idx]
        bin_labels = [labels[i] for i in idx]
        conf = sum(bin_scores) / len(bin_scores)
        emp = sum(bin_labels) / len(bin_labels)
        acc += abs(conf - emp) * (len(idx) / total)
    return float(acc)


def _best_f1_threshold(scores: list[float], labels: list[int]) -> float | None:
    if not scores or not labels:
        return None
    positives = sum(labels)
    negatives = len(labels) - positives
    if positives == 0 or negatives == 0:
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
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
        if f1 > best_f1:
            best_f1 = f1
            best_thr = thr
    return best_thr


def _ops_thresholds(ranked: pl.DataFrame, capacities: list[int]) -> list[dict[str, float | int]]:
    out: list[dict[str, float | int]] = []
    for k in capacities:
        k_eff = min(k, ranked.height)
        if k_eff == 0:
            out.append({"review_capacity": k, "min_score": 1.0, "hit_rate": 0.0})
            continue
        top = ranked.head(k_eff)
        min_score = float(top["confidence"].min() or 0.0)  # type: ignore[arg-type]
        known = top.filter(pl.col("y_true").is_not_null())
        hit_rate = (
            float(known["y_true"].cast(pl.Float64).mean() or 0.0) if not known.is_empty() else 0.0  # type: ignore[arg-type]
        )
        out.append(
            {
                "review_capacity": k,
                "min_score": round(min_score, 4),
                "hit_rate": round(hit_rate, 4),
            }
        )
    return out


def _stratified_metrics(
    labeled_ranked: pl.DataFrame, by_col: str
) -> list[dict[str, float | int | str | None]]:
    if by_col not in labeled_ranked.columns:
        return []
    rows: list[dict[str, float | int | str | None]] = []
    groups = labeled_ranked.group_by(by_col).len().rename({"len": "n"})
    for row in groups.iter_rows(named=True):
        key = row[by_col]
        n = int(row["n"])
        if n < 3:
            continue
        g = labeled_ranked.filter(pl.col(by_col) == key).sort("confidence", descending=True)
        labels = g["y_true"].cast(pl.Int8).to_list()
        scores = g["confidence"].to_list()
        positives = int(sum(labels))

        auroc = None
        if positives and positives != len(labels):
            auroc = float(roc_auc_score(labels, scores))

        rows.append(
            {
                by_col: str(key),
                "n": n,
                "positive_count": positives,
                "precision_at_50": round(_precision_at_k(g, 50), 4),
                "recall_at_100": round(_recall_at_k(g, 100, positives), 4),
                "auroc": round(auroc, 4) if auroc is not None else None,
            }
        )
    return rows


def _metric_ci(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"mean": None, "ci95_low": None, "ci95_high": None}
    n = len(values)
    mean = sum(values) / n
    if n == 1:
        return {"mean": round(mean, 4), "ci95_low": None, "ci95_high": None}
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    se = (var / n) ** 0.5
    delta = 1.96 * se
    return {
        "mean": round(mean, 4),
        "ci95_low": round(mean - delta, 4),
        "ci95_high": round(mean + delta, 4),
    }


def evaluate_window(window: BacktestWindow, capacities: list[int]) -> dict[str, object]:
    watchlist = read_parquet_uri(window.watchlist_path)
    if watchlist is None:
        raise FileNotFoundError(f"watchlist not found: {window.watchlist_path}")
    if "confidence" not in watchlist.columns:
        raise ValueError(f"watchlist missing 'confidence' column: {window.watchlist_path}")

    labels = _load_labels(window.labels_path)
    labeled_all = _label_watchlist(watchlist, labels).sort("confidence", descending=True)
    labeled = labeled_all.filter(pl.col("y_true").is_not_null())
    source_coverage = _source_positive_coverage(labeled_all, labels, capacities)

    if labeled.is_empty():
        metrics = {
            "candidate_count": watchlist.height,
            "labeled_count": 0,
            "positive_count": 0,
            "precision_at_50": 0.0,
            "precision_at_100": 0.0,
            "recall_at_100": 0.0,
            "recall_at_200": 0.0,
            "auroc": None,
            "pr_auc": None,
            "calibration_error": None,
        }
        return {
            "window_id": window.window_id,
            "start_date": window.start_date,
            "end_date": window.end_date,
            "region": window.region,
            "metrics": metrics,
            "ops_thresholds": _ops_thresholds(labeled_all, capacities),
            "stratified_by_vessel_type": [],
            "error_analysis": {"false_positives": [], "false_negatives": []},
            "recommended_threshold": None,
            "source_positive_coverage": source_coverage,
        }

    ranked = labeled.sort("confidence", descending=True)
    y_true = ranked["y_true"].cast(pl.Int8).to_list()
    scores = ranked["confidence"].to_list()
    positive_count = int(sum(y_true))

    auroc = None
    pr_auc = None
    if positive_count and positive_count != len(y_true):
        auroc = float(roc_auc_score(y_true, scores))
        pr_auc = float(average_precision_score(y_true, scores))

    threshold = _best_f1_threshold(scores, y_true)
    used_threshold = threshold if threshold is not None else 0.7

    fp = (
        ranked.filter((pl.col("confidence") >= used_threshold) & (pl.col("y_true") == 0))
        .head(10)
        .select(
            [
                c
                for c in ["mmsi", "imo", "vessel_name", "vessel_type", "confidence"]
                if c in ranked.columns
            ]
        )
        .to_dicts()
    )
    fn = (
        ranked.filter((pl.col("confidence") < used_threshold) & (pl.col("y_true") == 1))
        .head(10)
        .select(
            [
                c
                for c in ["mmsi", "imo", "vessel_name", "vessel_type", "confidence"]
                if c in ranked.columns
            ]
        )
        .to_dicts()
    )

    metrics = {
        "candidate_count": watchlist.height,
        "labeled_count": ranked.height,
        "positive_count": positive_count,
        "precision_at_50": round(_precision_at_k(ranked, 50), 4),
        "precision_at_100": round(_precision_at_k(ranked, 100), 4),
        "recall_at_100": round(_recall_at_k(ranked, 100, positive_count), 4),
        "recall_at_200": round(_recall_at_k(ranked, 200, positive_count), 4),
        "auroc": round(auroc, 4) if auroc is not None else None,
        "pr_auc": round(pr_auc, 4) if pr_auc is not None else None,
        "calibration_error": round(_ece(scores, y_true), 4),
    }

    return {
        "window_id": window.window_id,
        "start_date": window.start_date,
        "end_date": window.end_date,
        "region": window.region,
        "metrics": metrics,
        "ops_thresholds": _ops_thresholds(labeled_all, capacities),
        "stratified_by_vessel_type": _stratified_metrics(ranked, "vessel_type"),
        "error_analysis": {
            "false_positives": fp,
            "false_negatives": fn,
        },
        "recommended_threshold": round(float(used_threshold), 4),
        "source_positive_coverage": source_coverage,
    }


def run_backtest(manifest_path: str, output_path: str, capacities: list[int]) -> dict[str, object]:
    schema_version, windows = load_manifest(manifest_path)
    window_reports = [evaluate_window(w, capacities) for w in windows]

    p50 = [float(w["metrics"]["precision_at_50"]) for w in window_reports]  # type: ignore[index]
    p100 = [float(w["metrics"]["precision_at_100"]) for w in window_reports]  # type: ignore[index]
    r200 = [float(w["metrics"]["recall_at_200"]) for w in window_reports]  # type: ignore[index]

    report: dict[str, object] = {
        "schema_version": schema_version,
        "generated_at": datetime.now(UTC).isoformat(),
        "manifest_path": str(Path(manifest_path).resolve()),
        "windows": window_reports,
        "summary": {
            "window_count": len(window_reports),
            "precision_at_50": _metric_ci(p50),
            "precision_at_100": _metric_ci(p100),
            "recall_at_200": _metric_ci(r200),
        },
    }

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run historical backtest for watchlist ranking")
    parser.add_argument("--manifest", required=True, help="Path to evaluation manifest JSON")
    parser.add_argument(
        "--output",
        default="data/processed/backtest_report.json",
        help="Path to write backtest report JSON",
    )
    parser.add_argument(
        "--review-capacities",
        default="25,50,100",
        help="Comma-separated review capacities used for operational thresholds",
    )
    args = parser.parse_args()

    capacities = [int(x.strip()) for x in args.review_capacities.split(",") if x.strip()]
    report = run_backtest(args.manifest, args.output, capacities)
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
