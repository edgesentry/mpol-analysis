"""Validate watchlist quality against OFAC ground truth."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb
import polars as pl
from dotenv import load_dotenv
from sklearn.metrics import roc_auc_score

from pipeline.src.score.watchlist import DEFAULT_DB_PATH, build_candidate_watchlist
from pipeline.src.score.watchlist import DEFAULT_OUTPUT_PATH as DEFAULT_WATCHLIST_PATH
from pipeline.src.storage.config import read_parquet as read_parquet_uri

load_dotenv()

DEFAULT_METRICS_PATH = os.getenv(
    "VALIDATION_METRICS_PATH", "data/processed/validation_metrics.json"
)


def _positive_identifier_sets(db_path: str) -> tuple[set[str], set[str]]:
    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = con.execute(
            """
            SELECT DISTINCT COALESCE(mmsi, '') AS mmsi, COALESCE(imo, '') AS imo
            FROM sanctions_entities
            WHERE lower(COALESCE(list_source, '')) LIKE '%ofac%'
              AND (mmsi IS NOT NULL OR imo IS NOT NULL)
            """
        ).fetchall()
    finally:
        con.close()

    mmsi_set = {str(mmsi) for mmsi, _ in rows if mmsi}
    imo_set = {str(imo) for _, imo in rows if imo}
    return mmsi_set, imo_set


def label_watchlist_against_ofac(
    watchlist_df: pl.DataFrame, db_path: str = DEFAULT_DB_PATH
) -> pl.DataFrame:
    mmsi_set, imo_set = _positive_identifier_sets(db_path)
    if watchlist_df.is_empty():
        return watchlist_df.with_columns(pl.lit(False).alias("is_ofac_listed"))

    labels = [
        (str(row["mmsi"]) in mmsi_set) or (str(row.get("imo", "")) in imo_set)
        for row in watchlist_df.iter_rows(named=True)
    ]
    return watchlist_df.with_columns(pl.Series("is_ofac_listed", labels))


def compute_validation_metrics(labeled_watchlist_df: pl.DataFrame) -> dict[str, float | int | None]:
    if labeled_watchlist_df.is_empty():
        return {
            "candidate_count": 0,
            "positive_count": 0,
            "precision_at_50": 0.0,
            "recall_at_200": 0.0,
            "auroc": None,
        }

    ranked = labeled_watchlist_df.sort("confidence", descending=True)
    labels = ranked["is_ofac_listed"].cast(pl.Int8).to_list()
    scores = ranked["confidence"].to_list()
    positive_count = int(sum(labels))

    top_50 = ranked.head(min(50, ranked.height))
    top_200 = ranked.head(min(200, ranked.height))

    precision_at_50 = float(top_50["is_ofac_listed"].cast(pl.Int8).mean() or 0.0)  # type: ignore[arg-type]
    recall_hits = int(top_200["is_ofac_listed"].cast(pl.Int8).sum())
    recall_at_200 = float(recall_hits / positive_count) if positive_count else 0.0

    auroc: float | None
    if positive_count and positive_count != len(labels):
        auroc = float(roc_auc_score(labels, scores))
    else:
        auroc = None

    return {
        "candidate_count": ranked.height,
        "positive_count": positive_count,
        "precision_at_50": round(precision_at_50, 4),
        "recall_at_200": round(recall_at_200, 4),
        "auroc": round(auroc, 4) if auroc is not None else None,
    }


def write_validation_metrics(
    metrics: dict[str, float | int | None], output_path: str = DEFAULT_METRICS_PATH
) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metrics, indent=2) + "\n")


def validate_watchlist(
    db_path: str = DEFAULT_DB_PATH,
    watchlist_path: str = DEFAULT_WATCHLIST_PATH,
    metrics_path: str = DEFAULT_METRICS_PATH,
) -> dict[str, float | int | None]:
    watchlist_df = read_parquet_uri(watchlist_path)
    if watchlist_df is None:
        watchlist_df = build_candidate_watchlist(db_path)

    labeled = label_watchlist_against_ofac(watchlist_df, db_path)
    metrics = compute_validation_metrics(labeled)
    write_validation_metrics(metrics, metrics_path)
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate candidate watchlist against OFAC ground truth"
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--watchlist", default=DEFAULT_WATCHLIST_PATH)
    parser.add_argument("--output", default=DEFAULT_METRICS_PATH)
    args = parser.parse_args()

    metrics = validate_watchlist(args.db, args.watchlist, args.output)
    for key, val in metrics.items():
        print(f"  {key}: {val}")


if __name__ == "__main__":
    main()
