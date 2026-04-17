from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest

from pipeline.src.ingest.sanctions import download_opensanctions, load_jsonl_to_duckdb
from pipeline.src.ingest.schema import init_schema
from pipeline.src.score.backtest import run_backtest


@pytest.mark.integration
def test_public_sanctions_download_and_detection_backtest(tmp_path: Path) -> None:
    """Integration test using real public sanctions data.

    This test is intentionally opt-in because it downloads/loads a large public
    dataset and depends on local watchlist artifacts.
    """
    if os.getenv("RUN_PUBLIC_DATA_TESTS") != "1":
        pytest.skip("Set RUN_PUBLIC_DATA_TESTS=1 to run public-data integration test")

    watchlist_path = Path(
        os.getenv("PUBLIC_TEST_WATCHLIST", "data/processed/candidate_watchlist.parquet")
    )
    if not watchlist_path.exists():
        pytest.skip(f"Watchlist not found: {watchlist_path}")

    prepared_db = Path(os.getenv("PUBLIC_SANCTIONS_DB", "data/processed/public_eval.duckdb"))
    if not prepared_db.exists():
        if os.getenv("PREPARE_PUBLIC_DATA_IF_MISSING") == "1":
            init_schema(str(prepared_db))
            raw_path = Path("data/raw/sanctions/opensanctions_entities.jsonl")
            download_opensanctions(raw_path, force=False)
            load_jsonl_to_duckdb(raw_path, str(prepared_db))
        else:
            pytest.skip(
                f"public_eval.duckdb not found at {prepared_db}. "
                "Pull it from R2 with: uv run python scripts/sync_r2.py pull-sanctions-db  "
                "— or regenerate locally with: uv run python scripts/prepare_public_sanctions_db.py  "
                "— or set PREPARE_PUBLIC_DATA_IF_MISSING=1 to download automatically."
            )

    watchlist = pl.read_parquet(watchlist_path).select(["mmsi", "imo", "confidence"])

    # Build positive labels from practical public sources (OFAC/UN/EU-like tags).
    import duckdb

    con = duckdb.connect(str(prepared_db), read_only=True)
    try:
        positives = pl.from_pandas(
            con.execute(
                """
                SELECT DISTINCT
                    COALESCE(mmsi, '') AS mmsi,
                    COALESCE(imo, '') AS imo,
                    COALESCE(list_source, 'unknown') AS evidence_source
                FROM sanctions_entities
                WHERE (lower(COALESCE(list_source, '')) LIKE '%ofac%'
                    OR lower(COALESCE(list_source, '')) LIKE '%un%'
                    OR lower(COALESCE(list_source, '')) LIKE '%eu%')
                  AND (COALESCE(mmsi, '') <> '' OR COALESCE(imo, '') <> '')
                """
            ).fetchdf()
        )
    finally:
        con.close()

    # Normalize IMO prefix: sanctions_entities stores 'IMO9289491', watchlist uses '9289491'.
    positives = positives.with_columns(pl.col("imo").str.strip_prefix("IMO").alias("imo"))

    # Match by mmsi and imo independently — sanctions data may carry only one identifier.
    # Joining on both columns simultaneously fails when one field differs between sources.
    pos_by_mmsi = watchlist.join(
        positives.filter(pl.col("mmsi") != "").select(["mmsi", "evidence_source"]),
        on="mmsi",
        how="inner",
    )
    pos_by_imo = watchlist.join(
        positives.filter(pl.col("imo") != "").select(["imo", "evidence_source"]),
        on="imo",
        how="inner",
    )
    pos_labels = (
        pl.concat([pos_by_mmsi, pos_by_imo], how="vertical_relaxed")
        .unique(subset=["mmsi", "imo"])
        .sort("confidence", descending=True)
        .with_columns(
            pl.lit("positive").alias("label"),
            pl.lit("high").alias("label_confidence"),
            pl.lit(
                "https://data.opensanctions.org/datasets/latest/sanctions/entities.ftm.json"
            ).alias("evidence_url"),
            pl.lit("public source overlap with algorithm output").alias("notes"),
        )
        .select(
            ["mmsi", "imo", "label", "label_confidence", "evidence_source", "evidence_url", "notes"]
        )
    )

    # Add weak negatives from low-confidence tail to enable full metric computation.
    tail = watchlist.filter(pl.col("confidence") < 0.2)
    if not pos_labels.is_empty():
        tail = tail.filter(~pl.col("mmsi").is_in(pos_labels["mmsi"].to_list()))

    neg_labels = (
        tail.head(max(20, min(100, pos_labels.height * 2 if pos_labels.height else 20)))
        .with_columns(
            pl.lit("negative").alias("label"),
            pl.lit("weak").alias("label_confidence"),
            pl.lit("no_public_match_demo").alias("evidence_source"),
            pl.lit("").alias("evidence_url"),
            pl.lit("integration-test negative label; analyst review required").alias("notes"),
        )
        .select(
            ["mmsi", "imo", "label", "label_confidence", "evidence_source", "evidence_url", "notes"]
        )
    )

    labels = pl.concat([pos_labels, neg_labels], how="vertical_relaxed").unique(
        subset=["mmsi", "imo"]
    )
    labels_path = tmp_path / "eval_labels_public.csv"
    labels.write_csv(labels_path)

    manifest = {
        "schema_version": "1.0",
        "description": "public-data integration backtest",
        "windows": [
            {
                "window_id": "public-current",
                "region": "integration",
                "start_date": "2026-01-01",
                "end_date": "2026-12-31",
                "watchlist_path": str(watchlist_path.resolve()),
                "labels_path": str(labels_path.resolve()),
            }
        ],
    }

    manifest_path = tmp_path / "evaluation_manifest_public.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    report_path = tmp_path / "backtest_report_public.json"
    report = cast(dict[str, Any], run_backtest(str(manifest_path), str(report_path), [25, 50, 100]))

    assert report_path.exists()
    assert report["summary"]["window_count"] == 1

    window = report["windows"][0]
    cov = window["source_positive_coverage"]

    # 判定対象: 公開ソースで黒と判明したケースに対し、見つけた数と見逃し数を出す。
    assert cov["source_positive_total"] >= 0
    assert cov["matched_total"] + cov["missed_total"] == cov["source_positive_total"]
    assert isinstance(cov["matched_examples"], list)
    assert isinstance(cov["missed_examples"], list)

    # Precision@50 gate (#235 — updated after #231/#232/#233 fixes).
    #
    # P@50 is structurally capped at positives / labeled when labeled < 50.
    # With the Singapore-only watchlist the labeled set is ~39 rows (13 positives +
    # 26 weak negatives), giving a structural ceiling of 13/39 ≈ 0.333.
    # An AUROC of 1.0 means all positives rank above all negatives — P@50 ≈ 0.333
    # is the best achievable score in this configuration, not a regression.
    #
    # The former 0.68 target assumed a multi-region combined watchlist with >50
    # labeled positives (Option B, issue #218).  The floor is now 0.25, consistent
    # with the CI score-regression gate introduced in #237.
    p50 = window["metrics"]["precision_at_50"]
    auroc = window["metrics"].get("auroc")
    assert p50 >= 0.25, (
        f"Precision@50={p50:.4f} below floor 0.25 — scoring is broken. "
        "Regenerate candidate_watchlist.parquet by running: "
        "uv run python scripts/run_public_backtest_batch.py"
    )
    if auroc is not None:
        assert auroc >= 0.65, (
            f"AUROC={auroc:.4f} below floor 0.65 — worse than random. "
            "Check graph and anomaly score computation."
        )
