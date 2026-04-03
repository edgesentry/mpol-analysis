from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from scripts.prepare_public_sanctions_db import prepare_public_sanctions_db
from src.score.backtest import run_backtest


WATCHLIST_BY_REGION = {
    "singapore": "data/processed/singapore_watchlist.parquet",
    "japan": "data/processed/japansea_watchlist.parquet",
    "middleeast": "data/processed/middleeast_watchlist.parquet",
    "europe": "data/processed/europe_watchlist.parquet",
    "gulf": "data/processed/gulf_watchlist.parquet",
}


def _run_pipeline_for_region(
    scripts_dir: Path,
    region: str,
    gdelt_days: int,
    stream_duration: int,
    seed_dummy: bool,
    marine_cadastre_year: int | None,
) -> None:
    cmd = [
        sys.executable,
        str((scripts_dir / "run_pipeline.py").resolve()),
        "--region",
        region,
        "--non-interactive",
        "--gdelt-days",
        str(gdelt_days),
    ]
    if stream_duration > 0:
        cmd.extend(["--stream-duration", str(stream_duration)])
    if seed_dummy:
        cmd.append("--seed-dummy")
    if marine_cadastre_year is not None and region == "gulf":
        cmd.extend(["--marine-cadastre-year", str(marine_cadastre_year)])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Pipeline failed for region={region}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )


def _load_public_positives(db_path: Path) -> pl.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        pdf = con.execute(
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
        return pl.from_pandas(pdf)
    finally:
        con.close()


def _build_labels_for_watchlist(
    watchlist: pl.DataFrame,
    positives: pl.DataFrame,
    max_known_cases: int,
) -> pl.DataFrame:
    watchlist = watchlist.select(["mmsi", "imo", "confidence"])

    pos_m = watchlist.join(
        positives.select(["mmsi", "evidence_source"]).filter(pl.col("mmsi") != ""),
        on="mmsi",
        how="inner",
    )
    pos_i = watchlist.join(
        positives.select(["imo", "evidence_source"]).filter(pl.col("imo") != ""),
        on="imo",
        how="inner",
    )
    pos = pl.concat([pos_m, pos_i], how="vertical_relaxed").unique(subset=["mmsi", "imo"])
    pos = pos.sort("confidence", descending=True)
    if pos.height > max_known_cases:
        pos = pos.head(max_known_cases)

    pos_labels = pos.with_columns(
        pl.lit("positive").alias("label"),
        pl.lit("high").alias("label_confidence"),
        pl.lit("https://data.opensanctions.org/datasets/latest/sanctions/entities.ftm.json").alias("evidence_url"),
        pl.lit("public sanctions overlap (nightly OR match)").alias("notes"),
    ).select(["mmsi", "imo", "label", "label_confidence", "evidence_source", "evidence_url", "notes"])

    tail = watchlist.filter(pl.col("confidence") < 0.2)
    if not pos_labels.is_empty():
        tail = tail.filter(~pl.col("mmsi").is_in(pos_labels["mmsi"].to_list()))

    neg_size = max(20, min(max(100, max_known_cases), max(pos_labels.height * 2, 20)))
    neg_labels = tail.head(neg_size).with_columns(
        pl.lit("negative").alias("label"),
        pl.lit("weak").alias("label_confidence"),
        pl.lit("no_public_match_demo").alias("evidence_source"),
        pl.lit("").alias("evidence_url"),
        pl.lit("nightly synthetic negative; analyst review required").alias("notes"),
    ).select(["mmsi", "imo", "label", "label_confidence", "evidence_source", "evidence_url", "notes"])

    return pl.concat([pos_labels, neg_labels], how="vertical_relaxed").unique(subset=["mmsi", "imo"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run nightly public-data backtest batch")
    parser.add_argument(
        "--regions",
        default="singapore,japan,middleeast,europe,gulf",
        help="Comma-separated region list",
    )
    parser.add_argument("--gdelt-days", type=int, default=14)
    parser.add_argument("--stream-duration", type=int, default=0)
    parser.add_argument("--seed-dummy", action="store_true")
    parser.add_argument("--marine-cadastre-year", type=int, default=2023)
    parser.add_argument("--public-db", default="data/processed/public_eval.duckdb")
    parser.add_argument("--public-raw", default="data/raw/sanctions/opensanctions_entities.jsonl")
    parser.add_argument("--public-metadata", default="data/processed/public_eval_metadata.json")
    parser.add_argument("--manifest-out", default="data/processed/evaluation_manifest_public_nightly.json")
    parser.add_argument("--report-out", default="data/processed/backtest_report_public_nightly.json")
    parser.add_argument("--summary-out", default="data/processed/backtest_public_nightly_summary.json")
    parser.add_argument("--max-known-cases", type=int, default=200)
    parser.add_argument("--min-known-cases", type=int, default=30)
    parser.add_argument(
        "--strict-known-cases",
        action="store_true",
        help="Fail the batch if total known positive cases are below --min-known-cases",
    )
    parser.add_argument("--refresh-public-data", action="store_true")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    scripts_dir = project_root / "scripts"
    processed_dir = project_root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    public_db = (project_root / args.public_db).resolve()
    public_raw = (project_root / args.public_raw).resolve()
    prepare_summary = prepare_public_sanctions_db(
        db_path=public_db,
        raw_path=public_raw,
        force_download=args.refresh_public_data,
        force_reload=args.refresh_public_data,
    )

    metadata_path = (project_root / args.public_metadata).resolve()
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(prepare_summary, indent=2))

    regions = [r.strip() for r in args.regions.split(",") if r.strip()]
    for region in regions:
        if region not in WATCHLIST_BY_REGION:
            raise SystemExit(f"Unsupported region: {region}")
        _run_pipeline_for_region(
            scripts_dir=scripts_dir,
            region=region,
            gdelt_days=args.gdelt_days,
            stream_duration=args.stream_duration,
            seed_dummy=args.seed_dummy,
            marine_cadastre_year=args.marine_cadastre_year,
        )

    positives = _load_public_positives(public_db)

    windows: list[dict[str, Any]] = []
    label_counts: dict[str, int] = {}
    for region in regions:
        watchlist_path = (project_root / WATCHLIST_BY_REGION[region]).resolve()
        if not watchlist_path.exists():
            continue
        watchlist = pl.read_parquet(watchlist_path)
        labels = _build_labels_for_watchlist(watchlist, positives, args.max_known_cases)
        labels_path = (processed_dir / f"eval_labels_public_{region}_nightly.csv").resolve()
        labels.write_csv(labels_path)
        label_counts[region] = int(labels.filter(pl.col("label") == "positive").height)

        windows.append(
            {
                "window_id": f"{region}-nightly-public",
                "region": region,
                "start_date": "2026-01-01",
                "end_date": datetime.now(UTC).date().isoformat(),
                "watchlist_path": str(watchlist_path),
                "labels_path": str(labels_path),
            }
        )

    manifest = {
        "schema_version": "1.0",
        "description": "Nightly public-data backtest batch",
        "windows": windows,
    }
    manifest_path = (project_root / args.manifest_out).resolve()
    manifest_path.write_text(json.dumps(manifest, indent=2))

    report_path = (project_root / args.report_out).resolve()
    report = run_backtest(str(manifest_path), str(report_path), [25, 50, 100, 200])

    total_known_cases = 0
    region_summary: list[dict[str, Any]] = []
    for window in report.get("windows", []):
        cov = window.get("source_positive_coverage", {})
        total = int(cov.get("source_positive_total", 0))
        total_known_cases += total
        region_summary.append(
            {
                "region": window.get("region", "unknown"),
                "source_positive_total": total,
                "matched_total": int(cov.get("matched_total", 0)),
                "missed_total": int(cov.get("missed_total", 0)),
                "source_recall_in_watchlist": float(cov.get("source_recall_in_watchlist", 0.0)),
            }
        )

    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "regions": regions,
        "label_positive_counts": label_counts,
        "total_known_cases": total_known_cases,
        "min_known_cases_target": args.min_known_cases,
        "report_path": str(report_path),
        "manifest_path": str(manifest_path),
        "region_summary": region_summary,
        "metrics_summary": report.get("summary", {}),
    }

    summary_path = (project_root / args.summary_out).resolve()
    summary_path.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))

    if args.strict_known_cases and total_known_cases < args.min_known_cases:
        raise SystemExit(
            f"Known-case floor not met: total_known_cases={total_known_cases}, "
            f"min_known_cases={args.min_known_cases}"
        )


if __name__ == "__main__":
    main()
