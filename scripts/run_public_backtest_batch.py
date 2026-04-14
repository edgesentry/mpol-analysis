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

# MMSIs of vessels seeded by run_pipeline.py --seed-dummy.
# These are real OFAC vessels injected artificially into the pipeline DB to
# meet the CI known-case floor.  Excluding them from backtest labels avoids
# inflating the positive count with fixtures that were by design on sanctions
# lists, which would overstate how well the model detects unknowns.
_DUMMY_MMSIS: frozenset[str] = frozenset(
    {
        "352001369",  # CELINE
        "314856000",  # ELINE
        "372979000",  # REX 1
        "312171000",  # ANHONA
        "352898820",  # AVENTUS I
        "352002316",  # SATINA
        "626152000",  # ASTRA
        "352001298",  # CRYSTAL ROSE
        "314925000",  # BENDIGO
        "352001565",  # ARABIAN ENERGY
    }
)

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
                COALESCE(name, '') AS name,
                COALESCE(type, '') AS entity_type,
                COALESCE(list_source, 'unknown') AS evidence_source
            FROM sanctions_entities
            WHERE (lower(COALESCE(list_source, '')) LIKE '%ofac%'
                OR lower(COALESCE(list_source, '')) LIKE '%un%'
                OR lower(COALESCE(list_source, '')) LIKE '%eu%')
              AND (COALESCE(mmsi, '') <> '' OR COALESCE(imo, '') <> ''
                   OR (COALESCE(name, '') <> '' AND lower(COALESCE(type, '')) = 'vessel'))
            """
        ).fetchdf()
        return pl.from_pandas(pdf)
    finally:
        con.close()


def _normalize_vessel_name(col: pl.Expr) -> pl.Expr:
    """Strip, uppercase, and remove punctuation for fuzzy vessel name matching."""
    return (
        col.str.strip_chars()
        .str.to_uppercase()
        .str.replace_all(r"[^A-Z0-9 ]", "")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
    )


def _build_labels_for_watchlist(
    watchlist: pl.DataFrame,
    positives: pl.DataFrame,
    max_known_cases: int,
    filter_dummy_mmsis: bool = True,
) -> pl.DataFrame:
    watchlist = watchlist.select(["mmsi", "imo", "vessel_name", "confidence"])

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

    # Third lookup path: vessel name matching against Vessel-type sanctions entities.
    # Catches vessels that changed MMSI/IMO but whose historical aliases are still on record.
    vessel_names = (
        positives.filter(
            (pl.col("entity_type").str.to_lowercase() == "vessel") & (pl.col("name") != "")
        )
        .with_columns(_normalize_vessel_name(pl.col("name")).alias("name_norm"))
        .select(["name_norm", "evidence_source"])
    )
    watchlist_named = watchlist.filter(pl.col("vessel_name") != "").with_columns(
        _normalize_vessel_name(pl.col("vessel_name")).alias("vessel_name_norm")
    )
    pos_name = watchlist_named.join(
        vessel_names,
        left_on="vessel_name_norm",
        right_on="name_norm",
        how="inner",
    ).drop("vessel_name_norm")

    # Coverage audit: log how many unique Vessel-type sanctions names were matchable
    n_vessel_sanctions = vessel_names.height
    n_name_matches = pos_name.height
    print(
        f"[coverage] sanctions Vessel-type names available={n_vessel_sanctions}, "
        f"watchlist name matches={n_name_matches}",
        flush=True,
    )

    pos = pl.concat([pos_m, pos_i, pos_name], how="vertical_relaxed").unique(subset=["mmsi", "imo"])

    # Remove seeded dummy vessels so that known-case fixtures don't inflate
    # the positive label count.  Dummy MMSIs are real OFAC vessels but were
    # artificially injected into the pipeline DB via --seed-dummy; keeping them
    # in the evaluation overstates detection capability for unknowns.
    if filter_dummy_mmsis:
        n_before = pos.height
        pos = pos.filter(~pl.col("mmsi").is_in(_DUMMY_MMSIS))
        n_filtered = n_before - pos.height
        if n_filtered:
            print(
                f"[label-cleanup] filtered {n_filtered} seeded dummy vessel(s) from positives",
                flush=True,
            )

    pos = pos.sort("confidence", descending=True)
    if pos.height > max_known_cases:
        pos = pos.head(max_known_cases)

    pos_labels = pos.with_columns(
        pl.lit("positive").alias("label"),
        pl.lit("high").alias("label_confidence"),
        pl.lit("https://data.opensanctions.org/datasets/latest/sanctions/entities.ftm.json").alias(
            "evidence_url"
        ),
        pl.lit("public sanctions overlap (integration OR match)").alias("notes"),
    ).select(
        ["mmsi", "imo", "label", "label_confidence", "evidence_source", "evidence_url", "notes"]
    )

    tail = watchlist.filter(pl.col("confidence") < 0.2)
    if not pos_labels.is_empty():
        tail = tail.filter(~pl.col("mmsi").is_in(pos_labels["mmsi"].to_list()))

    # Ensure total labeled (pos + neg) >= 50 so P@50 uses the full denominator.
    # At least (50 - n_pos) negatives, or 2× positives, whichever is larger.
    neg_size = max(50 - pos_labels.height, pos_labels.height * 2, 20)
    neg_labels = (
        tail.head(neg_size)
        .with_columns(
            pl.lit("negative").alias("label"),
            pl.lit("weak").alias("label_confidence"),
            pl.lit("no_public_match_demo").alias("evidence_source"),
            pl.lit("").alias("evidence_url"),
            pl.lit("integration synthetic negative; analyst review required").alias("notes"),
        )
        .select(
            ["mmsi", "imo", "label", "label_confidence", "evidence_source", "evidence_url", "notes"]
        )
    )

    return pl.concat([pos_labels, neg_labels], how="vertical_relaxed").unique(
        subset=["mmsi", "imo"]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run public-data backtest integration batch")
    parser.add_argument(
        "--regions",
        default="singapore,japan,middleeast,europe,gulf",
        help="Comma-separated region list",
    )
    parser.add_argument("--gdelt-days", type=int, default=14)
    parser.add_argument("--stream-duration", type=int, default=0)
    parser.add_argument("--seed-dummy", action="store_true")
    parser.add_argument(
        "--marine-cadastre-year",
        type=int,
        default=None,
        help="Optional year for Gulf marine cadastre backfill. Disabled by default for CI portability.",
    )
    parser.add_argument("--public-db", default="data/processed/public_eval.duckdb")
    parser.add_argument("--public-raw", default="data/raw/sanctions/opensanctions_entities.jsonl")
    parser.add_argument("--public-metadata", default="data/processed/public_eval_metadata.json")
    parser.add_argument(
        "--manifest-out", default="data/processed/evaluation_manifest_public_integration.json"
    )
    parser.add_argument(
        "--report-out", default="data/processed/backtest_report_public_integration.json"
    )
    parser.add_argument(
        "--summary-out", default="data/processed/backtest_public_integration_summary.json"
    )
    parser.add_argument("--max-known-cases", type=int, default=200)
    parser.add_argument("--min-known-cases", type=int, default=30)
    parser.add_argument(
        "--strict-known-cases",
        action="store_true",
        help="Fail the batch if total known positive cases are below --min-known-cases",
    )
    parser.add_argument(
        "--min-watchlist-size",
        type=int,
        default=100,
        help=(
            "Minimum number of vessels a regional watchlist must contain to be included in the "
            "evaluation. Regions below this threshold are skipped with a warning — they likely "
            "contain only seeded dummy data rather than a real pipeline run. Default: 100."
        ),
    )
    parser.add_argument("--refresh-public-data", action="store_true")
    parser.add_argument(
        "--no-filter-dummy-mmsis",
        action="store_true",
        help=(
            "Disable filtering of seeded dummy vessel MMSIs from positive labels. "
            "By default, vessels injected via --seed-dummy are excluded so they don't "
            "artificially inflate the positive count."
        ),
    )
    parser.add_argument(
        "--skip-pipeline",
        action="store_true",
        help=(
            "Skip running the regional pipeline and use existing watchlist parquets directly. "
            "Useful in CI when real watchlists have been pre-pulled from R2."
        ),
    )
    args = parser.parse_args()

    # When the pipeline was seeded with dummy vessels, those vessels ARE the
    # intended positive cases for CI verification — don't filter them out.
    filter_dummy_mmsis = not args.no_filter_dummy_mmsis and not args.seed_dummy

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
        if args.skip_pipeline:
            print(
                f"[skip-pipeline] {region}: using existing watchlist parquet",
                flush=True,
            )
        else:
            _run_pipeline_for_region(
                scripts_dir=scripts_dir,
                region=region,
                gdelt_days=args.gdelt_days,
                stream_duration=args.stream_duration,
                seed_dummy=args.seed_dummy,
                marine_cadastre_year=args.marine_cadastre_year,
            )

    positives = _load_public_positives(public_db)

    skipped_regions: list[str] = []
    windows: list[dict[str, Any]] = []
    label_counts: dict[str, int] = {}
    for region in regions:
        watchlist_path = (project_root / WATCHLIST_BY_REGION[region]).resolve()
        if not watchlist_path.exists():
            print(f"[skip] {region}: watchlist not found at {watchlist_path}", flush=True)
            skipped_regions.append(region)
            continue
        watchlist = pl.read_parquet(watchlist_path)
        if watchlist.height < args.min_watchlist_size:
            print(
                f"[skip] {region}: watchlist has only {watchlist.height} vessels "
                f"(< --min-watchlist-size={args.min_watchlist_size}). "
                "This region likely contains only seeded dummy data — run a real AIS pipeline "
                "for this region before including it in the evaluation.",
                flush=True,
            )
            skipped_regions.append(region)
            continue
        labels = _build_labels_for_watchlist(
            watchlist,
            positives,
            args.max_known_cases,
            filter_dummy_mmsis=filter_dummy_mmsis,
        )
        labels_path = (processed_dir / f"eval_labels_public_{region}_integration.csv").resolve()
        labels.write_csv(labels_path)
        label_counts[region] = int(labels.filter(pl.col("label") == "positive").height)

        windows.append(
            {
                "window_id": f"{region}-integration-public",
                "region": region,
                "start_date": "2026-01-01",
                "end_date": datetime.now(UTC).date().isoformat(),
                "watchlist_path": str(watchlist_path),
                "labels_path": str(labels_path),
            }
        )

    # Combine evaluated region watchlists into candidate_watchlist.parquet.
    # Only regions that passed the --min-watchlist-size guard are included.
    # Deduplicate on (mmsi, imo) keeping the highest-confidence row so that
    # a vessel scored in multiple regions is represented once at its best score.
    evaluated_regions = [r for r in regions if r not in skipped_regions]
    watchlist_parts = [
        pl.read_parquet((project_root / WATCHLIST_BY_REGION[r]).resolve())
        for r in evaluated_regions
        if (project_root / WATCHLIST_BY_REGION[r]).exists()
    ]
    if watchlist_parts:
        combined_raw = pl.concat(watchlist_parts, how="vertical_relaxed")
        combined_watchlist = (
            combined_raw.sort("confidence", descending=True)
            .unique(subset=["mmsi", "imo"], keep="first")
            .sort("confidence", descending=True)
        )
        candidate_path = processed_dir / "candidate_watchlist.parquet"
        combined_watchlist.write_parquet(candidate_path)
        print(
            f"Combined candidate watchlist: {combined_raw.height} raw rows "
            f"→ {combined_watchlist.height} unique vessels → {candidate_path}",
            flush=True,
        )

    manifest = {
        "schema_version": "1.0",
        "description": "Main-merge public-data backtest integration batch",
        "windows": windows,
    }
    manifest_path = (project_root / args.manifest_out).resolve()
    manifest_path.write_text(json.dumps(manifest, indent=2))

    if not windows:
        print(
            "All regions were skipped — no backtest windows to evaluate. "
            "Run a real AIS pipeline for at least one region before running this batch.",
            flush=True,
        )
        summary = {
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "regions": [],
            "skipped_regions": skipped_regions,
            "skipped_reason": (
                f"watchlist below --min-watchlist-size={args.min_watchlist_size} "
                "(likely seeded dummy data, not a real pipeline run)"
            ),
            "label_positive_counts": label_counts,
            "total_known_cases": 0,
            "min_known_cases_target": args.min_known_cases,
            "report_path": str(report_path := (project_root / args.report_out).resolve()),
            "manifest_path": str(manifest_path),
            "region_summary": [],
            "metrics_summary": {},
        }
        summary_path = (project_root / args.summary_out).resolve()
        summary_path.write_text(json.dumps(summary, indent=2))
        print(json.dumps(summary, indent=2))
        return

    report_path = (project_root / args.report_out).resolve()
    report = run_backtest(str(manifest_path), str(report_path), [25, 50, 100, 200])
    report_dict = report if isinstance(report, dict) else {}

    total_known_cases = 0
    region_summary: list[dict[str, Any]] = []
    windows_data = report_dict.get("windows", [])
    if not isinstance(windows_data, list):
        windows_data = []

    for window in windows_data:
        if not isinstance(window, dict):
            continue
        cov = window.get("source_positive_coverage", {})
        if not isinstance(cov, dict):
            cov = {}
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
        "regions": evaluated_regions,
        "skipped_regions": skipped_regions,
        "skipped_reason": (
            f"watchlist below --min-watchlist-size={args.min_watchlist_size} "
            "(likely seeded dummy data, not a real pipeline run)"
            if skipped_regions
            else None
        ),
        "label_positive_counts": label_counts,
        "total_known_cases": total_known_cases,
        "min_known_cases_target": args.min_known_cases,
        "report_path": str(report_path),
        "manifest_path": str(manifest_path),
        "region_summary": region_summary,
        "metrics_summary": report_dict.get("summary", {}),
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
