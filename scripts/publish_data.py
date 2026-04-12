"""Publish pipeline artifacts to Cloudflare R2 (arktrace-public bucket).

This script mirrors what the data-publish CI workflow does, but runs locally.
It skips pipeline steps whose outputs already exist, so you can re-run it
quickly after changing a single region without regenerating everything.

Workflow
--------
1. For each requested region, check whether the key artifacts exist in
   data/processed/.  If any are missing, run the pipeline for that region.
2. Push all artifacts as a single snapshot zip to R2 (sync_r2.py push).
3. Push gdelt.lance if it exists and hasn't been pushed yet (sync_r2.py
   push-gdelt).

Usage
-----
  # Check what's missing, run pipeline for those regions, then push
  uv run python scripts/publish_data.py

  # Force re-run of the pipeline even if artifacts exist
  uv run python scripts/publish_data.py --force-pipeline

  # Only push already-generated artifacts (skip pipeline entirely)
  uv run python scripts/publish_data.py --no-pipeline

  # Specific regions only
  uv run python scripts/publish_data.py --regions singapore,japan

  # Push gdelt.lance even if it already exists in R2
  uv run python scripts/publish_data.py --force-gdelt

Required env vars (load from .env automatically)
-------------------------------------------------
  S3_BUCKET             arktrace-public
  S3_ENDPOINT           https://<account_id>.r2.cloudflarestorage.com
  AWS_ACCESS_KEY_ID     R2 write key
  AWS_SECRET_ACCESS_KEY R2 write secret
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_REPO_ROOT = Path(__file__).parent.parent
_DATA_DIR = _REPO_ROOT / "data" / "processed"

# For each region: the files that must exist for artifacts to be considered
# "ready to push".  If any are missing the pipeline is run for that region.
_REGION_ARTIFACTS: dict[str, list[str]] = {
    "singapore": [
        "singapore.duckdb",
        "singapore_watchlist.parquet",
        "singapore_causal_effects.parquet",
        "singapore_graph",
    ],
    "japan": [
        "japansea.duckdb",
        "japansea_watchlist.parquet",
        "japansea_causal_effects.parquet",
        "japansea_graph",
    ],
    "middleeast": [
        "middleeast.duckdb",
        "middleeast_watchlist.parquet",
        "middleeast_causal_effects.parquet",
        "middleeast_graph",
    ],
    "europe": [
        "europe.duckdb",
        "europe_watchlist.parquet",
        "europe_causal_effects.parquet",
        "europe_graph",
    ],
    "gulf": [
        "gulf.duckdb",
        "gulf_watchlist.parquet",
        "gulf_causal_effects.parquet",
        "gulf_graph",
    ],
}


def _artifacts_ready(region: str) -> list[str]:
    """Return list of missing artifact paths for a region (empty = all present)."""
    missing = []
    for name in _REGION_ARTIFACTS[region]:
        p = _DATA_DIR / name
        if not p.exists():
            missing.append(name)
    return missing


def _run_pipeline(regions: list[str], gdelt_days: int) -> None:
    """Run run_public_backtest_batch.py for the given regions."""
    sys.stdout.flush()
    cmd = [
        sys.executable,
        str(_REPO_ROOT / "scripts" / "run_public_backtest_batch.py"),
        "--regions",
        ",".join(regions),
        "--gdelt-days",
        str(gdelt_days),
        "--stream-duration",
        "0",
        "--seed-dummy",
        "--max-known-cases",
        "200",
        "--min-known-cases",
        "30",
        "--strict-known-cases",
    ]
    print(f"\n▶ Running pipeline for: {', '.join(regions)}")
    print(f"  {' '.join(cmd)}\n")
    result = subprocess.run(cmd, cwd=_REPO_ROOT)
    if result.returncode != 0:
        print("\nPipeline failed. Fix errors above before pushing.", file=sys.stderr)
        sys.exit(1)


def _run_sync(args: list[str]) -> int:
    sys.stdout.flush()
    cmd = [sys.executable, str(_REPO_ROOT / "scripts" / "sync_r2.py")] + args
    result = subprocess.run(cmd, cwd=_REPO_ROOT)
    return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate pipeline artifacts and publish to arktrace-public R2 bucket.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--regions",
        default="singapore,japan,middleeast,europe,gulf",
        help="Comma-separated regions to process (default: all five)",
    )
    parser.add_argument(
        "--gdelt-days",
        type=int,
        default=14,
        metavar="N",
        help="Days of GDELT history to ingest when running the pipeline (default: 14)",
    )
    parser.add_argument(
        "--no-pipeline",
        action="store_true",
        help="Skip pipeline entirely — push whatever is already in data/processed/",
    )
    parser.add_argument(
        "--force-pipeline",
        action="store_true",
        help="Re-run pipeline for all requested regions even if artifacts exist",
    )
    parser.add_argument(
        "--force-gdelt",
        action="store_true",
        help="Re-upload gdelt.lance.zip even if it already exists in R2",
    )
    args = parser.parse_args()

    regions = [r.strip().lower() for r in args.regions.split(",")]
    unknown = [r for r in regions if r not in _REGION_ARTIFACTS]
    if unknown:
        print(f"Unknown region(s): {', '.join(unknown)}", file=sys.stderr)
        print(f"Available: {', '.join(_REGION_ARTIFACTS)}", file=sys.stderr)
        return 1

    # ── Step 1: check / generate artifacts ───────────────────────────────────
    if args.no_pipeline:
        print("--no-pipeline: skipping artifact generation.")
    else:
        needs_pipeline: list[str] = []
        for region in regions:
            missing = _artifacts_ready(region)
            if args.force_pipeline or missing:
                if missing:
                    print(f"  {region}: missing {missing}")
                else:
                    print(f"  {region}: --force-pipeline")
                needs_pipeline.append(region)
            else:
                print(f"  {region}: artifacts present — skipping pipeline")

        if needs_pipeline:
            _run_pipeline(needs_pipeline, args.gdelt_days)
        else:
            print("\nAll artifacts present. Use --force-pipeline to regenerate.")

    # ── Step 2: push snapshot zip ─────────────────────────────────────────────
    print("\n▶ Pushing snapshot zip to R2 …")
    rc = _run_sync(["push"])
    if rc != 0:
        print("sync_r2.py push failed.", file=sys.stderr)
        return rc

    # ── Step 3: push gdelt.lance if it exists ────────────────────────────────
    gdelt_dir = _DATA_DIR / "gdelt.lance"
    if gdelt_dir.exists():
        print("\n▶ Pushing gdelt.lance …")
        gdelt_args = ["push-gdelt"]
        if args.force_gdelt:
            gdelt_args.append("--force")
        rc = _run_sync(gdelt_args)
        if rc != 0:
            print("sync_r2.py push-gdelt failed.", file=sys.stderr)
            return rc
    else:
        print(f"\nSkipping gdelt push — {gdelt_dir} not found.")
        print("To generate it: uv run python src/ingest/gdelt.py --days 14")

    # ── Done ──────────────────────────────────────────────────────────────────
    print("\n✓ Done. Verify with:")
    print("  uv run python scripts/sync_r2.py list")
    return 0


if __name__ == "__main__":
    sys.exit(main())
