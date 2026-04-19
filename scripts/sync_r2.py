"""Sync processed pipeline artifacts to/from Cloudflare R2 (or any S3-compatible store).

Storage layout in R2
--------------------
  arktrace-public/                ← dedicated public bucket (no sub-prefix needed)
    latest                        ← plain-text file: "20260412T120000Z"
    20260412T120000Z.zip           ← single generation zip (1 kept by default)
    gdelt.lance.zip                ← shared; push separately with `push-gdelt`

Each generation is a single .zip file, so push/pull is always 1 object.
3 generations are kept by default (--keep 3): ~2 GB/snapshot × 3 ≈ 6 GB,
well within the 10 GB R2 free tier.  Pass --keep N to adjust.

The bucket is fully public (Cloudflare R2 → Settings → Public Access).
Users can pull without any credentials — just set S3_BUCKET and S3_ENDPOINT
in .env.

Why gdelt.lance is separate
---------------------------
At 1.2 GB it dominates the bucket.  It only changes when GDELT news data is
re-ingested, not on every pipeline run.  Keeping it outside the rotation
avoids duplication and keeps snapshot pushes fast.

Files included in snapshots
---------------------------
  INCLUDED  — what the API reads at runtime:
    *.duckdb                 (all except backtest_demo, public_eval)
    candidate_watchlist.parquet
    {region}_watchlist.parquet × 5
    causal_effects.parquet
    {region}_causal_effects.parquet × 5
    validation_metrics.json
    {region}_graph/  × 5    (Lance Graph ownership chains)

  EXCLUDED  — intermediate pipeline artefacts (not read by the API):
    anomaly_scores.parquet, composite_scores.parquet, mpol_baseline.parquet
    mpol_graph/              (used during feature engineering, not serving)

  EXCLUDED from rotation zip — distributed separately or not at all:
    public_eval.duckdb       distributed as a standalone R2 object (push/pull-sanctions-db)
    backtest_demo.duckdb     local test fixture only
    backtest_*.json, evaluation_manifest_*.json, backtracking_report.*
    eval_labels_public_*.csv, prelabel_evaluation.json, public_eval_metadata.json
    *.bak

  EXCLUDED  — Lance internal history (not needed to read the dataset):
    */_transactions/*        write-coordination logs, only needed during writes
    */_versions/*.manifest   old version manifests; only the latest is kept per dataset

Commands
--------
  push                upload snapshot as a single zip to R2, prune old zips
  pull                download + extract latest (or named) snapshot zip → data/processed/
  push-gdelt          upload gdelt.lance as gdelt.lance.zip (run after re-ingesting GDELT data)
  pull-gdelt          download + extract gdelt.lance.zip → data/processed/gdelt.lance
  push-sanctions-db   upload public_eval.duckdb (OpenSanctions DB) to R2
  pull-sanctions-db   download public_eval.duckdb from R2 — needed for integration tests
  push-watchlists     upload *_watchlist.parquet files as watchlists.zip (<1 MB) — run after
                      a real pipeline run so CI can pull real watchlists for the backtest
  pull-watchlists     download watchlists.zip from R2 and extract into data/processed/ — used
                      by data-publish CI job (replaces seeded pipeline run)
  push-demo           upload fixed-key demo bundle (candidate_watchlist.parquet,
                      composite_scores.parquet, causal_effects.parquet,
                      validation_metrics.json) to R2 — requires credentials; run after
                      a real pipeline run or from the data-publish CI job
  pull-demo           download the demo bundle from R2 into data/processed/ — no credentials
                      required; intended for developers who want to run the dashboard
                      without running the full pipeline locally
  push-reviews        export vessel_reviews table → reviews.parquet and upload to R2;
                      run after an analyst session to back up / share review decisions
  pull-reviews        download reviews.parquet from R2 and upsert into the local DuckDB
                      vessel_reviews table (conflict resolution: newer reviewed_at wins)
  push-custom-feeds   upload files from _inputs/custom_feeds/ to the private
                      arktrace-private-capvista R2 bucket (requires AWS_ACCESS_KEY_ID /
                      AWS_SECRET_ACCESS_KEY — same key used for arktrace-public)
  pull-custom-feeds   download all feed files from the private arktrace-private-capvista R2
                      bucket into _inputs/custom_feeds/ — same AWS_* credentials; skips
                      gracefully when absent (forks / local dev without access)
  push-ducklake-public  upload DuckLake catalog.duckdb + data/ Parquet files to
                      arktrace-public/ (overwrites on every run — no rotation)
  push-ducklake-private upload DuckLake catalog.duckdb + private output files to
                      arktrace-private-capvista/outputs/ (authenticated analysts only)
  list                show all snapshot zips and shared objects in R2

Env vars (loaded from .env automatically)
------------------------------------------
  S3_BUCKET               R2 bucket name. Default: arktrace-public
  S3_ENDPOINT             R2 endpoint URL. Default: arktrace-public R2 endpoint
  AWS_REGION              Default: "auto" (correct for R2)
  AWS_ACCESS_KEY_ID       R2 access key ID (required for push commands and pull-custom-feeds)
  AWS_SECRET_ACCESS_KEY   R2 secret access key (required for push commands and pull-custom-feeds)
                          The same key must have Object Read & Write on both arktrace-public
                          and arktrace-private-capvista.  App users never need credentials —
                          they only pull from the public bucket anonymously.

Examples
--------
  uv run python scripts/sync_r2.py push                      # push new zip, prune old
  uv run python scripts/sync_r2.py push-gdelt                # upload/update gdelt.lance.zip
  uv run python scripts/sync_r2.py push-sanctions-db         # upload/update public_eval.duckdb
  uv run python scripts/sync_r2.py push-watchlists           # upload *_watchlist.parquet (<1 MB)
  uv run python scripts/sync_r2.py push-demo                 # upload demo bundle (CI runs this)
  uv run python scripts/sync_r2.py pull                      # pull latest (no credentials needed)
  uv run python scripts/sync_r2.py pull --timestamp 20260411T080000Z
  uv run python scripts/sync_r2.py pull-gdelt                # pull gdelt.lance.zip
  uv run python scripts/sync_r2.py pull-sanctions-db         # pull public_eval.duckdb for tests
  uv run python scripts/sync_r2.py pull-watchlists           # pull watchlists.zip (used by CI)
  uv run python scripts/sync_r2.py pull-demo                 # pull demo bundle (no credentials)
  uv run python scripts/sync_r2.py push-reviews              # back up analyst reviews to R2
  uv run python scripts/sync_r2.py pull-reviews              # restore / merge reviews from R2
  uv run python scripts/sync_r2.py push-custom-feeds         # upload feeds to private bucket
  uv run python scripts/sync_r2.py pull-custom-feeds         # pull feeds from private bucket
  uv run python scripts/sync_r2.py push-ducklake-public      # upload DuckLake catalog to public bucket
  uv run python scripts/sync_r2.py push-ducklake-private     # upload private outputs to private bucket
  uv run python scripts/sync_r2.py list                      # show all generations in R2
"""

from __future__ import annotations

import argparse
import fnmatch
import os
import re
import sys
import tempfile
import zipfile as zipmod
from datetime import UTC, datetime
from pathlib import Path

# Load .env before resolving any defaults that read env vars.
try:
    from dotenv import load_dotenv as _load_dotenv

    _load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


def _resolve_default_data_dir() -> str:
    """Return the default data directory.

    Resolution order:
    1. ``ARKTRACE_DATA_DIR`` env var (explicit override)
    2. ``~/.arktrace/data`` (canonical user-level data location)
    """
    import os as _os

    if explicit := _os.getenv("ARKTRACE_DATA_DIR"):
        return str(Path(explicit).expanduser())
    return str(Path.home() / ".arktrace" / "data")


_DEFAULT_DATA_DIR = _resolve_default_data_dir()
_DEFAULT_REGION = "singapore"
_DEFAULT_KEEP = 3  # ~2 GB/snapshot × 3 ≈ 6 GB; well within the 10 GB free tier
_DEFAULT_BUCKET = "arktrace-public"
_DEFAULT_ENDPOINT = "https://b8a0b09feb89390fb6e8cf4ef9294f48.r2.cloudflarestorage.com"
# Public custom domain — used for unauthenticated (curl/urllib) downloads only.
# The S3 API (push, pull with credentials) still uses _DEFAULT_ENDPOINT.
_PUBLIC_BASE_URL = "https://arktrace-public.edgesentry.io"
# The dedicated arktrace-public bucket contains only public OSS artifacts,
# so no sub-prefix is needed — all objects live at the bucket root.
_LATEST_KEY = "latest"  # plain-text pointer to newest timestamp
_GDELT_R2_KEY = "gdelt.lance.zip"  # single zip for gdelt
_SANCTIONS_DB_R2_KEY = "public_eval.duckdb"  # OpenSanctions DB; separate from rotation zip
_WATCHLISTS_R2_KEY = "watchlists.zip"  # lightweight bundle of *_watchlist.parquet files
_DEMO_R2_KEY = "demo.zip"  # fixed-key public demo bundle; overwritten on every push-demo
_REVIEWS_R2_KEY = "reviews.parquet"  # analyst review decisions; overwritten on every push-reviews

# DuckLake catalog keys — public bucket (root) and private bucket (outputs/ prefix)
# catalog.duckdb is a fixed-key file overwritten on every push-ducklake-* run.
_DUCKLAKE_CATALOG_KEY = "catalog.duckdb"  # public: arktrace-public/catalog.duckdb
_DUCKLAKE_DATA_PREFIX = "data/"  # public: arktrace-public/data/...
_DUCKLAKE_PRIVATE_PREFIX = "outputs/"  # private: arktrace-private-capvista/outputs/

# Private files that are also pushed to arktrace-private-capvista/outputs/
# These are the full pipeline outputs available to authenticated Cap Vista reviewers.
_PRIVATE_OUTPUT_FILES = [
    "candidate_watchlist.parquet",
    "causal_effects.parquet",
    "validation_metrics.json",
]

# Private bucket for proprietary customer feeds (e.g. Cap Vista MPOL data).
# Uses separate credentials so it is never confused with the public bucket.
_PRIVATE_BUCKET = "arktrace-private-capvista"
# Custom domain for the private bucket — used as the S3 endpoint for CI reads/writes.
# The domain IS the bucket, so S3 paths are bare keys (no bucket-name prefix).
_PRIVATE_ENDPOINT = "https://arktrace-private-capvista.edgesentry.io"
_PRIVATE_FEEDS_DIR = Path(__file__).resolve().parents[1] / "_inputs" / "custom_feeds"

# Files included in the demo bundle — lightweight artifacts that let developers run the
# dashboard without re-running the full pipeline.  No heavy DuckDB or Lance files.
_DEMO_FILES = [
    "candidate_watchlist.parquet",
    "composite_scores.parquet",
    "causal_effects.parquet",
    "score_history.parquet",
    "validation_metrics.json",
]

# Maps user-facing region name → file prefix used in data/processed/
# e.g. "japan" → files are japansea.duckdb, japansea_graph/, japansea_watchlist.parquet
_REGION_PREFIX: dict[str, str] = {
    "singapore": "singapore",
    "japan": "japansea",
    "middleeast": "middleeast",
    "europe": "europe",
    "persiangulf": "persiangulf",
    "gulfofguinea": "gulfofguinea",
    "gulfofaden": "gulfofaden",
    "gulfofmexico": "gulfofmexico",
}

# Files always downloaded regardless of region (shared by the API across all regions)
_SHARED_FILES = {
    "mpol.duckdb",
    "candidate_watchlist.parquet",
    "causal_effects.parquet",
    "validation_metrics.json",
}
_GDELT_LOCAL_DIR = "gdelt.lance"
_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

# Files / patterns excluded from snapshots (intermediate and evaluation artefacts).
# Matched against the relative path from data/processed/.
_SNAPSHOT_EXCLUDE: list[str] = [
    # Intermediate pipeline outputs (not read by the API)
    "anomaly_scores.parquet",
    "composite_scores.parquet",
    "mpol_baseline.parquet",
    "mpol_graph/*",  # Lance graph used during feature engineering only
    # Evaluation / backtest artefacts
    "backtest_demo.duckdb",
    "public_eval.duckdb",  # distributed separately — see push-sanctions-db / pull-sanctions-db
    "backtest_*.json",
    "backtracking_report.*",
    "evaluation_manifest_*.json",
    "eval_labels_public_*.csv",
    "prelabel_evaluation.json",
    "public_eval_metadata.json",
    # GDELT — stored separately outside snapshots
    "gdelt.lance",
    "gdelt.lance/*",
    # Scratch / backup files
    "*.bak",
    ".gitkeep",
]


def _is_excluded(rel: str) -> bool:
    """Return True if the relative path should be excluded from snapshots."""
    for pattern in _SNAPSHOT_EXCLUDE:
        if fnmatch.fnmatch(rel, pattern):
            return True
        # Also match directory prefixes (e.g. "gdelt.lance/v1/..." matches "gdelt.lance/*")
        if fnmatch.fnmatch(rel.split("/")[0], pattern.rstrip("/*")):
            first = pattern.rstrip("/*")
            if rel == first or rel.startswith(first + "/"):
                return True
    # Lance transaction logs — only needed during writes, not for read-only use
    parts = rel.split("/")
    if "_transactions" in parts:
        return True
    return False


def _collect_snapshot_files(data_dir: Path) -> dict[str, int]:
    """List files for a snapshot, keeping only the latest Lance manifest per dataset.

    Lance stores version history under ``<dataset>/_versions/*.manifest``.
    Old manifests are dead weight for read-only deployments — only the latest
    (highest filename) is needed to open each dataset.  This function scans the
    _versions directories and drops all but the newest manifest.
    """
    # First pass: collect everything that passes the basic exclusion filter.
    all_files = _list_local(data_dir, exclude_fn=_is_excluded)

    # Second pass: for each _versions/ directory found, keep only the latest manifest.
    # Group manifest paths by their parent _versions/ directory.
    versions_dirs: dict[str, list[str]] = {}
    for rel in list(all_files):
        parts = rel.split("/")
        if "_versions" in parts:
            vi = parts.index("_versions")
            versions_dir = "/".join(parts[: vi + 1])
            versions_dirs.setdefault(versions_dir, []).append(rel)

    for versions_dir, manifests in versions_dirs.items():
        if len(manifests) <= 1:
            continue
        # Keep only the manifest with the lexicographically largest filename
        # (Lance version numbers encoded as zero-padded uint64 strings).
        latest = max(manifests, key=lambda p: Path(p).name)
        for m in manifests:
            if m != latest:
                del all_files[m]

    return all_files


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------


def _build_r2_fs(
    anonymous: bool = False, endpoint: str | None = None
):  # -> pyarrow.fs.S3FileSystem
    """Build an S3FileSystem for R2.

    Pass ``anonymous=True`` for public-bucket reads that need no credentials.
    Pass ``endpoint`` to override the default R2 endpoint (e.g. a custom domain).
    """
    import pyarrow.fs as pafs

    endpoint = endpoint or os.getenv("S3_ENDPOINT", _DEFAULT_ENDPOINT)
    if not endpoint:
        # Plain AWS S3
        kwargs: dict = {"region": os.getenv("AWS_REGION", "us-east-1")}
        if not anonymous:
            kwargs["access_key"] = os.environ["AWS_ACCESS_KEY_ID"]
            kwargs["secret_key"] = os.environ["AWS_SECRET_ACCESS_KEY"]
        return pafs.S3FileSystem(anonymous=anonymous, **kwargs)

    host = endpoint.split("://", 1)[-1].rstrip("/")
    scheme = "https" if endpoint.startswith("https://") else "http"
    kwargs = {
        "endpoint_override": host,
        "scheme": scheme,
        "region": os.getenv("AWS_REGION", "auto"),
    }
    if not anonymous:
        kwargs["access_key"] = os.environ["AWS_ACCESS_KEY_ID"]
        kwargs["secret_key"] = os.environ["AWS_SECRET_ACCESS_KEY"]
    return pafs.S3FileSystem(anonymous=anonymous, **kwargs)


def _list_local(data_dir: Path, exclude_fn=None) -> dict[str, int]:
    """Return {relative_path: size_bytes} for files under data_dir."""
    result: dict[str, int] = {}
    for p in sorted(data_dir.rglob("*")):
        if not p.is_file():
            continue
        rel = str(p.relative_to(data_dir))
        if exclude_fn and exclude_fn(rel):
            continue
        result[rel] = p.stat().st_size
    return result


def _r2_zip_path(bucket: str, timestamp: str) -> str:
    return f"{bucket}/{timestamp}.zip"


def _read_latest(fs, bucket: str) -> str | None:
    try:
        with fs.open_input_stream(f"{bucket}/{_LATEST_KEY}") as f:
            return f.read().decode().strip() or None
    except Exception:
        return None


def _write_latest(fs, bucket: str, timestamp: str) -> None:
    with fs.open_output_stream(f"{bucket}/{_LATEST_KEY}") as f:
        f.write(timestamp.encode())


def _list_timestamps(fs, bucket: str) -> list[str]:
    """Return timestamp names (without .zip) sorted oldest-first.

    Uses recursive=True and filters to root-level files only because
    pyarrow S3FileSystem does not enumerate flat (no-slash) keys when
    recursive=False is set on a bucket-root FileSelector.
    """
    import pyarrow.fs as pafs

    selector = pafs.FileSelector(f"{bucket}/", recursive=True)
    try:
        infos = fs.get_file_info(selector)
    except Exception:
        return []
    pat = re.compile(r"^\d{8}T\d{6}Z\.zip$")
    # Keep only root-level files: path == bucket/filename (no extra slash)
    names = [
        Path(i.path).name for i in infos if i.type == pafs.FileType.File and i.path.count("/") == 1
    ]
    return sorted(n.removesuffix(".zip") for n in names if pat.match(n))


def _delete_timestamp(fs, bucket: str, timestamp: str) -> int:
    """Delete the snapshot zip for a given timestamp; return 1 on success."""
    r2_path = _r2_zip_path(bucket, timestamp)
    try:
        fs.delete_file(r2_path)
        return 1
    except Exception:
        return 0


def _upload_file(fs, local_path: Path, r2_path: str) -> int:
    with local_path.open("rb") as src:
        with fs.open_output_stream(r2_path) as dst:
            total = 0
            while chunk := src.read(_CHUNK_SIZE):
                dst.write(chunk)
                total += len(chunk)
    return total


def _download_file(fs, r2_path: str, local_path: Path) -> int:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with fs.open_input_stream(r2_path) as src:
        with local_path.open("wb") as dst:
            total = 0
            while chunk := src.read(_CHUNK_SIZE):
                dst.write(chunk)
                total += len(chunk)
    return total


def _region_filter_names(names: list[str], regions: list[str]) -> list[str]:
    """Filter zip entry names to shared files + per-region files only."""
    prefixes = tuple(f"{_REGION_PREFIX[r]}_" for r in regions)
    stems = tuple(f"{_REGION_PREFIX[r]}." for r in regions)
    result = []
    for name in names:
        top = name.split("/")[0]
        if top in _SHARED_FILES:
            result.append(name)
        elif any(top.startswith(p) for p in prefixes):
            result.append(name)
        elif any(top.startswith(s) for s in stems):
            result.append(name)
    return result


def _create_snapshot_zip(local_files: dict[str, int], data_dir: Path, zip_path: Path) -> None:
    """Pack local_files into a ZIP_STORED archive at zip_path.

    ZIP_STORED skips redundant compression — parquet and duckdb files are
    already internally compressed, so deflating them again wastes CPU for
    negligible (or negative) size gains.
    """
    with zipmod.ZipFile(zip_path, "w", compression=zipmod.ZIP_STORED, allowZip64=True) as zf:
        for rel in sorted(local_files):
            zf.write(data_dir / rel, arcname=rel)


def _pull_zip(
    fs,
    bucket: str,
    timestamp: str,
    data_dir: Path,
    regions: list[str],
) -> int:
    """Download snapshot zip and extract region-filtered files. Returns bytes downloaded."""
    r2_path = _r2_zip_path(bucket, timestamp)

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        downloaded = _download_file(fs, r2_path, tmp_path)
        data_dir.mkdir(parents=True, exist_ok=True)

        with zipmod.ZipFile(tmp_path, "r") as zf:
            all_names = zf.namelist()
            to_extract = _region_filter_names(all_names, regions)
            for name in to_extract:
                zf.extract(name, data_dir)

        return downloaded
    finally:
        tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_push(args: argparse.Namespace) -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = Path(args.data_dir)
    keep = args.keep

    if not data_dir.exists():
        print(f"Error: data directory does not exist: {data_dir}", file=sys.stderr)
        return 1

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    fs = _build_r2_fs()

    print(f"Scanning {data_dir} (excluding intermediate, evaluation, and stale Lance history) ...")
    local_files = _collect_snapshot_files(data_dir)

    if not local_files:
        print("No files to upload after exclusions.", file=sys.stderr)
        return 1

    total_size = sum(local_files.values())
    print(f"{len(local_files)} files ({total_size / 1_048_576:.1f} MB) → {timestamp}.zip\n")

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        print("Creating zip archive (ZIP_STORED — no redundant compression) ...")
        _create_snapshot_zip(local_files, data_dir, tmp_path)
        zip_size = tmp_path.stat().st_size
        print(f"Archive: {zip_size / 1_048_576:.1f} MB\n")

        r2_path = _r2_zip_path(bucket, timestamp)
        print(f"Uploading {timestamp}.zip ...")
        uploaded = _upload_file(fs, tmp_path, r2_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    _write_latest(fs, bucket, timestamp)

    print(f"\n{timestamp}.zip pushed ({uploaded / 1_048_576:.1f} MB).")
    print(f"latest → {timestamp}")

    # Prune old generations beyond keep limit
    all_ts = _list_timestamps(fs, bucket)
    to_delete = all_ts[:-keep] if len(all_ts) > keep else []
    if to_delete:
        print(f"\nPruning {len(to_delete)} old generation(s) (keeping {keep}):")
        for old in to_delete:
            n = _delete_timestamp(fs, bucket, old)
            print(f"  deleted {old}.zip" + (" ✓" if n else " (not found)"))
    else:
        print(f"\n{len(all_ts)}/{keep} generation slot(s) used — nothing to prune.")

    print("\nTip: to also push updated GDELT news data, run:")
    print("  uv run python scripts/sync_r2.py push-gdelt")
    return 0


def cmd_pull(args: argparse.Namespace) -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = Path(args.data_dir)

    # Use anonymous access when credentials are absent (public bucket)
    anon = not (os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
    fs = _build_r2_fs(anonymous=anon)

    # Resolve timestamp
    timestamp = args.timestamp
    if not timestamp:
        timestamp = _read_latest(fs, bucket)
        if not timestamp:
            print(
                "No 'latest' pointer found in R2. Run a push first or pass --timestamp explicitly.",
                file=sys.stderr,
            )
            return 1
        print(f"Latest: {timestamp}")

    # Parse and validate regions
    if args.region.lower() == "all":
        regions = list(_REGION_PREFIX)
    else:
        regions = [r.strip().lower() for r in args.region.split(",")]
        unknown = [r for r in regions if r not in _REGION_PREFIX]
        if unknown:
            print(
                f"Error: unknown region(s): {', '.join(unknown)}\n"
                f"Available: {', '.join(_REGION_PREFIX)}",
                file=sys.stderr,
            )
            return 1

    print(f"Downloading {timestamp}.zip ...")
    try:
        downloaded = _pull_zip(fs, bucket, timestamp, data_dir, regions)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"\nDone. {downloaded / 1_048_576:.1f} MB downloaded, extracted to {data_dir}/")
    print(f"Region(s): {', '.join(regions)}")
    print("\nOptional extras:")
    print(
        "  uv run python scripts/sync_r2.py pull-sanctions-db  # OpenSanctions DB for integration tests"
    )
    print(
        "  uv run python scripts/sync_r2.py pull-gdelt         # GDELT news data (analyst briefs)"
    )
    print("\nOpen the dashboard:")
    print("  https://arktrace.edgesentry.io")
    return 0


def cmd_push_gdelt(args: argparse.Namespace) -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = Path(args.data_dir)
    gdelt_dir = data_dir / _GDELT_LOCAL_DIR

    if not gdelt_dir.exists():
        print(f"Error: {gdelt_dir} does not exist. Run the GDELT ingest first:", file=sys.stderr)
        print("  uv run python src/ingest/gdelt.py", file=sys.stderr)
        return 1

    fs = _build_r2_fs()
    r2_path = f"{bucket}/{_GDELT_R2_KEY}"

    if not args.force:
        import pyarrow.fs as pafs

        try:
            infos = fs.get_file_info([r2_path])
            if infos[0].type == pafs.FileType.File:
                print(
                    f"gdelt.lance.zip already exists in R2 ({infos[0].size / 1_048_576:.1f} MB). "
                    "Use --force to re-upload."
                )
                return 0
        except Exception:
            pass

    print(f"Scanning {gdelt_dir} ...")
    local_files = _list_local(gdelt_dir)
    if not local_files:
        print("gdelt.lance directory is empty.", file=sys.stderr)
        return 1

    total_size = sum(local_files.values())
    print(f"{len(local_files)} files ({total_size / 1_048_576:.1f} MB) → gdelt.lance.zip\n")

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        print("Creating gdelt.lance.zip (ZIP_STORED) ...")
        with zipmod.ZipFile(tmp_path, "w", compression=zipmod.ZIP_STORED, allowZip64=True) as zf:
            for rel in sorted(local_files):
                zf.write(gdelt_dir / rel, arcname=rel)
        zip_size = tmp_path.stat().st_size
        print(f"Archive: {zip_size / 1_048_576:.1f} MB\n")

        print("Uploading gdelt.lance.zip ...")
        uploaded = _upload_file(fs, tmp_path, r2_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    print(f"\nDone. Uploaded {uploaded / 1_048_576:.1f} MB to R2 {r2_path}")
    return 0


def cmd_pull_gdelt(args: argparse.Namespace) -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = Path(args.data_dir)
    gdelt_dir = data_dir / _GDELT_LOCAL_DIR

    anon = not (os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
    fs = _build_r2_fs(anonymous=anon)
    r2_path = f"{bucket}/{_GDELT_R2_KEY}"

    import pyarrow.fs as pafs

    try:
        infos = fs.get_file_info([r2_path])
        if infos[0].type == pafs.FileType.NotFound:
            raise FileNotFoundError
        zip_size_mb = infos[0].size / 1_048_576
    except Exception:
        print(
            "No gdelt.lance.zip found in R2. Push it first with:\n"
            "  uv run python scripts/sync_r2.py push-gdelt",
            file=sys.stderr,
        )
        return 1

    print(f"Downloading gdelt.lance.zip ({zip_size_mb:.1f} MB) ...")

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        downloaded = _download_file(fs, r2_path, tmp_path)
        gdelt_dir.mkdir(parents=True, exist_ok=True)
        print("Extracting ...")
        with zipmod.ZipFile(tmp_path, "r") as zf:
            zf.extractall(gdelt_dir)
    finally:
        tmp_path.unlink(missing_ok=True)

    print(f"\nDone. {downloaded / 1_048_576:.1f} MB downloaded to {gdelt_dir}/")
    return 0


def cmd_push_sanctions_db(args: argparse.Namespace) -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    local_path = Path(args.data_dir) / "public_eval.duckdb"

    if not local_path.exists():
        print(
            f"Error: {local_path} does not exist. Generate it first:\n"
            "  uv run python scripts/prepare_public_sanctions_db.py",
            file=sys.stderr,
        )
        return 1

    fs = _build_r2_fs()
    r2_path = f"{bucket}/{_SANCTIONS_DB_R2_KEY}"

    if not args.force:
        import pyarrow.fs as pafs

        try:
            infos = fs.get_file_info([r2_path])
            if infos[0].type == pafs.FileType.File:
                print(
                    f"public_eval.duckdb already exists in R2 ({infos[0].size / 1_048_576:.1f} MB). "
                    "Use --force to re-upload."
                )
                return 0
        except Exception:
            pass

    size_mb = local_path.stat().st_size / 1_048_576
    print(f"Uploading public_eval.duckdb ({size_mb:.1f} MB) → R2 {r2_path} ...")
    uploaded = _upload_file(fs, local_path, r2_path)
    print(f"Done. {uploaded / 1_048_576:.1f} MB uploaded.")
    return 0


def cmd_pull_sanctions_db(args: argparse.Namespace) -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    local_path = Path(args.data_dir) / "public_eval.duckdb"

    anon = not (os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
    fs = _build_r2_fs(anonymous=anon)
    r2_path = f"{bucket}/{_SANCTIONS_DB_R2_KEY}"

    import pyarrow.fs as pafs

    try:
        infos = fs.get_file_info([r2_path])
        if infos[0].type == pafs.FileType.NotFound:
            raise FileNotFoundError
        size_mb = infos[0].size / 1_048_576
    except Exception:
        print(
            "No public_eval.duckdb found in R2. Push it first with:\n"
            "  uv run python scripts/sync_r2.py push-sanctions-db",
            file=sys.stderr,
        )
        return 1

    print(f"Downloading public_eval.duckdb ({size_mb:.1f} MB) ...")
    downloaded = _download_file(fs, r2_path, local_path)
    print(f"Done. {downloaded / 1_048_576:.1f} MB downloaded to {local_path}")
    print("\nYou can now run the public-data integration test:")
    print("  RUN_PUBLIC_DATA_TESTS=1 uv run pytest tests/test_public_data_backtest_integration.py")
    return 0


def cmd_push_watchlists(args: argparse.Namespace) -> int:
    """Upload *_watchlist.parquet + candidate_watchlist.parquet as watchlists.zip.

    The resulting zip is tiny (< 1 MB) and is pulled by the data-publish CI job
    via ``pull-watchlists`` so the backtest can use real watchlists instead of
    seeded dummy data.
    """
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = Path(args.data_dir)
    r2_path = f"{bucket}/{_WATCHLISTS_R2_KEY}"

    # *_watchlist.parquet already matches candidate_watchlist.parquet; use a
    # set to avoid duplicates when both patterns hit the same file.
    seen: set[Path] = set()
    watchlist_files = []
    for pattern in ["*_watchlist.parquet", "candidate_watchlist.parquet"]:
        for f in sorted(data_dir.glob(pattern)):
            if f not in seen:
                seen.add(f)
                watchlist_files.append(f)

    if not watchlist_files:
        print(
            f"No watchlist parquets found in {data_dir}. "
            "Run the pipeline for at least one region first.",
            file=sys.stderr,
        )
        return 1

    fs = _build_r2_fs()

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        with zipmod.ZipFile(tmp_path, "w", compression=zipmod.ZIP_STORED) as zf:
            for f in watchlist_files:
                zf.write(f, arcname=f.name)
                print(f"  + {f.name} ({f.stat().st_size / 1024:.1f} KB)")
        size_mb = tmp_path.stat().st_size / 1_048_576
        print(f"Uploading watchlists.zip ({size_mb:.2f} MB) → R2 {r2_path} ...")
        uploaded = _upload_file(fs, tmp_path, r2_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    print(f"Done. {uploaded / 1_048_576:.2f} MB uploaded.")
    return 0


def cmd_pull_watchlists(args: argparse.Namespace) -> int:
    """Download watchlists.zip from R2 and extract into data/processed/."""
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = Path(args.data_dir)
    r2_path = f"{bucket}/{_WATCHLISTS_R2_KEY}"

    anon = not (os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
    fs = _build_r2_fs(anonymous=anon)

    import pyarrow.fs as pafs

    try:
        infos = fs.get_file_info([r2_path])
        if infos[0].type == pafs.FileType.NotFound:
            raise FileNotFoundError
        size_mb = infos[0].size / 1_048_576
    except Exception:
        print(
            "No watchlists.zip found in R2. Push real watchlists first with:\n"
            "  uv run python scripts/sync_r2.py push-watchlists",
            file=sys.stderr,
        )
        return 1

    print(f"Downloading watchlists.zip ({size_mb:.2f} MB) ...")
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        downloaded = _download_file(fs, r2_path, tmp_path)
        data_dir.mkdir(parents=True, exist_ok=True)
        with zipmod.ZipFile(tmp_path, "r") as zf:
            zf.extractall(data_dir)
            names = zf.namelist()
        print(f"Extracted {len(names)} files to {data_dir}/: {', '.join(names)}")
    finally:
        tmp_path.unlink(missing_ok=True)

    print(f"Done. {downloaded / 1_048_576:.2f} MB downloaded.")
    return 0


def cmd_push_reviews(args: argparse.Namespace) -> int:
    """Export vessel_reviews from local DuckDB → reviews.parquet and upload to R2.

    Overwrites the fixed-key reviews.parquet on every run.  Run after an analyst
    session to back up decisions and make them available to other machines.
    """
    import duckdb as _duckdb

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: DuckDB not found at {db_path}", file=sys.stderr)
        return 1

    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    r2_path = f"{bucket}/{_REVIEWS_R2_KEY}"

    con = _duckdb.connect(str(db_path), read_only=True)
    try:
        row = con.execute("SELECT COUNT(*) FROM vessel_reviews").fetchone()
        n_rows = row[0] if row else 0
    except Exception:
        print("No vessel_reviews table found — nothing to push.", file=sys.stderr)
        return 1
    finally:
        con.close()

    if n_rows == 0:
        print("vessel_reviews is empty — nothing to push.")
        return 0

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        con = _duckdb.connect(str(db_path), read_only=True)
        try:
            con.execute(f"COPY vessel_reviews TO '{tmp_path}' (FORMAT PARQUET)")
        finally:
            con.close()

        size_kb = tmp_path.stat().st_size / 1024
        print(f"Exporting {n_rows} reviews ({size_kb:.1f} KB) → R2 {r2_path} ...")
        fs = _build_r2_fs()
        uploaded = _upload_file(fs, tmp_path, r2_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    print(f"Done. {uploaded / 1024:.1f} KB uploaded.")
    return 0


def cmd_pull_reviews(args: argparse.Namespace) -> int:
    """Download reviews.parquet from R2 and upsert into the local DuckDB vessel_reviews table.

    Conflict resolution: newer ``reviewed_at`` timestamp wins.  Safe to run on
    multiple machines — duplicate MMSIs are merged, not duplicated.
    """
    import duckdb as _duckdb

    db_path = Path(args.db)
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    r2_path = f"{bucket}/{_REVIEWS_R2_KEY}"

    anon = not (os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
    fs = _build_r2_fs(anonymous=anon)

    import pyarrow.fs as pafs

    try:
        infos = fs.get_file_info([r2_path])
        if infos[0].type == pafs.FileType.NotFound:
            raise FileNotFoundError
        size_kb = infos[0].size / 1024
    except Exception:
        print(
            "No reviews.parquet found in R2. Push reviews first with:\n"
            "  uv run python scripts/sync_r2.py push-reviews",
            file=sys.stderr,
        )
        return 1

    print(f"Downloading reviews.parquet ({size_kb:.1f} KB) ...")
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        downloaded = _download_file(fs, r2_path, tmp_path)

        # Upsert into local DuckDB: newer reviewed_at wins per mmsi
        db_path.parent.mkdir(parents=True, exist_ok=True)
        con = _duckdb.connect(str(db_path))
        try:
            # Ensure table exists (in case this is a fresh DB)
            con.execute("""
                CREATE TABLE IF NOT EXISTS vessel_reviews (
                    mmsi               VARCHAR NOT NULL,
                    review_tier        VARCHAR NOT NULL,
                    handoff_state      VARCHAR NOT NULL DEFAULT 'queued_review',
                    rationale          TEXT,
                    evidence_refs_json TEXT,
                    reviewed_by        VARCHAR,
                    reviewed_at        TIMESTAMPTZ DEFAULT now()
                )
            """)
            # Load incoming reviews; keep only rows newer than what we already have
            con.execute(f"""
                INSERT INTO vessel_reviews
                SELECT src.*
                FROM read_parquet('{tmp_path}') AS src
                WHERE NOT EXISTS (
                    SELECT 1 FROM vessel_reviews dst
                    WHERE dst.mmsi = src.mmsi
                      AND dst.reviewed_at >= src.reviewed_at
                )
            """)
            merged = con.execute("SELECT COUNT(*) FROM vessel_reviews").fetchone()[0]
        finally:
            con.close()
    finally:
        tmp_path.unlink(missing_ok=True)

    print(f"Done. {downloaded / 1024:.1f} KB downloaded. vessel_reviews now has {merged} rows.")
    return 0


def cmd_push_demo(args: argparse.Namespace) -> int:
    """Upload the fixed-key demo bundle to R2 (requires credentials).

    Overwrites demo.zip on every run — there is no rotation; the file always
    reflects the most recent pipeline run.  Intended to be called from the
    data-publish CI job after the main push step.
    """
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = Path(args.data_dir)
    r2_path = f"{bucket}/{_DEMO_R2_KEY}"

    missing = [f for f in _DEMO_FILES if not (data_dir / f).exists()]
    if missing:
        print(
            f"Error: the following demo files are missing from {data_dir}:\n"
            + "".join(f"  {f}\n" for f in missing)
            + "Run the pipeline first or check _DEMO_FILES in sync_r2.py.",
            file=sys.stderr,
        )
        return 1

    fs = _build_r2_fs()

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        with zipmod.ZipFile(tmp_path, "w", compression=zipmod.ZIP_STORED) as zf:
            for name in _DEMO_FILES:
                local = data_dir / name
                zf.write(local, arcname=name)
                print(f"  + {name} ({local.stat().st_size / 1024:.1f} KB)")
        size_mb = tmp_path.stat().st_size / 1_048_576
        print(f"Uploading demo.zip ({size_mb:.2f} MB) → R2 {r2_path} ...")
        uploaded = _upload_file(fs, tmp_path, r2_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    print(f"Done. {uploaded / 1_048_576:.2f} MB uploaded.")
    print(
        "\nDevelopers can now fetch the demo bundle without credentials:\n"
        "  uv run python scripts/sync_r2.py pull-demo\n"
        "  # or: bash scripts/fetch_demo_data.sh"
    )
    return 0


def cmd_pull_demo(args: argparse.Namespace) -> int:
    """Download the demo bundle from the public custom domain (no credentials required).

    Uses a plain HTTPS GET against arktrace-public.edgesentry.io so callers
    don't need pyarrow or R2 credentials — only stdlib urllib is required.
    """
    import urllib.error
    import urllib.request

    data_dir = Path(args.data_dir)
    url = f"{_PUBLIC_BASE_URL}/{_DEMO_R2_KEY}"

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        print(f"Downloading {_DEMO_R2_KEY} from {_PUBLIC_BASE_URL} …")
        urllib.request.urlretrieve(url, tmp_path)
        size_mb = tmp_path.stat().st_size / 1_048_576
        print(f"  {size_mb:.2f} MB downloaded.")
        data_dir.mkdir(parents=True, exist_ok=True)
        with zipmod.ZipFile(tmp_path, "r") as zf:
            names = zf.namelist()
            zf.extractall(data_dir)
        print(f"Extracted {len(names)} files to {data_dir}/: {', '.join(names)}")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            print(
                "demo.zip not found at public URL. The app owner needs to run:\n"
                "  uv run python scripts/run_pipeline.py --region singapore --non-interactive\n"
                "  uv run python scripts/sync_r2.py push-demo",
                file=sys.stderr,
            )
        else:
            print(f"HTTP {exc.code} downloading demo bundle: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error downloading demo bundle: {exc}", file=sys.stderr)
        return 1
    finally:
        tmp_path.unlink(missing_ok=True)

    print(f"Done. {size_mb:.2f} MB downloaded.")
    print("\nData synced. Open the dashboard:\n  https://arktrace.edgesentry.io")
    return 0


def cmd_push_custom_feeds(args: argparse.Namespace) -> int:
    """Upload files from _inputs/custom_feeds/ to the private arktrace-private-capvista bucket.

    Uses the standard AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY credentials — the same
    key that writes to arktrace-public must also have write access on arktrace-private-capvista.

    Only uploads files whose names do NOT end with ``_sample`` (sample fixtures are
    local smoke-test data only and must never be pushed to the private bucket).
    """
    feeds_dir = Path(args.feeds_dir)

    if not feeds_dir.exists():
        print(f"Error: feeds directory does not exist: {feeds_dir}", file=sys.stderr)
        return 1

    _SKIP = {".gitkeep", ".gitignore"}
    candidates = sorted(
        f
        for f in feeds_dir.iterdir()
        if f.is_file() and f.name not in _SKIP and not f.stem.endswith("_sample")
    )
    if not candidates:
        print(
            f"No uploadable feed files found in {feeds_dir}. "
            "Add CSVs (without _sample suffix) and retry.",
            file=sys.stderr,
        )
        return 1

    # S3 API operations use the account-level R2 endpoint with an explicit bucket
    # prefix — pyarrow's S3FileSystem does not support custom domains as endpoints.
    # _PRIVATE_ENDPOINT is the public-facing URL; S3 writes go through _DEFAULT_ENDPOINT.
    fs = _build_r2_fs()
    print(f"Uploading {len(candidates)} file(s) to {_PRIVATE_BUCKET}/")
    for local_path in candidates:
        r2_path = f"{_PRIVATE_BUCKET}/{local_path.name}"
        size_kb = local_path.stat().st_size / 1024
        print(f"  {local_path.name} ({size_kb:.1f} KB) → {r2_path} ...", end="", flush=True)
        _upload_file(fs, local_path, r2_path)
        print(" ✓")

    print(f"\nDone. Custom feeds uploaded to {_PRIVATE_BUCKET}/")
    print("Pull them in CI with: uv run python scripts/sync_r2.py pull-custom-feeds")
    return 0


def cmd_pull_custom_feeds(args: argparse.Namespace) -> int:
    """Download all feed files from the private arktrace-private-capvista bucket.

    Extracts files into ``_inputs/custom_feeds/`` so the pipeline's auto-detection
    step picks them up on the next run.

    Uses the standard AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY credentials — the
    same key that accesses arktrace-public.  Skips gracefully when credentials are
    absent (forks without repo secrets, local dev machines without .env).
    """
    if not (os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY")):
        print(
            "[skip] AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY not set — skipping pull-custom-feeds",
            file=sys.stderr,
        )
        return 0

    import pyarrow.fs as pafs

    feeds_dir = Path(args.feeds_dir)
    feeds_dir.mkdir(parents=True, exist_ok=True)

    # S3 API operations use the account-level R2 endpoint with an explicit bucket
    # prefix — pyarrow's S3FileSystem does not support custom domains as endpoints
    # (FileSelector("") triggers ListBuckets which R2 custom domains reject).
    # _PRIVATE_ENDPOINT is the public-facing URL; S3 reads go through _DEFAULT_ENDPOINT.
    fs = _build_r2_fs()

    selector = pafs.FileSelector(f"{_PRIVATE_BUCKET}/", recursive=True)
    try:
        infos = fs.get_file_info(selector)
    except Exception as exc:
        print(f"Error listing {_PRIVATE_BUCKET}: {exc}", file=sys.stderr)
        return 1

    files = [i for i in infos if i.type == pafs.FileType.File]
    if not files:
        print(f"No files found in {_PRIVATE_BUCKET}/ — nothing to download.")
        return 0

    print(f"Downloading {len(files)} file(s) from {_PRIVATE_BUCKET}/ → {feeds_dir}/")
    for info in files:
        rel = info.path.removeprefix(f"{_PRIVATE_BUCKET}/")
        local_path = feeds_dir / rel
        local_path.parent.mkdir(parents=True, exist_ok=True)
        size_kb = info.size / 1024
        print(f"  {rel} ({size_kb:.1f} KB) ...", end="", flush=True)
        try:
            _download_file(fs, info.path, local_path)
            print(" ✓")
        except Exception as exc:
            print(f" ERROR: {exc}", file=sys.stderr)

    print(f"\nDone. Custom feeds extracted to {feeds_dir}/")
    print("The pipeline will auto-detect and ingest these files on the next run.")
    return 0


def cmd_push_ducklake_public(args: argparse.Namespace) -> int:
    """Upload DuckLake catalog.duckdb + data/ Parquet files to arktrace-public/.

    Objects written:
      arktrace-public/catalog.duckdb          ← DuckLake metadata catalog
      arktrace-public/data/main/<table>/*.parquet  ← materialised Parquet files

    Run ``scripts/checkpoint_ducklake.py`` first to materialise the catalog.
    The catalog is overwritten on every push (no rotation).
    """
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    catalog_dir = Path(args.catalog_dir)
    catalog_file = catalog_dir / "catalog.duckdb"
    parquet_dir = catalog_dir / "data"

    if not catalog_file.exists():
        print(
            f"Error: {catalog_file} not found.  Run checkpoint_ducklake.py first:\n"
            "  uv run python scripts/checkpoint_ducklake.py",
            file=sys.stderr,
        )
        return 1

    fs = _build_r2_fs()

    # Upload catalog.duckdb
    catalog_r2 = f"{bucket}/{_DUCKLAKE_CATALOG_KEY}"
    sz = catalog_file.stat().st_size
    print(f"Uploading catalog.duckdb ({sz / 1024:.1f} KB) → {catalog_r2} ...")
    _upload_file(fs, catalog_file, catalog_r2)
    print("  ✓")

    # Upload all Parquet files under data/
    parquets = sorted(parquet_dir.rglob("*.parquet")) if parquet_dir.exists() else []
    if not parquets:
        print("[warn] No Parquet files found under data/ — CHECKPOINT may not have run.")
    else:
        total_bytes = 0
        for p in parquets:
            rel = p.relative_to(catalog_dir)
            r2_path = f"{bucket}/{rel}"
            sz = p.stat().st_size
            total_bytes += sz
            print(f"  {rel}  ({sz / 1024:.1f} KB) → {r2_path} ...")
            _upload_file(fs, p, r2_path)
        print(f"Uploaded {len(parquets)} Parquet file(s) ({total_bytes / 1_048_576:.2f} MB)  ✓")

    # Upload ducklake_manifest.json — consumed by the browser OPFS sync (Phase 2)
    manifest_file = catalog_dir / "ducklake_manifest.json"
    if manifest_file.exists():
        manifest_r2 = f"{bucket}/ducklake_manifest.json"
        sz = manifest_file.stat().st_size
        print(f"Uploading ducklake_manifest.json ({sz} B) → {manifest_r2} ...")
        _upload_file(fs, manifest_file, manifest_r2)
        print("  ✓")
    else:
        print("[warn] ducklake_manifest.json not found — browser OPFS sync will not work.")

    print(
        f"\nDone. Public DuckLake catalog available at:\n"
        f"  {_PUBLIC_BASE_URL}/{_DUCKLAKE_CATALOG_KEY}\n"
        f"  {_PUBLIC_BASE_URL}/ducklake_manifest.json  ← browser OPFS sync manifest\n"
        f"  {_PUBLIC_BASE_URL}/{_DUCKLAKE_DATA_PREFIX}..."
    )
    return 0


def cmd_push_ducklake_private(args: argparse.Namespace) -> int:
    """Upload DuckLake catalog + private pipeline outputs to arktrace-private-capvista/outputs/.

    Objects written:
      arktrace-private-capvista/outputs/catalog.duckdb
      arktrace-private-capvista/outputs/data/main/<table>/*.parquet
      arktrace-private-capvista/outputs/candidate_watchlist.parquet
      arktrace-private-capvista/outputs/causal_effects.parquet
      arktrace-private-capvista/outputs/validation_metrics.json

    Requires AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY with write access on the
    private bucket.  Authenticated Cap Vista reviewers can download these files
    after logging in via Cloudflare Access (#313).
    """
    catalog_dir = Path(args.catalog_dir)
    data_dir = Path(args.data_dir)
    catalog_file = catalog_dir / "catalog.duckdb"
    parquet_dir = catalog_dir / "data"

    if not catalog_file.exists():
        print(
            f"Error: {catalog_file} not found.  Run checkpoint_ducklake.py first:\n"
            "  uv run python scripts/checkpoint_ducklake.py",
            file=sys.stderr,
        )
        return 1

    fs = _build_r2_fs()
    prefix = f"{_PRIVATE_BUCKET}/{_DUCKLAKE_PRIVATE_PREFIX}"

    # Upload DuckLake catalog
    catalog_r2 = f"{prefix}{_DUCKLAKE_CATALOG_KEY}"
    sz = catalog_file.stat().st_size
    print(f"Uploading catalog.duckdb ({sz / 1024:.1f} KB) → {catalog_r2} ...")
    _upload_file(fs, catalog_file, catalog_r2)
    print("  ✓")

    # Upload Parquet files from ducklake/data/
    parquets = sorted(parquet_dir.rglob("*.parquet")) if parquet_dir.exists() else []
    if parquets:
        total_bytes = 0
        for p in parquets:
            rel = p.relative_to(catalog_dir)
            r2_path = f"{prefix}{_DUCKLAKE_DATA_PREFIX}{rel}"
            sz = p.stat().st_size
            total_bytes += sz
            print(f"  {rel}  ({sz / 1024:.1f} KB) → {r2_path} ...")
            _upload_file(fs, p, r2_path)
        print(f"Uploaded {len(parquets)} Parquet file(s) ({total_bytes / 1_048_576:.2f} MB)  ✓")

    # Upload additional private output files (watchlist, causal effects, metrics)
    for filename in _PRIVATE_OUTPUT_FILES:
        local = data_dir / filename
        if not local.exists():
            print(f"  [skip] {filename} not found in {data_dir}")
            continue
        r2_path = f"{prefix}{filename}"
        sz = local.stat().st_size
        print(f"  {filename} ({sz / 1024:.1f} KB) → {r2_path} ...")
        _upload_file(fs, local, r2_path)
        print("  ✓")

    # Generate and upload private ducklake_manifest.json — rewrites the public
    # manifest URLs to point at the private bucket so the browser OPFS sync can
    # discover private files when the user is authenticated.
    public_manifest_file = catalog_dir / "ducklake_manifest.json"
    if public_manifest_file.exists():
        import json as _json

        private_base = f"{_PRIVATE_ENDPOINT}/{_DUCKLAKE_PRIVATE_PREFIX.rstrip('/')}"
        public_base = "https://arktrace-public.edgesentry.io"
        manifest_data = _json.loads(public_manifest_file.read_text())
        for entry in manifest_data.get("files", []):
            entry["url"] = entry["url"].replace(public_base, private_base)
            entry["key"] = f"{_DUCKLAKE_PRIVATE_PREFIX}{entry['key']}"
        manifest_data["base_url"] = private_base
        private_manifest_bytes = _json.dumps(manifest_data, indent=2).encode()
        manifest_r2 = f"{prefix}ducklake_manifest.json"
        print(
            f"Uploading private ducklake_manifest.json ({len(private_manifest_bytes)} B) → {manifest_r2} ..."
        )
        with fs.open_output_stream(manifest_r2) as dst:
            dst.write(private_manifest_bytes)
        print("  ✓")
    else:
        print("[warn] ducklake_manifest.json not found — private browser OPFS sync will not work.")

    print(
        f"\nDone. Private DuckLake outputs available at:\n"
        f"  {_PRIVATE_ENDPOINT}/{_DUCKLAKE_PRIVATE_PREFIX}  (authenticated access only)\n"
        f"  Manifest: {_PRIVATE_ENDPOINT}/{_DUCKLAKE_PRIVATE_PREFIX}ducklake_manifest.json"
    )
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    anon = not (os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
    fs = _build_r2_fs(anonymous=anon)

    latest = _read_latest(fs, bucket)
    timestamps = _list_timestamps(fs, bucket)

    if not timestamps:
        print("No generations found in R2. Run: uv run python scripts/sync_r2.py push")
        return 0

    import pyarrow.fs as pafs

    print(f"{'TIMESTAMP':<22}  {'SIZE':>10}  NOTE")
    print("-" * 46)
    for ts in reversed(timestamps):  # newest first
        r2_path = _r2_zip_path(bucket, ts)
        try:
            infos = fs.get_file_info([r2_path])
            mb = infos[0].size / 1_048_576 if infos[0].type == pafs.FileType.File else 0.0
        except Exception:
            mb = 0.0
        note = "<- latest" if ts == latest else ""
        print(f"{ts:<22}  {mb:>8.1f} MB  {note}")

    print()
    for r2_key, label, push_cmd in [
        (_GDELT_R2_KEY, "gdelt.lance.zip", "push-gdelt"),
        (_SANCTIONS_DB_R2_KEY, "public_eval.duckdb", "push-sanctions-db"),
    ]:
        r2_path = f"{bucket}/{r2_key}"
        try:
            infos = fs.get_file_info([r2_path])
            if infos[0].type == pafs.FileType.File:
                mb = infos[0].size / 1_048_576
                print(f"{label:<28}  {mb:>6.1f} MB  (shared, outside rotation)")
            else:
                print(f"{label:<28}  (not yet uploaded — run {push_cmd})")
        except Exception:
            print(f"{label:<28}  (not yet uploaded — run {push_cmd})")
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _check_env(require_credentials: bool = True) -> bool:
    required = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"] if require_credentials else []
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        print(f"Error: missing env vars: {', '.join(missing)}", file=sys.stderr)
        print("Set them in .env or export them. See .env.example for reference.", file=sys.stderr)
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync processed pipeline artifacts to/from Cloudflare R2.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    push_p = sub.add_parser("push", help="Push new generation zip to R2, prune old zips")
    push_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")
    push_p.add_argument(
        "--keep",
        type=int,
        default=_DEFAULT_KEEP,
        metavar="N",
        help=f"Number of generations to keep in R2 (default: {_DEFAULT_KEEP})",
    )

    pull_p = sub.add_parser(
        "pull", help="Download + extract latest (or named) generation zip → data/processed/"
    )
    pull_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")
    pull_p.add_argument(
        "--timestamp",
        default=None,
        metavar="YYYYMMDDTHHMMSSZ",
        help="Specific generation to pull (default: latest)",
    )
    pull_p.add_argument(
        "--region",
        default=_DEFAULT_REGION,
        metavar="REGION",
        help=(
            f"Region(s) to extract: {', '.join(_REGION_PREFIX)} or 'all' "
            f"(default: {_DEFAULT_REGION}, comma-separate for multiple)"
        ),
    )

    push_gdelt_p = sub.add_parser(
        "push-gdelt", help="Upload gdelt.lance as gdelt.lance.zip (run after re-ingesting GDELT)"
    )
    push_gdelt_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")
    push_gdelt_p.add_argument(
        "--force",
        action="store_true",
        help="Re-upload even if gdelt.lance.zip already exists in R2",
    )

    pull_gdelt_p = sub.add_parser(
        "pull-gdelt", help="Download + extract gdelt.lance.zip → data/processed/gdelt.lance"
    )
    pull_gdelt_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")

    push_sanctions_p = sub.add_parser(
        "push-sanctions-db",
        help="Upload public_eval.duckdb (OpenSanctions DB) to R2 — run after prepare_public_sanctions_db.py",
    )
    push_sanctions_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")
    push_sanctions_p.add_argument(
        "--force",
        action="store_true",
        help="Re-upload even if public_eval.duckdb already exists in R2",
    )

    pull_sanctions_p = sub.add_parser(
        "pull-sanctions-db",
        help="Download public_eval.duckdb from R2 — required to run test_public_data_backtest_integration.py",
    )
    pull_sanctions_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")

    push_watchlists_p = sub.add_parser(
        "push-watchlists",
        help=(
            "Upload *_watchlist.parquet files as watchlists.zip — run after a real pipeline "
            "run to make watchlists available to CI via pull-watchlists"
        ),
    )
    push_watchlists_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")

    pull_watchlists_p = sub.add_parser(
        "pull-watchlists",
        help="Download watchlists.zip from R2 and extract into data/processed/ — used by CI",
    )
    pull_watchlists_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")

    push_demo_p = sub.add_parser(
        "push-demo",
        help=(
            "Upload fixed-key demo bundle (watchlist + scores + causal effects + metrics) "
            "to R2 — requires credentials; run after a pipeline run or from CI"
        ),
    )
    push_demo_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")

    pull_demo_p = sub.add_parser(
        "pull-demo",
        help=(
            "Download demo bundle from R2 into data/processed/ — no credentials required; "
            "lets developers run the dashboard without a local pipeline run"
        ),
    )
    pull_demo_p.add_argument("--data-dir", default=_DEFAULT_DATA_DIR, metavar="DIR")

    push_reviews_p = sub.add_parser(
        "push-reviews",
        help="Export vessel_reviews → reviews.parquet and upload to R2 (requires credentials)",
    )
    push_reviews_p.add_argument(
        "--db", default=_DEFAULT_DATA_DIR + "/singapore.duckdb", metavar="DB"
    )

    pull_reviews_p = sub.add_parser(
        "pull-reviews",
        help="Download reviews.parquet from R2 and upsert into local DuckDB vessel_reviews table",
    )
    pull_reviews_p.add_argument(
        "--db", default=_DEFAULT_DATA_DIR + "/singapore.duckdb", metavar="DB"
    )

    _default_feeds_dir = str(_PRIVATE_FEEDS_DIR)

    push_custom_feeds_p = sub.add_parser(
        "push-custom-feeds",
        help=(
            "Upload non-sample feed files from _inputs/custom_feeds/ to the private "
            "arktrace-private-capvista R2 bucket (requires AWS_ACCESS_KEY_ID / "
            "AWS_SECRET_ACCESS_KEY with write access on both buckets)"
        ),
    )
    push_custom_feeds_p.add_argument(
        "--bucket",
        default=_PRIVATE_BUCKET,
        metavar="BUCKET",
        help=f"Private R2 bucket name (default: {_PRIVATE_BUCKET})",
    )
    push_custom_feeds_p.add_argument(
        "--feeds-dir",
        default=_default_feeds_dir,
        metavar="DIR",
        help=f"Local directory containing feed files to upload (default: {_default_feeds_dir})",
    )

    pull_custom_feeds_p = sub.add_parser(
        "pull-custom-feeds",
        help=(
            "Download feed files from the private arktrace-private-capvista R2 bucket into "
            "_inputs/custom_feeds/ — skips gracefully when credentials are absent"
        ),
    )
    pull_custom_feeds_p.add_argument(
        "--bucket",
        default=_PRIVATE_BUCKET,
        metavar="BUCKET",
        help=f"Private R2 bucket name (default: {_PRIVATE_BUCKET})",
    )
    pull_custom_feeds_p.add_argument(
        "--feeds-dir",
        default=_default_feeds_dir,
        metavar="DIR",
        help=f"Local directory to extract feeds into (default: {_default_feeds_dir})",
    )

    _default_ducklake_catalog_dir = str(Path(_DEFAULT_DATA_DIR) / "ducklake")

    push_ducklake_public_p = sub.add_parser(
        "push-ducklake-public",
        help=(
            "Upload DuckLake catalog.duckdb + data/ Parquet files to arktrace-public/ "
            "(run checkpoint_ducklake.py first)"
        ),
    )
    push_ducklake_public_p.add_argument(
        "--catalog-dir",
        default=_default_ducklake_catalog_dir,
        metavar="DIR",
        help=(
            "Directory containing catalog.duckdb and data/ "
            f"(default: {_default_ducklake_catalog_dir})"
        ),
    )

    push_ducklake_private_p = sub.add_parser(
        "push-ducklake-private",
        help=(
            "Upload DuckLake catalog + private output files to "
            "arktrace-private-capvista/outputs/ (requires credentials)"
        ),
    )
    push_ducklake_private_p.add_argument(
        "--catalog-dir",
        default=_default_ducklake_catalog_dir,
        metavar="DIR",
        help=(
            "Directory containing catalog.duckdb and data/ "
            f"(default: {_default_ducklake_catalog_dir})"
        ),
    )
    push_ducklake_private_p.add_argument(
        "--data-dir",
        default=_DEFAULT_DATA_DIR,
        metavar="DIR",
        help=(
            "Pipeline data directory for additional private output files "
            f"(default: {_DEFAULT_DATA_DIR})"
        ),
    )

    sub.add_parser("list", help="List snapshot zips and shared objects in R2")

    args = parser.parse_args()

    from dotenv import load_dotenv

    load_dotenv()

    # pull-custom-feeds uses its own private credentials (checked inside cmd_pull_custom_feeds)
    # so it is treated as read_only here to skip the public-bucket credential check.
    read_only = args.command in (
        "pull",
        "pull-gdelt",
        "pull-sanctions-db",
        "pull-watchlists",
        "pull-demo",
        "pull-reviews",
        "pull-custom-feeds",
        "list",
    )
    if not _check_env(require_credentials=not read_only):
        return 1

    dispatch = {
        "push": cmd_push,
        "pull": cmd_pull,
        "push-gdelt": cmd_push_gdelt,
        "pull-gdelt": cmd_pull_gdelt,
        "push-sanctions-db": cmd_push_sanctions_db,
        "pull-sanctions-db": cmd_pull_sanctions_db,
        "push-watchlists": cmd_push_watchlists,
        "pull-watchlists": cmd_pull_watchlists,
        "push-demo": cmd_push_demo,
        "pull-demo": cmd_pull_demo,
        "push-reviews": cmd_push_reviews,
        "pull-reviews": cmd_pull_reviews,
        "push-custom-feeds": cmd_push_custom_feeds,
        "pull-custom-feeds": cmd_pull_custom_feeds,
        "push-ducklake-public": cmd_push_ducklake_public,
        "push-ducklake-private": cmd_push_ducklake_private,
        "list": cmd_list,
    }
    return dispatch[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
