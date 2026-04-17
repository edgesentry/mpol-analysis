"""Checkpoint the arktrace DuckLake catalog and optionally push to R2.

Reads existing pipeline output files from ``data/processed/`` (or ``--data-dir``),
writes them into a local DuckLake catalog, runs CHECKPOINT to materialise clean
Parquet files, then optionally calls ``sync_r2.py`` to push to both R2 buckets.

Tables written to the catalog
------------------------------
  watchlist         — combined candidate watchlist from all regions (adds ``region`` col)
  causal_effects    — C3 causal model outputs (regime-level ATT estimates)
  validation_metrics — pipeline evaluation metrics (P@50, AUROC, R@200 …)
  composite_scores  — full composite risk scores for all vessels

Phase 1 gate
------------
If CHECKPOINT fails or the resulting Parquet files are unreadable by plain DuckDB,
this script exits with code 1.  That is the signal to pivot to plain Parquet writes
and remove DuckLake from subsequent phases (see #302 Phase 1 gate criteria).

Usage:
    # Checkpoint only (no R2 push)
    uv run python scripts/checkpoint_ducklake.py

    # Checkpoint + push public catalog to arktrace-public
    uv run python scripts/checkpoint_ducklake.py --push-public

    # Checkpoint + push private outputs to arktrace-private-capvista/outputs/
    uv run python scripts/checkpoint_ducklake.py --push-private

    # Checkpoint + push both buckets (CI usage)
    uv run python scripts/checkpoint_ducklake.py --push-public --push-private

    # Dry-run: show what would be written without creating/modifying the catalog
    uv run python scripts/checkpoint_ducklake.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import polars as pl

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_DATA_DIR = os.getenv("ARKTRACE_DATA_DIR", str(Path.home() / ".arktrace" / "data"))
_DEFAULT_CATALOG_DIR_REL = "ducklake"  # relative to data_dir


def _catalog_path(data_dir: Path) -> Path:
    return data_dir / _DEFAULT_CATALOG_DIR_REL / "catalog.duckdb"


def _data_path(data_dir: Path) -> Path:
    return data_dir / _DEFAULT_CATALOG_DIR_REL / "data"


# ---------------------------------------------------------------------------
# Source file discovery
# ---------------------------------------------------------------------------

_REGION_PREFIXES = [
    "singapore",
    "japansea",
    "middleeast",
    "europe",
    "persiangulf",
    "gulfofguinea",
    "gulfofaden",
    "gulfofmexico",
    "blacksea",
]


def _find_watchlists(data_dir: Path) -> dict[str, Path]:
    """Return {region_name: path} for every *_watchlist.parquet found."""
    found: dict[str, Path] = {}

    # Per-region watchlists (e.g. singapore_watchlist.parquet)
    for prefix in _REGION_PREFIXES:
        p = data_dir / f"{prefix}_watchlist.parquet"
        if p.exists():
            found[prefix] = p

    return found


def _find_causal_effects(data_dir: Path) -> list[Path]:
    """Return all causal effects Parquet files."""
    paths = []
    # Shared/global causal_effects.parquet
    shared = data_dir / "causal_effects.parquet"
    if shared.exists():
        paths.append(shared)
    # Per-region files (e.g. singapore_causal_effects.parquet)
    for prefix in _REGION_PREFIXES:
        p = data_dir / f"{prefix}_causal_effects.parquet"
        if p.exists():
            paths.append(p)
    return paths


def _find_composite_scores(data_dir: Path) -> Path | None:
    p = data_dir / "composite_scores.parquet"
    return p if p.exists() else None


def _find_validation_metrics(data_dir: Path) -> Path | None:
    p = data_dir / "validation_metrics.json"
    return p if p.exists() else None


# ---------------------------------------------------------------------------
# Table builders
# ---------------------------------------------------------------------------


def _build_watchlist_table(watchlists: dict[str, Path]) -> pl.DataFrame | None:
    frames = []
    for region, path in watchlists.items():
        try:
            df = pl.read_parquet(path)
            # Tag with region so the browser can filter without a JOIN
            if "region" not in df.columns:
                df = df.with_columns(pl.lit(region).alias("region"))
            frames.append(df)
        except Exception as exc:
            print(f"  [warn] Could not read {path}: {exc}", file=sys.stderr)

    if not frames:
        return None

    return pl.concat(frames, how="diagonal_relaxed")


def _build_causal_effects_table(paths: list[Path]) -> pl.DataFrame | None:
    frames = []
    for path in paths:
        try:
            df = pl.read_parquet(path)
            # Tag with source filename when there is no region column already
            if "region" not in df.columns:
                stem = path.stem.removesuffix("_causal_effects")
                df = df.with_columns(pl.lit(stem).alias("region"))
            frames.append(df)
        except Exception as exc:
            print(f"  [warn] Could not read {path}: {exc}", file=sys.stderr)

    if not frames:
        return None

    return pl.concat(frames, how="diagonal_relaxed")


def _build_validation_metrics_table(json_path: Path) -> pl.DataFrame | None:
    try:
        with open(json_path) as f:
            data = json.load(f)
    except Exception as exc:
        print(f"  [warn] Could not read {json_path}: {exc}", file=sys.stderr)
        return None

    # Flatten nested structures into a single-row table of scalar metrics.
    # E.g. {"backtest": {"p_at_50": 0.2, "auroc": 0.94}, "regions": [...]}
    flat: dict[str, object] = {}

    def _flatten(obj: object, prefix: str) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                _flatten(v, f"{prefix}{k}_" if prefix else f"{k}_")
        elif isinstance(obj, list):
            # Skip list values for now (e.g. per-window arrays)
            pass
        else:
            key = prefix.rstrip("_") if prefix.endswith("_") else prefix
            flat[key] = obj

    _flatten(data, "")

    if not flat:
        return None

    return pl.DataFrame([flat])


# ---------------------------------------------------------------------------
# Checkpoint gate validation
# ---------------------------------------------------------------------------


def _validate_parquet_readable(parquets: list[Path]) -> bool:
    """Verify that each materialised Parquet file is readable by plain DuckDB.

    Returns True if all files pass, False (and prints errors) otherwise.
    This is the Phase 1 gate check.
    """
    import duckdb

    all_ok = True
    for p in parquets:
        try:
            con = duckdb.connect(":memory:")
            row = con.execute(f"SELECT COUNT(*) FROM read_parquet('{p}')").fetchone()
            n = row[0] if row else 0
            con.close()
            print(f"  gate OK: {p.name} ({n} rows readable by plain DuckDB)")
        except Exception as exc:
            print(
                f"  GATE FAIL: {p.name} is NOT readable by plain DuckDB: {exc}",
                file=sys.stderr,
            )
            all_ok = False
    return all_ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run(
    data_dir: Path,
    dry_run: bool = False,
    push_public: bool = False,
    push_private: bool = False,
    verbose: bool = False,
) -> int:
    cat = _catalog_path(data_dir)
    dat = _data_path(data_dir)

    print(f"DuckLake catalog : {cat}")
    print(f"DuckLake data dir: {dat}")
    print()

    # ------------------------------------------------------------------
    # Discover source files
    # ------------------------------------------------------------------
    watchlists = _find_watchlists(data_dir)
    causal_paths = _find_causal_effects(data_dir)
    comp_path = _find_composite_scores(data_dir)
    metrics_path = _find_validation_metrics(data_dir)

    print(f"Watchlists found   : {len(watchlists)} ({', '.join(watchlists) or 'none'})")
    print(f"Causal effects     : {len(causal_paths)} file(s)")
    print(f"Composite scores   : {'yes' if comp_path else 'no'}")
    print(f"Validation metrics : {'yes' if metrics_path else 'no'}")
    print()

    if dry_run:
        print("[dry-run] No files created or modified.")
        return 0

    # ------------------------------------------------------------------
    # Build DataFrames
    # ------------------------------------------------------------------
    from pipeline.src.storage.ducklake import checkpoint as dl_checkpoint
    from pipeline.src.storage.ducklake import write_table

    tables_written: list[str] = []

    watchlist_df = _build_watchlist_table(watchlists)
    if watchlist_df is not None:
        print(
            f"Writing watchlist ({len(watchlist_df)} rows, "
            f"{len(watchlist_df.columns)} cols) → lake.watchlist ..."
        )
        write_table(watchlist_df, "watchlist", cat, dat)
        tables_written.append("watchlist")
    else:
        print("[skip] No watchlist data found — watchlist table not written.")

    causal_df = _build_causal_effects_table(causal_paths)
    if causal_df is not None:
        print(f"Writing causal_effects ({len(causal_df)} rows) → lake.causal_effects ...")
        write_table(causal_df, "causal_effects", cat, dat)
        tables_written.append("causal_effects")
    else:
        print("[skip] No causal effects data found.")

    if comp_path:
        try:
            comp_df = pl.read_parquet(comp_path)
            print(f"Writing composite_scores ({len(comp_df)} rows) → lake.composite_scores ...")
            write_table(comp_df, "composite_scores", cat, dat)
            tables_written.append("composite_scores")
        except Exception as exc:
            print(f"[warn] Could not read composite_scores.parquet: {exc}", file=sys.stderr)
    else:
        print("[skip] No composite_scores.parquet found.")

    if metrics_path:
        metrics_df = _build_validation_metrics_table(metrics_path)
        if metrics_df is not None:
            print(
                f"Writing validation_metrics ({len(metrics_df)} rows) → lake.validation_metrics ..."
            )
            write_table(metrics_df, "validation_metrics", cat, dat)
            tables_written.append("validation_metrics")
    else:
        print("[skip] No validation_metrics.json found.")

    if not tables_written:
        print(
            "No tables written — nothing to checkpoint.  "
            "Run the pipeline first to generate output files.",
            file=sys.stderr,
        )
        return 1

    # ------------------------------------------------------------------
    # CHECKPOINT — materialise inlined data to Parquet
    # ------------------------------------------------------------------
    print()
    print("Running CHECKPOINT lake ...")
    try:
        parquets = dl_checkpoint(cat, dat)
    except Exception as exc:
        print(f"CHECKPOINT failed: {exc}", file=sys.stderr)
        print(
            "Phase 1 gate: DuckLake CHECKPOINT is not stable.  "
            "Pivot to plain Parquet writes — remove DuckLake from Phase 2.",
            file=sys.stderr,
        )
        return 1

    print(f"Materialised {len(parquets)} Parquet file(s):")
    total_bytes = 0
    for p in parquets:
        sz = p.stat().st_size
        total_bytes += sz
        if verbose:
            print(f"  {p.relative_to(dat.parent)}  ({sz / 1024:.1f} KB)")

    print(f"Total: {total_bytes / 1_048_576:.2f} MB")
    print()

    # ------------------------------------------------------------------
    # Phase 1 gate: validate Parquet readability by plain DuckDB
    # ------------------------------------------------------------------
    print("Phase 1 gate — verifying Parquet files are readable by plain DuckDB ...")
    if not _validate_parquet_readable(parquets):
        print(
            "\nPhase 1 gate FAILED.  One or more Parquet files are not readable "
            "by plain DuckDB.  Do not proceed to Phase 2 browser integration "
            "until this is resolved.  See #302.",
            file=sys.stderr,
        )
        return 1

    print()
    print(f"Phase 1 gate PASSED.  {len(parquets)} Parquet file(s) readable by plain DuckDB.")
    print(f"Catalog: {cat}")
    print(f"Tables : {', '.join(tables_written)}")
    print()

    # ------------------------------------------------------------------
    # Write ducklake_manifest.json — consumed by the browser OPFS sync
    # ------------------------------------------------------------------
    # Maps each Parquet file to:
    #   - its R2 URL (so the browser can fetch it)
    #   - a stable `register_as` name (e.g. "watchlist.parquet") that
    #     DuckDB-WASM uses as the file key in read_parquet() queries
    _TABLE_REGISTER: dict[str, str] = {
        "watchlist": "watchlist.parquet",
        "causal_effects": "causal_effects.parquet",
        "composite_scores": "composite_scores.parquet",
        "validation_metrics": "validation_metrics.parquet",
    }
    _PUBLIC_BASE_URL = "https://arktrace-public.edgesentry.io"

    manifest_files = []
    for p in parquets:
        rel = p.relative_to(dat.parent)  # e.g. data/main/watchlist/ducklake-*.parquet
        # Derive table name from path: data/main/<table>/ducklake-*.parquet
        parts = rel.parts
        table_name = parts[2] if len(parts) >= 3 else p.stem
        register_as = _TABLE_REGISTER.get(table_name, f"{table_name}.parquet")
        manifest_files.append(
            {
                "key": str(rel).replace("\\", "/"),
                "url": f"{_PUBLIC_BASE_URL}/{str(rel).replace(chr(92), '/')}",
                "size_bytes": p.stat().st_size,
                "table": table_name,
                "register_as": register_as,
            }
        )

    from datetime import UTC, datetime

    manifest = {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "base_url": _PUBLIC_BASE_URL,
        "files": manifest_files,
    }
    manifest_path = cat.parent / "ducklake_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"Manifest written: {manifest_path}  ({len(manifest_files)} file entries)")
    print()

    # ------------------------------------------------------------------
    # Optional R2 push
    # ------------------------------------------------------------------
    sync_py = str(_REPO_ROOT / "scripts" / "sync_r2.py")

    if push_public:
        print("Pushing public DuckLake catalog to arktrace-public ...")
        ret = subprocess.run(
            [sys.executable, sync_py, "push-ducklake-public", "--catalog-dir", str(cat.parent)],
        ).returncode
        if ret != 0:
            print("push-ducklake-public failed.", file=sys.stderr)
            return ret
        print()

    if push_private:
        print("Pushing private DuckLake outputs to arktrace-private-capvista/outputs/ ...")
        ret = subprocess.run(
            [
                sys.executable,
                sync_py,
                "push-ducklake-private",
                "--catalog-dir",
                str(cat.parent),
                "--data-dir",
                str(data_dir),
            ],
        ).returncode
        if ret != 0:
            print("push-ducklake-private failed.", file=sys.stderr)
            return ret
        print()

    if not push_public and not push_private:
        print(
            "Tip: push the catalog to R2 with:\n"
            f"  uv run python {sync_py} push-ducklake-public "
            f"--catalog-dir {cat.parent}\n"
            f"  uv run python {sync_py} push-ducklake-private "
            f"--catalog-dir {cat.parent} --data-dir {data_dir}"
        )

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Checkpoint DuckLake catalog and push to R2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--data-dir",
        default=_DEFAULT_DATA_DIR,
        metavar="DIR",
        help=(f"Directory containing pipeline output files (default: {_DEFAULT_DATA_DIR})"),
    )
    parser.add_argument(
        "--push-public",
        action="store_true",
        help="After checkpointing, push public catalog to arktrace-public via sync_r2.py",
    )
    parser.add_argument(
        "--push-private",
        action="store_true",
        help=(
            "After checkpointing, push private outputs to "
            "arktrace-private-capvista/outputs/ via sync_r2.py"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be written without creating or modifying any files",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print each materialised Parquet file and its size",
    )
    args = parser.parse_args()

    return run(
        data_dir=Path(args.data_dir).expanduser(),
        dry_run=args.dry_run,
        push_public=args.push_public,
        push_private=args.push_private,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    sys.exit(main())
