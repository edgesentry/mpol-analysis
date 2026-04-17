"""Auto-detected drop-in ingestion for proprietary data feeds.

Drop any CSV file into ``_inputs/custom_feeds/`` and it will be automatically
schema-mapped and ingested into the appropriate DuckDB table on the next
pipeline run — no code changes required.

Supported feed types (detected by column signature):

    AIS positions feed
        Required columns: mmsi (or MMSI), lat (or LAT), lon (or LON), timestamp (or BaseDateTime)
        Target table:     ais_positions
        Column map:       same as MarineCadastre defaults; override with a
                          <stem>.columnmap.json sidecar file

    SAR detection feed
        Required columns: lat, lon, detected_at
        Target table:     sar_detections
        Optional columns: detection_id, length_m, source_scene, confidence

    Cargo manifest feed
        Required columns: reporter, partner, hs_code, period
        Target table:     trade_flow
        Optional columns: trade_value_usd, route_key

    Custom sanctions feed
        Required columns: name, list_source
        Target table:     sanctions_entities
        Optional columns: entity_id, mmsi, imo, flag, type

Filename convention (used when column detection is ambiguous):
    ais_*.csv          → AIS positions
    sar_*.csv          → SAR detections
    cargo_*.csv  /  manifest_*.csv  → Cargo manifest
    sanctions_*.csv    → Custom sanctions

Sample files (skipped automatically):
    Files whose stem ends with ``_sample`` (e.g. ``ais_sample.csv``,
    ``sanctions_sample.csv``) are always skipped so smoke-test fixtures
    in ``_inputs/custom_feeds/`` never pollute the live pipeline DB.

Column-map sidecar (optional, for AIS feeds with non-standard column names):
    Create a JSON file alongside your CSV with the same stem:
        _inputs/custom_feeds/my_feed.csv
        _inputs/custom_feeds/my_feed.columnmap.json   ← {"mmsi": "vessel_id", ...}

Usage:
    # Run standalone to ingest all pending files:
    uv run python src/ingest/custom_feeds.py

    # Specify a custom directory:
    uv run python src/ingest/custom_feeds.py --dir /path/to/feeds

    # Dry-run (detect and log without ingesting):
    uv run python src/ingest/custom_feeds.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from pathlib import Path

import duckdb
import polars as pl
from dotenv import load_dotenv

from pipeline.src.ingest.ais_csv import ingest_csv
from pipeline.src.ingest.schema import init_schema

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")
DEFAULT_FEEDS_DIR = Path("_inputs/custom_feeds")

# ---------------------------------------------------------------------------
# Feed type signatures — (required_cols, feed_type)
# ---------------------------------------------------------------------------

_FEED_SIGNATURES: list[tuple[set[str], str]] = [
    # Order matters: most specific first
    ({"lat", "lon", "detected_at"}, "sar"),
    ({"reporter", "partner", "hs_code", "period"}, "cargo"),
    ({"name", "list_source"}, "sanctions"),
    ({"mmsi", "lat", "lon"}, "ais"),
    # MarineCadastre uppercase variant
    ({"MMSI", "LAT", "LON"}, "ais"),
]

_FILENAME_HINTS: dict[str, str] = {
    "sar": "sar",
    "cargo": "cargo",
    "manifest": "cargo",
    "sanctions": "sanctions",
    "ais": "ais",
}


def _detect_feed_type(columns: list[str], stem: str) -> str | None:
    """Infer feed type from column names, falling back to filename prefix."""
    col_set = set(columns)

    # Column-based detection
    for required, feed_type in _FEED_SIGNATURES:
        if required.issubset(col_set):
            return feed_type

    # Filename-based fallback
    stem_lower = stem.lower()
    for hint, feed_type in _FILENAME_HINTS.items():
        if stem_lower.startswith(hint) or f"_{hint}_" in stem_lower:
            return feed_type

    return None


def _load_column_map(csv_path: Path) -> dict[str, str] | None:
    """Load optional <stem>.columnmap.json sidecar file."""
    sidecar = csv_path.with_suffix(".columnmap.json")
    if sidecar.exists():
        with open(sidecar) as fh:
            return json.load(fh)
    return None


# ---------------------------------------------------------------------------
# Per-feed-type ingestors
# ---------------------------------------------------------------------------


def _ingest_ais(csv_path: Path, db_path: str) -> int:
    col_map = _load_column_map(csv_path)
    return ingest_csv(csv_path, db_path=db_path, column_map=col_map)


def _ingest_sar(csv_path: Path, db_path: str) -> int:
    df = pl.read_csv(csv_path, infer_schema_length=1000, try_parse_dates=False)

    # Normalise column names to lowercase
    df = df.rename({c: c.lower() for c in df.columns})

    # Parse detected_at
    if df["detected_at"].dtype == pl.Utf8:
        df = df.with_columns(
            pl.col("detected_at")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f", strict=False)
            .fill_null(
                pl.col("detected_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
            )
            .dt.replace_time_zone("UTC")
            .alias("detected_at")
        )

    # Synthesise missing optional columns
    if "detection_id" not in df.columns:
        df = df.with_columns(
            pl.Series("detection_id", [str(uuid.uuid4()) for _ in range(df.height)])
        )
    if "length_m" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Float32).alias("length_m"))
    if "source_scene" not in df.columns:
        df = df.with_columns(pl.lit(csv_path.stem).alias("source_scene"))
    if "confidence" not in df.columns:
        df = df.with_columns(pl.lit(1.0).cast(pl.Float32).alias("confidence"))

    df = df.select(
        ["detection_id", "detected_at", "lat", "lon", "length_m", "source_scene", "confidence"]
    )
    df = df.with_columns(
        pl.col("lat").cast(pl.Float64),
        pl.col("lon").cast(pl.Float64),
        pl.col("length_m").cast(pl.Float32),
        pl.col("confidence").cast(pl.Float32),
    )
    df = df.drop_nulls(subset=["detection_id", "detected_at", "lat", "lon"])

    con = duckdb.connect(db_path)
    try:
        con.execute("INSERT OR IGNORE INTO sar_detections SELECT * FROM df")
        return df.height
    finally:
        con.close()


def _ingest_cargo(csv_path: Path, db_path: str) -> int:
    df = pl.read_csv(csv_path, infer_schema_length=1000, try_parse_dates=False)
    df = df.rename({c: c.lower() for c in df.columns})

    if "trade_value_usd" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("trade_value_usd"))
    if "route_key" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("route_key"))

    df = df.select(["reporter", "partner", "hs_code", "period", "trade_value_usd", "route_key"])
    df = df.with_columns(
        pl.col("reporter").cast(pl.Utf8),
        pl.col("partner").cast(pl.Utf8),
        pl.col("hs_code").cast(pl.Utf8),
        pl.col("period").cast(pl.Utf8),
        pl.col("trade_value_usd").cast(pl.Float64),
    )
    df = df.drop_nulls(subset=["reporter", "partner", "hs_code", "period"])

    con = duckdb.connect(db_path)
    try:
        con.execute("INSERT OR IGNORE INTO trade_flow SELECT * FROM df")
        return df.height
    finally:
        con.close()


def _ingest_sanctions(csv_path: Path, db_path: str) -> int:
    df = pl.read_csv(csv_path, infer_schema_length=1000, try_parse_dates=False)
    df = df.rename({c: c.lower() for c in df.columns})

    if "entity_id" not in df.columns:
        df = df.with_columns(pl.Series("entity_id", [str(uuid.uuid4()) for _ in range(df.height)]))
    for col in ("mmsi", "imo", "flag", "type"):
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    df = df.select(["entity_id", "name", "mmsi", "imo", "flag", "type", "list_source"])
    df = df.drop_nulls(subset=["entity_id", "name", "list_source"])

    con = duckdb.connect(db_path)
    try:
        con.execute("INSERT OR IGNORE INTO sanctions_entities SELECT * FROM df")
        return df.height
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Schema requirement strings (for error messages)
# ---------------------------------------------------------------------------

_SCHEMA_REQUIREMENTS = {
    "ais": "mmsi (or MMSI), lat (or LAT), lon (or LON), timestamp (or BaseDateTime)",
    "sar": "lat, lon, detected_at",
    "cargo": "reporter, partner, hs_code, period",
    "sanctions": "name, list_source",
}

_INGESTORS = {
    "ais": _ingest_ais,
    "sar": _ingest_sar,
    "cargo": _ingest_cargo,
    "sanctions": _ingest_sanctions,
}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def ingest_custom_feeds(
    feeds_dir: Path = DEFAULT_FEEDS_DIR,
    db_path: str = DEFAULT_DB_PATH,
    dry_run: bool = False,
) -> dict[str, int]:
    """Scan feeds_dir for CSV files and ingest each into the appropriate table.

    Returns a dict mapping filename → rows inserted (or 0 for dry-run).
    Raises no exceptions — errors are printed and the file is skipped.
    """
    results: dict[str, int] = {}

    if not feeds_dir.exists():
        return results

    csv_files = sorted(feeds_dir.glob("*.csv"))
    if not csv_files:
        return results

    init_schema(db_path)

    for csv_path in csv_files:
        if csv_path.stem.endswith("_sample"):
            print(f"  [custom_feeds] SKIP {csv_path.name} — sample fixture, not for live pipeline")
            continue
        try:
            header = pl.read_csv(csv_path, n_rows=0)
            columns = header.columns
        except Exception as exc:
            print(f"  [custom_feeds] SKIP {csv_path.name} — cannot read header: {exc}")
            results[csv_path.name] = 0
            continue

        feed_type = _detect_feed_type(columns, csv_path.stem)

        if feed_type is None:
            print(
                f"  [custom_feeds] UNKNOWN SCHEMA: {csv_path.name}\n"
                f"    Columns found: {', '.join(columns)}\n"
                f"    Supported feed types and required columns:\n"
                + "\n".join(f"      {ft}: {req}" for ft, req in _SCHEMA_REQUIREMENTS.items())
            )
            results[csv_path.name] = 0
            continue

        print(f"  [custom_feeds] {csv_path.name} → {feed_type} feed")

        if dry_run:
            print("    (dry-run — skipping ingest)")
            results[csv_path.name] = 0
            continue

        try:
            n = _INGESTORS[feed_type](csv_path, db_path)
            print(f"    {n} rows inserted into {_target_table(feed_type)}")
            results[csv_path.name] = n
        except Exception as exc:
            print(f"    ERROR: {exc}")
            results[csv_path.name] = 0

    return results


def _target_table(feed_type: str) -> str:
    return {
        "ais": "ais_positions",
        "sar": "sar_detections",
        "cargo": "trade_flow",
        "sanctions": "sanctions_entities",
    }[feed_type]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest all CSV files from _inputs/custom_feeds/ into DuckDB"
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=DEFAULT_FEEDS_DIR,
        help=f"Custom feeds directory (default: {DEFAULT_FEEDS_DIR})",
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect feed types and log without ingesting",
    )
    args = parser.parse_args()

    results = ingest_custom_feeds(args.dir, args.db, dry_run=args.dry_run)
    total = sum(results.values())

    if not results:
        print(f"No CSV files found in {args.dir}")
    else:
        print(f"\nCustom feeds summary: {len(results)} file(s), {total} rows inserted")


if __name__ == "__main__":
    main()
