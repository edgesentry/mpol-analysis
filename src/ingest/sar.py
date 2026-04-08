"""
SAR vessel detection ingestion.

Ingests Sentinel-1 derived vessel detections (from Copernicus or similar
SAR analytics providers) into the sar_detections DuckDB table.

Expected CSV schema:
    detection_id  – unique identifier (UUID or provider ID)
    detected_at   – ISO-8601 UTC timestamp
    lat           – WGS-84 latitude (decimal degrees)
    lon           – WGS-84 longitude (decimal degrees)
    length_m      – estimated vessel length from SAR backscatter (optional)
    source_scene  – Sentinel-1 scene ID (optional, e.g. S1A_IW_GRDH_...)
    confidence    – detection confidence 0–1 (optional, default 1.0)

Usage:
    uv run python src/ingest/sar.py --csv path/to/detections.csv
    uv run python src/ingest/sar.py --csv path/to/detections.csv --db data/processed/mpol.duckdb
"""

from __future__ import annotations

import argparse
import os
import uuid
from datetime import UTC, datetime

import duckdb
import polars as pl
from dotenv import load_dotenv

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")

REQUIRED_COLUMNS = {"detection_id", "detected_at", "lat", "lon"}
OPTIONAL_DEFAULTS = {"length_m": None, "source_scene": None, "confidence": 1.0}


def ingest_sar_csv(csv_path: str, db_path: str = DEFAULT_DB_PATH) -> int:
    """Load SAR detections from *csv_path* and upsert into sar_detections.

    Returns the number of rows inserted.
    """
    df = pl.read_csv(csv_path, try_parse_dates=True)

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"SAR CSV missing required columns: {missing}")

    for col, default in OPTIONAL_DEFAULTS.items():
        if col not in df.columns:
            if default is None:
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
            else:
                df = df.with_columns(pl.lit(default).alias(col))

    df = df.select(
        [
            pl.col("detection_id").cast(pl.Utf8),
            pl.col("detected_at").cast(pl.Datetime("us", "UTC")),
            pl.col("lat").cast(pl.Float64),
            pl.col("lon").cast(pl.Float64),
            pl.col("length_m").cast(pl.Float32),
            pl.col("source_scene").cast(pl.Utf8),
            pl.col("confidence").cast(pl.Float32),
        ]
    )

    con = duckdb.connect(db_path)
    try:
        con.execute("BEGIN")
        con.execute(
            """
            INSERT OR IGNORE INTO sar_detections
                (detection_id, detected_at, lat, lon, length_m, source_scene, confidence)
            SELECT detection_id, detected_at, lat, lon, length_m, source_scene, confidence
            FROM df
            """
        )
        con.execute("COMMIT")
        return len(df)
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def ingest_sar_records(
    records: list[dict],
    db_path: str = DEFAULT_DB_PATH,
) -> int:
    """Insert a list of SAR detection dicts directly (for testing / programmatic use).

    Each dict must have: detected_at (datetime), lat, lon.
    detection_id is auto-generated if absent.
    """
    rows = []
    for r in records:
        rows.append(
            {
                "detection_id": r.get("detection_id", str(uuid.uuid4())),
                "detected_at": r["detected_at"],
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
                "length_m": r.get("length_m"),
                "source_scene": r.get("source_scene"),
                "confidence": float(r.get("confidence", 1.0)),
            }
        )

    df = pl.DataFrame(
        rows,
        schema={
            "detection_id": pl.Utf8,
            "detected_at": pl.Datetime("us", "UTC"),
            "lat": pl.Float64,
            "lon": pl.Float64,
            "length_m": pl.Float32,
            "source_scene": pl.Utf8,
            "confidence": pl.Float32,
        },
    )

    con = duckdb.connect(db_path)
    try:
        con.execute("BEGIN")
        con.execute(
            """
            INSERT OR IGNORE INTO sar_detections
                (detection_id, detected_at, lat, lon, length_m, source_scene, confidence)
            SELECT detection_id, detected_at, lat, lon, length_m, source_scene, confidence
            FROM df
            """
        )
        con.execute("COMMIT")
        return len(df)
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def _now_utc() -> datetime:
    return datetime.now(UTC)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest SAR vessel detections")
    parser.add_argument("--csv", required=True, help="Path to SAR detections CSV")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to DuckDB file")
    args = parser.parse_args()

    inserted = ingest_sar_csv(args.csv, args.db)
    print(f"Inserted {inserted} SAR detections into {args.db}")
