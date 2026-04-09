"""Generic AIS CSV / NMEA 0183 ingestion.

Supports any S-AIS provider (Spire, exactEarth, Orbcomm, etc.) that delivers
data as a CSV file or a file of NMEA 0183 VDM/VDO sentences.

CSV mode (default)
------------------
Column names are mapped to the ais_positions schema via --column-map.
Defaults match the MarineCadastre NOAA layout, which is the most common
open-format reference.  Override any field with a comma-separated key=value
string, e.g.::

    --column-map mmsi=vessel_id,lat=latitude,lon=longitude,timestamp=time_utc

NMEA mode (--nmea)
------------------
Parses NMEA 0183 VDM/VDO sentences (AIS message types 1, 2, 3, 18).
Multi-part sentences are assembled before decoding.  Non-position messages
(types 5, 24, etc.) are silently skipped.

Usage
-----
    # CSV with default column mapping (MarineCadastre-compatible):
    uv run python src/ingest/ais_csv.py --file data/raw/ais_2024.csv

    # CSV with custom column mapping (Spire format example):
    uv run python src/ingest/ais_csv.py --file spire_feed.csv \\
        --column-map mmsi=vessel_id,lat=latitude,lon=longitude,\\
timestamp=time_utc,sog=speed,cog=course

    # NMEA sentence file (one sentence per line, or interleaved with NMEA noise):
    uv run python src/ingest/ais_csv.py --file feed.nmea --nmea

    # Bounding-box filter (lat_min lon_min lat_max lon_max):
    uv run python src/ingest/ais_csv.py --file feed.csv \\
        --bbox -5 92 22 122
"""

from __future__ import annotations

import argparse
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl
from dotenv import load_dotenv

from src.ingest.schema import init_schema

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")

# Default column mapping: MarineCadastre NOAA CSV layout → internal schema
DEFAULT_COLUMN_MAP: dict[str, str] = {
    "mmsi": "MMSI",
    "timestamp": "BaseDateTime",
    "lat": "LAT",
    "lon": "LON",
    "sog": "SOG",
    "cog": "COG",
    "nav_status": "Status",
    "ship_type": "VesselType",
}

# Columns we write to ais_positions
_SCHEMA_COLS = ["mmsi", "timestamp", "lat", "lon", "sog", "cog", "nav_status", "ship_type"]


# ---------------------------------------------------------------------------
# CSV ingestion
# ---------------------------------------------------------------------------


def _parse_column_map(raw: str) -> dict[str, str]:
    """Parse 'mmsi=vessel_id,lat=latitude,...' into {internal: provider} mapping."""
    mapping: dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if "=" not in part:
            raise ValueError(f"Invalid column-map entry (expected key=value): {part!r}")
        internal, _, provider = part.partition("=")
        mapping[internal.strip()] = provider.strip()
    return mapping


def ingest_csv(
    file_path: str | Path,
    db_path: str = DEFAULT_DB_PATH,
    column_map: dict[str, str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    batch_size: int = 10_000,
) -> int:
    """Load AIS positions from a CSV file into DuckDB.

    Parameters
    ----------
    file_path:
        Path to the CSV file (any delimiter auto-detected by Polars).
    db_path:
        Target DuckDB path.
    column_map:
        Mapping from internal schema field name → provider column name.
        Defaults to MarineCadastre layout.
    bbox:
        Optional (lat_min, lon_min, lat_max, lon_max) bounding box filter.
    batch_size:
        Rows per DuckDB insert batch.

    Returns
    -------
    int
        Number of rows inserted.
    """
    col_map = {**DEFAULT_COLUMN_MAP, **(column_map or {})}
    file_path = Path(file_path)

    df = pl.read_csv(file_path, infer_schema_length=1000, try_parse_dates=False)

    # Rename provider columns → internal names
    rename: dict[str, str] = {}
    for internal, provider in col_map.items():
        if provider in df.columns:
            rename[provider] = internal
    df = df.rename(rename)

    # Keep only schema columns that are present
    present = [c for c in _SCHEMA_COLS if c in df.columns]
    df = df.select(present)

    # Fill missing optional columns with nulls
    for col in _SCHEMA_COLS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    # Cast types
    df = df.with_columns(
        [
            pl.col("mmsi").cast(pl.Utf8),
            pl.col("lat").cast(pl.Float64),
            pl.col("lon").cast(pl.Float64),
            pl.col("sog").cast(pl.Float32),
            pl.col("cog").cast(pl.Float32),
            pl.col("nav_status").cast(pl.Int8).fill_null(0),
            pl.col("ship_type").cast(pl.Int8).fill_null(0),
        ]
    )

    # Parse timestamp — try ISO first, then common provider formats
    if df["timestamp"].dtype == pl.Utf8:
        df = df.with_columns(
            pl.col("timestamp")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f", strict=False)
            .fill_null(
                pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
            )
            .dt.replace_time_zone("UTC")
            .alias("timestamp")
        )

    # Drop rows with null required fields
    df = df.drop_nulls(subset=["mmsi", "timestamp", "lat", "lon"])

    # Bounding box filter
    if bbox:
        lat_min, lon_min, lat_max, lon_max = bbox
        df = df.filter(
            (pl.col("lat") >= lat_min)
            & (pl.col("lat") <= lat_max)
            & (pl.col("lon") >= lon_min)
            & (pl.col("lon") <= lon_max)
        )

    if df.is_empty():
        return 0

    init_schema(db_path)
    return _flush_dataframe(df, db_path, batch_size)


# ---------------------------------------------------------------------------
# NMEA 0183 VDM/VDO ingestion
# ---------------------------------------------------------------------------


def _armored_to_bits(payload: str, fill_bits: int) -> list[int]:
    """Decode NMEA 6-bit ASCII armoring to a flat bit list."""
    bits: list[int] = []
    for ch in payload:
        v = ord(ch) - 48
        if v > 39:
            v -= 8
        for shift in range(5, -1, -1):
            bits.append((v >> shift) & 1)
    if fill_bits:
        del bits[-fill_bits:]
    return bits


def _uint(bits: list[int], start: int, length: int) -> int:
    v = 0
    for i in range(length):
        v = (v << 1) | bits[start + i]
    return v


def _sint(bits: list[int], start: int, length: int) -> int:
    v = _uint(bits, start, length)
    if bits[start]:
        v -= 1 << length
    return v


def _decode_position_report(bits: list[int]) -> dict | None:
    """Decode AIS message types 1/2/3 (Class A position report)."""
    if len(bits) < 168:
        return None
    mmsi = str(_uint(bits, 8, 30))
    nav_status = _uint(bits, 38, 4)
    sog = _uint(bits, 50, 10) / 10.0  # knots
    lon = _sint(bits, 61, 28) / 600_000.0
    lat = _sint(bits, 89, 27) / 600_000.0
    cog = _uint(bits, 116, 12) / 10.0
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return None
    return {
        "mmsi": mmsi,
        "lat": lat,
        "lon": lon,
        "sog": float(sog),
        "cog": float(cog),
        "nav_status": nav_status,
        "ship_type": 0,
    }


def _decode_class_b_report(bits: list[int]) -> dict | None:
    """Decode AIS message type 18 (Class B position report)."""
    if len(bits) < 168:
        return None
    mmsi = str(_uint(bits, 8, 30))
    sog = _uint(bits, 46, 10) / 10.0
    lon = _sint(bits, 57, 28) / 600_000.0
    lat = _sint(bits, 85, 27) / 600_000.0
    cog = _uint(bits, 112, 12) / 10.0
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return None
    return {
        "mmsi": mmsi,
        "lat": lat,
        "lon": lon,
        "sog": float(sog),
        "cog": float(cog),
        "nav_status": 0,
        "ship_type": 0,
    }


def _iter_nmea_records(
    file_path: Path,
    bbox: tuple[float, float, float, float] | None = None,
) -> Iterator[dict]:
    """Yield decoded AIS position records from an NMEA 0183 sentence file.

    Handles multi-part VDM/VDO sentences (sentence count > 1) by buffering
    parts until all fragments are available.
    """
    # Buffer for multi-part sentences: key = (total, seq_id)
    multipart: dict[tuple[int, str], list[tuple[int, str, int]]] = {}

    with open(file_path, encoding="utf-8", errors="replace") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line:
                continue

            # Strip leading ! or $ and checksum
            sentence = line.lstrip("!$")
            if "*" in sentence:
                sentence = sentence[: sentence.index("*")]

            parts = sentence.split(",")
            if len(parts) < 7:
                continue
            tag = parts[0].upper()
            if tag not in ("AIVDM", "AIVDO"):
                continue

            try:
                total = int(parts[1])
                seq_num = int(parts[2]) if parts[2] else 1
                seq_id = parts[3]  # channel/seq label for multi-part matching
                payload = parts[5]
                fill_bits = int(parts[6]) if parts[6] else 0
            except (ValueError, IndexError):
                continue

            if total == 1:
                # Single-sentence message
                assembled_payload = payload
                assembled_fill = fill_bits
            else:
                # Buffer fragments
                key = (total, seq_id)
                bucket = multipart.setdefault(key, [])
                bucket.append((seq_num, payload, fill_bits))
                if len(bucket) < total:
                    continue
                # All parts received — assemble in order
                bucket.sort(key=lambda x: x[0])
                assembled_payload = "".join(p for _, p, _ in bucket)
                assembled_fill = bucket[-1][2]  # fill bits apply to last part only
                del multipart[key]

            bits = _armored_to_bits(assembled_payload, assembled_fill)
            if len(bits) < 6:
                continue

            msg_type = _uint(bits, 0, 6)

            if msg_type in (1, 2, 3):
                record = _decode_position_report(bits)
            elif msg_type == 18:
                record = _decode_class_b_report(bits)
            else:
                continue

            if record is None:
                continue

            if bbox:
                lat_min, lon_min, lat_max, lon_max = bbox
                if not (
                    lat_min <= record["lat"] <= lat_max and lon_min <= record["lon"] <= lon_max
                ):
                    continue

            yield record


def ingest_nmea(
    file_path: str | Path,
    db_path: str = DEFAULT_DB_PATH,
    bbox: tuple[float, float, float, float] | None = None,
    timestamp_col: str | None = None,
    batch_size: int = 10_000,
) -> int:
    """Load AIS positions from an NMEA 0183 VDM/VDO sentence file into DuckDB.

    NMEA sentences carry no wall-clock timestamp.  If the provider wraps
    each sentence with a timestamp prefix (e.g. ``2024-01-15T10:30:00Z,!AIVDM,...``),
    pass ``--timestamp-col 0`` (column index) to parse it.  Otherwise,
    ``now()`` is used as the ingestion timestamp, which is suitable for
    real-time feeds but not historical replay.

    Parameters
    ----------
    file_path:
        Path to the NMEA sentence file.
    db_path:
        Target DuckDB path.
    bbox:
        Optional (lat_min, lon_min, lat_max, lon_max) bounding box filter.
    timestamp_col:
        Optional column index (0-based) or name prefix in the line containing
        an ISO-8601 timestamp.  When None, ingestion timestamp is used.
    batch_size:
        Records per DuckDB insert batch.

    Returns
    -------
    int
        Number of rows inserted.
    """
    file_path = Path(file_path)
    init_schema(db_path)

    batch: list[dict] = []
    total_inserted = 0
    ingest_ts = datetime.now(UTC)

    for record in _iter_nmea_records(file_path, bbox=bbox):
        record["timestamp"] = ingest_ts
        batch.append(record)
        if len(batch) >= batch_size:
            total_inserted += _flush_records(batch, db_path)
            batch.clear()

    if batch:
        total_inserted += _flush_records(batch, db_path)

    return total_inserted


# ---------------------------------------------------------------------------
# DuckDB flush helpers
# ---------------------------------------------------------------------------


def _flush_dataframe(df: pl.DataFrame, db_path: str, batch_size: int) -> int:
    con = duckdb.connect(db_path)
    inserted = 0
    try:
        for start in range(0, df.height, batch_size):
            chunk = df.slice(start, batch_size)
            con.execute(
                """
                INSERT OR IGNORE INTO ais_positions
                    (mmsi, timestamp, lat, lon, sog, cog, nav_status, ship_type)
                SELECT mmsi, timestamp, lat, lon, sog, cog, nav_status, ship_type
                FROM chunk
                """
            )
            inserted += chunk.height
    finally:
        con.close()
    return inserted


def _flush_records(records: list[dict], db_path: str) -> int:
    df = pl.DataFrame(
        {
            "mmsi": [r["mmsi"] for r in records],
            "timestamp": [r["timestamp"] for r in records],
            "lat": [r["lat"] for r in records],
            "lon": [r["lon"] for r in records],
            "sog": [float(r.get("sog") or 0) for r in records],
            "cog": [float(r.get("cog") or 0) for r in records],
            "nav_status": [int(r.get("nav_status") or 0) for r in records],
            "ship_type": [int(r.get("ship_type") or 0) for r in records],
        },
        schema={
            "mmsi": pl.Utf8,
            "timestamp": pl.Datetime(time_unit="us", time_zone="UTC"),
            "lat": pl.Float64,
            "lon": pl.Float64,
            "sog": pl.Float32,
            "cog": pl.Float32,
            "nav_status": pl.Int8,
            "ship_type": pl.Int8,
        },
    )
    return _flush_dataframe(df, db_path, len(records))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest AIS positions from a CSV or NMEA 0183 file into DuckDB"
    )
    parser.add_argument("--file", required=True, help="Path to the CSV or NMEA file")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="DuckDB path")
    parser.add_argument(
        "--nmea",
        action="store_true",
        help="Parse file as NMEA 0183 VDM/VDO sentences instead of CSV",
    )
    parser.add_argument(
        "--column-map",
        default=None,
        metavar="KEY=VAL,...",
        help=(
            "Comma-separated internal=provider column name overrides "
            "(e.g. mmsi=vessel_id,lat=latitude,lon=longitude,timestamp=time_utc). "
            "Unspecified fields use the MarineCadastre defaults."
        ),
    )
    parser.add_argument(
        "--bbox",
        type=float,
        nargs=4,
        metavar=("LAT_MIN", "LON_MIN", "LAT_MAX", "LON_MAX"),
        default=None,
        help="Bounding box filter — only keep records inside this region",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help="Rows per DuckDB insert batch (default: 10000)",
    )
    args = parser.parse_args()

    bbox: tuple[float, float, float, float] | None = tuple(args.bbox) if args.bbox else None

    if args.nmea:
        n = ingest_nmea(args.file, db_path=args.db, bbox=bbox, batch_size=args.batch_size)
    else:
        col_map = _parse_column_map(args.column_map) if args.column_map else None
        n = ingest_csv(
            args.file, db_path=args.db, column_map=col_map, bbox=bbox, batch_size=args.batch_size
        )

    print(f"Rows inserted: {n}")


if __name__ == "__main__":
    main()
