"""
Marine Cadastre AIS bulk loader.

Downloads NOAA Marine Cadastre AIS CSV archives (US coastal zones) and loads
records that fall within the area of interest into DuckDB. Marine Cadastre
provides supplementary historical coverage for vessels that transited
US-monitored zones; it is not the primary source for the Singapore/Malacca area.

Download URL pattern:
  https://coast.noaa.gov/htdata/CMSP/AISDataCatalogue/AIS_{year}_{month:02d}_Zone{zone:02d}.zip

Usage:
    # Load Pacific zone for 6 months of 2024 (supplementary historical backfill)
    uv run python src/ingest/marine_cadastre.py --year 2024 --months 1-6 --zones 10

    # Load multiple zones
    uv run python src/ingest/marine_cadastre.py --year 2024 --months 1-12 --zones 10,17,18
"""

import argparse
import io
import os
import zipfile
from pathlib import Path

import duckdb
import httpx
import polars as pl
from dotenv import load_dotenv

from src.ingest.schema import init_schema

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")
DEFAULT_RAW_DIR = "data/raw/marine_cadastre"

# Area of interest: Singapore + Malacca Strait (up to 1600nm)
BBOX = {
    "lat_min": -5.0,
    "lat_max": 22.0,
    "lon_min": 92.0,
    "lon_max": 122.0,
}

BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataCatalogue"

# Marine Cadastre CSV column mapping → internal schema
_MC_COLUMNS = {
    "MMSI": "mmsi",
    "BaseDateTime": "timestamp",
    "LAT": "lat",
    "LON": "lon",
    "SOG": "sog",
    "COG": "cog",
    "Status": "nav_status",
    "VesselType": "ship_type",
    "VesselName": "vessel_name",
    "IMO": "imo",
    "Flag": "flag",
    "GrossTonnage": "gross_tonnage",
}


def _archive_url(year: int, month: int, zone: int) -> str:
    return f"{BASE_URL}/AIS_{year}_{month:02d}_Zone{zone:02d}.zip"


def download_zone(
    year: int,
    month: int,
    zone: int,
    out_dir: str = DEFAULT_RAW_DIR,
) -> Path | None:
    """Download one zone archive and return the path to the extracted CSV.

    Returns None if the archive is not available (HTTP 404).
    Already-downloaded files are skipped.
    """
    out_path = Path(out_dir) / f"AIS_{year}_{month:02d}_Zone{zone:02d}"
    csv_path = out_path / f"AIS_{year}_{month:02d}_Zone{zone:02d}.csv"

    if csv_path.exists():
        return csv_path

    out_path.mkdir(parents=True, exist_ok=True)
    url = _archive_url(year, month, zone)

    with httpx.Client(timeout=300, follow_redirects=True) as client:
        resp = client.get(url)
        if resp.status_code == 404:
            print(f"  Not found (404): {url}")
            return None
        resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        zf.extractall(out_path)

    if not csv_path.exists():
        # Some archives use a flat structure without subdirectory
        candidates = list(out_path.glob("*.csv"))
        if candidates:
            return candidates[0]
        return None

    return csv_path


def load_csv_to_duckdb(
    csv_path: Path,
    db_path: str = DEFAULT_DB_PATH,
    bbox: dict = BBOX,
) -> int:
    """Filter CSV to bbox and INSERT OR IGNORE into ais_positions / vessel_meta.

    Returns number of rows inserted into ais_positions.
    """
    available = pl.scan_csv(csv_path, try_parse_dates=False).collect_schema().names()
    select_cols = [c for c in _MC_COLUMNS if c in available]

    lf = (
        pl.scan_csv(csv_path, try_parse_dates=False)
        .select(select_cols)
        .rename({k: v for k, v in _MC_COLUMNS.items() if k in select_cols})
        .filter(
            (pl.col("lat") >= bbox["lat_min"])
            & (pl.col("lat") <= bbox["lat_max"])
            & (pl.col("lon") >= bbox["lon_min"])
            & (pl.col("lon") <= bbox["lon_max"])
        )
        .with_columns(
            pl.col("mmsi").cast(pl.Utf8),
            pl.col("timestamp").str.to_datetime(
                "%Y-%m-%dT%H:%M:%S", strict=False
            ),
            pl.col("sog").cast(pl.Float32, strict=False),
            pl.col("cog").cast(pl.Float32, strict=False),
            pl.col("nav_status").cast(pl.Int8, strict=False),
            pl.col("ship_type").cast(pl.Int8, strict=False),
        )
        .drop_nulls(subset=["mmsi", "timestamp", "lat", "lon"])
    )
    df = lf.collect()

    if df.is_empty():
        return 0

    con = duckdb.connect(db_path)
    try:
        pos_cols = ["mmsi", "timestamp", "lat", "lon", "sog", "cog", "nav_status", "ship_type"]
        pos_df = df.select([c for c in pos_cols if c in df.columns])
        before = con.execute("SELECT count(*) FROM ais_positions").fetchone()[0]
        con.execute("""
            INSERT OR IGNORE INTO ais_positions
            SELECT mmsi, timestamp, lat, lon, sog, cog, nav_status, ship_type
            FROM pos_df
        """)
        inserted = con.execute("SELECT count(*) FROM ais_positions").fetchone()[0] - before

        # Upsert vessel_meta from static fields where available
        meta_src_cols = ["mmsi", "vessel_name", "imo", "flag", "ship_type", "gross_tonnage"]
        meta_avail = [c for c in meta_src_cols if c in df.columns]
        if len(meta_avail) > 1:
            meta_df = (
                df.select(meta_avail)
                .unique(subset=["mmsi"], keep="first")
            )
            con.execute("""
                INSERT OR IGNORE INTO vessel_meta (mmsi, imo, name, flag, ship_type, gross_tonnage)
                SELECT
                    mmsi,
                    NULLIF(TRIM(imo), '') AS imo,
                    NULLIF(TRIM(vessel_name), '') AS name,
                    NULLIF(TRIM(flag), '') AS flag,
                    ship_type,
                    gross_tonnage
                FROM meta_df
                WHERE mmsi IS NOT NULL
            """ if "vessel_name" in meta_avail else """
                INSERT OR IGNORE INTO vessel_meta (mmsi, imo, flag, ship_type, gross_tonnage)
                SELECT mmsi, NULLIF(TRIM(imo), ''), NULLIF(TRIM(flag), ''), ship_type, gross_tonnage
                FROM meta_df
                WHERE mmsi IS NOT NULL
            """)
    finally:
        con.close()

    return inserted


def load_zone(
    year: int,
    month: int,
    zone: int,
    db_path: str = DEFAULT_DB_PATH,
    raw_dir: str = DEFAULT_RAW_DIR,
    bbox: dict = BBOX,
) -> int:
    """Download (if needed) and load one zone/month into DuckDB."""
    print(f"Zone {zone:02d} {year}-{month:02d}: downloading …")
    csv_path = download_zone(year, month, zone, raw_dir)
    if csv_path is None:
        return 0
    print(f"  Loading {csv_path} …")
    n = load_csv_to_duckdb(csv_path, db_path, bbox)
    print(f"  Inserted {n} rows")
    return n


def _parse_range(s: str) -> list[int]:
    """Parse '1-6' or '1,3,10' into a list of ints."""
    if "-" in s:
        lo, hi = s.split("-", 1)
        return list(range(int(lo), int(hi) + 1))
    return [int(x) for x in s.split(",")]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Marine Cadastre AIS data into DuckDB")
    parser.add_argument("--year", type=int, required=True, help="Year (e.g. 2024)")
    parser.add_argument("--months", default="1-12", help="Month range or list, e.g. '1-6' or '1,3,6'")
    parser.add_argument("--zones", default="10", help="UTM zone(s), e.g. '10' or '10,17,18'")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="DuckDB path")
    parser.add_argument("--raw-dir", default=DEFAULT_RAW_DIR, help="Raw download directory")
    args = parser.parse_args()

    init_schema(args.db)
    months = _parse_range(args.months)
    zones = _parse_range(args.zones)
    total = 0
    for zone in zones:
        for month in months:
            total += load_zone(args.year, month, zone, args.db, args.raw_dir)
    print(f"\nTotal rows inserted: {total}")
