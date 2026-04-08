"""
Marine Cadastre AIS bulk loader.

Downloads NOAA Marine Cadastre AIS annual archives (US coastal waters) and loads
records that fall within the area of interest into DuckDB. Marine Cadastre
provides supplementary historical coverage for vessels that transited
US-monitored zones; it is not the primary source for the Singapore/Malacca area.

Download URL pattern (current as of 2025):
  https://marinecadastre.gov/downloads/data/ais/ais{year}/AISVesselTracks{year}.zip

Usage:
    uv run python src/ingest/marine_cadastre.py --year 2023
    uv run python src/ingest/marine_cadastre.py --year 2022 --year 2023
"""

import argparse
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

BASE_URL = "https://marinecadastre.gov/downloads/data/ais"

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


def _archive_url(year: int) -> str:
    return f"{BASE_URL}/ais{year}/AISVesselTracks{year}.zip"


def download_year(
    year: int,
    out_dir: str = DEFAULT_RAW_DIR,
) -> list[Path]:
    """Download the annual archive for *year* and return paths to extracted CSVs.

    Returns an empty list if the archive is not available (HTTP 404).
    Already-downloaded files are skipped.
    """
    out_path = Path(out_dir) / str(year)
    existing_csvs = list(out_path.glob("*.csv"))
    if existing_csvs:
        return existing_csvs

    out_path.mkdir(parents=True, exist_ok=True)
    url = _archive_url(year)
    print(f"  Downloading {url} …")

    zip_path = out_path / f"AISVesselTracks{year}.zip"
    with httpx.Client(timeout=600, follow_redirects=True) as client:
        with client.stream("GET", url) as resp:
            if resp.status_code == 404:
                print(f"  Not found (404): {url}")
                return []
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            downloaded = 0
            with open(zip_path, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded / total * 100
                        print(
                            f"\r  {downloaded / 1e6:.0f} MB / {total / 1e6:.0f} MB ({pct:.0f}%)",
                            end="",
                            flush=True,
                        )
            print()

    print("  Extracting …")
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(out_path)
    zip_path.unlink()

    return list(out_path.glob("**/*.csv"))


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
            pl.col("timestamp").str.to_datetime("%Y-%m-%dT%H:%M:%S", strict=False),
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
        pos_df = df.select([c for c in pos_cols if c in df.columns])  # noqa: F841 — referenced by DuckDB via `FROM pos_df`
        before = con.execute("SELECT count(*) FROM ais_positions").fetchone()[0]  # type: ignore[index]
        con.execute("""
            INSERT OR IGNORE INTO ais_positions
            SELECT mmsi, timestamp, lat, lon, sog, cog, nav_status, ship_type
            FROM pos_df
        """)
        inserted = con.execute("SELECT count(*) FROM ais_positions").fetchone()[0] - before  # type: ignore[index]

        # Upsert vessel_meta from static fields where available
        meta_src_cols = ["mmsi", "vessel_name", "imo", "flag", "ship_type", "gross_tonnage"]
        meta_avail = [c for c in meta_src_cols if c in df.columns]
        if len(meta_avail) > 1:
            meta_df = (  # noqa: F841 — referenced by DuckDB via `FROM meta_df`
                df.select(meta_avail).unique(subset=["mmsi"], keep="first")
            )
            con.execute(
                """
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
            """
                if "vessel_name" in meta_avail
                else """
                INSERT OR IGNORE INTO vessel_meta (mmsi, imo, flag, ship_type, gross_tonnage)
                SELECT mmsi, NULLIF(TRIM(imo), ''), NULLIF(TRIM(flag), ''), ship_type, gross_tonnage
                FROM meta_df
                WHERE mmsi IS NOT NULL
            """
            )
    finally:
        con.close()

    return inserted


def load_year(
    year: int,
    db_path: str = DEFAULT_DB_PATH,
    raw_dir: str = DEFAULT_RAW_DIR,
    bbox: dict = BBOX,
) -> int:
    """Download (if needed) and load all CSVs for *year* into DuckDB."""
    print(f"Year {year}: downloading archive …")
    csv_paths = download_year(year, raw_dir)
    if not csv_paths:
        return 0

    total = 0
    for csv_path in csv_paths:
        print(f"  Loading {csv_path.name} …")
        n = load_csv_to_duckdb(csv_path, db_path, bbox)
        print(f"  Inserted {n} rows")
        total += n
    return total


def _parse_range(s: str) -> list[int]:
    """Parse '1-6' or '1,3,10' into a list of ints."""
    if "-" in s:
        lo, hi = s.split("-", 1)
        return list(range(int(lo), int(hi) + 1))
    return [int(x) for x in s.split(",")]


def _parse_bbox(values: list[float]) -> dict:
    lat_min, lon_min, lat_max, lon_max = values
    return {"lat_min": lat_min, "lat_max": lat_max, "lon_min": lon_min, "lon_max": lon_max}


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Marine Cadastre AIS data into DuckDB")
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        action="append",
        dest="years",
        help="Year to download (repeat for multiple, e.g. --year 2022 --year 2023)",
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="DuckDB path")
    parser.add_argument("--raw-dir", default=DEFAULT_RAW_DIR, help="Raw download directory")
    parser.add_argument(
        "--bbox",
        type=float,
        nargs=4,
        metavar=("LAT_MIN", "LON_MIN", "LAT_MAX", "LON_MAX"),
        default=None,
        help=(
            "Bounding box filter as four floats: lat_min lon_min lat_max lon_max. "
            "Defaults to the Singapore / Malacca Strait preset. "
            "Example (US Gulf): --bbox 8 -98 32 -60"
        ),
    )
    args = parser.parse_args()

    bbox = _parse_bbox(args.bbox) if args.bbox else BBOX
    init_schema(args.db)
    total = 0
    for year in args.years:
        total += load_year(year, args.db, args.raw_dir, bbox)
    print(f"\nTotal rows inserted: {total}")


if __name__ == "__main__":
    main()
