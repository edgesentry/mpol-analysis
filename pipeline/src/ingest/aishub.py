"""AISHub real-time AIS fetcher.

AISHub (aishub.net) is a free cooperative AIS data-sharing network.
Members receive access to an HTTP API that returns live vessel positions
for any bounding box.

Registration: https://www.aishub.net/join-us

API endpoint:
    http://data.aishub.net/ws.php
    ?username=YOUR_USERNAME
    &format=1          # 1 = JSON
    &output=json
    &compress=0
    &latmin=1.0&latmax=1.5&lonmin=103.5&lonmax=104.5

Response: two-element JSON array —
    [0] header dict: {"ERROR": false, "USERNAME": "...", "RECORDS": N}
    [1] list of position dicts (one per vessel)

Usage:
    # Set AISHUB_USERNAME in .env or as an environment variable, then:
    uv run python -m src.ingest.aishub --db data/processed/mpol.duckdb
    uv run python -m src.ingest.aishub --bbox -5 92 22 122   # Singapore/Malacca
    uv run python -m src.ingest.aishub --bbox 1.0 103.5 1.5 104.5  # Strait of Singapore
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime

import duckdb
import httpx
import polars as pl
from dotenv import load_dotenv

from pipeline.src.ingest.schema import init_schema

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")
AISHUB_API_URL = "http://data.aishub.net/ws.php"

# Singapore / Malacca Strait — default AOI matching the rest of arktrace
DEFAULT_BBOX = {
    "lat_min": -2.0,
    "lat_max": 8.0,
    "lon_min": 98.0,
    "lon_max": 110.0,
}

# AISHub JSON field → internal name
_FIELD_MAP = {
    "MMSI": "mmsi",
    "LONGITUDE": "lon",
    "LATITUDE": "lat",
    "COG": "cog",
    "SOG": "sog",
    "HEADING": "heading",
    "SHIPTYPE": "ship_type",
    "NAME": "vessel_name",
    "CALLSIGN": "callsign",
    "FLAG": "flag",
    "IMO": "imo",
    "TIME": "timestamp",
}


def fetch(
    username: str,
    bbox: dict | None = None,
    timeout: int = 30,
) -> list[dict]:
    """Fetch live vessel positions from AISHub for the given bounding box.

    Parameters
    ----------
    username:
        AISHub API username (from aishub.net account).
    bbox:
        Dict with lat_min, lat_max, lon_min, lon_max. Defaults to
        Singapore / Malacca Strait.
    timeout:
        HTTP timeout in seconds.

    Returns
    -------
    List of raw position dicts as returned by the AISHub API.
    """
    bb = bbox or DEFAULT_BBOX
    params = {
        "username": username,
        "format": "1",
        "output": "json",
        "compress": "0",
        "latmin": bb["lat_min"],
        "latmax": bb["lat_max"],
        "lonmin": bb["lon_min"],
        "lonmax": bb["lon_max"],
    }

    with httpx.Client(timeout=timeout) as client:
        resp = client.get(AISHUB_API_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

    if not isinstance(data, list) or len(data) < 2:
        raise ValueError(f"Unexpected AISHub response shape: {data!r}")

    header = data[0]
    if header.get("ERROR"):
        raise RuntimeError(f"AISHub API error: {header}")

    records = data[1] if isinstance(data[1], list) else []
    print(f"  AISHub returned {header.get('RECORDS', len(records))} vessel positions")
    return records


def _parse_timestamp(raw: str | None) -> datetime | None:
    """Parse AISHub TIME field ('2024-01-01 12:00:00') to UTC datetime."""
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    except ValueError:
        return None


def load_to_duckdb(records: list[dict], db_path: str = DEFAULT_DB_PATH) -> int:
    """Insert AISHub positions into ais_positions and vessel_meta.

    Returns the number of rows inserted into ais_positions.
    """
    if not records:
        return 0

    rows = []
    meta_rows = []
    now = datetime.now(UTC)

    for r in records:
        mmsi = str(r.get("MMSI", "")).strip()
        if not mmsi:
            continue

        ts = _parse_timestamp(r.get("TIME")) or now
        lat = r.get("LATITUDE")
        lon = r.get("LONGITUDE")
        if lat is None or lon is None:
            continue

        rows.append(
            {
                "mmsi": mmsi,
                "timestamp": ts,
                "lat": float(lat),
                "lon": float(lon),
                "sog": float(r["SOG"]) if r.get("SOG") is not None else None,
                "cog": float(r["COG"]) if r.get("COG") is not None else None,
                "nav_status": None,
                "ship_type": int(r["SHIPTYPE"]) if r.get("SHIPTYPE") else None,
            }
        )

        imo = str(r.get("IMO", "")).strip()
        name = str(r.get("NAME", "")).strip()
        flag = str(r.get("FLAG", "")).strip()
        meta_rows.append(
            {
                "mmsi": mmsi,
                "imo": imo or None,
                "name": name or None,
                "flag": flag or None,
                "ship_type": int(r["SHIPTYPE"]) if r.get("SHIPTYPE") else None,
                "gross_tonnage": None,
            }
        )

    if not rows:
        return 0

    pos_df = pl.DataFrame(rows)  # noqa: F841 — referenced by DuckDB
    meta_df = pl.DataFrame(meta_rows).unique(subset=["mmsi"], keep="first")  # noqa: F841

    con = duckdb.connect(db_path)
    try:
        before = con.execute("SELECT count(*) FROM ais_positions").fetchone()[0]  # type: ignore[index]
        con.execute("""
            INSERT OR IGNORE INTO ais_positions
                (mmsi, timestamp, lat, lon, sog, cog, nav_status, ship_type)
            SELECT mmsi, timestamp, lat, lon, sog, cog, nav_status, ship_type
            FROM pos_df
        """)
        inserted = con.execute("SELECT count(*) FROM ais_positions").fetchone()[0] - before  # type: ignore[index]

        con.execute("""
            INSERT OR IGNORE INTO vessel_meta (mmsi, imo, name, flag, ship_type, gross_tonnage)
            SELECT mmsi, imo, name, flag, ship_type, gross_tonnage
            FROM meta_df
            WHERE mmsi IS NOT NULL
        """)
    finally:
        con.close()

    return inserted


def fetch_and_load(
    username: str,
    db_path: str = DEFAULT_DB_PATH,
    bbox: dict | None = None,
) -> int:
    """Fetch from AISHub and load into DuckDB. Returns rows inserted."""
    init_schema(db_path)
    records = fetch(username, bbox)
    return load_to_duckdb(records, db_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch AISHub live positions into DuckDB")
    parser.add_argument(
        "--username",
        default=os.getenv("AISHUB_USERNAME", ""),
        help="AISHub username (or set AISHUB_USERNAME in .env)",
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument(
        "--bbox",
        type=float,
        nargs=4,
        metavar=("LAT_MIN", "LON_MIN", "LAT_MAX", "LON_MAX"),
        default=None,
        help=(
            "Bounding box: lat_min lon_min lat_max lon_max. "
            "Default: Singapore/Malacca Strait (-2 98 8 110). "
            "Strait of Singapore only: 1.0 103.5 1.5 104.5"
        ),
    )
    args = parser.parse_args()

    if not args.username:
        parser.error(
            "AISHub username required. Set AISHUB_USERNAME in .env or pass --username. "
            "Register free at https://www.aishub.net/join-us"
        )

    bbox = None
    if args.bbox:
        lat_min, lon_min, lat_max, lon_max = args.bbox
        bbox = {"lat_min": lat_min, "lat_max": lat_max, "lon_min": lon_min, "lon_max": lon_max}

    inserted = fetch_and_load(args.username, args.db, bbox)
    print(f"Rows inserted into ais_positions: {inserted}")


if __name__ == "__main__":
    main()
