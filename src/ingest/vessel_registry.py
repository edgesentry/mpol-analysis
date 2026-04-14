"""
Vessel ownership registry — Lance Graph builder.

Builds the ownership graph as Lance datasets from two sources:

1. **Vessel nodes** seeded from DuckDB vessel_meta (populated by AIS ingestion).
2. **Company, ownership, and sanctions relationships** derived from DuckDB
   sanctions_entities (populated by src/ingest/sanctions.py).
3. **Equasis-style ownership chains** from an optional CSV export.
4. **STS_CONTACT edges** inferred from AIS co-location (vessels in the same
   H3 resolution-8 cell within the same 30-minute bucket, at least twice).

No external graph server required — data is stored as Lance files on disk.

Usage:
    uv run python src/ingest/vessel_registry.py

    # Also load Equasis ownership chains
    uv run python src/ingest/vessel_registry.py --equasis-csv data/raw/equasis.csv
"""

import argparse
import csv
import os

import duckdb
import polars as pl
import pyarrow as pa
from dotenv import load_dotenv

from src.graph.store import NODE_SCHEMAS, REL_SCHEMAS, write_tables

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _rows_to_table(rows: list[dict], schema: pa.Schema) -> pa.Table:
    """Convert a list of dicts to a PyArrow table, filling missing fields with None."""
    if not rows:
        return schema.empty_table()
    arrays = {field.name: [r.get(field.name) for r in rows] for field in schema}
    return pa.table(arrays, schema=schema)


# ---------------------------------------------------------------------------
# STS co-location inference
# ---------------------------------------------------------------------------

H3_RESOLUTION = 8  # ~0.74 km² cells — appropriate for STS proximity detection
STS_MIN_CO_LOCATIONS = 2  # require ≥2 bucket overlaps to exclude transient proximity


def _geo_to_h3(lat: float, lon: float, res: int = H3_RESOLUTION) -> str:
    """H3 cell index — compatible with h3-py 3.x and 4.x (mirrors ais_behavior.py)."""
    import h3

    try:
        return h3.latlng_to_cell(lat, lon, res)  # h3-py >= 4
    except AttributeError:
        return h3.geo_to_h3(lat, lon, res)  # h3-py < 4


def build_sts_contacts_from_ais(db_path: str) -> list[dict]:
    """Infer STS contact pairs from AIS H3 co-location.

    Two vessels are co-located when they share the same H3 resolution-8 cell
    within the same 30-minute time bucket.  Pairs must co-locate at least
    STS_MIN_CO_LOCATIONS times to filter out transient proximity (port
    approaches, traffic lane crossings).

    H3 cells are computed with the Python h3 library (already a project
    dependency via ais_behavior.py) rather than the DuckDB h3 community
    extension, which requires a separate install step.

    Returns a list of {"src_id": mmsi_a, "dst_id": mmsi_b} dicts ready to be
    written to the STS_CONTACT Lance table.  Each pair appears once
    (src_id < dst_id lexicographically).
    """
    con = duckdb.connect(db_path, read_only=True)
    try:
        # Use .pl() to avoid the pytz dependency that fetchall() requires for
        # TIMESTAMP WITH TIME ZONE columns.
        pos_df = con.execute(
            "SELECT mmsi, timestamp, lat, lon FROM ais_positions "
            "WHERE lat IS NOT NULL AND lon IS NOT NULL AND mmsi IS NOT NULL"
        ).pl()
    finally:
        con.close()

    if pos_df.is_empty():
        return []

    # Compute H3 cells and 30-minute floor buckets in Python
    mmsis = pos_df["mmsi"].to_list()
    lats = pos_df["lat"].to_list()
    lons = pos_df["lon"].to_list()
    # Cast to UTC-naive milliseconds for bucket arithmetic
    ts_ms = pos_df["timestamp"].cast(pl.Datetime("ms")).to_list()

    cells: list[str] = []
    buckets: list[str] = []
    for ts, lat, lon in zip(ts_ms, lats, lons):
        cells.append(_geo_to_h3(lat, lon))
        # Floor to the nearest 30-minute boundary
        bucket = ts.replace(minute=(ts.minute // 30) * 30, second=0, microsecond=0)
        buckets.append(bucket.isoformat())

    bucketed_df = pl.DataFrame({"mmsi": mmsis, "cell": cells, "bucket": buckets})

    mem = duckdb.connect()
    mem.register("bucketed", bucketed_df)
    result = mem.execute(f"""
        SELECT a.mmsi AS src_id, b.mmsi AS dst_id
        FROM bucketed a
        JOIN bucketed b
          ON a.cell = b.cell
         AND a.bucket = b.bucket
         AND a.mmsi < b.mmsi
        GROUP BY a.mmsi, b.mmsi
        HAVING COUNT(*) >= {STS_MIN_CO_LOCATIONS}
    """).fetchall()
    mem.close()

    return [{"src_id": r[0], "dst_id": r[1]} for r in result]


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------


def build_graph_tables(
    db_path: str,
    equasis_csv: str | None = None,
) -> dict[str, pa.Table]:
    """Build all graph tables from DuckDB and an optional Equasis CSV.

    Returns a dict of PyArrow tables ready to be written as Lance datasets.
    """
    con = duckdb.connect(db_path, read_only=True)
    try:
        vessel_rows = con.execute(
            "SELECT mmsi, COALESCE(imo,'') AS imo, COALESCE(name,'') AS name "
            "FROM vessel_meta WHERE mmsi IS NOT NULL"
        ).fetchall()

        company_rows = con.execute(
            "SELECT entity_id, COALESCE(name,'') AS name, COALESCE(flag,'') AS flag, "
            "list_source FROM sanctions_entities "
            "WHERE type IN ('Company','Organization','LegalEntity')"
        ).fetchall()

        sanctioned_vessel_rows = con.execute(
            "SELECT entity_id, COALESCE(mmsi,'') AS mmsi, COALESCE(imo,'') AS imo, "
            "list_source FROM sanctions_entities "
            "WHERE (type = 'Vessel' OR (mmsi IS NOT NULL AND mmsi <> '')) "
            "AND (mmsi IS NOT NULL OR imo IS NOT NULL)"
        ).fetchall()

        sanctioned_company_rows = con.execute(
            "SELECT entity_id, COALESCE(name,'') AS name, list_source "
            "FROM sanctions_entities "
            "WHERE type IN ('Company','Organization','LegalEntity')"
        ).fetchall()
    finally:
        con.close()

    # ------------------------------------------------------------------
    # Node accumulators (dict keyed by unique ID for MERGE semantics)
    # ------------------------------------------------------------------
    vessels: dict[str, dict] = {}
    for r in vessel_rows:
        vessels[r[0]] = {"mmsi": r[0], "imo": r[1], "name": r[2]}

    companies: dict[str, dict] = {}
    for entity_id, name, flag, _ in company_rows:
        companies[entity_id] = {"id": entity_id, "name": name, "country": flag}

    countries: dict[str, dict] = {}
    for _, _, flag, _ in company_rows:
        if flag:
            countries[flag] = {"code": flag}

    regimes: set[str] = set()
    for *_, list_source in sanctioned_vessel_rows:
        regimes.add(list_source)
    for *_, list_source in sanctioned_company_rows:
        regimes.add(list_source)

    addresses: dict[str, dict] = {}
    vessel_names: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Relationship accumulators (list; duplicates checked via seen sets)
    # ------------------------------------------------------------------
    sanctioned_by: list[dict] = []
    _sb_seen: set[tuple] = set()

    registered_in: list[dict] = []
    _ri_seen: set[tuple] = set()

    registered_at: list[dict] = []
    owned_by: list[dict] = []
    managed_by: list[dict] = []
    aliases: list[dict] = []

    # ------------------------------------------------------------------
    # SANCTIONED_BY edges
    # ------------------------------------------------------------------
    # Build mmsi→vessel and imo→vessel indices for sanctions lookup
    imo_index: dict[str, str] = {v["imo"]: v["mmsi"] for v in vessels.values() if v["imo"]}

    for entity_id, mmsi, imo, list_source in sanctioned_vessel_rows:
        vessel_id = mmsi or imo_index.get(imo)
        if vessel_id and (vessel_id, list_source) not in _sb_seen:
            sanctioned_by.append(
                {
                    "src_id": vessel_id,
                    "dst_id": list_source,
                    "list": list_source,
                    "date": entity_id,
                }
            )
            _sb_seen.add((vessel_id, list_source))

    for entity_id, _name, list_source in sanctioned_company_rows:
        if (entity_id, list_source) not in _sb_seen:
            sanctioned_by.append(
                {
                    "src_id": entity_id,
                    "dst_id": list_source,
                    "list": list_source,
                    "date": "",
                }
            )
            _sb_seen.add((entity_id, list_source))

    # ------------------------------------------------------------------
    # REGISTERED_IN edges (company → country)
    # ------------------------------------------------------------------
    for entity_id, _, flag, _ in company_rows:
        if flag and (entity_id, flag) not in _ri_seen:
            registered_in.append({"src_id": entity_id, "dst_id": flag})
            _ri_seen.add((entity_id, flag))

    # ------------------------------------------------------------------
    # Equasis CSV — ownership / management chains
    # ------------------------------------------------------------------
    if equasis_csv:
        with open(equasis_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mmsi = (row.get("mmsi") or "").strip()
                if not mmsi:
                    continue

                imo = (row.get("imo") or "").strip()
                vessel_name = (row.get("vessel_name") or "").strip()
                since = (row.get("since") or "").strip()
                until = (row.get("until") or "").strip()

                # Upsert vessel
                existing = vessels.get(mmsi, {})
                vessels[mmsi] = {
                    "mmsi": mmsi,
                    "imo": imo or existing.get("imo", ""),
                    "name": vessel_name or existing.get("name", ""),
                }

                owner_id = (row.get("owner_id") or "").strip()
                if owner_id:
                    owner_name = (row.get("owner_name") or "").strip()
                    owner_country = (row.get("owner_country") or "").strip()

                    existing_co = companies.get(owner_id, {})
                    companies[owner_id] = {
                        "id": owner_id,
                        "name": owner_name or existing_co.get("name", ""),
                        "country": owner_country or existing_co.get("country", ""),
                    }
                    if owner_country:
                        countries[owner_country] = {"code": owner_country}
                        if (owner_id, owner_country) not in _ri_seen:
                            registered_in.append({"src_id": owner_id, "dst_id": owner_country})
                            _ri_seen.add((owner_id, owner_country))

                    owned_by.append(
                        {
                            "src_id": mmsi,
                            "dst_id": owner_id,
                            "since": since,
                            "until": until,
                        }
                    )

                    addr_id = (row.get("owner_address_id") or "").strip()
                    if addr_id:
                        addresses[addr_id] = {
                            "address_id": addr_id,
                            "street": (row.get("owner_address") or "").strip(),
                        }
                        registered_at.append({"src_id": owner_id, "dst_id": addr_id})

                manager_id = (row.get("manager_id") or "").strip()
                if manager_id:
                    manager_name = (row.get("manager_name") or "").strip()
                    existing_mgr = companies.get(manager_id, {})
                    companies[manager_id] = {
                        "id": manager_id,
                        "name": manager_name or existing_mgr.get("name", ""),
                        "country": existing_mgr.get("country", ""),
                    }
                    managed_by.append(
                        {
                            "src_id": mmsi,
                            "dst_id": manager_id,
                            "since": since,
                            "until": until,
                        }
                    )

    # ------------------------------------------------------------------
    # Assemble PyArrow tables
    # ------------------------------------------------------------------
    return {
        "Vessel": _rows_to_table(list(vessels.values()), NODE_SCHEMAS["Vessel"]),
        "Company": _rows_to_table(list(companies.values()), NODE_SCHEMAS["Company"]),
        "Country": _rows_to_table(list(countries.values()), NODE_SCHEMAS["Country"]),
        "Address": _rows_to_table(list(addresses.values()), NODE_SCHEMAS["Address"]),
        "VesselName": _rows_to_table(list(vessel_names.values()), NODE_SCHEMAS["VesselName"]),
        "SanctionsRegime": _rows_to_table(
            [{"name": n} for n in regimes], NODE_SCHEMAS["SanctionsRegime"]
        ),
        "ALIAS": _rows_to_table(aliases, REL_SCHEMAS["ALIAS"]),
        "OWNED_BY": _rows_to_table(owned_by, REL_SCHEMAS["OWNED_BY"]),
        "MANAGED_BY": _rows_to_table(managed_by, REL_SCHEMAS["MANAGED_BY"]),
        "SANCTIONED_BY": _rows_to_table(sanctioned_by, REL_SCHEMAS["SANCTIONED_BY"]),
        "REGISTERED_IN": _rows_to_table(registered_in, REL_SCHEMAS["REGISTERED_IN"]),
        "REGISTERED_AT": _rows_to_table(registered_at, REL_SCHEMAS["REGISTERED_AT"]),
        # CONTROLLED_BY is populated externally (e.g. company hierarchy data)
        "CONTROLLED_BY": REL_SCHEMAS["CONTROLLED_BY"].empty_table(),
        # STS_CONTACT: inferred from AIS co-location (H3 res-8, 30-min buckets)
        "STS_CONTACT": _rows_to_table(
            build_sts_contacts_from_ais(db_path), REL_SCHEMAS["STS_CONTACT"]
        ),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build vessel ownership graph (Lance Graph)")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument(
        "--equasis-csv", default=None, help="Path to Equasis ownership chain CSV export"
    )
    args = parser.parse_args()

    print("Building ownership graph tables …")
    tables = build_graph_tables(args.db, equasis_csv=args.equasis_csv)

    print("Writing Lance datasets …")
    write_tables(args.db, tables)

    for name, table in tables.items():
        print(f"  {name}: {len(table)} rows")
