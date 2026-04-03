"""
Lance Graph storage layer for the arktrace ownership graph.

Stores nodes and relationships as Lance datasets (columnar, serverless, embedded).
No external graph server required.

Directory layout (alongside the DuckDB file, e.g. data/processed/mpol.duckdb):
    data/processed/mpol_graph/
        Vessel.lance/
        Company.lance/
        Country.lance/
        Address.lance/
        VesselName.lance/
        SanctionsRegime.lance/
        ALIAS.lance/
        OWNED_BY.lance/
        MANAGED_BY.lance/
        SANCTIONED_BY.lance/
        REGISTERED_IN.lance/
        REGISTERED_AT.lance/
        CONTROLLED_BY.lance/
        STS_CONTACT.lance/
"""

import os
from pathlib import Path

import lance
import pyarrow as pa


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

NODE_SCHEMAS: dict[str, pa.Schema] = {
    "Vessel": pa.schema([
        pa.field("mmsi", pa.string()),
        pa.field("imo", pa.string()),
        pa.field("name", pa.string()),
    ]),
    "Company": pa.schema([
        pa.field("id", pa.string()),
        pa.field("name", pa.string()),
        pa.field("country", pa.string()),
    ]),
    "Country": pa.schema([
        pa.field("code", pa.string()),
    ]),
    "Address": pa.schema([
        pa.field("address_id", pa.string()),
        pa.field("street", pa.string()),
    ]),
    "VesselName": pa.schema([
        pa.field("name", pa.string()),
    ]),
    "SanctionsRegime": pa.schema([
        pa.field("name", pa.string()),
    ]),
}

# Relationship tables: src_id → dst_id plus optional edge properties.
REL_SCHEMAS: dict[str, pa.Schema] = {
    "ALIAS": pa.schema([
        pa.field("src_id", pa.string()),   # vessel mmsi
        pa.field("dst_id", pa.string()),   # vessel name
        pa.field("date", pa.string()),
    ]),
    "OWNED_BY": pa.schema([
        pa.field("src_id", pa.string()),   # vessel mmsi
        pa.field("dst_id", pa.string()),   # company id
        pa.field("since", pa.string()),
        pa.field("until", pa.string()),
    ]),
    "MANAGED_BY": pa.schema([
        pa.field("src_id", pa.string()),   # vessel mmsi
        pa.field("dst_id", pa.string()),   # company id
        pa.field("since", pa.string()),
        pa.field("until", pa.string()),
    ]),
    "SANCTIONED_BY": pa.schema([
        pa.field("src_id", pa.string()),   # vessel mmsi OR company id
        pa.field("dst_id", pa.string()),   # sanctions regime name
        pa.field("list", pa.string()),
        pa.field("date", pa.string()),
    ]),
    "REGISTERED_IN": pa.schema([
        pa.field("src_id", pa.string()),   # company id
        pa.field("dst_id", pa.string()),   # country code
    ]),
    "REGISTERED_AT": pa.schema([
        pa.field("src_id", pa.string()),   # company id
        pa.field("dst_id", pa.string()),   # address id
    ]),
    "CONTROLLED_BY": pa.schema([
        pa.field("src_id", pa.string()),   # child company id
        pa.field("dst_id", pa.string()),   # parent company id
    ]),
    "STS_CONTACT": pa.schema([
        pa.field("src_id", pa.string()),   # vessel mmsi
        pa.field("dst_id", pa.string()),   # vessel mmsi
    ]),
}

ALL_SCHEMAS: dict[str, pa.Schema] = {**NODE_SCHEMAS, **REL_SCHEMAS}


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def graph_dir(db_path: str) -> str:
    """Return the Lance graph directory for a given DuckDB path."""
    p = Path(db_path)
    return str(p.parent / (p.stem + "_graph"))


def _dataset_path(db_path: str, name: str) -> str:
    return os.path.join(graph_dir(db_path), f"{name}.lance")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def write_tables(db_path: str, tables: dict[str, pa.Table]) -> None:
    """Write (overwrite) a set of named tables to the Lance graph directory."""
    gdir = graph_dir(db_path)
    os.makedirs(gdir, exist_ok=True)
    for name, table in tables.items():
        path = _dataset_path(db_path, name)
        lance.write_dataset(table, path, mode="overwrite")


def load_tables(db_path: str) -> dict[str, pa.Table]:
    """Load all graph tables from the Lance graph directory as PyArrow tables.

    Missing datasets are returned as empty tables with the correct schema.
    """
    tables: dict[str, pa.Table] = {}
    for name, schema in ALL_SCHEMAS.items():
        path = _dataset_path(db_path, name)
        if os.path.exists(path):
            tables[name] = lance.dataset(path).to_table()
        else:
            tables[name] = schema.empty_table()
    return tables
