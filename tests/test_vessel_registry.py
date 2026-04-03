"""
Unit tests for vessel_registry.py (Lance Graph backend).
"""

import csv
from pathlib import Path

import duckdb
import pyarrow as pa

from src.ingest.vessel_registry import build_graph_tables


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_vessel_meta(db_path: str, rows: list[tuple]) -> None:
    """Insert (mmsi, imo, name) rows into vessel_meta."""
    con = duckdb.connect(db_path)
    for mmsi, imo, name in rows:
        con.execute(
            "INSERT OR IGNORE INTO vessel_meta (mmsi, imo, name) VALUES (?, ?, ?)",
            [mmsi, imo, name],
        )
    con.close()


def _seed_sanctions(db_path: str, rows: list[tuple]) -> None:
    """Insert (entity_id, name, mmsi, imo, flag, type, list_source) into sanctions_entities."""
    con = duckdb.connect(db_path)
    for row in rows:
        con.execute(
            "INSERT OR IGNORE INTO sanctions_entities "
            "(entity_id, name, mmsi, imo, flag, type, list_source) VALUES (?,?,?,?,?,?,?)",
            list(row),
        )
    con.close()


def _write_equasis_csv(path: Path, rows: list[dict]) -> None:
    fieldnames = [
        "mmsi", "imo", "vessel_name",
        "owner_id", "owner_name", "owner_country", "owner_address_id", "owner_address",
        "manager_id", "manager_name", "manager_country",
        "since", "until",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# build_graph_tables — vessels
# ---------------------------------------------------------------------------

def test_build_graph_empty_db(tmp_db):
    tables = build_graph_tables(tmp_db)
    assert isinstance(tables["Vessel"], pa.Table)
    assert len(tables["Vessel"]) == 0


def test_build_graph_vessel_nodes(tmp_db):
    _seed_vessel_meta(tmp_db, [("123456789", "IMO001", "SHIP A"), ("999999999", "", "SHIP B")])
    tables = build_graph_tables(tmp_db)
    vessel_mmsis = set(tables["Vessel"]["mmsi"].to_pylist())
    assert vessel_mmsis == {"123456789", "999999999"}


def test_build_graph_vessel_imo_name(tmp_db):
    _seed_vessel_meta(tmp_db, [("123456789", "IMO001", "SHIP A")])
    tables = build_graph_tables(tmp_db)
    row = tables["Vessel"].to_pydict()
    assert row["imo"][0] == "IMO001"
    assert row["name"][0] == "SHIP A"


# ---------------------------------------------------------------------------
# build_graph_tables — sanctions
# ---------------------------------------------------------------------------

def test_build_graph_sanctions_empty(tmp_db):
    tables = build_graph_tables(tmp_db)
    assert len(tables["SANCTIONED_BY"]) == 0
    assert len(tables["Company"]) == 0


def test_build_graph_sanctions_company(tmp_db):
    _seed_sanctions(tmp_db, [
        ("co-001", "EVIL CORP", None, None, "KP", "Company", "ofac_sdn"),
    ])
    tables = build_graph_tables(tmp_db)
    assert len(tables["Company"]) == 1
    assert "co-001" in tables["Company"]["id"].to_pylist()
    sb_src = tables["SANCTIONED_BY"]["src_id"].to_pylist()
    assert "co-001" in sb_src


def test_build_graph_sanctions_vessel_by_mmsi(tmp_db):
    _seed_vessel_meta(tmp_db, [("123456789", "IMO001", "SHADOW")])
    _seed_sanctions(tmp_db, [
        ("v-001", "SHADOW SHIP", "123456789", "IMO001", "KP", "Vessel", "ofac_sdn"),
    ])
    tables = build_graph_tables(tmp_db)
    sb_src = tables["SANCTIONED_BY"]["src_id"].to_pylist()
    assert "123456789" in sb_src


def test_build_graph_regime_nodes(tmp_db):
    _seed_sanctions(tmp_db, [
        ("co-001", "EVIL CORP", None, None, "KP", "Company", "ofac_sdn"),
    ])
    tables = build_graph_tables(tmp_db)
    regime_names = tables["SanctionsRegime"]["name"].to_pylist()
    assert "ofac_sdn" in regime_names


def test_build_graph_registered_in(tmp_db):
    _seed_sanctions(tmp_db, [
        ("co-001", "EVIL CORP", None, None, "KP", "Company", "ofac_sdn"),
    ])
    tables = build_graph_tables(tmp_db)
    ri_src = tables["REGISTERED_IN"]["src_id"].to_pylist()
    assert "co-001" in ri_src
    ri_dst = tables["REGISTERED_IN"]["dst_id"].to_pylist()
    assert "KP" in ri_dst


# ---------------------------------------------------------------------------
# build_graph_tables — equasis CSV
# ---------------------------------------------------------------------------

def test_build_graph_equasis_ownership(tmp_db, tmp_path):
    csv_path = tmp_path / "equasis.csv"
    _write_equasis_csv(csv_path, [{
        "mmsi": "123456789", "imo": "IMO001", "vessel_name": "SHIP A",
        "owner_id": "co-001", "owner_name": "ACME LTD", "owner_country": "PA",
        "owner_address_id": "addr-001", "owner_address": "PO Box 1, Panama",
        "manager_id": "mgr-001", "manager_name": "MGMT CO", "manager_country": "SG",
        "since": "2022-01-01", "until": "",
    }])
    tables = build_graph_tables(tmp_db, equasis_csv=str(csv_path))
    assert len(tables["OWNED_BY"]) == 1
    assert tables["OWNED_BY"]["src_id"][0].as_py() == "123456789"
    assert tables["OWNED_BY"]["dst_id"][0].as_py() == "co-001"
    assert len(tables["MANAGED_BY"]) == 1
    assert len(tables["REGISTERED_AT"]) == 1


def test_build_graph_equasis_skips_missing_mmsi(tmp_db, tmp_path):
    csv_path = tmp_path / "equasis.csv"
    _write_equasis_csv(csv_path, [
        {"mmsi": "", "owner_id": "co-001"},
        {"mmsi": "123456789", "owner_id": "co-002", "owner_name": "X"},
    ])
    tables = build_graph_tables(tmp_db, equasis_csv=str(csv_path))
    assert len(tables["OWNED_BY"]) == 1


def test_build_graph_equasis_no_address_if_id_missing(tmp_db, tmp_path):
    csv_path = tmp_path / "equasis.csv"
    _write_equasis_csv(csv_path, [{
        "mmsi": "123456789", "owner_id": "co-001", "owner_name": "X",
        "owner_address_id": "",
        "owner_address": "Some street",
    }])
    tables = build_graph_tables(tmp_db, equasis_csv=str(csv_path))
    assert len(tables["REGISTERED_AT"]) == 0
