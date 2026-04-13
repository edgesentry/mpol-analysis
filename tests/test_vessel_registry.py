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
        "mmsi",
        "imo",
        "vessel_name",
        "owner_id",
        "owner_name",
        "owner_country",
        "owner_address_id",
        "owner_address",
        "manager_id",
        "manager_name",
        "manager_country",
        "since",
        "until",
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
    _seed_sanctions(
        tmp_db,
        [
            ("co-001", "EVIL CORP", None, None, "KP", "Company", "ofac_sdn"),
        ],
    )
    tables = build_graph_tables(tmp_db)
    assert len(tables["Company"]) == 1
    assert "co-001" in tables["Company"]["id"].to_pylist()
    sb_src = tables["SANCTIONED_BY"]["src_id"].to_pylist()
    assert "co-001" in sb_src


def test_build_graph_sanctions_vessel_by_mmsi(tmp_db):
    _seed_vessel_meta(tmp_db, [("123456789", "IMO001", "SHADOW")])
    _seed_sanctions(
        tmp_db,
        [
            ("v-001", "SHADOW SHIP", "123456789", "IMO001", "KP", "Vessel", "ofac_sdn"),
        ],
    )
    tables = build_graph_tables(tmp_db)
    sb_src = tables["SANCTIONED_BY"]["src_id"].to_pylist()
    assert "123456789" in sb_src


def test_build_graph_regime_nodes(tmp_db):
    _seed_sanctions(
        tmp_db,
        [
            ("co-001", "EVIL CORP", None, None, "KP", "Company", "ofac_sdn"),
        ],
    )
    tables = build_graph_tables(tmp_db)
    regime_names = tables["SanctionsRegime"]["name"].to_pylist()
    assert "ofac_sdn" in regime_names


def test_build_graph_registered_in(tmp_db):
    _seed_sanctions(
        tmp_db,
        [
            ("co-001", "EVIL CORP", None, None, "KP", "Company", "ofac_sdn"),
        ],
    )
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
    _write_equasis_csv(
        csv_path,
        [
            {
                "mmsi": "123456789",
                "imo": "IMO001",
                "vessel_name": "SHIP A",
                "owner_id": "co-001",
                "owner_name": "ACME LTD",
                "owner_country": "PA",
                "owner_address_id": "addr-001",
                "owner_address": "PO Box 1, Panama",
                "manager_id": "mgr-001",
                "manager_name": "MGMT CO",
                "manager_country": "SG",
                "since": "2022-01-01",
                "until": "",
            }
        ],
    )
    tables = build_graph_tables(tmp_db, equasis_csv=str(csv_path))
    assert len(tables["OWNED_BY"]) == 1
    assert tables["OWNED_BY"]["src_id"][0].as_py() == "123456789"
    assert tables["OWNED_BY"]["dst_id"][0].as_py() == "co-001"
    assert len(tables["MANAGED_BY"]) == 1
    assert len(tables["REGISTERED_AT"]) == 1


def test_build_graph_equasis_skips_missing_mmsi(tmp_db, tmp_path):
    csv_path = tmp_path / "equasis.csv"
    _write_equasis_csv(
        csv_path,
        [
            {"mmsi": "", "owner_id": "co-001"},
            {"mmsi": "123456789", "owner_id": "co-002", "owner_name": "X"},
        ],
    )
    tables = build_graph_tables(tmp_db, equasis_csv=str(csv_path))
    assert len(tables["OWNED_BY"]) == 1


def test_build_graph_equasis_no_address_if_id_missing(tmp_db, tmp_path):
    csv_path = tmp_path / "equasis.csv"
    _write_equasis_csv(
        csv_path,
        [
            {
                "mmsi": "123456789",
                "owner_id": "co-001",
                "owner_name": "X",
                "owner_address_id": "",
                "owner_address": "Some street",
            }
        ],
    )
    tables = build_graph_tables(tmp_db, equasis_csv=str(csv_path))
    assert len(tables["REGISTERED_AT"]) == 0


# ---------------------------------------------------------------------------
# #231 — MMSI-only sanctioned vessels (no IMO, or non-'Vessel' FtM type)
# ---------------------------------------------------------------------------


def test_mmsi_only_sanctioned_vessel_gets_node_and_edge(tmp_db):
    """A vessel on OFAC with MMSI but no IMO must appear in Vessel table and SANCTIONED_BY."""
    # Vessel is in AIS (vessel_meta) but NOT yet linked to sanctions
    _seed_vessel_meta(tmp_db, [("613490000", "", "")])
    # Sanctioned entry has MMSI only (no IMO), and type='Vessel'
    _seed_sanctions(
        tmp_db,
        [("v-mmsi-only", "SHADOW TANKER", "613490000", None, "IR", "Vessel", "us_ofac_sdn")],
    )
    tables = build_graph_tables(tmp_db)
    vessel_mmsis = set(tables["Vessel"]["mmsi"].to_pylist())
    assert "613490000" in vessel_mmsis
    sb_src = tables["SANCTIONED_BY"]["src_id"].to_pylist()
    assert "613490000" in sb_src


def test_non_vessel_type_with_mmsi_gets_sanctioned_by_edge(tmp_db):
    """An entity stored as non-'Vessel' FtM type but carrying an MMSI must still get
    a SANCTIONED_BY edge (issue #231 — some OFAC SDN entries use LegalEntity type)."""
    _seed_vessel_meta(tmp_db, [("620999538", "", "")])
    # type='LegalEntity', not 'Vessel' — previously excluded by the query
    _seed_sanctions(
        tmp_db,
        [("le-001", "MARITIME LLC", "620999538", None, "SY", "LegalEntity", "us_ofac_sdn")],
    )
    tables = build_graph_tables(tmp_db)
    sb_src = tables["SANCTIONED_BY"]["src_id"].to_pylist()
    assert "620999538" in sb_src


def test_sanctioned_mmsi_not_in_vessel_meta_has_no_vessel_node(tmp_db):
    """A sanctioned vessel whose MMSI never appeared in AIS must NOT get a Vessel
    node — adding stub nodes would inflate watchlists with zero-AIS vessels.
    The DuckDB fallback in ownership_graph.py handles distance correction instead."""
    # No vessel_meta row for this MMSI
    _seed_sanctions(
        tmp_db,
        [("v-ghost", "GHOST VESSEL", "256869000", None, "KP", "Vessel", "un_sc_sanctions")],
    )
    tables = build_graph_tables(tmp_db)
    vessel_mmsis = set(tables["Vessel"]["mmsi"].to_pylist())
    assert "256869000" not in vessel_mmsis
    # SANCTIONED_BY edge still exists for use by other graph lookups
    sb_src = tables["SANCTIONED_BY"]["src_id"].to_pylist()
    assert "256869000" in sb_src


# ---------------------------------------------------------------------------
# build_sts_contacts_from_ais
# ---------------------------------------------------------------------------


def _seed_ais_positions(db_path: str, rows: list[tuple]) -> None:
    """Insert (mmsi, timestamp, lat, lon) rows into ais_positions."""
    con = duckdb.connect(db_path)
    for mmsi, ts, lat, lon in rows:
        con.execute(
            "INSERT INTO ais_positions (mmsi, timestamp, lat, lon) VALUES (?, ?, ?, ?)",
            [mmsi, ts, lat, lon],
        )
    con.close()


def test_sts_contacts_empty_db(tmp_db):
    from src.ingest.vessel_registry import build_sts_contacts_from_ais

    assert build_sts_contacts_from_ais(tmp_db) == []


def test_sts_contacts_pair_detected(tmp_db):
    """Two vessels in the same H3 cell at the same 30-min bucket ≥2 times → STS contact."""
    from src.ingest.vessel_registry import build_sts_contacts_from_ais

    # Anchor point in Singapore Strait
    lat, lon = 1.26, 103.85
    _seed_ais_positions(
        tmp_db,
        [
            # Bucket 1: both vessels at the same spot
            ("111111111", "2026-01-01 02:05:00+00", lat, lon),
            ("222222222", "2026-01-01 02:10:00+00", lat, lon),
            # Bucket 2 (different 30-min window): both vessels again
            ("111111111", "2026-01-01 02:35:00+00", lat, lon),
            ("222222222", "2026-01-01 02:40:00+00", lat, lon),
        ],
    )
    contacts = build_sts_contacts_from_ais(tmp_db)
    pairs = {(r["src_id"], r["dst_id"]) for r in contacts}
    assert ("111111111", "222222222") in pairs


def test_sts_contacts_single_overlap_excluded(tmp_db):
    """Only 1 bucket overlap → below STS_MIN_CO_LOCATIONS → not a contact."""
    from src.ingest.vessel_registry import build_sts_contacts_from_ais

    lat, lon = 1.26, 103.85
    _seed_ais_positions(
        tmp_db,
        [
            ("111111111", "2026-01-01 02:05:00+00", lat, lon),
            ("222222222", "2026-01-01 02:10:00+00", lat, lon),
            # Second position: each vessel departs to a completely different area
            ("111111111", "2026-01-01 03:05:00+00", 5.0, 110.0),  # South China Sea
            ("222222222", "2026-01-01 03:10:00+00", 35.0, 139.0),  # Tokyo Bay
        ],
    )
    contacts = build_sts_contacts_from_ais(tmp_db)
    assert contacts == []


def test_sts_contacts_pair_ordering(tmp_db):
    """src_id < dst_id lexicographically — each pair appears exactly once."""
    from src.ingest.vessel_registry import build_sts_contacts_from_ais

    lat, lon = 1.26, 103.85
    _seed_ais_positions(
        tmp_db,
        [
            ("333333333", "2026-01-01 02:05:00+00", lat, lon),
            ("111111111", "2026-01-01 02:08:00+00", lat, lon),
            ("333333333", "2026-01-01 02:35:00+00", lat, lon),
            ("111111111", "2026-01-01 02:38:00+00", lat, lon),
        ],
    )
    contacts = build_sts_contacts_from_ais(tmp_db)
    assert len(contacts) == 1
    assert contacts[0]["src_id"] < contacts[0]["dst_id"]


def test_sts_contacts_different_cells_not_paired(tmp_db):
    """Vessels in different H3 cells must not be paired even in the same time bucket."""
    from src.ingest.vessel_registry import build_sts_contacts_from_ais

    _seed_ais_positions(
        tmp_db,
        [
            # Far apart — clearly different H3 cells
            ("111111111", "2026-01-01 02:05:00+00", 1.26, 103.85),
            ("222222222", "2026-01-01 02:07:00+00", 35.0, 139.0),
            ("111111111", "2026-01-01 02:35:00+00", 1.26, 103.85),
            ("222222222", "2026-01-01 02:37:00+00", 35.0, 139.0),
        ],
    )
    assert build_sts_contacts_from_ais(tmp_db) == []


def test_build_graph_tables_includes_sts_contacts(tmp_db):
    """build_graph_tables() populates STS_CONTACT from AIS data."""
    lat, lon = 1.26, 103.85
    _seed_ais_positions(
        tmp_db,
        [
            ("111111111", "2026-01-01 02:05:00+00", lat, lon),
            ("222222222", "2026-01-01 02:10:00+00", lat, lon),
            ("111111111", "2026-01-01 02:35:00+00", lat, lon),
            ("222222222", "2026-01-01 02:40:00+00", lat, lon),
        ],
    )
    tables = build_graph_tables(tmp_db)
    sts = tables["STS_CONTACT"]
    assert len(sts) >= 1
    src_ids = sts["src_id"].to_pylist()
    dst_ids = sts["dst_id"].to_pylist()
    assert "111111111" in src_ids or "111111111" in dst_ids
