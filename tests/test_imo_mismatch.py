"""Unit tests for IMO identity spoofing detection features.

Tests cover compute_imo_mismatch_features() and the _ship_type_category()
helper.  All tests use a temporary DuckDB — no Equasis API required.
"""

import csv

import duckdb
import polars as pl

from pipeline.src.features.identity import (
    _ship_type_category,
    compute_imo_mismatch_features,
)
from pipeline.src.ingest.vessel_registry import upsert_equasis_vessel_ref

# ---------------------------------------------------------------------------
# _ship_type_category
# ---------------------------------------------------------------------------


def test_ship_type_category_tanker():
    for code in range(80, 90):
        assert _ship_type_category(code) == 5


def test_ship_type_category_cargo():
    for code in range(70, 80):
        assert _ship_type_category(code) == 4


def test_ship_type_category_passenger():
    for code in range(60, 70):
        assert _ship_type_category(code) == 3


def test_ship_type_category_fishing():
    assert _ship_type_category(30) == 1


def test_ship_type_category_unknown():
    assert _ship_type_category(0) == 0
    assert _ship_type_category(99) == 0
    assert _ship_type_category(None) == 0


# ---------------------------------------------------------------------------
# upsert_equasis_vessel_ref
# ---------------------------------------------------------------------------


def _write_equasis_ref_csv(path, rows: list[dict]) -> None:
    fieldnames = ["imo", "vessel_type", "build_year", "scrapped"]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def test_upsert_equasis_vessel_ref_inserts_rows(tmp_db, tmp_path):
    csv_path = tmp_path / "equasis_ref.csv"
    _write_equasis_ref_csv(
        csv_path,
        [
            {"imo": "9999991", "vessel_type": 80, "build_year": 2005, "scrapped": "false"},
            {"imo": "9999992", "vessel_type": 70, "build_year": 2018, "scrapped": "false"},
        ],
    )
    n = upsert_equasis_vessel_ref(tmp_db, str(csv_path))
    assert n == 2

    con = duckdb.connect(tmp_db, read_only=True)
    try:
        rows = con.execute(
            "SELECT imo, vessel_type, build_year, scrapped FROM equasis_vessel_ref ORDER BY imo"
        ).fetchall()
    finally:
        con.close()

    assert len(rows) == 2
    assert rows[0] == ("9999991", 80, 2005, False)
    assert rows[1] == ("9999992", 70, 2018, False)


def test_upsert_equasis_vessel_ref_scrapped_flag(tmp_db, tmp_path):
    csv_path = tmp_path / "equasis_ref.csv"
    _write_equasis_ref_csv(
        csv_path,
        [{"imo": "1234567", "vessel_type": 80, "build_year": 1998, "scrapped": "true"}],
    )
    upsert_equasis_vessel_ref(tmp_db, str(csv_path))

    con = duckdb.connect(tmp_db, read_only=True)
    try:
        scrapped = con.execute(
            "SELECT scrapped FROM equasis_vessel_ref WHERE imo='1234567'"
        ).fetchone()[0]
    finally:
        con.close()

    assert scrapped is True


def test_upsert_equasis_vessel_ref_skips_missing_imo(tmp_db, tmp_path):
    csv_path = tmp_path / "equasis_ref.csv"
    _write_equasis_ref_csv(csv_path, [{"imo": "", "vessel_type": 80, "build_year": 2000}])
    n = upsert_equasis_vessel_ref(tmp_db, str(csv_path))
    assert n == 0


# ---------------------------------------------------------------------------
# compute_imo_mismatch_features
# ---------------------------------------------------------------------------


def _seed_vessel_meta(db_path: str, rows: list[dict]) -> None:
    con = duckdb.connect(db_path)
    try:
        for r in rows:
            con.execute(
                "INSERT OR IGNORE INTO vessel_meta (mmsi, imo, ship_type) VALUES (?, ?, ?)",
                [r["mmsi"], r.get("imo", ""), r.get("ship_type")],
            )
    finally:
        con.close()


def _seed_equasis_ref(db_path: str, rows: list[dict]) -> None:
    con = duckdb.connect(db_path)
    try:
        for r in rows:
            con.execute(
                "INSERT OR IGNORE INTO equasis_vessel_ref (imo, vessel_type, build_year, scrapped) VALUES (?, ?, ?, ?)",
                [r["imo"], r.get("vessel_type"), r.get("build_year"), r.get("scrapped", False)],
            )
    finally:
        con.close()


def test_imo_type_mismatch_detected(tmp_db):
    """Vessel reports AIS ship_type=70 (cargo) but IMO is registered as tanker (80)."""
    _seed_vessel_meta(tmp_db, [{"mmsi": "111111111", "imo": "9000001", "ship_type": 70}])
    _seed_equasis_ref(tmp_db, [{"imo": "9000001", "vessel_type": 80}])

    result = compute_imo_mismatch_features(tmp_db)
    row = result.filter(pl.col("mmsi") == "111111111")
    assert not row.is_empty()
    assert row["imo_type_mismatch"][0] is True
    assert row["imo_scrapped_flag"][0] is False


def test_imo_type_match_not_flagged(tmp_db):
    """Vessel reports tanker (82) and IMO is registered as tanker (80) — same category."""
    _seed_vessel_meta(tmp_db, [{"mmsi": "222222222", "imo": "9000002", "ship_type": 82}])
    _seed_equasis_ref(tmp_db, [{"imo": "9000002", "vessel_type": 83}])

    result = compute_imo_mismatch_features(tmp_db)
    row = result.filter(pl.col("mmsi") == "222222222")
    assert not row.is_empty()
    assert row["imo_type_mismatch"][0] is False


def test_imo_scrapped_flag_detected(tmp_db):
    """Vessel's IMO is marked as scrapped in Equasis reference data."""
    _seed_vessel_meta(tmp_db, [{"mmsi": "333333333", "imo": "9000003", "ship_type": 80}])
    _seed_equasis_ref(tmp_db, [{"imo": "9000003", "vessel_type": 80, "scrapped": True}])

    result = compute_imo_mismatch_features(tmp_db)
    row = result.filter(pl.col("mmsi") == "333333333")
    assert not row.is_empty()
    assert row["imo_scrapped_flag"][0] is True


def test_imo_unknown_category_not_flagged(tmp_db):
    """When either AIS or Equasis type is unknown (0), mismatch is not raised."""
    _seed_vessel_meta(tmp_db, [{"mmsi": "444444444", "imo": "9000004", "ship_type": 0}])
    _seed_equasis_ref(tmp_db, [{"imo": "9000004", "vessel_type": 80}])

    result = compute_imo_mismatch_features(tmp_db)
    row = result.filter(pl.col("mmsi") == "444444444")
    assert not row.is_empty()
    assert row["imo_type_mismatch"][0] is False


def test_imo_no_equasis_ref_returns_false_defaults(tmp_db):
    """When equasis_vessel_ref is empty, both features default to False."""
    _seed_vessel_meta(tmp_db, [{"mmsi": "555555555", "imo": "9000005", "ship_type": 80}])

    result = compute_imo_mismatch_features(tmp_db)
    row = result.filter(pl.col("mmsi") == "555555555")
    assert not row.is_empty()
    assert row["imo_type_mismatch"][0] is False
    assert row["imo_scrapped_flag"][0] is False


def test_imo_mismatch_empty_db_returns_correct_schema(tmp_db):
    result = compute_imo_mismatch_features(tmp_db)
    assert "mmsi" in result.columns
    assert "imo_type_mismatch" in result.columns
    assert "imo_scrapped_flag" in result.columns
