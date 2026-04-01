import csv
from pathlib import Path

import duckdb
import polars as pl

from src.ingest.marine_cadastre import BBOX, _parse_range, load_csv_to_duckdb


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def _make_row(mmsi="123456789", lat=1.3, lon=103.8, sog=12.0, cog=180.0,
              status=0, vessel_type=80, vessel_name="TEST VESSEL",
              dt="2024-06-01T08:00:00"):
    return {
        "MMSI": mmsi,
        "BaseDateTime": dt,
        "LAT": lat,
        "LON": lon,
        "SOG": sog,
        "COG": cog,
        "Status": status,
        "VesselType": vessel_type,
        "VesselName": vessel_name,
        "IMO": "IMO1234567",
        "Flag": "SG",
        "GrossTonnage": 50000,
    }


def test_load_csv_inserts_in_bbox(tmp_path, tmp_db):
    csv_path = tmp_path / "test.csv"
    _write_csv(csv_path, [_make_row(lat=1.3, lon=103.8)])  # inside bbox

    n = load_csv_to_duckdb(csv_path, tmp_db, BBOX)
    assert n == 1

    con = duckdb.connect(tmp_db)
    count = con.execute("SELECT count(*) FROM ais_positions").fetchone()[0]
    con.close()
    assert count == 1


def test_load_csv_filters_outside_bbox(tmp_path, tmp_db):
    csv_path = tmp_path / "test.csv"
    # San Francisco (38N, 122W) — outside the SG bbox
    _write_csv(csv_path, [_make_row(lat=37.8, lon=-122.4)])

    n = load_csv_to_duckdb(csv_path, tmp_db, BBOX)
    assert n == 0


def test_load_csv_deduplicates(tmp_path, tmp_db):
    csv_path = tmp_path / "test.csv"
    row = _make_row()
    _write_csv(csv_path, [row, row])  # two identical rows

    n = load_csv_to_duckdb(csv_path, tmp_db, BBOX)
    assert n == 1  # second row is a duplicate and ignored

    con = duckdb.connect(tmp_db)
    count = con.execute("SELECT count(*) FROM ais_positions").fetchone()[0]
    con.close()
    assert count == 1


def test_load_csv_mixed_bbox(tmp_path, tmp_db):
    csv_path = tmp_path / "test.csv"
    rows = [
        _make_row(mmsi="111111111", lat=1.3, lon=103.8),   # inside
        _make_row(mmsi="222222222", lat=37.8, lon=-122.4),  # outside
        _make_row(mmsi="333333333", lat=5.0, lon=100.0),    # inside
    ]
    _write_csv(csv_path, rows)

    n = load_csv_to_duckdb(csv_path, tmp_db, BBOX)
    assert n == 2


def test_parse_range_dash():
    assert _parse_range("1-3") == [1, 2, 3]


def test_parse_range_comma():
    assert _parse_range("1,3,6") == [1, 3, 6]


def test_parse_range_single():
    assert _parse_range("10") == [10]
