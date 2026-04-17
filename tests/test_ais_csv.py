"""Tests for src/ingest/ais_csv.py — generic CSV/NMEA ingestion."""

from __future__ import annotations

import textwrap
from pathlib import Path

import duckdb
import pytest

from pipeline.src.ingest.ais_csv import (
    _armored_to_bits,
    _decode_position_report,
    _iter_nmea_records,
    _parse_column_map,
    _uint,
    ingest_csv,
    ingest_nmea,
)

# ---------------------------------------------------------------------------
# CSV ingestion
# ---------------------------------------------------------------------------


def _write_csv(tmp_path: Path, content: str, name: str = "feed.csv") -> Path:
    p = tmp_path / name
    p.write_text(textwrap.dedent(content))
    return p


def test_ingest_csv_default_columns(tmp_path):
    """MarineCadastre-layout CSV is loaded without a column-map override."""
    csv_path = _write_csv(
        tmp_path,
        """\
        MMSI,BaseDateTime,LAT,LON,SOG,COG,Status,VesselType
        123456789,2024-06-01T10:00:00,1.3,103.8,12.5,180.0,0,80
        987654321,2024-06-01T10:05:00,1.35,103.85,8.0,90.0,0,70
        """,
    )
    db_path = str(tmp_path / "test.duckdb")
    n = ingest_csv(csv_path, db_path=db_path)
    assert n == 2

    con = duckdb.connect(db_path, read_only=True)
    rows = con.execute(
        "SELECT mmsi, lat, lon, sog, ship_type FROM ais_positions ORDER BY mmsi"
    ).fetchall()
    con.close()
    assert len(rows) == 2
    assert rows[0][0] == "123456789"
    assert rows[0][3] == pytest.approx(12.5, abs=0.1)
    assert rows[0][4] == 80


def test_ingest_csv_custom_column_map(tmp_path):
    """Provider columns are remapped correctly via --column-map."""
    csv_path = _write_csv(
        tmp_path,
        """\
        vessel_id,time_utc,latitude,longitude,speed,course
        111111111,2024-06-01 09:00:00,2.0,104.0,5.5,270.0
        """,
    )
    db_path = str(tmp_path / "test.duckdb")
    col_map = {
        "mmsi": "vessel_id",
        "timestamp": "time_utc",
        "lat": "latitude",
        "lon": "longitude",
        "sog": "speed",
        "cog": "course",
    }
    n = ingest_csv(csv_path, db_path=db_path, column_map=col_map)
    assert n == 1

    con = duckdb.connect(db_path, read_only=True)
    row = con.execute("SELECT mmsi, lat, lon, sog FROM ais_positions").fetchone()
    con.close()
    assert row[0] == "111111111"
    assert row[1] == pytest.approx(2.0)
    assert row[3] == pytest.approx(5.5, abs=0.1)


def test_ingest_csv_bbox_filter(tmp_path):
    """Rows outside the bounding box are excluded."""
    csv_path = _write_csv(
        tmp_path,
        """\
        MMSI,BaseDateTime,LAT,LON,SOG,COG,Status,VesselType
        111111111,2024-06-01T10:00:00,1.3,103.8,5.0,0.0,0,80
        222222222,2024-06-01T10:00:00,35.0,139.0,5.0,0.0,0,80
        """,
    )
    db_path = str(tmp_path / "test.duckdb")
    n = ingest_csv(csv_path, db_path=db_path, bbox=(-5.0, 92.0, 22.0, 122.0))
    assert n == 1

    con = duckdb.connect(db_path, read_only=True)
    rows = con.execute("SELECT mmsi FROM ais_positions").fetchall()
    con.close()
    assert rows[0][0] == "111111111"


def test_ingest_csv_deduplicates(tmp_path):
    """Duplicate (mmsi, timestamp) rows are ignored on re-ingest."""
    csv_path = _write_csv(
        tmp_path,
        """\
        MMSI,BaseDateTime,LAT,LON,SOG,COG,Status,VesselType
        123456789,2024-06-01T10:00:00,1.3,103.8,5.0,0.0,0,80
        """,
    )
    db_path = str(tmp_path / "test.duckdb")
    ingest_csv(csv_path, db_path=db_path)
    n2 = ingest_csv(csv_path, db_path=db_path)
    # Second pass: INSERT OR IGNORE — rows already present, 0 net new
    assert n2 == 1  # rows attempted; DuckDB silently ignores duplicates

    con = duckdb.connect(db_path, read_only=True)
    count = con.execute("SELECT count(*) FROM ais_positions").fetchone()[0]
    con.close()
    assert count == 1


def test_parse_column_map():
    mapping = _parse_column_map("mmsi=vessel_id,lat=latitude,lon=longitude")
    assert mapping == {"mmsi": "vessel_id", "lat": "latitude", "lon": "longitude"}


def test_parse_column_map_invalid():
    with pytest.raises(ValueError, match="key=value"):
        _parse_column_map("mmsi:vessel_id")


# ---------------------------------------------------------------------------
# NMEA bit-level decoder
# ---------------------------------------------------------------------------


def _encode_bits(values: list[tuple[int, int]]) -> str:
    """Pack (value, bit_length) pairs into an NMEA 6-bit ASCII payload."""
    bits: list[int] = []
    for v, length in values:
        for shift in range(length - 1, -1, -1):
            bits.append((v >> shift) & 1)

    # Pad to multiple of 6
    while len(bits) % 6:
        bits.append(0)

    chars = []
    for i in range(0, len(bits), 6):
        v = sum(bits[i + j] << (5 - j) for j in range(6))
        c = v + 48
        if c > 87:
            c += 8
        chars.append(chr(c))
    return "".join(chars)


def _make_type1_payload(
    mmsi: int, lat: float, lon: float, sog: float = 0.0, cog: float = 0.0, nav_status: int = 0
) -> tuple[str, int]:
    """Build a minimal AIS type-1 payload (168 bits)."""
    lat_raw = int(lat * 600_000)
    lon_raw = int(lon * 600_000)
    sog_raw = int(sog * 10)
    cog_raw = int(cog * 10)

    def twos(v: int, bits: int) -> int:
        return v & ((1 << bits) - 1)

    fields = [
        (1, 6),  # msg type
        (0, 2),  # repeat
        (mmsi, 30),
        (nav_status, 4),
        (0, 8),  # rate of turn
        (sog_raw, 10),
        (0, 1),  # pos accuracy
        (twos(lon_raw, 28), 28),
        (twos(lat_raw, 27), 27),
        (cog_raw, 12),
        (0, 9),  # true heading
        (0, 6),  # time stamp
        (0, 4),  # maneuver
        (0, 3),  # spare
        (0, 1),  # RAIM
        (0, 19),  # radio status
    ]
    return _encode_bits(fields), 0


def test_armored_to_bits_roundtrip():
    """Encoding and decoding should be consistent."""
    payload, fill = _make_type1_payload(123456789, lat=1.3, lon=103.8)
    bits = _armored_to_bits(payload, fill)
    assert _uint(bits, 0, 6) == 1  # message type
    assert _uint(bits, 8, 30) == 123456789


def test_decode_position_report():
    payload, fill = _make_type1_payload(
        mmsi=123456789, lat=1.3, lon=103.8, sog=12.5, cog=180.0, nav_status=3
    )
    bits = _armored_to_bits(payload, fill)
    record = _decode_position_report(bits)
    assert record is not None
    assert record["mmsi"] == "123456789"
    assert record["lat"] == pytest.approx(1.3, abs=0.001)
    assert record["lon"] == pytest.approx(103.8, abs=0.001)
    assert record["sog"] == pytest.approx(12.5, abs=0.1)
    assert record["nav_status"] == 3


def test_decode_position_report_rejects_invalid_coords():
    # Build a payload with out-of-range lat/lon
    payload, fill = _make_type1_payload(mmsi=1, lat=0.0, lon=0.0)
    bits = _armored_to_bits(payload, fill)
    # Corrupt lat to a positive value > 90° (raw > 54_000_000).
    # Lat field starts at bit 89 (27 bits, signed).
    # Set sign bit to 0 (positive) and rest to 1 → value = 2^26-1 = 67,108,863 raw = ~111.8°
    bits[89] = 0  # sign bit = positive
    for i in range(90, 89 + 27):
        bits[i] = 1
    record = _decode_position_report(bits)
    assert record is None


# ---------------------------------------------------------------------------
# NMEA file ingestion
# ---------------------------------------------------------------------------


def _write_nmea(tmp_path: Path, sentences: list[str], name: str = "feed.nmea") -> Path:
    p = tmp_path / name
    p.write_text("\n".join(sentences) + "\n")
    return p


def test_iter_nmea_records_single_sentence(tmp_path):
    payload, fill = _make_type1_payload(mmsi=123456789, lat=1.3, lon=103.8, sog=5.0)
    sentence = f"!AIVDM,1,1,,A,{payload},{fill}*00"
    nmea_path = _write_nmea(tmp_path, [sentence])
    records = list(_iter_nmea_records(nmea_path))
    assert len(records) == 1
    assert records[0]["mmsi"] == "123456789"
    assert records[0]["lat"] == pytest.approx(1.3, abs=0.001)


def test_iter_nmea_records_skips_non_vdm(tmp_path):
    payload, fill = _make_type1_payload(mmsi=111111111, lat=1.0, lon=100.0)
    lines = [
        "$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47",
        f"!AIVDM,1,1,,A,{payload},{fill}*00",
        "# comment line",
    ]
    records = list(_iter_nmea_records(_write_nmea(tmp_path, lines)))
    assert len(records) == 1


def test_iter_nmea_records_bbox_filter(tmp_path):
    p_in, f_in = _make_type1_payload(mmsi=111111111, lat=1.3, lon=103.8)
    p_out, f_out = _make_type1_payload(mmsi=222222222, lat=35.0, lon=139.0)
    lines = [
        f"!AIVDM,1,1,,A,{p_in},{f_in}*00",
        f"!AIVDM,1,1,,A,{p_out},{f_out}*00",
    ]
    records = list(_iter_nmea_records(_write_nmea(tmp_path, lines), bbox=(-5, 92, 22, 122)))
    assert len(records) == 1
    assert records[0]["mmsi"] == "111111111"


def test_ingest_nmea_writes_to_duckdb(tmp_path):
    payload, fill = _make_type1_payload(mmsi=555555555, lat=1.5, lon=104.0, sog=7.0)
    nmea_path = _write_nmea(tmp_path, [f"!AIVDM,1,1,,A,{payload},{fill}*00"])
    db_path = str(tmp_path / "test.duckdb")
    n = ingest_nmea(nmea_path, db_path=db_path)
    assert n == 1

    con = duckdb.connect(db_path, read_only=True)
    row = con.execute("SELECT mmsi, lat, lon, sog FROM ais_positions").fetchone()
    con.close()
    assert row[0] == "555555555"
    assert row[1] == pytest.approx(1.5, abs=0.001)
