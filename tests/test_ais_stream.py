from datetime import UTC, datetime

import duckdb

from pipeline.src.ingest.ais_stream import _flush_batch, _parse_position_report


def _make_msg(mmsi=123456789, lat=1.3, lon=103.8, nav_status=0, ship_type=80):
    return {
        "MessageType": "PositionReport",
        "MetaData": {
            "MMSI": mmsi,
            "latitude": lat,
            "longitude": lon,
            "time_utc": "2024-06-01 08:00:00",
        },
        "Message": {
            "PositionReport": {
                "UserID": mmsi,
                "Latitude": lat,
                "Longitude": lon,
                "Sog": 12.5,
                "Cog": 180.0,
                "NavigationalStatus": nav_status,
                "Type": ship_type,
            }
        },
    }


def test_parse_position_report_basic():
    record = _parse_position_report(_make_msg())
    assert record is not None
    assert record["mmsi"] == "123456789"
    assert record["lat"] == 1.3
    assert record["lon"] == 103.8
    assert record["sog"] == 12.5
    assert record["nav_status"] == 0
    assert record["ship_type"] == 80
    assert record["timestamp"] == datetime(2024, 6, 1, 8, 0, 0, tzinfo=UTC)


def test_parse_ignores_non_position_messages():
    msg = {"MessageType": "ShipStaticData", "MetaData": {}, "Message": {}}
    assert _parse_position_report(msg) is None


def test_parse_ignores_missing_required_fields():
    msg = {
        "MessageType": "PositionReport",
        "MetaData": {"MMSI": 123456789},  # missing lat/lon/time
        "Message": {"PositionReport": {}},
    }
    assert _parse_position_report(msg) is None


def test_parse_ignores_bad_timestamp():
    msg = _make_msg()
    msg["MetaData"]["time_utc"] = "not-a-date"
    assert _parse_position_report(msg) is None


def test_flush_batch_inserts(tmp_db):
    record = _parse_position_report(_make_msg())
    assert record is not None
    n = _flush_batch([record], tmp_db)
    assert n == 1

    con = duckdb.connect(tmp_db)
    count = con.execute("SELECT count(*) FROM ais_positions").fetchone()[0]
    con.close()
    assert count == 1


def test_flush_batch_deduplicates(tmp_db):
    record = _parse_position_report(_make_msg())
    _flush_batch([record], tmp_db)
    # Flush the same record again
    n = _flush_batch([record], tmp_db)
    assert n == 0  # duplicate ignored

    con = duckdb.connect(tmp_db)
    count = con.execute("SELECT count(*) FROM ais_positions").fetchone()[0]
    con.close()
    assert count == 1


def test_flush_batch_empty(tmp_db):
    n = _flush_batch([], tmp_db)
    assert n == 0


def test_flush_batch_multiple_vessels(tmp_db):
    records = [
        _parse_position_report(_make_msg(mmsi=111111111, lat=1.0, lon=103.0)),
        _parse_position_report(_make_msg(mmsi=222222222, lat=2.0, lon=104.0)),
        _parse_position_report(_make_msg(mmsi=333333333, lat=3.0, lon=105.0)),
    ]
    n = _flush_batch(records, tmp_db)
    assert n == 3
