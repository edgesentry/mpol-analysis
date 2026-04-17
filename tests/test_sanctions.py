import json
from pathlib import Path

import duckdb

from pipeline.src.ingest.sanctions import (
    _flush_batch,
    _normalize_imo,
    load_jsonl_to_duckdb,
    parse_ftm_entity,
)

# ---------------------------------------------------------------------------
# parse_ftm_entity
# ---------------------------------------------------------------------------


def _vessel_entity(
    entity_id="ofac-vessel-001",
    name="OCEAN GLORY",
    mmsi="123456789",
    imo="IMO9876543",
    flag="KP",
    datasets=("ofac_sdn",),
):
    return {
        "id": entity_id,
        "caption": name,
        "schema": "Vessel",
        "properties": {
            "name": [name],
            "mmsi": [mmsi],
            "imoNumber": [imo],
            "flag": [flag],
        },
        "datasets": list(datasets),
    }


def _company_entity(entity_id="co-001", name="EVIL CORP", country="KP", datasets=("ofac_sdn",)):
    return {
        "id": entity_id,
        "caption": name,
        "schema": "Company",
        "properties": {"name": [name], "country": [country]},
        "datasets": list(datasets),
    }


def test_parse_vessel():
    row = parse_ftm_entity(_vessel_entity())
    assert row is not None
    assert row["entity_id"] == "ofac-vessel-001"
    assert row["name"] == "OCEAN GLORY"
    assert row["mmsi"] == "123456789"
    assert row["imo"] == "9876543"
    assert row["flag"] == "KP"
    assert row["type"] == "Vessel"
    assert row["list_source"] == "ofac_sdn"


def test_parse_company():
    row = parse_ftm_entity(_company_entity())
    assert row is not None
    assert row["type"] == "Company"
    assert row["mmsi"] is None
    assert row["imo"] is None
    assert row["flag"] == "KP"


def test_parse_unknown_schema_returns_none():
    entity = {"id": "x", "schema": "Event", "properties": {}, "datasets": []}
    assert parse_ftm_entity(entity) is None


def test_parse_missing_name_returns_none():
    entity = {"id": "x", "schema": "Vessel", "properties": {}, "datasets": [], "caption": ""}
    assert parse_ftm_entity(entity) is None


def test_parse_missing_id_returns_none():
    entity = {
        "id": "",
        "schema": "Vessel",
        "caption": "SHIP",
        "properties": {"name": ["SHIP"]},
        "datasets": [],
    }
    assert parse_ftm_entity(entity) is None


def test_parse_multiple_datasets():
    entity = _vessel_entity(datasets=["ofac_sdn", "un_sc_sanctions", "eu_fsf"])
    row = parse_ftm_entity(entity)
    assert row is not None
    parts = set(row["list_source"].split(";"))
    assert parts == {"ofac_sdn", "un_sc_sanctions", "eu_fsf"}


def test_parse_uses_caption_as_fallback_name():
    entity = {
        "id": "x",
        "caption": "CAPTION NAME",
        "schema": "Vessel",
        "properties": {},  # no name property
        "datasets": ["ofac_sdn"],
    }
    row = parse_ftm_entity(entity)
    assert row is not None
    assert row["name"] == "CAPTION NAME"


# ---------------------------------------------------------------------------
# _normalize_imo
# ---------------------------------------------------------------------------


def test_normalize_imo_strips_upper_prefix():
    assert _normalize_imo("IMO9305609") == "9305609"


def test_normalize_imo_strips_lower_prefix():
    assert _normalize_imo("imo9305609") == "9305609"


def test_normalize_imo_strips_mixed_prefix():
    assert _normalize_imo("Imo9305609") == "9305609"


def test_normalize_imo_bare_number_unchanged():
    assert _normalize_imo("9305609") == "9305609"


def test_normalize_imo_none_returns_none():
    assert _normalize_imo(None) is None


def test_normalize_imo_empty_returns_none():
    assert _normalize_imo("") is None


def test_normalize_imo_prefix_only_returns_none():
    assert _normalize_imo("IMO") is None


# ---------------------------------------------------------------------------
# _flush_batch + load_jsonl_to_duckdb
# ---------------------------------------------------------------------------


def test_flush_batch_inserts(tmp_db):
    con = duckdb.connect(tmp_db)
    batch = [parse_ftm_entity(_vessel_entity())]
    n = _flush_batch(con, batch)
    con.close()
    assert n == 1


def test_flush_batch_deduplicates(tmp_db):
    con = duckdb.connect(tmp_db)
    batch = [parse_ftm_entity(_vessel_entity())]
    _flush_batch(con, batch)
    n = _flush_batch(con, batch)  # duplicate
    con.close()
    assert n == 0


def test_flush_batch_multiple(tmp_db):
    con = duckdb.connect(tmp_db)
    batch = [
        parse_ftm_entity(_vessel_entity(entity_id="v1", name="SHIP A")),
        parse_ftm_entity(_company_entity(entity_id="c1")),
        parse_ftm_entity(_vessel_entity(entity_id="v2", name="SHIP B")),
    ]
    n = _flush_batch(con, batch)
    con.close()
    assert n == 3


def _write_jsonl(path: Path, entities: list) -> None:
    path.write_text("\n".join(json.dumps(e) for e in entities) + "\n")


def test_load_jsonl_to_duckdb(tmp_path, tmp_db):
    jsonl = tmp_path / "test.jsonl"
    _write_jsonl(
        jsonl,
        [
            _vessel_entity(entity_id="v1"),
            _company_entity(entity_id="c1"),
            {"id": "skip", "schema": "Event", "properties": {}, "datasets": []},
        ],
    )
    n = load_jsonl_to_duckdb(jsonl, tmp_db)
    assert n == 2


def test_load_jsonl_skips_blank_lines(tmp_path, tmp_db):
    jsonl = tmp_path / "test.jsonl"
    jsonl.write_text(
        json.dumps(_vessel_entity()) + "\n\n   \n" + json.dumps(_company_entity()) + "\n"
    )
    n = load_jsonl_to_duckdb(jsonl, tmp_db)
    assert n == 2


def test_load_jsonl_deduplicates_across_calls(tmp_path, tmp_db):
    jsonl = tmp_path / "test.jsonl"
    _write_jsonl(jsonl, [_vessel_entity()])
    load_jsonl_to_duckdb(jsonl, tmp_db)
    n = load_jsonl_to_duckdb(jsonl, tmp_db)  # second call — same entities
    assert n == 0
