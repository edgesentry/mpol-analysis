"""Tests for src/ingest/gdelt.py — GDELT event parsing and LanceDB ingest."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from src.ingest.gdelt import _parse_csv, _RELEVANT_ROOT_CODES


# ── CSV generation helpers ─────────────────────────────────────────────────

def _make_gdelt_row(
    event_id="1",
    event_date="20260401",
    actor1_name="Iran",
    actor1_country="IR",
    actor2_name="Cambodia",
    actor2_country="KH",
    event_code="163",
    event_root="16",
    quad_class="4",
    goldstein="-7.0",
    avg_tone="-3.5",
    action_geo="South China Sea",
    action_geo_country="SCS",
    source_url="http://example.com/news",
    n_cols=58,
) -> list[str]:
    row = [""] * n_cols
    row[0] = event_id
    row[1] = event_date
    row[6] = actor1_name
    row[7] = actor1_country
    row[16] = actor2_name
    row[17] = actor2_country
    row[26] = event_code
    row[28] = event_root
    row[29] = quad_class
    row[30] = goldstein
    row[34] = avg_tone
    row[52] = action_geo
    row[53] = action_geo_country
    row[57] = source_url
    return row


def _write_csv(tmp_path: Path, rows: list[list[str]]) -> Path:
    p = tmp_path / "gdelt_test.csv"
    with open(p, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        for row in rows:
            writer.writerow(row)
    return p


# ── _parse_csv ─────────────────────────────────────────────────────────────

def test_parse_csv_returns_relevant_events(tmp_path):
    rows = [
        _make_gdelt_row(event_root="16"),   # REDUCE RELATIONS — relevant
        _make_gdelt_row(event_root="19"),   # FIGHT — relevant
        _make_gdelt_row(event_root="05"),   # ENGAGE IN DIPLOMATIC COOPERATION — not relevant
    ]
    path = _write_csv(tmp_path, rows)
    records = _parse_csv(path)
    assert len(records) == 2


def test_parse_csv_filters_irrelevant_root_codes(tmp_path):
    rows = [_make_gdelt_row(event_root="01")]  # MAKE PUBLIC STATEMENT
    path = _write_csv(tmp_path, rows)
    records = _parse_csv(path)
    assert records == []


def test_parse_csv_constructs_description(tmp_path):
    row = _make_gdelt_row(
        actor1_name="Iran",
        actor2_name="Cambodia",
        event_root="16",
        action_geo="Strait of Malacca",
        event_date="20260401",
    )
    path = _write_csv(tmp_path, [row])
    records = _parse_csv(path)
    assert len(records) == 1
    desc = records[0]["description"]
    assert "Iran" in desc
    assert "Cambodia" in desc
    assert "Strait of Malacca" in desc
    assert "2026-04-01" in desc


def test_parse_csv_records_contain_required_fields(tmp_path):
    path = _write_csv(tmp_path, [_make_gdelt_row(event_root="13")])
    records = _parse_csv(path)
    assert len(records) == 1
    r = records[0]
    for field in ["event_id", "event_date", "actor1_country", "actor2_country",
                  "event_code", "quad_class", "description", "source_url"]:
        assert field in r, f"Missing field: {field}"


def test_parse_csv_empty_file_returns_empty(tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text("")
    records = _parse_csv(path)
    assert records == []


def test_parse_csv_malformed_rows_skipped(tmp_path):
    # Row with too few columns
    path = tmp_path / "short.csv"
    path.write_text("1\t20260401\n")
    records = _parse_csv(path)
    assert records == []


def test_all_relevant_root_codes_present():
    # Sanity-check that our filter set covers the key CAMEO conflict codes
    for code in ["10", "13", "16", "17", "18", "19", "20"]:
        assert code in _RELEVANT_ROOT_CODES
