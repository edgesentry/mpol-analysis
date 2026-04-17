"""Tests for src/ingest/custom_feeds.py — auto-detected drop-in feed ingestion."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import duckdb
import pytest

from pipeline.src.ingest.custom_feeds import (
    _detect_feed_type,
    ingest_custom_feeds,
)
from pipeline.src.ingest.schema import init_schema

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_csv(tmp_path: Path, content: str, name: str = "feed.csv") -> Path:
    p = tmp_path / name
    p.write_text(textwrap.dedent(content))
    return p


def _db(tmp_path: Path) -> str:
    db_path = str(tmp_path / "test.duckdb")
    init_schema(db_path)
    return db_path


# ---------------------------------------------------------------------------
# _detect_feed_type
# ---------------------------------------------------------------------------


class TestDetectFeedType:
    def test_sar_by_columns(self):
        assert _detect_feed_type(["lat", "lon", "detected_at"], "feed") == "sar"

    def test_cargo_by_columns(self):
        cols = ["reporter", "partner", "hs_code", "period", "trade_value_usd"]
        assert _detect_feed_type(cols, "feed") == "cargo"

    def test_sanctions_by_columns(self):
        assert _detect_feed_type(["name", "list_source", "mmsi"], "feed") == "sanctions"

    def test_ais_lowercase(self):
        assert _detect_feed_type(["mmsi", "lat", "lon", "timestamp"], "feed") == "ais"

    def test_ais_uppercase(self):
        assert _detect_feed_type(["MMSI", "LAT", "LON", "BaseDateTime"], "feed") == "ais"

    def test_filename_fallback_ais(self):
        assert _detect_feed_type(["a", "b", "c"], "ais_2024_01") == "ais"

    def test_filename_fallback_sar(self):
        assert _detect_feed_type(["a", "b", "c"], "sar_feed") == "sar"

    def test_filename_fallback_cargo(self):
        assert _detect_feed_type(["a", "b", "c"], "cargo_export") == "cargo"

    def test_filename_fallback_manifest(self):
        assert _detect_feed_type(["a", "b", "c"], "manifest_q1") == "cargo"

    def test_filename_fallback_sanctions(self):
        assert _detect_feed_type(["a", "b", "c"], "sanctions_ofac") == "sanctions"

    def test_unknown_returns_none(self):
        assert _detect_feed_type(["foo", "bar"], "unknown_feed") is None

    def test_sar_wins_over_ais_on_column_match(self):
        # SAR signature is checked before AIS; a file with both sets → sar
        cols = ["lat", "lon", "detected_at", "mmsi"]
        assert _detect_feed_type(cols, "feed") == "sar"


# ---------------------------------------------------------------------------
# SAR ingest
# ---------------------------------------------------------------------------


class TestIngestSar:
    def test_basic(self, tmp_path):
        _write_csv(
            tmp_path,
            """\
            lat,lon,detected_at
            1.2,103.8,2024-01-15T08:30:00
            1.3,103.9,2024-01-15T09:00:00
            """,
            "sar_feed.csv",
        )
        db_path = _db(tmp_path)
        results = ingest_custom_feeds(tmp_path, db_path)
        assert results["sar_feed.csv"] == 2

        con = duckdb.connect(db_path)
        rows = con.execute("SELECT COUNT(*) FROM sar_detections").fetchone()[0]
        con.close()
        assert rows == 2

    def test_optional_columns_synthesised(self, tmp_path):
        _write_csv(
            tmp_path,
            """\
            lat,lon,detected_at
            1.2,103.8,2024-01-15T08:30:00
            """,
            "sar_minimal.csv",
        )
        db_path = _db(tmp_path)
        ingest_custom_feeds(tmp_path, db_path)

        con = duckdb.connect(db_path)
        row = con.execute("SELECT confidence, source_scene FROM sar_detections LIMIT 1").fetchone()
        con.close()
        assert row[0] == pytest.approx(1.0)
        assert row[1] == "sar_minimal"  # stem used as source_scene

    def test_duplicate_ignored(self, tmp_path):
        # Deduplication via INSERT OR IGNORE requires a stable detection_id.
        # Provide one explicitly so re-ingestion doesn't create duplicate rows.
        content = """\
        detection_id,lat,lon,detected_at
        aaa-111,1.2,103.8,2024-01-15T08:30:00
        """
        _write_csv(tmp_path, content, "sar_feed.csv")
        db_path = _db(tmp_path)
        ingest_custom_feeds(tmp_path, db_path)
        ingest_custom_feeds(tmp_path, db_path)

        con = duckdb.connect(db_path)
        rows = con.execute("SELECT COUNT(*) FROM sar_detections").fetchone()[0]
        con.close()
        assert rows == 1


# ---------------------------------------------------------------------------
# Cargo ingest
# ---------------------------------------------------------------------------


class TestIngestCargo:
    def test_basic(self, tmp_path):
        _write_csv(
            tmp_path,
            """\
            reporter,partner,hs_code,period,trade_value_usd
            SG,CN,8703,2024-01,500000
            SG,US,8703,2024-01,300000
            """,
            "cargo_q1.csv",
        )
        db_path = _db(tmp_path)
        results = ingest_custom_feeds(tmp_path, db_path)
        assert results["cargo_q1.csv"] == 2

        con = duckdb.connect(db_path)
        rows = con.execute("SELECT COUNT(*) FROM trade_flow").fetchone()[0]
        con.close()
        assert rows == 2

    def test_optional_columns_synthesised(self, tmp_path):
        _write_csv(
            tmp_path,
            """\
            reporter,partner,hs_code,period
            SG,CN,8703,2024-01
            """,
            "manifest_q1.csv",
        )
        db_path = _db(tmp_path)
        ingest_custom_feeds(tmp_path, db_path)

        con = duckdb.connect(db_path)
        row = con.execute("SELECT trade_value_usd, route_key FROM trade_flow LIMIT 1").fetchone()
        con.close()
        assert row[0] is None
        assert row[1] is None


# ---------------------------------------------------------------------------
# Sanctions ingest
# ---------------------------------------------------------------------------


class TestIngestSanctions:
    def test_basic(self, tmp_path):
        _write_csv(
            tmp_path,
            """\
            name,list_source,mmsi
            Vessel Alpha,OFAC,123456789
            Vessel Beta,EU,987654321
            """,
            "sanctions_ofac.csv",
        )
        db_path = _db(tmp_path)
        results = ingest_custom_feeds(tmp_path, db_path)
        assert results["sanctions_ofac.csv"] == 2

        con = duckdb.connect(db_path)
        rows = con.execute("SELECT COUNT(*) FROM sanctions_entities").fetchone()[0]
        con.close()
        assert rows == 2

    def test_entity_id_synthesised(self, tmp_path):
        _write_csv(
            tmp_path,
            """\
            name,list_source
            Vessel Alpha,OFAC
            """,
            "sanctions_test.csv",
        )
        db_path = _db(tmp_path)
        ingest_custom_feeds(tmp_path, db_path)

        con = duckdb.connect(db_path)
        row = con.execute("SELECT entity_id FROM sanctions_entities LIMIT 1").fetchone()
        con.close()
        # Should be a UUID string
        assert len(row[0]) == 36
        assert row[0].count("-") == 4


# ---------------------------------------------------------------------------
# AIS ingest via custom_feeds (uses ais_csv.ingest_csv internally)
# ---------------------------------------------------------------------------


class TestIngestAis:
    def test_basic_marinecadastre(self, tmp_path):
        _write_csv(
            tmp_path,
            """\
            MMSI,BaseDateTime,LAT,LON,SOG,COG,Status,VesselType
            123456789,2024-01-15T08:00:00,1.2,103.8,5.0,180,0,70
            """,
            "ais_2024.csv",
        )
        db_path = _db(tmp_path)
        results = ingest_custom_feeds(tmp_path, db_path)
        assert results["ais_2024.csv"] == 1

        con = duckdb.connect(db_path)
        rows = con.execute("SELECT COUNT(*) FROM ais_positions").fetchone()[0]
        con.close()
        assert rows == 1

    def test_columnmap_sidecar(self, tmp_path):
        _write_csv(
            tmp_path,
            """\
            vessel_id,time_utc,latitude,longitude
            123456789,2024-01-15T08:00:00,1.2,103.8
            """,
            "ais_spire.csv",
        )
        sidecar = tmp_path / "ais_spire.columnmap.json"
        sidecar.write_text(
            json.dumps(
                {
                    "mmsi": "vessel_id",
                    "timestamp": "time_utc",
                    "lat": "latitude",
                    "lon": "longitude",
                }
            )
        )
        db_path = _db(tmp_path)
        results = ingest_custom_feeds(tmp_path, db_path)
        assert results["ais_spire.csv"] == 1


# ---------------------------------------------------------------------------
# Error handling / edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_directory(self, tmp_path):
        db_path = _db(tmp_path)
        feeds_dir = tmp_path / "feeds"
        feeds_dir.mkdir()
        results = ingest_custom_feeds(feeds_dir, db_path)
        assert results == {}

    def test_nonexistent_directory(self, tmp_path):
        db_path = _db(tmp_path)
        results = ingest_custom_feeds(tmp_path / "no_such_dir", db_path)
        assert results == {}

    def test_unknown_schema_skipped(self, tmp_path):
        _write_csv(
            tmp_path,
            """\
            foo,bar,baz
            1,2,3
            """,
            "unknown_feed.csv",
        )
        db_path = _db(tmp_path)
        results = ingest_custom_feeds(tmp_path, db_path)
        assert results["unknown_feed.csv"] == 0

    def test_dry_run_no_rows_inserted(self, tmp_path):
        _write_csv(
            tmp_path,
            """\
            lat,lon,detected_at
            1.2,103.8,2024-01-15T08:30:00
            """,
            "sar_feed.csv",
        )
        db_path = _db(tmp_path)
        results = ingest_custom_feeds(tmp_path, db_path, dry_run=True)
        assert results["sar_feed.csv"] == 0

        con = duckdb.connect(db_path)
        rows = con.execute("SELECT COUNT(*) FROM sar_detections").fetchone()[0]
        con.close()
        assert rows == 0

    def test_sample_files_skipped(self, tmp_path):
        # Smoke-test fixtures (e.g. ais_sample.csv) must never touch the live DB
        _write_csv(
            tmp_path,
            "lat,lon,detected_at\n1.2,103.8,2024-01-15T08:30:00\n",
            "sar_sample.csv",
        )
        _write_csv(
            tmp_path,
            "lat,lon,detected_at\n1.3,103.9,2024-01-16T09:00:00\n",
            "sar_real.csv",
        )
        db_path = _db(tmp_path)
        results = ingest_custom_feeds(tmp_path, db_path)
        # sample file must be absent from results
        assert "sar_sample.csv" not in results
        # real file is ingested normally
        assert results["sar_real.csv"] == 1

        con = duckdb.connect(db_path)
        rows = con.execute("SELECT COUNT(*) FROM sar_detections").fetchone()[0]
        con.close()
        assert rows == 1

    def test_multiple_files(self, tmp_path):
        _write_csv(
            tmp_path,
            "lat,lon,detected_at\n1.2,103.8,2024-01-15T08:30:00\n",
            "sar_a.csv",
        )
        _write_csv(
            tmp_path,
            "lat,lon,detected_at\n1.3,103.9,2024-01-16T09:00:00\n",
            "sar_b.csv",
        )
        db_path = _db(tmp_path)
        results = ingest_custom_feeds(tmp_path, db_path)
        assert results["sar_a.csv"] == 1
        assert results["sar_b.csv"] == 1

        con = duckdb.connect(db_path)
        rows = con.execute("SELECT COUNT(*) FROM sar_detections").fetchone()[0]
        con.close()
        assert rows == 2
