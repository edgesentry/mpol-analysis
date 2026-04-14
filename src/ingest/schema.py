"""
DuckDB schema initialisation.

Creates all tables needed by the MPOL screening pipeline. Safe to run multiple
times — all statements use IF NOT EXISTS / IF NOT EXISTS INDEX.

Usage:
    uv run python src/ingest/schema.py
    uv run python src/ingest/schema.py --db path/to/custom.duckdb
"""

import argparse
import os

import duckdb
from dotenv import load_dotenv

load_dotenv()


def _default_db_path() -> str:
    """Return the default DuckDB path.

    Resolution order:
    1. ``DB_PATH`` env var (explicit override — used in dev and CI)
    2. ``ARKTRACE_DATA_DIR`` + ``ARKTRACE_REGION`` (user config)
    3. ``~/.arktrace/data/<region>.duckdb`` (standard user-level location)
    """
    if explicit := os.getenv("DB_PATH"):
        return explicit
    from pathlib import Path

    _REGION_TO_STEM = {
        "singapore": "singapore",
        "japan": "japansea",
        "middleeast": "middleeast",
        "europe": "europe",
        "gulf": "gulf",
    }
    region = os.getenv("ARKTRACE_REGION", "singapore").lower().strip()
    stem = _REGION_TO_STEM.get(region, "singapore")
    data_dir = (
        Path(os.getenv("ARKTRACE_DATA_DIR", "")).expanduser()
        if os.getenv("ARKTRACE_DATA_DIR")
        else Path.home() / ".arktrace" / "data"
    )
    return str(data_dir / f"{stem}.duckdb")


DEFAULT_DB_PATH = _default_db_path()


def init_schema(db_path: str = DEFAULT_DB_PATH) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = duckdb.connect(db_path)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS ais_positions (
                mmsi        VARCHAR NOT NULL,
                timestamp   TIMESTAMPTZ NOT NULL,
                lat         DOUBLE NOT NULL,
                lon         DOUBLE NOT NULL,
                sog         FLOAT,
                cog         FLOAT,
                nav_status  TINYINT,
                ship_type   TINYINT,
                PRIMARY KEY (mmsi, timestamp)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS vessel_meta (
                mmsi          VARCHAR PRIMARY KEY,
                imo           VARCHAR,
                name          VARCHAR,
                flag          VARCHAR,
                ship_type     TINYINT,
                gross_tonnage FLOAT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS sanctions_entities (
                entity_id   VARCHAR PRIMARY KEY,
                name        VARCHAR NOT NULL,
                mmsi        VARCHAR,
                imo         VARCHAR,
                flag        VARCHAR,
                type        VARCHAR,
                list_source VARCHAR NOT NULL
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS trade_flow (
                reporter        VARCHAR NOT NULL,
                partner         VARCHAR NOT NULL,
                hs_code         VARCHAR NOT NULL,
                period          VARCHAR NOT NULL,
                trade_value_usd DOUBLE,
                route_key       VARCHAR,
                PRIMARY KEY (reporter, partner, hs_code, period)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS analyst_briefs (
                mmsi                VARCHAR NOT NULL,
                watchlist_version   VARCHAR NOT NULL,
                brief               TEXT NOT NULL,
                generated_at        TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (mmsi, watchlist_version)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS dispatch_briefs (
                mmsi                VARCHAR NOT NULL,
                watchlist_version   VARCHAR NOT NULL,
                brief               TEXT NOT NULL,
                generated_at        TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (mmsi, watchlist_version)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS chat_cache (
                cache_key           VARCHAR PRIMARY KEY,
                mmsi                VARCHAR,
                question_hash       VARCHAR NOT NULL,
                watchlist_version   VARCHAR NOT NULL,
                response            TEXT NOT NULL,
                created_at          TIMESTAMPTZ DEFAULT now()
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS cleared_vessels (
                mmsi                VARCHAR PRIMARY KEY,
                cleared_at          TIMESTAMPTZ DEFAULT now(),
                cleared_by          VARCHAR,
                investigation_id    VARCHAR,
                notes               TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS vessel_reviews (
                mmsi                VARCHAR NOT NULL,
                review_tier         VARCHAR NOT NULL,
                handoff_state       VARCHAR NOT NULL DEFAULT 'queued_review',
                rationale           TEXT,
                evidence_refs_json  TEXT,
                reviewed_by         VARCHAR,
                reviewed_at         TIMESTAMPTZ DEFAULT now()
            )
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_vessel_reviews_mmsi_time
            ON vessel_reviews (mmsi, reviewed_at)
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS analyst_prelabels (
                mmsi                VARCHAR NOT NULL,
                imo                 VARCHAR,
                pre_label           VARCHAR NOT NULL,
                confidence_tier     VARCHAR NOT NULL,
                region              VARCHAR,
                evidence_notes      TEXT,
                source_urls_json    TEXT,
                analyst_id          VARCHAR NOT NULL,
                evidence_timestamp  TIMESTAMPTZ NOT NULL,
                created_at          TIMESTAMPTZ DEFAULT now()
            )
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_analyst_prelabels_mmsi
            ON analyst_prelabels (mmsi, evidence_timestamp)
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS sar_detections (
                detection_id    VARCHAR PRIMARY KEY,
                detected_at     TIMESTAMPTZ NOT NULL,
                lat             DOUBLE NOT NULL,
                lon             DOUBLE NOT NULL,
                length_m        FLOAT,
                source_scene    VARCHAR,
                confidence      FLOAT DEFAULT 1.0
            )
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_sar_detections_time
            ON sar_detections (detected_at)
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS eo_detections (
                detection_id   VARCHAR PRIMARY KEY,
                detected_at    TIMESTAMPTZ NOT NULL,
                lat            DOUBLE NOT NULL,
                lon            DOUBLE NOT NULL,
                source         VARCHAR,
                confidence     FLOAT DEFAULT 1.0
            )
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_eo_detections_time
            ON eo_detections (detected_at)
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS vessel_features (
                mmsi                              VARCHAR PRIMARY KEY,
                ais_gap_count_30d                 INTEGER,
                ais_gap_max_hours                 FLOAT,
                position_jump_count               INTEGER,
                sts_candidate_count               INTEGER,
                port_call_ratio                   FLOAT,
                loitering_hours_30d               FLOAT,
                flag_changes_2y                   INTEGER,
                name_changes_2y                   INTEGER,
                owner_changes_2y                  INTEGER,
                high_risk_flag_ratio              FLOAT,
                ownership_depth                   INTEGER,
                sanctions_distance                INTEGER,
                cluster_sanctions_ratio           FLOAT,
                shared_manager_risk               INTEGER,
                shared_address_centrality         INTEGER,
                sts_hub_degree                    INTEGER,
                route_cargo_mismatch              FLOAT,
                declared_vs_estimated_cargo_value FLOAT,
                unmatched_sar_detections_30d      INTEGER,
                eo_dark_count_30d                 INTEGER,
                eo_ais_mismatch_ratio             FLOAT,
                sanctions_list_count              INTEGER,
                computed_at                       TIMESTAMPTZ DEFAULT now()
            )
        """)
        # Migrations: add columns introduced after initial schema creation
        con.execute("""
            ALTER TABLE vessel_features
            ADD COLUMN IF NOT EXISTS unmatched_sar_detections_30d INTEGER DEFAULT 0
        """)
        con.execute("""
            ALTER TABLE vessel_features
            ADD COLUMN IF NOT EXISTS eo_dark_count_30d INTEGER DEFAULT 0
        """)
        con.execute("""
            ALTER TABLE vessel_features
            ADD COLUMN IF NOT EXISTS eo_ais_mismatch_ratio FLOAT DEFAULT 0.0
        """)
        con.execute("""
            ALTER TABLE vessel_features
            ADD COLUMN IF NOT EXISTS sanctions_list_count INTEGER DEFAULT 0
        """)
    finally:
        con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialise DuckDB schema")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to DuckDB file")
    args = parser.parse_args()
    init_schema(args.db)
    print(f"Schema initialised: {args.db}")
