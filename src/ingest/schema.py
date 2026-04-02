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

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")


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
            CREATE TABLE IF NOT EXISTS vessel_features (
                mmsi                       VARCHAR PRIMARY KEY,
                ais_gap_count_30d          INTEGER,
                ais_gap_max_hours          FLOAT,
                position_jump_count        INTEGER,
                sts_candidate_count        INTEGER,
                port_call_ratio            FLOAT,
                loitering_hours_30d        FLOAT,
                flag_changes_2y            INTEGER,
                name_changes_2y            INTEGER,
                owner_changes_2y           INTEGER,
                high_risk_flag_ratio       FLOAT,
                ownership_depth            INTEGER,
                sanctions_distance         INTEGER,
                cluster_sanctions_ratio    FLOAT,
                shared_manager_risk        INTEGER,
                shared_address_centrality  INTEGER,
                sts_hub_degree             INTEGER,
                route_cargo_mismatch       FLOAT,
                declared_vs_estimated_cargo_value FLOAT,
                computed_at                TIMESTAMPTZ DEFAULT now()
            )
        """)
    finally:
        con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialise DuckDB schema")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to DuckDB file")
    args = parser.parse_args()
    init_schema(args.db)
    print(f"Schema initialised: {args.db}")
