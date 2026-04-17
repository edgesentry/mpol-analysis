"""Append realistic dummy vessels to the dev watchlist for dashboard testing.

With --db, also seeds the DuckDB and ownership graph so the backtracking loop
can be evaluated locally:
  - Inserts a confirmed review for CELINE (352001369)
  - Seeds 13 months of synthetic AIS history with a precursor go-dark pattern
    (dense baseline pings every 4 h → sparse precursor pings every 8 h)
  - Writes OWNED_BY edges so ELINE (314856000) is uplifted as a peer

Usage:
    uv run python scripts/seed_dev_watchlist.py
    uv run python scripts/seed_dev_watchlist.py --db data/processed/mpol.duckdb
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, datetime, timedelta

import duckdb
import polars as pl
import pyarrow as pa

logging.basicConfig(level=logging.INFO)

WATCHLIST_PATH = "data/processed/candidate_watchlist.parquet"

DUMMY_MMSIS = {
    "352001369",
    "314856000",
    "372979000",
    "312171000",
    "352898820",
    "352002316",
    "626152000",
    "352001298",
    "314925000",
    "352001565",
}

NEW_VESSELS = pl.DataFrame(
    {
        "mmsi": [
            "352001369",
            "314856000",
            "372979000",
            "312171000",
            "352898820",
            "352002316",
            "626152000",
            "352001298",
            "314925000",
            "352001565",
        ],
        "imo": [
            "9305609",
            "9292486",
            "9219056",
            "9354521",
            "9280873",
            "9308778",
            "9162928",
            "9292228",
            "9289491",
            "9417490",
        ],
        "vessel_name": [
            "CELINE",
            "ELINE",
            "REX 1",
            "ANHONA",
            "AVENTUS I",
            "SATINA",
            "ASTRA",
            "CRYSTAL ROSE",
            "BENDIGO",
            "ARABIAN ENERGY",
        ],
        "vessel_type": [
            "Tanker",
            "Tanker",
            "Tanker",
            "Tanker",
            "Tanker",
            "Tanker",
            "Tanker",
            "Tanker",
            "Tanker",
            "Tanker",
        ],
        "flag": ["PA", "BB", "PA", "BZ", "PA", "PA", "GA", "PA", "BB", "PA"],
        "confidence": [0.91, 0.87, 0.79, 0.72, 0.83, 0.75, 0.88, 0.82, 0.65, 0.95],
        "behavioral_deviation_score": [0.88, 0.84, 0.70, 0.55, 0.81, 0.68, 0.85, 0.79, 0.55, 0.92],
        "graph_risk_score": [0.92, 0.80, 0.75, 0.65, 0.78, 0.72, 0.82, 0.75, 0.60, 0.88],
        "identity_score": [0.75, 0.70, 0.25, 0.40, 0.69, 0.55, 0.72, 0.65, 0.45, 0.85],
        "top_signals": [
            json.dumps(
                [
                    {"feature": "ais_gap_count_30d", "value": 14, "contribution": 0.38},
                    {"feature": "sanctions_distance", "value": 0, "contribution": 0.28},
                    {"feature": "flag_changes_2y", "value": 2, "contribution": 0.15},
                ]
            ),
            json.dumps(
                [
                    {"feature": "route_cargo_mismatch", "value": 1.0, "contribution": 0.42},
                    {"feature": "position_jump_count", "value": 3, "contribution": 0.25},
                    {"feature": "high_risk_flag_ratio", "value": 0.80, "contribution": 0.18},
                ]
            ),
            json.dumps(
                [
                    {"feature": "sts_hub_degree", "value": 6, "contribution": 0.30},
                    {"feature": "shared_address_centrality", "value": 5, "contribution": 0.22},
                    {"feature": "cluster_sanctions_ratio", "value": 0.40, "contribution": 0.18},
                ]
            ),
            json.dumps(
                [
                    {"feature": "ownership_depth", "value": 5, "contribution": 0.28},
                    {"feature": "sanctions_distance", "value": 0, "contribution": 0.24},
                    {"feature": "name_changes_2y", "value": 1, "contribution": 0.12},
                ]
            ),
            json.dumps(
                [
                    {"feature": "ais_gap_count_30d", "value": 15, "contribution": 0.33},
                    {"feature": "sts_candidate_count", "value": 4, "contribution": 0.24},
                    {"feature": "position_jump_count", "value": 2, "contribution": 0.14},
                ]
            ),
            json.dumps(
                [
                    {"feature": "ais_gap_count_30d", "value": 9, "contribution": 0.25},
                    {"feature": "flag_changes_2y", "value": 1, "contribution": 0.20},
                ]
            ),
            json.dumps(
                [
                    {"feature": "sanctions_distance", "value": 0, "contribution": 0.40},
                    {"feature": "route_cargo_mismatch", "value": 1.0, "contribution": 0.30},
                ]
            ),
            json.dumps(
                [
                    {"feature": "ais_gap_count_30d", "value": 13, "contribution": 0.35},
                    {"feature": "sts_candidate_count", "value": 3, "contribution": 0.25},
                ]
            ),
            json.dumps(
                [
                    {"feature": "identity_score", "value": 0.45, "contribution": 0.20},
                ]
            ),
            json.dumps(
                [
                    {"feature": "sanctions_distance", "value": 0, "contribution": 0.45},
                    {"feature": "ais_gap_count_30d", "value": 16, "contribution": 0.30},
                ]
            ),
        ],
        # Realistic last-known positions
        "last_lat": [1.25, 1.35, 1.45, 1.55, 1.65, 1.75, 1.85, 1.95, 2.05, 2.15],
        "last_lon": [
            103.85,
            103.95,
            104.05,
            104.15,
            104.25,
            104.35,
            104.45,
            104.55,
            104.65,
            104.75,
        ],
        "last_seen": [
            datetime(2026, 3, 15, 0, 0, tzinfo=UTC),
            datetime(2026, 3, 15, 0, 0, tzinfo=UTC),
            datetime(2026, 3, 15, 0, 0, tzinfo=UTC),
            datetime(2026, 3, 15, 0, 0, tzinfo=UTC),
            datetime(2026, 3, 15, 0, 0, tzinfo=UTC),
            datetime(2026, 3, 15, 0, 0, tzinfo=UTC),
            datetime(2026, 3, 15, 0, 0, tzinfo=UTC),
            datetime(2026, 3, 15, 0, 0, tzinfo=UTC),
            datetime(2026, 3, 15, 0, 0, tzinfo=UTC),
            datetime(2026, 3, 15, 0, 0, tzinfo=UTC),
        ],
        "ais_gap_count_30d": [14, 12, 10, 8, 15, 9, 11, 13, 7, 16],
        "ais_gap_max_hours": [22.0, 18.0, 15.0, 12.0, 25.0, 14.0, 20.0, 21.0, 10.0, 28.0],
        "position_jump_count": [2, 1, 0, 3, 2, 1, 0, 2, 1, 3],
        "sts_candidate_count": [2, 1, 3, 0, 4, 2, 1, 3, 0, 5],
        "flag_changes_2y": [2, 1, 0, 1, 2, 1, 0, 2, 1, 3],
        "name_changes_2y": [1, 0, 1, 2, 1, 0, 2, 1, 0, 2],
        "owner_changes_2y": [1, 1, 1, 1, 2, 1, 1, 2, 1, 2],
        "sanctions_distance": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "shared_address_centrality": [3, 2, 4, 1, 5, 2, 3, 4, 1, 6],
        "sts_hub_degree": [3, 2, 4, 1, 5, 2, 3, 4, 1, 6],
        "cluster_label": [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1],
        "baseline_noise_score": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
    }
)


_CONFIRMED_MMSI = "352001369"  # CELINE — used as confirmed seed
_PEER_MMSI = "314856000"  # ELINE — uplifted by propagation
_CONFIRMED_AT = datetime(2026, 4, 1, tzinfo=UTC)


def _seed_db(db_path: str) -> None:
    from pipeline.src.graph.store import REL_SCHEMAS, write_tables
    from pipeline.src.ingest.schema import init_schema

    init_schema(db_path)
    con = duckdb.connect(db_path)
    try:
        # Confirmed review
        existing_review = con.execute(
            "SELECT COUNT(*) FROM vessel_reviews WHERE mmsi = ? AND review_tier = 'confirmed'",
            [_CONFIRMED_MMSI],
        ).fetchone()[0]
        if not existing_review:
            con.execute(
                "INSERT INTO vessel_reviews "
                "(mmsi, review_tier, handoff_state, rationale, reviewed_by, reviewed_at) "
                "VALUES (?, 'confirmed', 'handoff_completed', "
                "'Go-dark pattern + real OFAC sdn match', "
                "'demo-officer', ?)",
                [_CONFIRMED_MMSI, _CONFIRMED_AT.isoformat()],
            )
            print(f"  inserted confirmed review for {_CONFIRMED_MMSI}")
        else:
            print(f"  confirmed review for {_CONFIRMED_MMSI} already exists — skipping")

        # AIS history: dense baseline (4 h < 6 h gap threshold) + sparse precursor (8 h > threshold)
        existing_ais = con.execute(
            "SELECT COUNT(*) FROM ais_positions WHERE mmsi = ?", [_CONFIRMED_MMSI]
        ).fetchone()[0]
        if existing_ais <= 5:
            baseline_start = _CONFIRMED_AT - timedelta(days=365)
            precursor_start = _CONFIRMED_AT - timedelta(days=90)
            inserted = 0
            t = baseline_start
            while t < precursor_start:
                try:
                    con.execute(
                        "INSERT OR IGNORE INTO ais_positions "
                        "(mmsi, timestamp, lat, lon, sog, nav_status) VALUES (?,?,?,?,?,?)",
                        [_CONFIRMED_MMSI, t, 1.25, 103.85, 0.5, 0],
                    )
                    inserted += 1
                except Exception as exc:
                    logging.warning(
                        "Failed to insert baseline AIS position for mmsi=%s at %s: %s",
                        _CONFIRMED_MMSI,
                        t,
                        exc,
                    )
                t += timedelta(hours=4)
            while t < _CONFIRMED_AT:
                try:
                    con.execute(
                        "INSERT OR IGNORE INTO ais_positions "
                        "(mmsi, timestamp, lat, lon, sog, nav_status) VALUES (?,?,?,?,?,?)",
                        [_CONFIRMED_MMSI, t, 1.25, 103.85, 0.3, 0],
                    )
                    inserted += 1
                except Exception as exc:
                    logging.warning(
                        "Failed to insert precursor AIS position for mmsi=%s at %s: %s",
                        _CONFIRMED_MMSI,
                        t,
                        exc,
                    )
                t += timedelta(hours=8)
            print(
                f"  inserted {inserted} AIS positions for {_CONFIRMED_MMSI} "
                "(275d dense baseline + 90d sparse precursor)"
            )
        else:
            print(f"  {_CONFIRMED_MMSI} already has {existing_ais} AIS rows — skipping")
    finally:
        con.close()

    # Ownership graph
    table = pa.table(
        {
            "src_id": [_CONFIRMED_MMSI, _PEER_MMSI],
            "dst_id": ["company-REAL-SDN", "company-REAL-SDN"],
            "since": ["", ""],
            "until": ["", ""],
        },
        schema=REL_SCHEMAS["OWNED_BY"],
    )
    write_tables(db_path, {"OWNED_BY": table})
    print(f"  wrote OWNED_BY: {_CONFIRMED_MMSI} and {_PEER_MMSI} → company-REAL-SDN")

    print()
    print("Run the backtracking loop:")
    print(f"  uv run python scripts/run_backtracking.py --db {db_path}")
    print("  cat data/processed/backtracking_report.md")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed dev watchlist for dashboard testing")
    parser.add_argument(
        "--db",
        default=None,
        help="Also seed this DuckDB with confirmed review + AIS history + ownership graph "
        "for local backtracking evaluation (default: parquet only)",
    )
    args = parser.parse_args()

    try:
        existing = pl.read_parquet(WATCHLIST_PATH)
    except Exception:
        existing = pl.DataFrame(
            schema={
                "mmsi": pl.Utf8,
                "imo": pl.Utf8,
                "vessel_name": pl.Utf8,
                "vessel_type": pl.Utf8,
                "flag": pl.Utf8,
                "confidence": pl.Float32,
                "behavioral_deviation_score": pl.Float32,
                "graph_risk_score": pl.Float32,
                "identity_score": pl.Float32,
                "top_signals": pl.Utf8,
                "last_lat": pl.Float64,
                "last_lon": pl.Float64,
                "last_seen": pl.Datetime(time_unit="us", time_zone="UTC"),
                "ais_gap_count_30d": pl.Int64,
                "ais_gap_max_hours": pl.Float32,
                "position_jump_count": pl.Int64,
                "sts_candidate_count": pl.Int64,
                "flag_changes_2y": pl.Int64,
                "name_changes_2y": pl.Int64,
                "owner_changes_2y": pl.Int64,
                "sanctions_distance": pl.Int64,
                "shared_address_centrality": pl.Int64,
                "sts_hub_degree": pl.Int64,
                "cluster_label": pl.Int64,
                "baseline_noise_score": pl.Float32,
            }
        )

    # Remove any prior seeded rows so re-running is idempotent
    existing = existing.filter(~pl.col("mmsi").is_in(list(DUMMY_MMSIS)))

    # Cast last_seen to match existing timezone
    tz = existing.schema.get("last_seen", pl.Datetime).time_zone  # type: ignore[union-attr]
    new = NEW_VESSELS.with_columns(
        pl.col("last_seen").dt.convert_time_zone(tz or "UTC"),
        pl.col("confidence").cast(pl.Float32),
        pl.col("behavioral_deviation_score").cast(pl.Float32),
        pl.col("graph_risk_score").cast(pl.Float32),
        pl.col("identity_score").cast(pl.Float32),
        pl.col("ais_gap_max_hours").cast(pl.Float32),
        pl.col("baseline_noise_score").cast(pl.Float32),
    )

    # Align all overlapping columns to existing schema to keep concat stable
    cast_exprs: list[pl.Expr] = []
    for col, dtype in existing.schema.items():
        if col in new.columns:
            cast_exprs.append(pl.col(col).cast(dtype))
    new = new.with_columns(cast_exprs)

    combined = pl.concat([existing, new], how="vertical_relaxed").sort(
        "confidence", descending=True
    )
    combined.write_parquet(WATCHLIST_PATH)
    print(f"Watchlist updated: {combined.height} vessels ({len(DUMMY_MMSIS)} dummy added)")
    print(combined.select(["vessel_name", "flag", "confidence"]))

    if args.db:
        print(f"\nSeeding DuckDB: {args.db}")
        _seed_db(args.db)


if __name__ == "__main__":
    main()
