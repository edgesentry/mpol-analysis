"""Append realistic dummy vessels to the dev watchlist for dashboard testing.

With --db, also seeds the DuckDB and ownership graph so the backtracking loop
can be evaluated locally:
  - Inserts a confirmed review for PETROVSKY ZVEZDA (273456782)
  - Seeds 13 months of synthetic AIS history with a precursor go-dark pattern
    (dense baseline pings every 4 h → sparse precursor pings every 8 h)
  - Writes OWNED_BY edges so 613115678 (SARI NOUR) is uplifted as a peer

Usage:
    uv run python scripts/seed_dev_watchlist.py
    uv run python scripts/seed_dev_watchlist.py --db data/processed/mpol.duckdb
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
import logging

import duckdb
import polars as pl
import pyarrow as pa

logging.basicConfig(level=logging.INFO)

WATCHLIST_PATH = "data/processed/candidate_watchlist.parquet"

DUMMY_MMSIS = {"273456782", "613115678", "352123456", "538009876", "563889001"}

NEW_VESSELS = pl.DataFrame(
    {
        "mmsi": ["273456782", "613115678", "352123456", "538009876", "563889001"],
        "imo": ["IMO9234567", "IMO9345612", "IMO9456781", "IMO9678901", "IMO9789002"],
        "vessel_name": ["PETROVSKY ZVEZDA", "SARI NOUR", "OCEAN VOYAGER", "VERA SUNSET", "MERLION DAWN"],
        "vessel_type": ["Tanker", "Tanker", "Tanker", "Tanker", "Tanker"],
        "flag": ["RU", "CM", "PA", "MH", "SG"],
        "confidence": [0.91, 0.87, 0.79, 0.72, 0.83],
        "anomaly_score": [0.88, 0.84, 0.70, 0.55, 0.81],
        "graph_risk_score": [0.92, 0.80, 0.75, 0.65, 0.78],
        "identity_score": [0.75, 0.70, 0.25, 0.40, 0.69],
        "top_signals": [
            # AIS dark ops in Hormuz, 1 hop from OFAC entity, reflagged RU twice in 2y
            json.dumps([
                {"feature": "ais_gap_count_30d", "value": 14, "contribution": 0.38},
                {"feature": "sanctions_distance", "value": 1, "contribution": 0.28},
                {"feature": "flag_changes_2y", "value": 2, "contribution": 0.15},
            ]),
            # Kharg Island crude trades with no Comtrade record, 3 GPS spoofing jumps, IR→CM reflag
            json.dumps([
                {"feature": "route_cargo_mismatch", "value": 1.0, "contribution": 0.42},
                {"feature": "position_jump_count", "value": 3, "contribution": 0.25},
                {"feature": "high_risk_flag_ratio", "value": 0.85, "contribution": 0.18},
            ]),
            # 6 STS partners off Ceuta, shared Piraeus address with 5 vessels (40% OFAC-listed)
            json.dumps([
                {"feature": "sts_hub_degree", "value": 6, "contribution": 0.30},
                {"feature": "shared_address_centrality", "value": 5, "contribution": 0.22},
                {"feature": "cluster_sanctions_ratio", "value": 0.40, "contribution": 0.18},
            ]),
            # 5-layer ownership chain, beneficial owner 2 hops from designated entity, renamed once
            json.dumps([
                {"feature": "ownership_depth", "value": 5, "contribution": 0.28},
                {"feature": "sanctions_distance", "value": 2, "contribution": 0.24},
                {"feature": "name_changes_2y", "value": 1, "contribution": 0.12},
            ]),
            # Repeated AIS gaps in Singapore Strait anchorage + short-burst STS interactions
            json.dumps([
                {"feature": "ais_gap_count_30d", "value": 9, "contribution": 0.33},
                {"feature": "sts_candidate_count", "value": 3, "contribution": 0.24},
                {"feature": "position_jump_count", "value": 1, "contribution": 0.14},
            ]),
        ],
        # Realistic last-known positions
        "last_lat": [26.50, 29.10, 35.90, 25.10, 1.21],
        "last_lon": [55.50, 50.30, -5.50, 56.40, 103.92],
        "last_seen": [
            datetime(2026, 3, 15, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 20, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 25, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 2, 0, 0, tzinfo=timezone.utc),
        ],
        "ais_gap_count_30d": [14, 8, 3, 1, 9],
        "ais_gap_max_hours": [22.0, 14.0, 7.5, 3.0, 11.0],
        "position_jump_count": [2, 3, 0, 0, 1],
        "sts_candidate_count": [2, 1, 5, 0, 3],
        "flag_changes_2y": [2, 1, 0, 0, 1],
        "name_changes_2y": [1, 2, 0, 1, 1],
        "owner_changes_2y": [1, 1, 1, 2, 1],
        "sanctions_distance": [1, 2, 3, 2, 2],
        "shared_address_centrality": [3, 2, 5, 2, 3],
        "sts_hub_degree": [3, 2, 6, 1, 4],
        "cluster_label": [-1, -1, 0, 0, -1],
        "baseline_noise_score": [1.0, 0.95, 0.30, 0.20, 0.82],
    }
)


_CONFIRMED_MMSI = "273456782"  # PETROVSKY ZVEZDA — used as confirmed seed
_PEER_MMSI = "613115678"       # SARI NOUR — co-owned, uplifted by propagation
_CONFIRMED_AT = datetime(2026, 4, 1, tzinfo=timezone.utc)


def _seed_db(db_path: str) -> None:
    from src.graph.store import REL_SCHEMAS, write_tables
    from src.ingest.schema import init_schema

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
                "'Go-dark pattern + IR flag + co-owned with known-risk entity', "
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
                        [_CONFIRMED_MMSI, t, 26.50, 55.50, 0.5, 0],
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
                        [_CONFIRMED_MMSI, t, 26.50, 55.50, 0.3, 0],
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
            print(f"  inserted {inserted} AIS positions for {_CONFIRMED_MMSI} "
                  "(275d dense baseline + 90d sparse precursor)")
        else:
            print(f"  {_CONFIRMED_MMSI} already has {existing_ais} AIS rows — skipping")
    finally:
        con.close()

    # Ownership graph: PETROVSKY ZVEZDA and SARI NOUR share an owner
    table = pa.table(
        {
            "src_id": [_CONFIRMED_MMSI, _PEER_MMSI],
            "dst_id": ["company-DEMO", "company-DEMO"],
            "since": ["", ""],
            "until": ["", ""],
        },
        schema=REL_SCHEMAS["OWNED_BY"],
    )
    write_tables(db_path, {"OWNED_BY": table})
    print(f"  wrote OWNED_BY: {_CONFIRMED_MMSI} and {_PEER_MMSI} → company-DEMO")

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

    existing = pl.read_parquet(WATCHLIST_PATH)

    # Remove any prior seeded rows so re-running is idempotent
    existing = existing.filter(~pl.col("mmsi").is_in(list(DUMMY_MMSIS)))

    # Cast last_seen to match existing timezone
    tz = existing.schema["last_seen"].time_zone  # type: ignore[union-attr]
    new = NEW_VESSELS.with_columns(
        pl.col("last_seen").dt.convert_time_zone(tz),
        pl.col("confidence").cast(pl.Float32),
        pl.col("anomaly_score").cast(pl.Float32),
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

    combined = pl.concat([existing, new]).sort("confidence", descending=True)
    combined.write_parquet(WATCHLIST_PATH)
    print(f"Watchlist updated: {combined.height} vessels ({len(DUMMY_MMSIS)} dummy added)")
    print(combined.select(["vessel_name", "flag", "confidence"]))

    if args.db:
        print(f"\nSeeding DuckDB: {args.db}")
        _seed_db(args.db)


if __name__ == "__main__":
    main()
