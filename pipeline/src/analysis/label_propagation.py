"""
Ownership-graph label propagation for confirmed vessel labels.

Starting from confirmed-black MMSI labels (from vessel_reviews), propagates
risk uplift to related entities via:
  - Shared owner   (OWNED_BY → same company → peer vessels)
  - Shared manager (MANAGED_BY → same company → peer vessels)
  - STS co-location contact (STS_CONTACT edge, bidirectional)

Output DataFrame columns:
  mmsi, source_mmsi, hop, evidence_type, propagated_confidence, confirmed_at

Usage:
    uv run python src/analysis/label_propagation.py --db data/processed/mpol.duckdb
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import duckdb
import polars as pl
from dotenv import load_dotenv

from pipeline.src.graph.store import load_tables

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")
DEFAULT_OUTPUT_PATH = os.getenv("PROPAGATION_OUTPUT_PATH", "data/processed/label_propagation.json")

CONFIDENCE_DIRECT = 1.0
CONFIDENCE_SHARED_OWNER = 0.65
CONFIDENCE_SHARED_MANAGER = 0.60
CONFIDENCE_STS_CONTACT = 0.50


@dataclass(frozen=True)
class PropagationResult:
    seed_count: int
    propagated_count: int
    total_vessels: int
    as_of_utc: str


def _fetch_confirmed_mmsis(db_path: str, as_of_utc: str | None = None) -> pl.DataFrame:
    cutoff = as_of_utc or datetime.now(UTC).isoformat()
    con = duckdb.connect(db_path, read_only=True)
    try:
        return con.execute(
            """
            SELECT mmsi, reviewed_at
            FROM (
                SELECT mmsi, reviewed_at,
                       ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY reviewed_at DESC) AS rn
                FROM vessel_reviews
                WHERE review_tier = 'confirmed'
                  AND reviewed_at <= ?
            ) sub
            WHERE rn = 1
            """,
            [cutoff],
        ).pl()
    finally:
        con.close()


def propagate_labels(
    db_path: str,
    as_of_utc: str | None = None,
) -> tuple[pl.DataFrame, PropagationResult]:
    """Propagate confirmed labels through the ownership/STS graph.

    Returns (propagated_df, summary) where propagated_df includes both seed
    vessels (hop=0) and related entities (hop=1) with traceable evidence paths.
    """
    seeds_df = _fetch_confirmed_mmsis(db_path, as_of_utc)
    seed_mmsis: set[str] = set(seeds_df["mmsi"].to_list())
    confirmed_at_map: dict[str, str] = dict(
        zip(
            seeds_df["mmsi"].to_list(),
            seeds_df["reviewed_at"].cast(pl.Utf8).to_list(),
        )
    )

    # seen maps mmsi → row dict; seeds always take priority
    seen: dict[str, dict[str, Any]] = {
        m: {
            "mmsi": m,
            "source_mmsi": m,
            "hop": 0,
            "evidence_type": "confirmed_direct",
            "propagated_confidence": CONFIDENCE_DIRECT,
            "confirmed_at": confirmed_at_map[m],
        }
        for m in seed_mmsis
    }

    tables = load_tables(db_path)
    ob = pl.from_arrow(tables["OWNED_BY"])
    mb = pl.from_arrow(tables["MANAGED_BY"])
    sts = pl.from_arrow(tables["STS_CONTACT"])

    # ── Shared-owner propagation ─────────────────────────────────────────────
    if len(ob) > 0 and seed_mmsis:
        ob_df = ob.select(["src_id", "dst_id"]).rename({"src_id": "vessel", "dst_id": "company"})
        seed_companies = (
            ob_df.filter(pl.col("vessel").is_in(seed_mmsis))
            .select(["vessel", "company"])
            .rename({"vessel": "source_mmsi"})
        )
        if len(seed_companies) > 0:
            peers = (
                ob_df.join(seed_companies, on="company")
                .filter(pl.col("vessel") != pl.col("source_mmsi"))
                .unique(subset=["vessel"])
            )
            for row in peers.iter_rows(named=True):
                peer = row["vessel"]
                if peer not in seen:
                    src = row["source_mmsi"]
                    seen[peer] = {
                        "mmsi": peer,
                        "source_mmsi": src,
                        "hop": 1,
                        "evidence_type": "shared_owner",
                        "propagated_confidence": CONFIDENCE_SHARED_OWNER,
                        "confirmed_at": confirmed_at_map.get(src, ""),
                    }

    # ── Shared-manager propagation ───────────────────────────────────────────
    if len(mb) > 0 and seed_mmsis:
        mb_df = mb.select(["src_id", "dst_id"]).rename({"src_id": "vessel", "dst_id": "company"})
        seed_companies_m = (
            mb_df.filter(pl.col("vessel").is_in(seed_mmsis))
            .select(["vessel", "company"])
            .rename({"vessel": "source_mmsi"})
        )
        if len(seed_companies_m) > 0:
            peers_m = (
                mb_df.join(seed_companies_m, on="company")
                .filter(pl.col("vessel") != pl.col("source_mmsi"))
                .unique(subset=["vessel"])
            )
            for row in peers_m.iter_rows(named=True):
                peer = row["vessel"]
                if peer not in seen:
                    src = row["source_mmsi"]
                    seen[peer] = {
                        "mmsi": peer,
                        "source_mmsi": src,
                        "hop": 1,
                        "evidence_type": "shared_manager",
                        "propagated_confidence": CONFIDENCE_SHARED_MANAGER,
                        "confirmed_at": confirmed_at_map.get(src, ""),
                    }

    # ── STS-contact propagation ──────────────────────────────────────────────
    if len(sts) > 0 and seed_mmsis:
        sts_df = sts.select(["src_id", "dst_id"])
        forward = (
            sts_df.filter(pl.col("src_id").is_in(seed_mmsis))
            .rename({"src_id": "source_mmsi", "dst_id": "vessel"})
            .select(["source_mmsi", "vessel"])
        )
        backward = (
            sts_df.filter(pl.col("dst_id").is_in(seed_mmsis))
            .rename({"dst_id": "source_mmsi", "src_id": "vessel"})
            .select(["source_mmsi", "vessel"])
        )
        all_sts = pl.concat([forward, backward]).unique(subset=["vessel", "source_mmsi"])
        for row in all_sts.iter_rows(named=True):
            peer = row["vessel"]
            if peer not in seen:
                src = row["source_mmsi"]
                seen[peer] = {
                    "mmsi": peer,
                    "source_mmsi": src,
                    "hop": 1,
                    "evidence_type": "sts_contact",
                    "propagated_confidence": CONFIDENCE_STS_CONTACT,
                    "confirmed_at": confirmed_at_map.get(src, ""),
                }

    all_rows = list(seen.values())
    propagated_count = sum(1 for r in all_rows if r["hop"] > 0)

    schema = {
        "mmsi": pl.Utf8,
        "source_mmsi": pl.Utf8,
        "hop": pl.Int32,
        "evidence_type": pl.Utf8,
        "propagated_confidence": pl.Float32,
        "confirmed_at": pl.Utf8,
    }
    result_df = (
        pl.DataFrame(all_rows).cast({"hop": pl.Int32, "propagated_confidence": pl.Float32})
        if all_rows
        else pl.DataFrame(schema=schema)
    )

    result = PropagationResult(
        seed_count=len(seed_mmsis),
        propagated_count=propagated_count,
        total_vessels=len(all_rows),
        as_of_utc=as_of_utc or datetime.now(UTC).isoformat(),
    )
    return result_df, result


def run_label_propagation(
    db_path: str,
    output_path: str,
    as_of_utc: str | None = None,
) -> dict[str, Any]:
    result_df, result = propagate_labels(db_path, as_of_utc)
    report: dict[str, Any] = {
        "seed_count": result.seed_count,
        "propagated_count": result.propagated_count,
        "total_vessels": result.total_vessels,
        "as_of_utc": result.as_of_utc,
        "vessels": result_df.to_dicts(),
    }
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Propagate confirmed labels through ownership graph"
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--as-of-utc", default=None, help="ISO timestamp cutoff")
    args = parser.parse_args()

    report = run_label_propagation(args.db, args.output, args.as_of_utc)
    print(
        f"Seeds: {report['seed_count']}, "
        f"Propagated: {report['propagated_count']}, "
        f"Total: {report['total_vessels']}"
    )
    print(f"Artifact: {args.output}")


if __name__ == "__main__":
    main()
