"""
Ownership graph feature engineering.

Queries the Lance Graph datasets for graph-based risk features.

Output columns (one row per MMSI):
    mmsi, sanctions_distance, cluster_sanctions_ratio,
    shared_manager_risk, shared_address_centrality, sts_hub_degree

Usage:
    uv run python src/features/ownership_graph.py
"""

import os

import duckdb
import polars as pl
from dotenv import load_dotenv

from src.graph.store import load_tables

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")

MAX_HOPS = 99  # sentinel for "no sanctions connection found"


# ---------------------------------------------------------------------------
# Feature computations (polars joins on PyArrow tables)
# ---------------------------------------------------------------------------


def _compute_sanctions_distance(tables: dict) -> pl.DataFrame:
    """
    0 = vessel directly sanctioned
    1 = owner or manager is sanctioned
    2 = parent company (via CONTROLLED_BY) is sanctioned
    99 = no sanctions connection
    """
    vessels = pl.from_arrow(tables["Vessel"]).select("mmsi")
    sb = pl.from_arrow(tables["SANCTIONED_BY"])
    ob = pl.from_arrow(tables["OWNED_BY"])
    mb = pl.from_arrow(tables["MANAGED_BY"])
    cb = pl.from_arrow(tables["CONTROLLED_BY"])

    # All sanctioned entity IDs
    sanctioned_ids: set[str] = set(sb["src_id"].to_list()) if len(sb) else set()

    # Direct (vessel mmsi in sanctioned_ids)
    direct: set[str] = set(vessels.filter(pl.col("mmsi").is_in(sanctioned_ids))["mmsi"].to_list())

    # 1-hop: vessel → (OWNED_BY | MANAGED_BY) → company → sanctioned
    if len(ob) or len(mb):
        frames = []
        if len(ob):
            frames.append(ob.select(["src_id", "dst_id"]))
        if len(mb):
            frames.append(mb.select(["src_id", "dst_id"]))
        vessel_companies = pl.concat(frames).unique()
        one_hop_vessels = (
            vessel_companies.filter(pl.col("dst_id").is_in(sanctioned_ids))["src_id"]
            .unique()
            .to_list()
        )
        one_hop: set[str] = set(one_hop_vessels)
    else:
        one_hop = set()

    # 2-hop: vessel → company → (CONTROLLED_BY) → sanctioned parent
    two_hop: set[str] = set()
    if len(cb) and len(ob or mb):
        sanctioned_parents = set(
            cb.filter(pl.col("dst_id").is_in(sanctioned_ids))["src_id"].to_list()
        )
        if sanctioned_parents and (len(ob) or len(mb)):
            two_hop_vessels = (
                vessel_companies.filter(pl.col("dst_id").is_in(sanctioned_parents))["src_id"]
                .unique()
                .to_list()
            )
            two_hop = set(two_hop_vessels)

    rows = []
    for mmsi in vessels["mmsi"].to_list():
        if mmsi in direct:
            dist = 0
        elif mmsi in one_hop:
            dist = 1
        elif mmsi in two_hop:
            dist = 2
        else:
            dist = MAX_HOPS
        rows.append({"mmsi": mmsi, "sanctions_distance": dist})

    return (
        pl.DataFrame(rows, schema={"mmsi": pl.Utf8, "sanctions_distance": pl.Int32})
        if rows
        else pl.DataFrame(schema={"mmsi": pl.Utf8, "sanctions_distance": pl.Int32})
    )


def _compute_cluster_sanctions_ratio(tables: dict) -> pl.DataFrame:
    """Fraction of co-owned vessels (same direct owner) that are sanctioned."""
    vessels = pl.from_arrow(tables["Vessel"]).select("mmsi")
    ob = pl.from_arrow(tables["OWNED_BY"])
    sb = pl.from_arrow(tables["SANCTIONED_BY"])

    if len(ob) == 0:
        return vessels.with_columns(pl.lit(0.0).cast(pl.Float32).alias("cluster_sanctions_ratio"))

    sanctioned_ids: set[str] = set(sb["src_id"].to_list()) if len(sb) else set()

    own = ob.select(["src_id", "dst_id"]).rename({"src_id": "vessel", "dst_id": "owner"})

    # Self-join on owner to get (vessel, peer) pairs
    pairs = (
        own.join(own.rename({"vessel": "peer"}), on="owner")
        .filter(pl.col("vessel") != pl.col("peer"))
        .select(["vessel", "peer"])
        .unique()
    )

    if len(pairs) == 0:
        return vessels.with_columns(pl.lit(0.0).cast(pl.Float32).alias("cluster_sanctions_ratio"))

    ratio_df = (
        pairs.with_columns(pl.col("peer").is_in(sanctioned_ids).alias("peer_sanctioned"))
        .group_by("vessel")
        .agg(
            [
                pl.len().alias("cluster_size"),
                pl.col("peer_sanctioned").sum().alias("sanctioned_count"),
            ]
        )
        .with_columns(
            (pl.col("sanctioned_count").cast(pl.Float32) / pl.col("cluster_size")).alias(
                "cluster_sanctions_ratio"
            )
        )
        .select(["vessel", "cluster_sanctions_ratio"])
        .rename({"vessel": "mmsi"})
    )

    return vessels.join(ratio_df, on="mmsi", how="left").with_columns(
        pl.col("cluster_sanctions_ratio").fill_null(0.0).cast(pl.Float32)
    )


def _compute_shared_manager_risk(
    tables: dict,
    sanctions_map: dict[str, int],
) -> pl.DataFrame:
    """Min sanctions_distance among vessels sharing the same manager."""
    vessels = pl.from_arrow(tables["Vessel"]).select("mmsi")
    mb = pl.from_arrow(tables["MANAGED_BY"])

    if len(mb) == 0:
        return vessels.with_columns(pl.lit(MAX_HOPS).cast(pl.Int32).alias("shared_manager_risk"))

    mgd = mb.select(["src_id", "dst_id"]).rename({"src_id": "vessel", "dst_id": "manager"})

    peers_df = (
        mgd.join(mgd.rename({"vessel": "peer"}), on="manager")
        .filter(pl.col("vessel") != pl.col("peer"))
        .select(["vessel", "peer"])
        .unique()
        .group_by("vessel")
        .agg(pl.col("peer").alias("peer_mmsis"))
        .rename({"vessel": "mmsi"})
    )

    rows = []
    for row in peers_df.iter_rows(named=True):
        peers = row["peer_mmsis"] or []
        min_dist = min((sanctions_map.get(p, MAX_HOPS) for p in peers), default=MAX_HOPS)
        rows.append({"mmsi": row["mmsi"], "shared_manager_risk": min_dist})

    smr_df = (
        pl.DataFrame(rows, schema={"mmsi": pl.Utf8, "shared_manager_risk": pl.Int32})
        if rows
        else pl.DataFrame(schema={"mmsi": pl.Utf8, "shared_manager_risk": pl.Int32})
    )

    return vessels.join(smr_df, on="mmsi", how="left").with_columns(
        pl.col("shared_manager_risk").fill_null(MAX_HOPS).cast(pl.Int32)
    )


def _compute_shared_address_centrality(tables: dict) -> pl.DataFrame:
    """Count of distinct vessels sharing a registered address (via owner/manager)."""
    vessels = pl.from_arrow(tables["Vessel"]).select("mmsi")
    ob = pl.from_arrow(tables["OWNED_BY"])
    mb = pl.from_arrow(tables["MANAGED_BY"])
    ra = pl.from_arrow(tables["REGISTERED_AT"])

    if (len(ob) == 0 and len(mb) == 0) or len(ra) == 0:
        return vessels.with_columns(pl.lit(0).cast(pl.Int32).alias("shared_address_centrality"))

    frames = []
    if len(ob):
        frames.append(ob.select(["src_id", "dst_id"]))
    if len(mb):
        frames.append(mb.select(["src_id", "dst_id"]))
    vessel_company = pl.concat(frames).unique().rename({"src_id": "vessel", "dst_id": "company"})

    reg_at = ra.rename({"src_id": "company", "dst_id": "address"})
    vessel_address = (
        vessel_company.join(reg_at, on="company").select(["vessel", "address"]).unique()
    )

    if len(vessel_address) == 0:
        return vessels.with_columns(pl.lit(0).cast(pl.Int32).alias("shared_address_centrality"))

    centrality_df = (
        vessel_address.join(vessel_address.rename({"vessel": "peer"}), on="address")
        .filter(pl.col("vessel") != pl.col("peer"))
        .select(["vessel", "peer"])
        .unique()
        .group_by("vessel")
        .agg(pl.col("peer").n_unique().alias("shared_address_centrality"))
        .rename({"vessel": "mmsi"})
    )

    return vessels.join(centrality_df, on="mmsi", how="left").with_columns(
        pl.col("shared_address_centrality").fill_null(0).cast(pl.Int32)
    )


def _compute_sts_hub_degree(tables: dict) -> pl.DataFrame:
    """Count of distinct vessels with STS contact."""
    vessels = pl.from_arrow(tables["Vessel"]).select("mmsi")
    sts = pl.from_arrow(tables["STS_CONTACT"])

    if len(sts) == 0:
        return vessels.with_columns(pl.lit(0).cast(pl.Int32).alias("sts_hub_degree"))

    hub_df = (
        sts.rename({"src_id": "mmsi"})
        .group_by("mmsi")
        .agg(pl.col("dst_id").n_unique().alias("sts_hub_degree"))
    )

    return vessels.join(hub_df, on="mmsi", how="left").with_columns(
        pl.col("sts_hub_degree").fill_null(0).cast(pl.Int32)
    )


# ---------------------------------------------------------------------------
# DuckDB fallback for stale or incomplete Lance Graph
# ---------------------------------------------------------------------------


def _apply_direct_sanctions_fallback(sd_df: pl.DataFrame, db_path: str) -> pl.DataFrame:
    """Override sanctions_distance=99 → 0 for vessels directly in sanctions_entities.

    The Lance Graph is built once per pipeline run. If sanctions data was refreshed
    after the graph was last written, or if an entity's FtM schema type prevented it
    from being included in the SANCTIONED_BY edge set, its distance stays at 99 even
    though its MMSI is plainly on a sanctions list.

    This fallback does a live query against sanctions_entities (no graph traversal)
    and corrects any vessel whose MMSI appears there directly.
    """
    if sd_df.is_empty():
        return sd_df
    try:
        con = duckdb.connect(db_path, read_only=True)
        try:
            direct_mmsi: set[str] = set(
                con.execute(
                    "SELECT DISTINCT mmsi FROM sanctions_entities "
                    "WHERE mmsi IS NOT NULL AND mmsi <> ''"
                )
                .fetchdf()["mmsi"]
                .tolist()
            )
        finally:
            con.close()
    except Exception:
        return sd_df  # DB unavailable; keep graph-derived distances

    if not direct_mmsi:
        return sd_df

    return sd_df.with_columns(
        pl.when((pl.col("sanctions_distance") == MAX_HOPS) & pl.col("mmsi").is_in(direct_mmsi))
        .then(pl.lit(0, dtype=pl.Int32))
        .otherwise(pl.col("sanctions_distance"))
        .alias("sanctions_distance")
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def compute_ownership_graph_features(db_path: str) -> pl.DataFrame:
    """Load Lance datasets and compute all ownership graph features.

    Returns one row per MMSI.
    """
    tables = load_tables(db_path)

    sd_df = _compute_sanctions_distance(tables)

    if sd_df.is_empty():
        return pl.DataFrame(
            schema={
                "mmsi": pl.Utf8,
                "sanctions_distance": pl.Int32,
                "cluster_sanctions_ratio": pl.Float32,
                "shared_manager_risk": pl.Int32,
                "shared_address_centrality": pl.Int32,
                "sts_hub_degree": pl.Int32,
            }
        )

    sanctions_map: dict[str, int] = dict(
        zip(sd_df["mmsi"].to_list(), sd_df["sanctions_distance"].to_list())
    )

    cr_df = _compute_cluster_sanctions_ratio(tables)
    smr_df = _compute_shared_manager_risk(tables, sanctions_map)
    sa_df = _compute_shared_address_centrality(tables)
    sts_df = _compute_sts_hub_degree(tables)

    return (
        sd_df.lazy()
        .join(cr_df.lazy(), on="mmsi", how="left")
        .join(smr_df.lazy(), on="mmsi", how="left")
        .join(sa_df.lazy(), on="mmsi", how="left")
        .join(sts_df.lazy(), on="mmsi", how="left")
        .with_columns(
            [
                pl.col("sanctions_distance").fill_null(MAX_HOPS).cast(pl.Int32),
                pl.col("cluster_sanctions_ratio").fill_null(0.0).cast(pl.Float32),
                pl.col("shared_manager_risk").fill_null(MAX_HOPS).cast(pl.Int32),
                pl.col("shared_address_centrality").fill_null(0).cast(pl.Int32),
                pl.col("sts_hub_degree").fill_null(0).cast(pl.Int32),
            ]
        )
        .collect()
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Compute ownership graph features")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    args = parser.parse_args()

    result = compute_ownership_graph_features(args.db)
    print(f"Ownership graph features: {len(result)} vessels")
    print(result.head())
