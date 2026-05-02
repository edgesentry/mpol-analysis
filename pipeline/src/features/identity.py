"""
Identity volatility feature engineering.

Queries the Lance Graph datasets for per-vessel identity change counts and
ownership structure. Also reads vessel_meta from DuckDB for the current flag state.

Output columns (one row per MMSI):
    mmsi, flag_changes_2y, name_changes_2y, owner_changes_2y,
    high_risk_flag_ratio, ownership_depth

Usage:
    uv run python src/features/identity.py
"""

import os

import duckdb
import polars as pl
from dotenv import load_dotenv

from pipeline.src.graph.store import load_tables

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")

# Flags with weak Port State Control oversight (UNCTAD/Paris MOU grey/black list proxies)
HIGH_RISK_FLAGS = {
    "KP",
    "IR",
    "VE",
    "SY",
    "CU",  # sanctioned states
    "KM",
    "GA",
    "CM",
    "PW",  # high-risk open registries (Comoros, Gabon, Cameroon, Palau)
    "KI",
    "TG",
    "SL",
    "ST",  # frequently flagged in shadow fleet reports
}


# ---------------------------------------------------------------------------
# Feature computations (polars joins on PyArrow tables)
# ---------------------------------------------------------------------------


def _compute_name_changes(tables: dict) -> pl.DataFrame:
    vessels = pl.from_arrow(tables["Vessel"]).select("mmsi")
    aliases = pl.from_arrow(tables["ALIAS"])

    if len(aliases) == 0:
        return vessels.with_columns(pl.lit(0).cast(pl.Int32).alias("name_changes_2y"))

    counts = (
        aliases.rename({"src_id": "mmsi"}).group_by("mmsi").agg(pl.len().alias("name_changes_2y"))
    )
    return vessels.join(counts, on="mmsi", how="left").with_columns(
        pl.col("name_changes_2y").fill_null(0).cast(pl.Int32)
    )


def _compute_owner_changes(tables: dict) -> pl.DataFrame:
    vessels = pl.from_arrow(tables["Vessel"]).select("mmsi")
    ob = pl.from_arrow(tables["OWNED_BY"])

    if len(ob) == 0:
        return vessels.with_columns(pl.lit(0).cast(pl.Int32).alias("owner_changes_2y"))

    counts = ob.rename({"src_id": "mmsi"}).group_by("mmsi").agg(pl.len().alias("owner_changes_2y"))
    return vessels.join(counts, on="mmsi", how="left").with_columns(
        pl.col("owner_changes_2y").fill_null(0).cast(pl.Int32)
    )


def _compute_ownership_depth(tables: dict) -> pl.DataFrame:
    """Max ownership chain depth: OWNED_BY(1) + CONTROLLED_BY hops (0..5)."""
    vessels = pl.from_arrow(tables["Vessel"]).select("mmsi")
    ob = pl.from_arrow(tables["OWNED_BY"])
    cb = pl.from_arrow(tables["CONTROLLED_BY"])

    if len(ob) == 0:
        return vessels.with_columns(pl.lit(0).cast(pl.Int32).alias("ownership_depth"))

    # Vessels with at least one owner start at depth=1
    has_owner = set(ob["src_id"].to_list())

    if len(cb) == 0:
        # No CONTROLLED_BY edges → depth is 1 for any vessel with an owner, 0 otherwise
        return vessels.with_columns(
            pl.when(pl.col("mmsi").is_in(has_owner))
            .then(1)
            .otherwise(0)
            .cast(pl.Int32)
            .alias("ownership_depth")
        )

    # BFS through CONTROLLED_BY for each owned company (up to 5 hops)
    # Build adjacency: company → parent companies
    cb_map: dict[str, list[str]] = {}
    for src, dst in cb.select(["src_id", "dst_id"]).iter_rows():
        cb_map.setdefault(src, []).append(dst)

    # For each vessel, find all companies it owns, then BFS through CONTROLLED_BY
    vessel_companies: dict[str, set[str]] = {}
    for vessel, company in ob.select(["src_id", "dst_id"]).iter_rows():
        vessel_companies.setdefault(vessel, set()).add(company)

    depth_map: dict[str, int] = {}
    for mmsi, start_companies in vessel_companies.items():
        max_depth = 1  # OWNED_BY = 1
        frontier = set(start_companies)
        for hop in range(1, 6):
            next_frontier: set[str] = set()
            for company in frontier:
                next_frontier.update(cb_map.get(company, []))
            if not next_frontier:
                break
            frontier = next_frontier
            max_depth = 1 + hop
        depth_map[mmsi] = max_depth

    depth_df = pl.DataFrame(
        [{"mmsi": k, "ownership_depth": v} for k, v in depth_map.items()],
        schema={"mmsi": pl.Utf8, "ownership_depth": pl.Int32},
    )

    return vessels.join(depth_df, on="mmsi", how="left").with_columns(
        pl.col("ownership_depth").fill_null(0).cast(pl.Int32)
    )


def _compute_high_risk_flag_ratio(tables: dict) -> pl.DataFrame:
    """Fraction of owning-company country codes that are high-risk flags."""
    vessels = pl.from_arrow(tables["Vessel"]).select("mmsi")
    ob = pl.from_arrow(tables["OWNED_BY"])
    ri = pl.from_arrow(tables["REGISTERED_IN"])

    if len(ob) == 0 or len(ri) == 0:
        return vessels.with_columns(pl.lit(0.0).cast(pl.Float32).alias("high_risk_flag_ratio"))

    # vessel → company → country
    vessel_countries = (
        ob.select(["src_id", "dst_id"])
        .rename({"src_id": "mmsi", "dst_id": "company"})
        .join(ri.rename({"src_id": "company", "dst_id": "country"}), on="company")
        .select(["mmsi", "country"])
        .unique()
    )

    if len(vessel_countries) == 0:
        return vessels.with_columns(pl.lit(0.0).cast(pl.Float32).alias("high_risk_flag_ratio"))

    rows = []
    for mmsi, grp in vessel_countries.group_by("mmsi"):
        countries = grp["country"].to_list()
        if not countries:
            ratio = 0.0
        else:
            risky = sum(1 for c in countries if c in HIGH_RISK_FLAGS)
            ratio = risky / len(countries)
        rows.append(
            {"mmsi": mmsi[0] if isinstance(mmsi, tuple) else mmsi, "high_risk_flag_ratio": ratio}
        )

    hrisk_df = (
        pl.DataFrame(rows, schema={"mmsi": pl.Utf8, "high_risk_flag_ratio": pl.Float32})
        if rows
        else pl.DataFrame(schema={"mmsi": pl.Utf8, "high_risk_flag_ratio": pl.Float32})
    )

    return vessels.join(hrisk_df, on="mmsi", how="left").with_columns(
        pl.col("high_risk_flag_ratio").fill_null(0.0).cast(pl.Float32)
    )


# ---------------------------------------------------------------------------
# IMO identity spoofing features
# ---------------------------------------------------------------------------


# AIS ship_type → broad category (ITU-R M.1371 Annex 8 Table 22)
# Only categories that matter for cross-reference; anything else → 0 (unknown).
def _ship_type_category(ship_type: int | None) -> int:
    if ship_type is None:
        return 0
    if ship_type == 30:
        return 1  # fishing
    if 31 <= ship_type <= 57:
        return 2  # service (tug, pilot, SAR, etc.)
    if 60 <= ship_type <= 69:
        return 3  # passenger
    if 70 <= ship_type <= 79:
        return 4  # cargo
    if 80 <= ship_type <= 89:
        return 5  # tanker
    return 0  # unknown / other


def compute_imo_mismatch_features(db_path: str = DEFAULT_DB_PATH) -> pl.DataFrame:
    """imo_type_mismatch and imo_scrapped_flag per MMSI.

    Joins vessel_meta (AIS-reported ship_type + imo) against equasis_vessel_ref
    (registered vessel_type and scrapped status from Equasis CSV).

    imo_type_mismatch — True when the AIS-reported ship type category (cargo,
    tanker, passenger, fishing …) differs from the category registered for the
    same IMO number in Equasis.  A 2005-built VLCC (Equasis: tanker) cannot be
    a 2018-built chemical tanker (AIS: cargo).

    imo_scrapped_flag — True when the vessel's IMO number is marked as scrapped
    or deleted in the Equasis reference data.

    Returns a DataFrame with (mmsi, imo_type_mismatch, imo_scrapped_flag).
    If equasis_vessel_ref is empty, both features are False for all vessels.
    """
    _empty_schema = {
        "mmsi": pl.Utf8,
        "imo_type_mismatch": pl.Boolean,
        "imo_scrapped_flag": pl.Boolean,
    }

    con = duckdb.connect(db_path, read_only=True)
    try:
        meta = con.execute(
            "SELECT mmsi, COALESCE(imo,'') AS imo, ship_type "
            "FROM vessel_meta WHERE mmsi IS NOT NULL"
        ).pl()
        ref_count = con.execute("SELECT COUNT(*) FROM equasis_vessel_ref").fetchone()[0]
        if ref_count == 0:
            if meta.is_empty():
                return pl.DataFrame(schema=_empty_schema)
            return meta.select("mmsi").with_columns(
                [
                    pl.lit(False).alias("imo_type_mismatch"),
                    pl.lit(False).alias("imo_scrapped_flag"),
                ]
            )
        ref = con.execute("SELECT imo, vessel_type, scrapped FROM equasis_vessel_ref").pl()
    finally:
        con.close()

    if meta.is_empty():
        return pl.DataFrame(schema=_empty_schema)

    joined = meta.filter(pl.col("imo") != "").join(ref, on="imo", how="left")

    rows = []
    for row in joined.iter_rows(named=True):
        ais_cat = _ship_type_category(row["ship_type"])
        eq_cat = _ship_type_category(row.get("vessel_type"))
        mismatch = ais_cat != 0 and eq_cat != 0 and ais_cat != eq_cat
        scrapped = bool(row.get("scrapped") or False)
        rows.append(
            {
                "mmsi": row["mmsi"],
                "imo_type_mismatch": mismatch,
                "imo_scrapped_flag": scrapped,
            }
        )

    if not rows:
        return pl.DataFrame(schema=_empty_schema)

    result = pl.DataFrame(rows, schema=_empty_schema)

    # Vessels with no IMO in vessel_meta get False defaults
    all_mmsi = meta.select("mmsi")
    return all_mmsi.join(result, on="mmsi", how="left").with_columns(
        [
            pl.col("imo_type_mismatch").fill_null(False),
            pl.col("imo_scrapped_flag").fill_null(False),
        ]
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def compute_identity_features(db_path: str = DEFAULT_DB_PATH) -> pl.DataFrame:
    """Load Lance datasets + DuckDB for identity volatility features."""
    tables = load_tables(db_path)

    name_df = _compute_name_changes(tables)
    owner_df = _compute_owner_changes(tables)
    depth_df = _compute_ownership_depth(tables)
    hrisk_df = _compute_high_risk_flag_ratio(tables)
    imo_df = compute_imo_mismatch_features(db_path)

    # flag_changes_2y: hardcoded to 0 — historical flag-state records are not yet
    # ingested. This is an intentional deferral, not a broken feature.
    # TODO (Phase C): ingest flag-state history from VesselFinder / MarineTraffic
    # historical API or manual EQUASIS export, then remove this pl.lit(0) hardcode.
    # See docs/roadmap.md § A3 and arktrace#296.
    con = duckdb.connect(db_path, read_only=True)
    try:
        meta = con.execute(
            "SELECT mmsi, COALESCE(flag,'') AS flag FROM vessel_meta WHERE mmsi IS NOT NULL"
        ).pl()
    finally:
        con.close()

    flag_df = meta.with_columns(pl.lit(0).cast(pl.Int32).alias("flag_changes_2y")).select(
        ["mmsi", "flag_changes_2y"]
    )

    all_mmsi = name_df.select("mmsi")
    if all_mmsi.is_empty() and not flag_df.is_empty():
        all_mmsi = flag_df.select("mmsi")

    return (
        all_mmsi.lazy()
        .join(name_df.lazy(), on="mmsi", how="left")
        .join(owner_df.lazy(), on="mmsi", how="left")
        .join(depth_df.lazy(), on="mmsi", how="left")
        .join(hrisk_df.lazy(), on="mmsi", how="left")
        .join(flag_df.lazy(), on="mmsi", how="left")
        .join(imo_df.lazy(), on="mmsi", how="left")
        .with_columns(
            [
                pl.col("flag_changes_2y").fill_null(0).cast(pl.Int32),
                pl.col("name_changes_2y").fill_null(0).cast(pl.Int32),
                pl.col("owner_changes_2y").fill_null(0).cast(pl.Int32),
                pl.col("high_risk_flag_ratio").fill_null(0.0).cast(pl.Float32),
                pl.col("ownership_depth").fill_null(0).cast(pl.Int32),
                pl.col("imo_type_mismatch").fill_null(False),
                pl.col("imo_scrapped_flag").fill_null(False),
            ]
        )
        .collect()
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Compute identity volatility features")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    args = parser.parse_args()

    result = compute_identity_features(args.db)
    print(f"Identity features: {len(result)} vessels")
    print(result.head())
