"""Build and persist the Phase A3 vessel feature matrix.

This module orchestrates all feature families and writes one row per MMSI to
DuckDB table vessel_features.
"""

from __future__ import annotations

import argparse
import os

import duckdb
import polars as pl
from dotenv import load_dotenv

from src.features.ais_behavior import DEFAULT_DB_PATH, compute_ais_features
from src.features.identity import compute_identity_features
from src.features.ownership_graph import compute_ownership_graph_features
from src.features.sar_detections import compute_unmatched_sar_detections
from src.features.trade_mismatch import compute_trade_features

load_dotenv()

DEFAULTS = {
    "ais_gap_count_30d": 0,
    "ais_gap_max_hours": 0.0,
    "position_jump_count": 0,
    "sts_candidate_count": 0,
    "port_call_ratio": 0.5,
    "loitering_hours_30d": 0.0,
    "flag_changes_2y": 0,
    "name_changes_2y": 0,
    "owner_changes_2y": 0,
    "high_risk_flag_ratio": 0.0,
    "ownership_depth": 0,
    "sanctions_distance": 99,
    "cluster_sanctions_ratio": 0.0,
    "shared_manager_risk": 99,
    "shared_address_centrality": 0,
    "sts_hub_degree": 0,
    "route_cargo_mismatch": 0.0,
    "declared_vs_estimated_cargo_value": 0.0,
    "unmatched_sar_detections_30d": 0,
}

CORE_COLUMNS = [
    "ais_gap_count_30d",
    "position_jump_count",
    "sts_candidate_count",
    "loitering_hours_30d",
    "flag_changes_2y",
    "name_changes_2y",
    "owner_changes_2y",
    "sanctions_distance",
    "cluster_sanctions_ratio",
    "route_cargo_mismatch",
]


def _empty_identity() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "mmsi": pl.Utf8,
            "flag_changes_2y": pl.Int32,
            "name_changes_2y": pl.Int32,
            "owner_changes_2y": pl.Int32,
            "high_risk_flag_ratio": pl.Float32,
            "ownership_depth": pl.Int32,
        }
    )


def _empty_ownership() -> pl.DataFrame:
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


def _normalize(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df
    return df.with_columns(pl.col("mmsi").cast(pl.Utf8))


def _merge_feature_frames(
    ais_df: pl.DataFrame,
    identity_df: pl.DataFrame,
    ownership_df: pl.DataFrame,
    trade_df: pl.DataFrame,
    sar_df: pl.DataFrame,
) -> pl.DataFrame:
    frames = [
        _normalize(ais_df),
        _normalize(identity_df),
        _normalize(ownership_df),
        _normalize(trade_df),
        _normalize(sar_df),
    ]
    non_empty = [f for f in frames if not f.is_empty()]
    if not non_empty:
        return pl.DataFrame({"mmsi": []}, schema={"mmsi": pl.Utf8})

    all_mmsi = pl.concat([f.select("mmsi") for f in non_empty]).unique()

    out = (
        all_mmsi.lazy()
        .join(frames[0].lazy(), on="mmsi", how="left")
        .join(frames[1].lazy(), on="mmsi", how="left")
        .join(frames[2].lazy(), on="mmsi", how="left")
        .join(frames[3].lazy(), on="mmsi", how="left")
        .join(frames[4].lazy(), on="mmsi", how="left")
        .collect()
    )

    for col, default in DEFAULTS.items():
        if col not in out.columns:
            out = out.with_columns(pl.lit(default).alias(col))
        else:
            out = out.with_columns(pl.col(col).fill_null(default))

    return out.select(["mmsi", *DEFAULTS.keys()])


def _empty_sar() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "mmsi": pl.Utf8,
            "unmatched_sar_detections_30d": pl.Int32,
        }
    )


def build_feature_matrix(
    db_path: str = DEFAULT_DB_PATH,
    window_days: int = 30,
    skip_graph: bool = False,
) -> pl.DataFrame:
    ais_df = compute_ais_features(db_path=db_path, window_days=window_days)
    trade_df = compute_trade_features(db_path=db_path)
    sar_df = compute_unmatched_sar_detections(db_path=db_path, window_days=window_days)

    if skip_graph:
        identity_df = _empty_identity()
        ownership_df = _empty_ownership()
    else:
        identity_df = compute_identity_features(db_path=db_path)
        ownership_df = compute_ownership_graph_features(db_path=db_path)

    return _merge_feature_frames(ais_df, identity_df, ownership_df, trade_df, sar_df)


def write_vessel_features(db_path: str, feature_df: pl.DataFrame) -> int:
    if feature_df.is_empty():
        return 0

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = duckdb.connect(db_path)
    try:
        con.execute("BEGIN")
        con.execute("DELETE FROM vessel_features")
        con.execute(
            """
            INSERT INTO vessel_features (
                mmsi,
                ais_gap_count_30d,
                ais_gap_max_hours,
                position_jump_count,
                sts_candidate_count,
                port_call_ratio,
                loitering_hours_30d,
                flag_changes_2y,
                name_changes_2y,
                owner_changes_2y,
                high_risk_flag_ratio,
                ownership_depth,
                sanctions_distance,
                cluster_sanctions_ratio,
                shared_manager_risk,
                shared_address_centrality,
                sts_hub_degree,
                route_cargo_mismatch,
                declared_vs_estimated_cargo_value,
                unmatched_sar_detections_30d,
                computed_at
            )
            SELECT
                mmsi,
                ais_gap_count_30d,
                ais_gap_max_hours,
                position_jump_count,
                sts_candidate_count,
                port_call_ratio,
                loitering_hours_30d,
                flag_changes_2y,
                name_changes_2y,
                owner_changes_2y,
                high_risk_flag_ratio,
                ownership_depth,
                sanctions_distance,
                cluster_sanctions_ratio,
                shared_manager_risk,
                shared_address_centrality,
                sts_hub_degree,
                route_cargo_mismatch,
                declared_vs_estimated_cargo_value,
                unmatched_sar_detections_30d,
                now()
            FROM feature_df
            """
        )
        con.execute("COMMIT")
        return feature_df.height
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def validate_core_columns_non_null(feature_df: pl.DataFrame) -> None:
    if feature_df.is_empty():
        return

    null_checks = [pl.col(col).is_null().sum().alias(col) for col in CORE_COLUMNS]
    counts = feature_df.select(null_checks).row(0, named=True)
    failed = [col for col, cnt in counts.items() if cnt > 0]
    if failed:
        cols = ", ".join(failed)
        raise ValueError(f"Core feature columns contain nulls: {cols}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build and persist vessel feature matrix")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--window", type=int, default=30)
    parser.add_argument("--skip-graph", action="store_true")
    args = parser.parse_args()

    matrix = build_feature_matrix(
        db_path=args.db,
        window_days=args.window,
        skip_graph=args.skip_graph,
    )
    validate_core_columns_non_null(matrix)
    written = write_vessel_features(args.db, matrix)
    print(f"vessel_features rows written: {written}")


if __name__ == "__main__":
    main()
