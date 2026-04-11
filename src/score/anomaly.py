"""Isolation Forest-based anomaly scoring for vessel features."""

from __future__ import annotations

import argparse
import os

import duckdb
import numpy as np
import polars as pl
from dotenv import load_dotenv
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from src.score.mpol_baseline import build_mpol_baseline, load_cleared_mmsis

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")
DEFAULT_OUTPUT_PATH = os.getenv("ANOMALY_SCORES_PATH", "data/processed/anomaly_scores.parquet")

ANOMALY_FEATURE_COLUMNS = [
    "ais_gap_count_30d",
    "ais_gap_max_hours",
    "position_jump_count",
    "sts_candidate_count",
    "port_call_ratio",
    "loitering_hours_30d",
    "flag_changes_2y",
    "name_changes_2y",
    "owner_changes_2y",
    "high_risk_flag_ratio",
    "ownership_depth",
    "sanctions_distance",
    "cluster_sanctions_ratio",
    "shared_manager_risk",
    "shared_address_centrality",
    "sts_hub_degree",
    "route_cargo_mismatch",
    "declared_vs_estimated_cargo_value",
    "unmatched_sar_detections_30d",
    "eo_dark_count_30d",
    "eo_ais_mismatch_ratio",
]


def load_feature_frame(db_path: str = DEFAULT_DB_PATH) -> pl.DataFrame:
    con = duckdb.connect(db_path, read_only=True)
    try:
        return con.execute(
            f"SELECT mmsi, {', '.join(ANOMALY_FEATURE_COLUMNS)} FROM vessel_features ORDER BY mmsi"
        ).pl()
    finally:
        con.close()


def fit_isolation_forest(
    feature_df: pl.DataFrame,
    cleared_mmsis: frozenset[str] | None = None,
) -> tuple[StandardScaler, IsolationForest]:
    """Train Isolation Forest on the clean vessel population.

    Parameters
    ----------
    feature_df:
        Full feature frame for all vessels.
    cleared_mmsis:
        MMSIs of vessels confirmed normal by Phase B physical inspection.
        These are always included in the training set regardless of
        sanctions_distance, anchoring the "known normal" region.
    """
    matrix = feature_df.select(ANOMALY_FEATURE_COLUMNS).fill_null(0).to_numpy()
    if matrix.shape[0] == 0:
        raise ValueError("Cannot train anomaly model on empty feature set")

    # Base clean set: vessels with no graph proximity to sanctions
    clean_subset = feature_df.filter(pl.col("sanctions_distance") >= 3)

    # Add cleared vessels as hard negatives even if their sanctions_distance < 3
    if cleared_mmsis:
        cleared_df = feature_df.filter(pl.col("mmsi").is_in(list(cleared_mmsis)))
        if not cleared_df.is_empty():
            combined = pl.concat([clean_subset, cleared_df]).unique(subset=["mmsi"])
            clean_subset = combined

    train_df = clean_subset if clean_subset.height >= 4 else feature_df
    train_matrix = train_df.select(ANOMALY_FEATURE_COLUMNS).fill_null(0).to_numpy()

    scaler = StandardScaler()
    train_scaled = scaler.fit_transform(train_matrix)

    model = IsolationForest(
        n_estimators=200,
        contamination=0.03,  # ~3% matches observed OFAC positive rate in fleet
        random_state=42,
    )
    model.fit(train_scaled)
    return scaler, model


def score_anomalies(
    feature_df: pl.DataFrame,
    baseline_df: pl.DataFrame | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> tuple[pl.DataFrame, StandardScaler, IsolationForest]:
    if feature_df.is_empty():
        empty = pl.DataFrame(
            schema={
                "mmsi": pl.Utf8,
                "cluster_label": pl.Int32,
                "baseline_noise_score": pl.Float32,
                "isolation_raw_score": pl.Float32,
                "anomaly_score": pl.Float32,
            }
        )
        return empty, StandardScaler(), IsolationForest(random_state=42)

    if baseline_df is None:
        baseline_df = build_mpol_baseline(db_path)

    cleared = load_cleared_mmsis(db_path)
    scaler, model = fit_isolation_forest(feature_df, cleared_mmsis=cleared)
    matrix = feature_df.select(ANOMALY_FEATURE_COLUMNS).fill_null(0).to_numpy()
    scaled = scaler.transform(matrix)

    raw = -model.decision_function(scaled)
    if np.ptp(raw) == 0:
        normalized = np.full(raw.shape, 0.5, dtype=np.float32)
    else:
        normalized = ((raw - raw.min()) / np.ptp(raw)).astype(np.float32)

    joined = (
        feature_df.select("mmsi")
        .join(baseline_df, on="mmsi", how="left")
        .with_columns(
            [
                pl.col("cluster_label").fill_null(0).cast(pl.Int32),
                pl.col("baseline_noise_score").fill_null(0.0).cast(pl.Float32),
                pl.Series("isolation_raw_score", raw.astype(np.float32)),
                pl.Series("isolation_norm_score", normalized),
            ]
        )
    )

    result = joined.with_columns(
        (0.65 * pl.col("isolation_norm_score") + 0.35 * pl.col("baseline_noise_score"))
        .clip(0.0, 1.0)
        .alias("anomaly_score")
    ).select(
        [
            "mmsi",
            "cluster_label",
            "baseline_noise_score",
            "isolation_raw_score",
            "anomaly_score",
        ]
    )

    return result, scaler, model


def write_anomaly_scores(df: pl.DataFrame, output_path: str = DEFAULT_OUTPUT_PATH) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.write_parquet(output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Score anomalies from vessel_features")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    feature_df = load_feature_frame(args.db)
    baseline_df = build_mpol_baseline(args.db)
    anomaly_df, _, _ = score_anomalies(feature_df, baseline_df)
    write_anomaly_scores(anomaly_df, args.output)
    print(f"Anomaly rows written: {anomaly_df.height}")


if __name__ == "__main__":
    main()
