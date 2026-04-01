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

from src.score.mpol_baseline import DEFAULT_OUTPUT_PATH as DEFAULT_BASELINE_PATH
from src.score.mpol_baseline import build_mpol_baseline

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
]


def load_feature_frame(db_path: str = DEFAULT_DB_PATH) -> pl.DataFrame:
    con = duckdb.connect(db_path, read_only=True)
    try:
        return con.execute(
            f"SELECT mmsi, {', '.join(ANOMALY_FEATURE_COLUMNS)} FROM vessel_features ORDER BY mmsi"
        ).pl()
    finally:
        con.close()


def fit_isolation_forest(feature_df: pl.DataFrame) -> tuple[StandardScaler, IsolationForest]:
    matrix = feature_df.select(ANOMALY_FEATURE_COLUMNS).fill_null(0).to_numpy()
    if matrix.shape[0] == 0:
        raise ValueError("Cannot train anomaly model on empty feature set")

    clean_subset = feature_df.filter(pl.col("sanctions_distance") >= 3)
    train_df = clean_subset if clean_subset.height >= 4 else feature_df
    train_matrix = train_df.select(ANOMALY_FEATURE_COLUMNS).fill_null(0).to_numpy()

    scaler = StandardScaler()
    train_scaled = scaler.fit_transform(train_matrix)

    model = IsolationForest(
        n_estimators=200,
        contamination="auto",
        random_state=42,
    )
    model.fit(train_scaled)
    return scaler, model


def score_anomalies(
    feature_df: pl.DataFrame,
    baseline_df: pl.DataFrame | None = None,
) -> tuple[pl.DataFrame, StandardScaler, IsolationForest]:
    if feature_df.is_empty():
        empty = pl.DataFrame(schema={
            "mmsi": pl.Utf8,
            "cluster_label": pl.Int32,
            "baseline_noise_score": pl.Float32,
            "isolation_raw_score": pl.Float32,
            "anomaly_score": pl.Float32,
        })
        return empty, StandardScaler(), IsolationForest(random_state=42)

    if baseline_df is None:
        baseline_df = build_mpol_baseline(DEFAULT_DB_PATH)

    scaler, model = fit_isolation_forest(feature_df)
    matrix = feature_df.select(ANOMALY_FEATURE_COLUMNS).fill_null(0).to_numpy()
    scaled = scaler.transform(matrix)

    raw = -model.decision_function(scaled)
    if np.ptp(raw) == 0:
        normalized = np.full(raw.shape, 0.5, dtype=np.float32)
    else:
        normalized = ((raw - raw.min()) / np.ptp(raw)).astype(np.float32)

    joined = feature_df.select("mmsi").join(baseline_df, on="mmsi", how="left").with_columns([
        pl.col("cluster_label").fill_null(0).cast(pl.Int32),
        pl.col("baseline_noise_score").fill_null(0.0).cast(pl.Float32),
        pl.Series("isolation_raw_score", raw.astype(np.float32)),
        pl.Series("isolation_norm_score", normalized),
    ])

    result = joined.with_columns(
        (0.75 * pl.col("isolation_norm_score") + 0.25 * pl.col("baseline_noise_score"))
        .clip(0.0, 1.0)
        .alias("anomaly_score")
    ).select([
        "mmsi",
        "cluster_label",
        "baseline_noise_score",
        "isolation_raw_score",
        "anomaly_score",
    ])

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
