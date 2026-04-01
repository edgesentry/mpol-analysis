"""Build composite confidence scores and signal explanations."""

from __future__ import annotations

import argparse
import json
import os

import duckdb
import numpy as np
import polars as pl
from dotenv import load_dotenv

from src.score.anomaly import ANOMALY_FEATURE_COLUMNS, load_feature_frame, score_anomalies
from src.score.mpol_baseline import build_mpol_baseline

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")
DEFAULT_OUTPUT_PATH = os.getenv("COMPOSITE_SCORES_PATH", "data/processed/composite_scores.parquet")

FEATURE_VALUE_COLUMNS = [
    "ais_gap_count_30d",
    "ais_gap_max_hours",
    "position_jump_count",
    "sts_candidate_count",
    "flag_changes_2y",
    "name_changes_2y",
    "owner_changes_2y",
    "sanctions_distance",
    "shared_address_centrality",
    "sts_hub_degree",
]


def load_watchlist_context(db_path: str = DEFAULT_DB_PATH) -> pl.DataFrame:
    con = duckdb.connect(db_path, read_only=True)
    try:
        return con.execute(
            """
            WITH latest_positions AS (
                SELECT
                    mmsi,
                    lat AS last_lat,
                    lon AS last_lon,
                    timestamp AS last_seen,
                    ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY timestamp DESC) AS rn
                FROM ais_positions
            )
            SELECT
                vf.*, 
                COALESCE(vm.imo, '') AS imo,
                COALESCE(vm.name, vf.mmsi) AS vessel_name,
                COALESCE(vm.ship_type, 0) AS ship_type,
                COALESCE(vm.flag, '') AS flag,
                lp.last_lat,
                lp.last_lon,
                lp.last_seen
            FROM vessel_features vf
            LEFT JOIN vessel_meta vm ON vm.mmsi = vf.mmsi
            LEFT JOIN latest_positions lp ON lp.mmsi = vf.mmsi AND lp.rn = 1
            ORDER BY vf.mmsi
            """
        ).pl()
    finally:
        con.close()


def _ship_type_label(ship_type: int) -> str:
    if 80 <= ship_type <= 89:
        return "Tanker"
    if 70 <= ship_type <= 79:
        return "Cargo"
    if 60 <= ship_type <= 69:
        return "Passenger"
    if ship_type == 0:
        return "Unknown"
    return f"Type {ship_type}"


def _normalize_series(expr: pl.Expr, cap: float) -> pl.Expr:
    return (expr.cast(pl.Float32) / cap).clip(0.0, 1.0)


def _compute_graph_risk(df: pl.DataFrame) -> pl.Series:
    sanctions_component = np.where(
        df["sanctions_distance"].to_numpy() >= 99,
        0.0,
        np.clip(1.0 - (df["sanctions_distance"].to_numpy() / 5.0), 0.0, 1.0),
    )
    manager_component = np.where(
        df["shared_manager_risk"].to_numpy() >= 99,
        0.0,
        np.clip(1.0 - (df["shared_manager_risk"].to_numpy() / 5.0), 0.0, 1.0),
    )
    cluster_component = np.clip(df["cluster_sanctions_ratio"].to_numpy(), 0.0, 1.0)
    score = 0.6 * sanctions_component + 0.3 * cluster_component + 0.1 * manager_component
    return pl.Series("graph_risk_score", score.astype(np.float32))


def _compute_identity_score(df: pl.DataFrame) -> pl.Series:
    score = (
        0.30 * np.clip(df["flag_changes_2y"].to_numpy() / 5.0, 0.0, 1.0)
        + 0.25 * np.clip(df["name_changes_2y"].to_numpy() / 5.0, 0.0, 1.0)
        + 0.20 * np.clip(df["owner_changes_2y"].to_numpy() / 5.0, 0.0, 1.0)
        + 0.15 * np.clip(df["high_risk_flag_ratio"].to_numpy(), 0.0, 1.0)
        + 0.10 * np.clip(df["ownership_depth"].to_numpy() / 6.0, 0.0, 1.0)
    )
    return pl.Series("identity_score", score.astype(np.float32))


def _top_signals_fallback(feature_df: pl.DataFrame) -> list[str]:
    rows: list[str] = []
    for row in feature_df.iter_rows(named=True):
        candidates = []
        for feature in FEATURE_VALUE_COLUMNS:
            value = row.get(feature)
            if value is None:
                continue
            magnitude = abs(float(value))
            candidates.append((feature, value, magnitude))
        candidates.sort(key=lambda item: item[2], reverse=True)
        payload = [
            {
                "feature": feature,
                "value": value,
                "contribution": round(magnitude, 3),
            }
            for feature, value, magnitude in candidates[:3]
        ]
        rows.append(json.dumps(payload))
    return rows


def _compute_top_signals(feature_df: pl.DataFrame, model, scaled_matrix: np.ndarray) -> pl.Series:
    try:
        import shap

        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(scaled_matrix)
        values = np.asarray(shap_values)
        if values.ndim == 3:
            values = values[0]
        if values.shape[0] != feature_df.height:
            raise ValueError("Unexpected SHAP output shape")

        rows = []
        for idx, row in enumerate(feature_df.iter_rows(named=True)):
            contributions = []
            shap_row = values[idx]
            denom = float(np.abs(shap_row).sum()) or 1.0
            for col_idx, feature in enumerate(ANOMALY_FEATURE_COLUMNS):
                contributions.append(
                    {
                        "feature": feature,
                        "value": row.get(feature),
                        "contribution": round(abs(float(shap_row[col_idx])) / denom, 3),
                    }
                )
            contributions.sort(key=lambda item: item["contribution"], reverse=True)
            rows.append(json.dumps(contributions[:3]))
        return pl.Series("top_signals", rows)
    except Exception:
        return pl.Series("top_signals", _top_signals_fallback(feature_df))


def compute_composite_scores(db_path: str = DEFAULT_DB_PATH) -> pl.DataFrame:
    feature_df = load_feature_frame(db_path)
    context_df = load_watchlist_context(db_path)
    if feature_df.is_empty() or context_df.is_empty():
        return pl.DataFrame()

    baseline_df = build_mpol_baseline(db_path)
    anomaly_df, scaler, model = score_anomalies(feature_df, baseline_df)
    scaled = scaler.transform(feature_df.select(ANOMALY_FEATURE_COLUMNS).fill_null(0).to_numpy())
    top_signals = _compute_top_signals(feature_df, model, scaled)

    scored = context_df.join(anomaly_df, on="mmsi", how="left").with_columns([
        _compute_graph_risk(context_df),
        _compute_identity_score(context_df),
        top_signals,
    ])

    scored = scored.with_columns([
        (0.4 * pl.col("anomaly_score") + 0.4 * pl.col("graph_risk_score") + 0.2 * pl.col("identity_score"))
        .clip(0.0, 1.0)
        .alias("confidence"),
        pl.col("ship_type").map_elements(_ship_type_label, return_dtype=pl.Utf8).alias("vessel_type"),
    ])

    return scored.select([
        "mmsi",
        "imo",
        "vessel_name",
        "vessel_type",
        "flag",
        "confidence",
        "anomaly_score",
        "graph_risk_score",
        "identity_score",
        "top_signals",
        "last_lat",
        "last_lon",
        "last_seen",
        "ais_gap_count_30d",
        "ais_gap_max_hours",
        "position_jump_count",
        "sts_candidate_count",
        "flag_changes_2y",
        "name_changes_2y",
        "owner_changes_2y",
        "sanctions_distance",
        "shared_address_centrality",
        "sts_hub_degree",
        "cluster_label",
        "baseline_noise_score",
    ]).sort("confidence", descending=True)


def write_composite_scores(df: pl.DataFrame, output_path: str = DEFAULT_OUTPUT_PATH) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.write_parquet(output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute composite watchlist scores")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    df = compute_composite_scores(args.db)
    write_composite_scores(df, args.output)
    print(f"Composite rows written: {df.height}")


if __name__ == "__main__":
    main()
