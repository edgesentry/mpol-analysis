"""Compute a normal-behavior baseline using HDBSCAN-style clustering."""

from __future__ import annotations

import argparse
import os

import duckdb
import numpy as np
import polars as pl
from dotenv import load_dotenv
from sklearn.preprocessing import StandardScaler

try:
    from sklearn.cluster import HDBSCAN
except ImportError:  # pragma: no cover - fallback path for older sklearn builds
    HDBSCAN = None

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")
DEFAULT_OUTPUT_PATH = os.getenv("MPOL_BASELINE_PATH", "data/processed/mpol_baseline.parquet")

BEHAVIOR_COLUMNS = [
    "ais_gap_count_30d",
    "ais_gap_max_hours",
    "position_jump_count",
    "sts_candidate_count",
    "port_call_ratio",
    "loitering_hours_30d",
]


def load_behavior_frame(db_path: str = DEFAULT_DB_PATH) -> pl.DataFrame:
    con = duckdb.connect(db_path, read_only=True)
    try:
        return con.execute(
            """
            SELECT
                vf.mmsi,
                COALESCE(vm.ship_type, 0) AS ship_type,
                COALESCE(vf.ais_gap_count_30d, 0) AS ais_gap_count_30d,
                COALESCE(vf.ais_gap_max_hours, 0.0) AS ais_gap_max_hours,
                COALESCE(vf.position_jump_count, 0) AS position_jump_count,
                COALESCE(vf.sts_candidate_count, 0) AS sts_candidate_count,
                COALESCE(vf.port_call_ratio, 0.5) AS port_call_ratio,
                COALESCE(vf.loitering_hours_30d, 0.0) AS loitering_hours_30d
            FROM vessel_features vf
            LEFT JOIN vessel_meta vm ON vm.mmsi = vf.mmsi
            ORDER BY vf.mmsi
            """
        ).pl()
    finally:
        con.close()


def _cluster_group(group: pl.DataFrame) -> pl.DataFrame:
    if group.is_empty():
        return pl.DataFrame(schema={
            "mmsi": pl.Utf8,
            "cluster_label": pl.Int32,
            "baseline_noise_score": pl.Float32,
        })

    matrix = group.select(BEHAVIOR_COLUMNS).to_numpy()
    if len(group) < 3 or np.unique(matrix, axis=0).shape[0] < 2 or HDBSCAN is None:
        labels = np.zeros(len(group), dtype=np.int32)
        noise = np.zeros(len(group), dtype=np.float32)
    else:
        scaler = StandardScaler()
        scaled = scaler.fit_transform(matrix)
        min_cluster_size = max(2, min(10, len(group) // 2 or 2))
        model = HDBSCAN(min_cluster_size=min_cluster_size, min_samples=1, allow_single_cluster=True)
        labels = model.fit_predict(scaled).astype(np.int32)
        noise = (labels == -1).astype(np.float32)

    return pl.DataFrame({
        "mmsi": group["mmsi"],
        "cluster_label": labels,
        "baseline_noise_score": noise,
    })


def compute_mpol_baseline(feature_df: pl.DataFrame) -> pl.DataFrame:
    if feature_df.is_empty():
        return pl.DataFrame(schema={
            "mmsi": pl.Utf8,
            "cluster_label": pl.Int32,
            "baseline_noise_score": pl.Float32,
        })

    outputs: list[pl.DataFrame] = []
    for ship_type, group in feature_df.partition_by("ship_type", as_dict=True).items():
        _ = ship_type
        outputs.append(_cluster_group(group))

    return pl.concat(outputs).sort("mmsi")


def build_mpol_baseline(db_path: str = DEFAULT_DB_PATH) -> pl.DataFrame:
    return compute_mpol_baseline(load_behavior_frame(db_path))


def write_mpol_baseline(df: pl.DataFrame, output_path: str = DEFAULT_OUTPUT_PATH) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.write_parquet(output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute MPOL clustering baseline")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    baseline = build_mpol_baseline(args.db)
    write_mpol_baseline(baseline, args.output)
    print(f"Baseline rows written: {baseline.height}")


if __name__ == "__main__":
    main()
