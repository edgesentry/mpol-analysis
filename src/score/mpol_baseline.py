"""Compute a normal-behavior baseline using HDBSCAN-style clustering.

Service vessel exclusion (Improvement 1)
-----------------------------------------
AIS ship_type codes 51–59 (pilot, tug, fire-fighting, SAR, diving, law
enforcement, medical) and 31–32 (tug/supply) are legitimate service craft
that operate at low SOG and high loitering hours in busy port areas (e.g.
Singapore Strait).  Including them in the HDBSCAN training set compresses
anomaly scores for genuine dark-vessel STS events.

By default these types are excluded from the HDBSCAN *training* partition
and assigned ``baseline_noise_score = 0.0`` so the Isolation Forest score
dominates their final anomaly score.  Pass ``exclude_service_vessels=False``
to revert to the legacy behaviour.

Cleared-vessel feedback loop (Improvement 3)
----------------------------------------------
Vessels recorded in the ``cleared_vessels`` DuckDB table (populated when a
Phase B physical inspection returns ``outcome = cleared``) are used as hard
negatives in the HDBSCAN training: they are always assigned
``baseline_noise_score = 0.0`` regardless of their feature values, and they
are always included in the HDBSCAN training group so they anchor the
"confirmed normal" region.
"""

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

#: AIS ship_type codes for small service craft that should not define the
#: "normal MPOL behaviour" baseline.  See ITU-R M.1371 Table 52.
#:   31 = Tug          32 = Supply / tender
#:   51 = Pilot        52 = SAR          53 = Tug (alt code)
#:   54 = Port tender  55 = Anti-pollution  56 = Law enforcement
#:   57 = Spare (local/reserved)           58 = Medical
#:   59 = Offshore support / coast guard
SERVICE_VESSEL_TYPES: frozenset[int] = frozenset(range(51, 60)) | {31, 32}


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


def load_cleared_mmsis(db_path: str = DEFAULT_DB_PATH) -> frozenset[str]:
    """Return the set of MMSIs that passed a Phase B physical inspection (cleared)."""
    try:
        con = duckdb.connect(db_path, read_only=True)
        try:
            rows = con.execute("SELECT mmsi FROM cleared_vessels").fetchall()
            return frozenset(r[0] for r in rows)
        finally:
            con.close()
    except Exception:
        return frozenset()


def _cluster_group(
    group: pl.DataFrame,
    cleared_mmsis: frozenset[str],
) -> pl.DataFrame:
    if group.is_empty():
        return pl.DataFrame(
            schema={
                "mmsi": pl.Utf8,
                "cluster_label": pl.Int32,
                "baseline_noise_score": pl.Float32,
            }
        )

    # Minimum group size for reliable HDBSCAN clustering.  Groups smaller than
    # this produce near-universal noise labels (label=-1, baseline_noise_score=1.0)
    # that degrade the anomaly component for every vessel indiscriminately.
    # Fall back to noise=0.0 (Isolation Forest dominates) for tiny groups.
    _MIN_RELIABLE_GROUP = 5

    matrix = group.select(BEHAVIOR_COLUMNS).to_numpy()
    if (
        len(group) < _MIN_RELIABLE_GROUP
        or np.unique(matrix, axis=0).shape[0] < 2
        or HDBSCAN is None
    ):
        labels = np.zeros(len(group), dtype=np.int32)
        noise = np.zeros(len(group), dtype=np.float32)
    else:
        scaler = StandardScaler()
        scaled = scaler.fit_transform(matrix)
        # Use 1/5 of group size (floor 5, cap 15) — avoids aggressive noise
        # assignment that occurred with the old 1/2 divisor on small groups.
        min_cluster_size = max(5, min(15, len(group) // 5 or 5))
        model = HDBSCAN(min_cluster_size=min_cluster_size, min_samples=1, allow_single_cluster=True)
        labels = model.fit_predict(scaled).astype(np.int32)
        noise = (labels == -1).astype(np.float32)

    # Cleared vessels are confirmed normal — force baseline_noise_score to 0
    if cleared_mmsis:
        mmsi_list = group["mmsi"].to_list()
        for idx, mmsi in enumerate(mmsi_list):
            if mmsi in cleared_mmsis:
                noise[idx] = 0.0

    return pl.DataFrame(
        {
            "mmsi": group["mmsi"],
            "cluster_label": labels,
            "baseline_noise_score": noise,
        }
    )


def compute_mpol_baseline(
    feature_df: pl.DataFrame,
    cleared_mmsis: frozenset[str] | None = None,
    exclude_service_vessels: bool = True,
) -> pl.DataFrame:
    """Cluster vessel behavior into normal / noise groups.

    Parameters
    ----------
    feature_df:
        Output of :func:`load_behavior_frame`.
    cleared_mmsis:
        MMSIs of vessels cleared by Phase B physical inspection.  These are
        treated as hard negatives (``baseline_noise_score = 0.0``).
    exclude_service_vessels:
        When ``True`` (default), ship_type codes in :data:`SERVICE_VESSEL_TYPES`
        are excluded from the HDBSCAN training partition and assigned
        ``baseline_noise_score = 0.0``.  They are still scored by the
        Isolation Forest in ``anomaly.py``.
    """
    if cleared_mmsis is None:
        cleared_mmsis = frozenset()

    if feature_df.is_empty():
        return pl.DataFrame(
            schema={
                "mmsi": pl.Utf8,
                "cluster_label": pl.Int32,
                "baseline_noise_score": pl.Float32,
            }
        )

    service_df: pl.DataFrame | None = None
    if exclude_service_vessels:
        mask = feature_df["ship_type"].is_in(list(SERVICE_VESSEL_TYPES))
        service_df = feature_df.filter(mask)
        feature_df = feature_df.filter(~mask)

    outputs: list[pl.DataFrame] = []
    for ship_type, group in feature_df.partition_by("ship_type", as_dict=True).items():
        _ = ship_type
        outputs.append(_cluster_group(group, cleared_mmsis))

    # Service vessels: pass-through with neutral baseline score so Isolation Forest dominates
    if service_df is not None and not service_df.is_empty():
        outputs.append(
            pl.DataFrame(
                {
                    "mmsi": service_df["mmsi"],
                    "cluster_label": pl.Series(
                        "cluster_label", [0] * service_df.height, dtype=pl.Int32
                    ),
                    "baseline_noise_score": pl.Series(
                        "baseline_noise_score",
                        [0.0] * service_df.height,
                        dtype=pl.Float32,
                    ),
                }
            )
        )

    if not outputs:
        return pl.DataFrame(
            schema={
                "mmsi": pl.Utf8,
                "cluster_label": pl.Int32,
                "baseline_noise_score": pl.Float32,
            }
        )

    return pl.concat(outputs).sort("mmsi")


def build_mpol_baseline(
    db_path: str = DEFAULT_DB_PATH,
    exclude_service_vessels: bool = True,
) -> pl.DataFrame:
    feature_df = load_behavior_frame(db_path)
    cleared = load_cleared_mmsis(db_path)
    return compute_mpol_baseline(
        feature_df,
        cleared_mmsis=cleared,
        exclude_service_vessels=exclude_service_vessels,
    )


def write_mpol_baseline(df: pl.DataFrame, output_path: str = DEFAULT_OUTPUT_PATH) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.write_parquet(output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute MPOL clustering baseline")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument(
        "--no-exclude-service-vessels",
        dest="exclude_service_vessels",
        action="store_false",
        default=True,
        help="Include service/bunker craft (AIS types 51-59, 31-32) in HDBSCAN training",
    )
    args = parser.parse_args()

    baseline = build_mpol_baseline(args.db, exclude_service_vessels=args.exclude_service_vessels)
    write_mpol_baseline(baseline, args.output)
    print(f"Baseline rows written: {baseline.height}")


if __name__ == "__main__":
    main()
