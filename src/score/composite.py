"""Build composite confidence scores and signal explanations.

Weight calibration
------------------
The ``w_graph`` parameter (default 0.40) can be automatically calibrated using
the C3 causal sanction-response model::

    from src.score.causal_sanction import run_causal_model, calibrate_graph_weight
    effects = run_causal_model(db_path)
    w_graph = calibrate_graph_weight(effects)

See ``src/score/causal_sanction.py`` and ``docs/roadmap.md`` Phase C, C3.

Geopolitical rerouting filter (Improvement 2)
---------------------------------------------
Pass ``--geopolitical-event-filter events.json`` to down-weight the anomaly
score for vessels whose last known position falls within a declared rerouting
corridor during an active date window.  This reduces false positives caused
by legitimate commercial rerouting (e.g. Cape of Good Hope diversion since
2024 due to Houthi Red Sea attacks).

The JSON file format::

    {
      "events": [
        {
          "name": "Red Sea / Cape of Good Hope rerouting",
          "active_from": "2023-11-01",
          "active_to": "2026-12-31",
          "corridors": [
            {"lat_min": -40, "lon_min": 10, "lat_max": -25, "lon_max": 40}
          ],
          "down_weight": 0.5
        }
      ]
    }

``down_weight`` is a multiplier applied to ``anomaly_score`` for matching
vessels (e.g. 0.5 halves their anomaly contribution).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import date

import duckdb
import numpy as np
import polars as pl
from dotenv import load_dotenv

from src.score.anomaly import ANOMALY_FEATURE_COLUMNS, load_feature_frame, score_anomalies
from src.score.mpol_baseline import build_mpol_baseline
from src.storage.config import output_uri
from src.storage.config import write_parquet as write_parquet_uri

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")
DEFAULT_OUTPUT_PATH = os.getenv("COMPOSITE_SCORES_PATH") or output_uri("composite_scores.parquet")

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


# ---------------------------------------------------------------------------
# Geopolitical rerouting filter
# ---------------------------------------------------------------------------


@dataclass
class _GeoCorridorBbox:
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float

    def contains(self, lat: float, lon: float) -> bool:
        return self.lat_min <= lat <= self.lat_max and self.lon_min <= lon <= self.lon_max


@dataclass
class GeoEvent:
    """A declared geopolitical rerouting event."""
    name: str
    active_from: date
    active_to: date
    corridors: list[_GeoCorridorBbox] = field(default_factory=list)
    down_weight: float = 0.5   # multiplier on anomaly_score for affected vessels

    def is_active(self, reference_date: date | None = None) -> bool:
        ref = reference_date or date.today()
        return self.active_from <= ref <= self.active_to

    def vessel_in_corridor(self, lat: float | None, lon: float | None) -> bool:
        if lat is None or lon is None:
            return False
        return any(c.contains(lat, lon) for c in self.corridors)


def load_geopolitical_filter(json_path: str) -> list[GeoEvent]:
    """Load a geopolitical event filter from a JSON file.

    See module docstring for the expected JSON schema.
    """
    with open(json_path) as fh:
        data = json.load(fh)

    events: list[GeoEvent] = []
    for ev in data.get("events", []):
        corridors = [
            _GeoCorridorBbox(
                lat_min=c["lat_min"],
                lat_max=c["lat_max"],
                lon_min=c["lon_min"],
                lon_max=c["lon_max"],
            )
            for c in ev.get("corridors", [])
        ]
        events.append(GeoEvent(
            name=ev["name"],
            active_from=date.fromisoformat(ev["active_from"]),
            active_to=date.fromisoformat(ev["active_to"]),
            corridors=corridors,
            down_weight=float(ev.get("down_weight", 0.5)),
        ))
    return events


def apply_geopolitical_filter(
    scored_df: pl.DataFrame,
    events: list[GeoEvent],
    reference_date: date | None = None,
) -> pl.DataFrame:
    """Down-weight ``anomaly_score`` for vessels in active rerouting corridors.

    Vessels whose last known position falls within an active corridor have their
    ``anomaly_score`` multiplied by ``event.down_weight`` before the composite
    ``confidence`` is recalculated.  ``last_lat`` and ``last_lon`` must be
    present in *scored_df*.
    """
    active = [e for e in events if e.is_active(reference_date)]
    if not active:
        return scored_df

    anomaly_scores = scored_df["anomaly_score"].to_list()
    lats = scored_df["last_lat"].to_list()
    lons = scored_df["last_lon"].to_list()

    for i, (lat, lon) in enumerate(zip(lats, lons)):
        for ev in active:
            if ev.vessel_in_corridor(lat, lon):
                anomaly_scores[i] = float(anomaly_scores[i]) * ev.down_weight
                break  # apply the most relevant active event only

    return scored_df.with_columns(
        pl.Series("anomaly_score", anomaly_scores, dtype=pl.Float32)
    )


# ITU Maritime Identification Digits (MID) → ISO 3166-1 alpha-2 country code.
# Keys are the first three digits of the MMSI string.  Only covers MIDs
# relevant to shadow-fleet regions; unknown prefixes return empty string.
_MID_TO_FLAG: dict[str, str] = {
    # North Asia
    "412": "CN", "413": "CN", "414": "CN",
    "431": "JP", "432": "JP", "433": "JP",
    "440": "KR", "441": "KR",
    "445": "KP",
    "477": "HK",
    # South-East Asia
    "525": "ID",
    "533": "MY",
    "563": "SG", "564": "SG", "565": "SG", "566": "SG", "567": "SG",
    "574": "VN", "576": "VN",
    "567": "SG",
    # South Asia
    "419": "IN",
    "403": "SA",
    # Middle East / Gulf
    "422": "IR",
    "447": "KW",
    "466": "QA",
    "470": "AE", "471": "AE",
    # Russia / CIS
    "273": "RU",
    "272": "UA",
    # Europe
    "209": "CY", "210": "CY",
    "232": "GB", "233": "GB", "234": "GB", "235": "GB",
    "237": "GR", "239": "GR", "240": "GR", "241": "GR",
    "248": "MT", "249": "MT",
    "257": "NO", "258": "NO", "259": "NO",
    "271": "TR",
    # Flag-of-convenience / open registries
    "308": "BS", "309": "BS", "310": "BS", "311": "BS",
    "351": "PA", "352": "PA", "353": "PA", "354": "PA",
    "355": "PA", "356": "PA", "357": "PA",
    "370": "PA", "371": "PA", "372": "PA", "373": "PA", "374": "PA",
    "538": "MH",
    "636": "LR",
    "667": "SL",
    # Africa
    "613": "CM",
    # Americas
    "303": "US", "338": "US", "366": "US", "367": "US", "368": "US", "369": "US",
    "305": "AG",
    "710": "BR",
    "725": "CL",
    "730": "CO",
    "734": "VE",
    # Additional open registries / flag-of-convenience
    "205": "BE",
    "209": "CY", "210": "CY",
    "212": "CY",
    "219": "DK",
    "253": "IE",
    "256": "MT",
    "312": "BZ",
    "416": "TW",
    "518": "CK",
    "548": "PH",
}


def _mmsi_to_flag(mmsi: str) -> str:
    """Return ISO 2-letter flag code derived from the MMSI MID prefix, or ''."""
    if not mmsi or len(mmsi) < 3:
        return ""
    return _MID_TO_FLAG.get(mmsi[:3], "")


def load_watchlist_context(db_path: str = DEFAULT_DB_PATH) -> pl.DataFrame:
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute(
            """
            WITH latest_positions AS (
                SELECT
                    mmsi,
                    lat AS last_lat,
                    lon AS last_lon,
                    timestamp AS last_seen,
                    ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY timestamp DESC) AS rn
                FROM ais_positions
            ),
            ais_meta AS (
                SELECT
                    mmsi,
                    mode(ship_type) FILTER (WHERE ship_type IS NOT NULL AND ship_type > 0)
                        AS ais_ship_type
                FROM ais_positions
                GROUP BY mmsi
            )
            SELECT
                vf.*,
                COALESCE(vm.imo, '') AS imo,
                COALESCE(vm.name, vf.mmsi) AS vessel_name,
                COALESCE(vm.ship_type, am.ais_ship_type, 0) AS ship_type,
                COALESCE(NULLIF(vm.flag, ''), '') AS flag,
                lp.last_lat,
                lp.last_lon,
                lp.last_seen
            FROM vessel_features vf
            LEFT JOIN vessel_meta vm ON vm.mmsi = vf.mmsi
            LEFT JOIN latest_positions lp ON lp.mmsi = vf.mmsi AND lp.rn = 1
            LEFT JOIN ais_meta am ON am.mmsi = vf.mmsi
            ORDER BY vf.mmsi
            """
        ).pl()
    finally:
        con.close()

    # Fallback: derive flag from MMSI prefix (ITU MID) when registry data is absent
    return df.with_columns(
        pl.struct(["mmsi", "flag"]).map_elements(
            lambda r: r["flag"] if r["flag"] else _mmsi_to_flag(r["mmsi"]),
            return_dtype=pl.Utf8,
        ).alias("flag")
    )


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


def compute_composite_scores(
    db_path: str = DEFAULT_DB_PATH,
    w_anomaly: float = 0.4,
    w_graph: float = 0.4,
    w_identity: float = 0.2,
    geo_filter_path: str | None = None,
    auto_calibrate: bool = False,
) -> pl.DataFrame:
    if auto_calibrate:
        print("Auto-calibrating graph risk weight using C3 causal model...")
        try:
            from src.score.causal_sanction import run_causal_model, calibrate_graph_weight
            effects = run_causal_model(db_path)
            w_graph = calibrate_graph_weight(effects)
            print(f"C3 auto-calibrated graph_risk_score weight: {w_graph:.3f}")
        except Exception as e:
            print(f"Warning: Auto-calibration failed ({e}). Falling back to w_graph={w_graph}")

    feature_df = load_feature_frame(db_path)
    context_df = load_watchlist_context(db_path)
    if feature_df.is_empty() or context_df.is_empty():
        return pl.DataFrame()

    baseline_df = build_mpol_baseline(db_path)
    anomaly_df, scaler, model = score_anomalies(feature_df, baseline_df, db_path)
    scaled = scaler.transform(feature_df.select(ANOMALY_FEATURE_COLUMNS).fill_null(0).to_numpy())
    top_signals = _compute_top_signals(feature_df, model, scaled)

    scored = context_df.join(anomaly_df, on="mmsi", how="left").with_columns([
        _compute_graph_risk(context_df),
        _compute_identity_score(context_df),
        top_signals,
    ])

    # Apply geopolitical rerouting filter before computing confidence
    if geo_filter_path:
        geo_events = load_geopolitical_filter(geo_filter_path)
        scored = apply_geopolitical_filter(scored, geo_events)

    scored = scored.with_columns([
        (w_anomaly * pl.col("anomaly_score") + w_graph * pl.col("graph_risk_score") + w_identity * pl.col("identity_score"))
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
    write_parquet_uri(df, output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute composite watchlist scores")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--w-anomaly", type=float, default=0.4,
                        help="Weight for anomaly score (default: 0.4)")
    parser.add_argument("--w-graph", type=float, default=0.4,
                        help="Weight for graph risk score (default: 0.4)")
    parser.add_argument("--w-identity", type=float, default=0.2,
                        help="Weight for identity score (default: 0.2)")
    parser.add_argument("--auto-calibrate", action="store_true",
                        help="Auto-calibrate graph risk weight using C3 causal model")
    parser.add_argument(
        "--geopolitical-event-filter",
        default=None,
        metavar="PATH",
        help=(
            "Path to a JSON file declaring geopolitical rerouting events. "
            "Vessels in active corridors have their anomaly_score down-weighted "
            "to reduce false positives from legitimate commercial rerouting."
        ),
    )
    args = parser.parse_args()

    w_graph = args.w_graph
    auto_calibrate = args.auto_calibrate
    if auto_calibrate and "--w-graph" in sys.argv:
        print("Warning: Both --auto-calibrate and --w-graph are specified. Preferring --w-graph.")
        auto_calibrate = False

    df = compute_composite_scores(
        args.db,
        args.w_anomaly,
        w_graph,
        args.w_identity,
        geo_filter_path=args.geopolitical_event_filter,
        auto_calibrate=auto_calibrate,
    )
    write_composite_scores(df, args.output)
    print(f"Composite rows written: {df.height}")


if __name__ == "__main__":
    main()
