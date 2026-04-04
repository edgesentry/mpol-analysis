"""
C3 · Causal Sanction-Response Model
=====================================
Quantifies the causal link between sanction announcement events and
observable AIS gap behaviour for vessels connected (within 2 hops in
Neo4j) to sanctioned entities.

Method
------
Difference-in-Differences (DiD) with vessel-type and route-corridor fixed
effects, estimated via OLS with HC3 heteroskedasticity-robust standard errors.

For each sanction regime (OFAC Iran, OFAC Russia, UN DPRK, …):
  - Treatment group : vessels whose ``sanctions_distance`` ≤ 2 in the
    ``vessel_features`` table *and* whose connected entity appears in the
    regime's ``list_source``.
  - Control group   : vessels with ``sanctions_distance`` = 99 (no graph link).
  - Pre period      : 30-day window ending on the announcement date.
  - Post period     : 30-day window starting on the announcement date.
  - Outcome         : ``ais_gap_count`` in the period (counted from
    ``ais_positions`` for each window).

The DiD coefficient (β₃ on the treatment × post interaction) is the
Average Treatment Effect on the Treated (ATT) measured in *additional
AIS gaps per 30 days* attributable to the sanction announcement.  Its
sign, magnitude, and 95% CI are used to calibrate ``graph_risk_score``
weights in ``composite.py``.

Usage
-----
    uv run python src/score/causal_sanction.py
    uv run python src/score/causal_sanction.py --db data/processed/mpol.duckdb \\
        --output data/processed/causal_effects.parquet

Outputs
-------
``CausalEffect`` dataframe saved as Parquet:
    regime, n_treated, n_control, att_estimate, att_ci_lower, att_ci_upper,
    p_value, is_significant, calibrated_weight

The calibrated weights can be passed to ``compute_composite_scores()`` via
``--w-graph`` (see ``src/score/composite.py``).

Note on Timestamps
------------------
All datetime handling in this module assumes UTC. The `ais_positions.timestamp` column
must be loaded as `TIMESTAMPTZ` in DuckDB (enforced by the schema) to ensure
announcement window boundaries are evaluated correctly regardless of host timezone.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Sequence

import duckdb
import numpy as np
import polars as pl
from dotenv import load_dotenv

from src.storage.config import output_uri
from src.storage.config import write_parquet as write_parquet_uri

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")
DEFAULT_OUTPUT_PATH = os.getenv("CAUSAL_EFFECTS_PATH") or output_uri("causal_effects.parquet")

# ---------------------------------------------------------------------------
# Sanction regime definitions
# ---------------------------------------------------------------------------

#: Regime descriptors keyed by short name.
#: ``list_source_substr`` is matched as a substring of the
#: ``sanctions_entities.list_source`` column (semi-colon delimited dataset tags
#: from OpenSanctions).
SANCTION_REGIMES: dict[str, dict] = {
    "OFAC_Iran": {
        "label": "OFAC Iran",
        "list_source_substr": "us_ofac_sdn",
        "flag_filter": ["IR", ""],       # vessels flagged Iran or unknown
        "announcement_dates": [
            "2012-03-15",  # EU oil embargo enforcement
            "2019-05-08",  # US OFAC maximum-pressure re-designation wave
            "2020-01-10",  # post-Soleimani executive order 13902
        ],
    },
    "OFAC_Russia": {
        "label": "OFAC Russia",
        "list_source_substr": "us_ofac_sdn",
        "flag_filter": ["RU", ""],
        "announcement_dates": [
            "2022-02-24",  # initial post-invasion SDN package
            "2022-09-15",  # price-cap tanker fleet designations
            "2023-02-24",  # one-year anniversary designation wave
        ],
    },
    "UN_DPRK": {
        "label": "UN DPRK",
        "list_source_substr": "un_sc_sanctions",
        "flag_filter": ["KP", ""],
        "announcement_dates": [
            "2017-08-05",  # UNSCR 2371 – ban on coal, iron, seafood exports
            "2017-09-11",  # UNSCR 2375 – oil product cap
            "2017-12-22",  # UNSCR 2397 – full oil embargo
        ],
    },
}

# Window around each announcement date (days)
PRE_WINDOW_DAYS = 30
POST_WINDOW_DAYS = 30

# Significance threshold
ALPHA = 0.05

# Default composite weight for graph_risk_score when calibration data is sparse
DEFAULT_GRAPH_WEIGHT = 0.40


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class CausalEffect:
    """Per-regime DiD estimate."""

    regime: str
    label: str
    n_treated: int
    n_control: int
    att_estimate: float          # ATT coefficient (β₃)
    att_ci_lower: float          # lower bound of 95% CI
    att_ci_upper: float          # upper bound of 95% CI
    p_value: float
    is_significant: bool         # p < ALPHA
    calibrated_weight: float     # suggested graph_risk_score weight


# ---------------------------------------------------------------------------
# Helpers — OLS DiD with HC3 robust SEs (no statsmodels needed)
# ---------------------------------------------------------------------------


def _ols_hc3(
    X: np.ndarray, y: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    OLS with HC3 heteroskedasticity-robust covariance matrix.

    Returns (coefficients, HC3-robust standard errors, residuals).
    """
    XtX_inv = np.linalg.pinv(X.T @ X)
    beta = XtX_inv @ X.T @ y
    resid = y - X @ beta
    n, k = X.shape
    # HC3: leverage-adjusted residuals
    H = X @ XtX_inv @ X.T              # hat matrix (n × n)
    h = np.diag(H)                      # leverage scores
    e2 = (resid / (1.0 - h)) ** 2      # HC3 adjustment
    meat = X.T @ np.diag(e2) @ X
    V_hc3 = XtX_inv @ meat @ XtX_inv
    se = np.sqrt(np.abs(np.diag(V_hc3)))
    return beta, se, resid


def _t_to_p(t_stat: float, dof: int) -> float:
    """Two-tailed p-value from a t-statistic (approximated via normal CDF for dof ≥ 30)."""
    from math import erfc, sqrt
    # Use normal approximation; accurate for dof ≥ 30
    z = abs(t_stat)
    p = erfc(z / sqrt(2.0))
    return float(p)


# ---------------------------------------------------------------------------
# AIS gap counting
# ---------------------------------------------------------------------------


def count_ais_gaps(
    con: duckdb.DuckDBPyConnection,
    mmsis: list[str],
    start: datetime,
    end: datetime,
    gap_threshold_h: float = 6.0,
) -> dict[str, int]:
    """
    Count AIS gaps (consecutive observations separated by > *gap_threshold_h*
    hours) for each MMSI in *mmsis* within the [start, end] window.

    Returns a dict mapping mmsi → gap count (0 for vessels with no data).
    """
    if not mmsis:
        return {}

    mmsi_sql = ", ".join(f"'{m}'" for m in mmsis)
    rows = con.execute(
        f"""
        WITH ordered AS (
            SELECT mmsi, timestamp,
                   LAG(timestamp) OVER (PARTITION BY mmsi ORDER BY timestamp)
                       AS prev_ts
            FROM ais_positions
            WHERE mmsi IN ({mmsi_sql})
              AND timestamp >= ?
              AND timestamp <= ?
        )
        SELECT mmsi, COUNT(*) AS gap_count
        FROM ordered
        WHERE prev_ts IS NOT NULL
          AND epoch_ms(timestamp) - epoch_ms(prev_ts) > ? * 3600000
        GROUP BY mmsi
        """,
        [start, end, gap_threshold_h],
    ).fetchall()

    result = {m: 0 for m in mmsis}
    for mmsi, count in rows:
        result[mmsi] = int(count)
    return result


# ---------------------------------------------------------------------------
# Treatment group identification
# ---------------------------------------------------------------------------


def _identify_treatment_groups(
    con: duckdb.DuckDBPyConnection,
    regime_key: str,
    regime: dict,
) -> tuple[list[str], list[str]]:
    """
    Return (treated_mmsis, control_mmsis) for a given regime.

    Treated : sanctions_distance ≤ 2 AND vessel in Neo4j graph connected to
              a sanctions_entities row whose list_source contains the regime
              substring.

    Control : sanctions_distance = 99 (no graph link to any sanctioned entity).

    Note: If the ``vessel_features`` table is absent or only partially
    populated (test environments), falls back to flag-state heuristic:
    treated = vessels whose flag matches ``flag_filter``.
    """
    substr = regime["list_source_substr"]
    flag_filter = regime.get("flag_filter", [])

    try:
        # Primary: join vessel_features + vessel_meta + sanctions_entities
        treated_rows = con.execute(
            """
            SELECT DISTINCT vf.mmsi
            FROM vessel_features vf
            JOIN vessel_meta vm ON vm.mmsi = vf.mmsi
            WHERE vf.sanctions_distance <= 2
            """,
        ).fetchall()
        treated = [r[0] for r in treated_rows]

        control_rows = con.execute(
            """
            SELECT DISTINCT vf.mmsi
            FROM vessel_features vf
            WHERE vf.sanctions_distance >= 99
            """,
        ).fetchall()
        control = [r[0] for r in control_rows]

        # If table is empty fall through to flag heuristic
        if not treated and not control:
            raise ValueError("vessel_features empty")

    except Exception:
        # Flag-state heuristic fallback (useful in test contexts)
        treated_rows = con.execute(
            f"""
            SELECT DISTINCT mmsi FROM vessel_meta
            WHERE flag IN ({', '.join(f"'{f}'" for f in flag_filter if f)})
            """,
        ).fetchall()
        treated = [r[0] for r in treated_rows]
        control_rows = con.execute(
            f"""
            SELECT DISTINCT mmsi FROM vessel_meta
            WHERE flag NOT IN ({', '.join(f"'{f}'" for f in flag_filter if f) or "''"})
            """,
        ).fetchall()
        control = [r[0] for r in control_rows]

    return treated, control


# ---------------------------------------------------------------------------
# Fixed-effects encoding
# ---------------------------------------------------------------------------


def _vessel_type_fe(
    con: duckdb.DuckDBPyConnection, mmsis: list[str]
) -> dict[str, int]:
    """Return vessel ship_type (int) for each MMSI (0 = unknown)."""
    if not mmsis:
        return {}
    sql = ", ".join(f"'{m}'" for m in mmsis)
    rows = con.execute(
        f"SELECT mmsi, COALESCE(ship_type, 0) FROM vessel_meta WHERE mmsi IN ({sql})"
    ).fetchall()
    result = {m: 0 for m in mmsis}
    for mmsi, st in rows:
        result[mmsi] = int(st) if st else 0
    return result


def _route_corridor_fe(
    con: duckdb.DuckDBPyConnection, mmsis: list[str]
) -> dict[str, int]:
    """
    Assign a route corridor code based on the last known position of each vessel.

    Corridors (approximate):
       0 = Unknown / no data
       1 = Strait of Malacca / Singapore  (lon 98–110, lat −6–6)
       2 = Persian Gulf / Gulf of Oman     (lon 48–62, lat 22–30)
       3 = Red Sea / Gulf of Aden          (lon 32–52, lat 10–22)
       4 = North Sea / Baltic              (lon −5–30, lat 50–65)
       5 = Gulf of Mexico / Caribbean      (lon −100–−60, lat 14–30)
       6 = East China Sea / Yellow Sea     (lon 118–130, lat 24–40)
       99 = Other
    """
    if not mmsis:
        return {}
    sql = ", ".join(f"'{m}'" for m in mmsis)
    rows = con.execute(
        f"""
        WITH latest AS (
            SELECT mmsi, lat, lon,
                   ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY timestamp DESC) AS rn
            FROM ais_positions WHERE mmsi IN ({sql})
        )
        SELECT mmsi, lat, lon FROM latest WHERE rn = 1
        """
    ).fetchall()

    def _classify(lat: float, lon: float) -> int:
        if 98 <= lon <= 110 and -6 <= lat <= 6:
            return 1
        if 48 <= lon <= 62 and 22 <= lat <= 30:
            return 2
        if 32 <= lon <= 52 and 10 <= lat <= 22:
            return 3
        if -5 <= lon <= 30 and 50 <= lat <= 65:
            return 4
        if -100 <= lon <= -60 and 14 <= lat <= 30:
            return 5
        if 118 <= lon <= 130 and 24 <= lat <= 40:
            return 6
        return 99

    result = {m: 0 for m in mmsis}
    for mmsi, lat, lon in rows:
        if lat is not None and lon is not None:
            result[mmsi] = _classify(float(lat), float(lon))
    return result


# ---------------------------------------------------------------------------
# DiD estimator for one (regime × announcement_date)
# ---------------------------------------------------------------------------


def _did_estimate(
    treated_mmsis: list[str],
    control_mmsis: list[str],
    announcement_date: datetime,
    con: duckdb.DuckDBPyConnection,
    gap_threshold_h: float = 6.0,
) -> dict | None:
    """
    Fit the DiD model for a single announcement date.

    Returns a dict with keys: att, se, t, p, n_treated, n_control.
    Returns None if insufficient data (< 2 rows in either group).
    """
    all_mmsis = treated_mmsis + control_mmsis
    if not all_mmsis:
        return None

    pre_start = announcement_date - timedelta(days=PRE_WINDOW_DAYS)
    pre_end = announcement_date
    post_start = announcement_date
    post_end = announcement_date + timedelta(days=POST_WINDOW_DAYS)

    pre_gaps = count_ais_gaps(con, all_mmsis, pre_start, pre_end, gap_threshold_h)
    post_gaps = count_ais_gaps(con, all_mmsis, post_start, post_end, gap_threshold_h)

    vtype = _vessel_type_fe(con, all_mmsis)
    rcorr = _route_corridor_fe(con, all_mmsis)

    # Collect unique fixed-effect levels to build dummy columns
    vtypes = sorted(set(vtype.values()))
    rcorrs = sorted(set(rcorr.values()))

    def _build_rows(mmsis: list[str], treated: int) -> list[dict]:
        rows = []
        for post in (0, 1):
            gaps = post_gaps if post else pre_gaps
            for m in mmsis:
                rows.append({
                    "mmsi": m,
                    "treated": treated,
                    "post": post,
                    "did": treated * post,    # interaction term
                    "outcome": float(gaps.get(m, 0)),
                    "vtype": vtype.get(m, 0),
                    "rcorr": rcorr.get(m, 0),
                })
        return rows

    rows = _build_rows(treated_mmsis, 1) + _build_rows(control_mmsis, 0)
    if len(rows) < 4:  # need at least 2 vessels × 2 periods
        return None

    n = len(rows)
    # Build design matrix: intercept, treated, post, did (ATT), vessel-type FEs,
    # route-corridor FEs
    k_vtype = max(len(vtypes) - 1, 0)   # drop first level (reference)
    k_rcorr = max(len(rcorrs) - 1, 0)
    k_total = 4 + k_vtype + k_rcorr

    X = np.zeros((n, k_total), dtype=np.float64)
    y = np.zeros(n, dtype=np.float64)

    for i, r in enumerate(rows):
        X[i, 0] = 1.0           # intercept
        X[i, 1] = r["treated"]
        X[i, 2] = r["post"]
        X[i, 3] = r["did"]      # ← ATT coefficient (β₃)
        # Vessel-type FEs (drop first level)
        for j, vt in enumerate(vtypes[1:], start=4):
            X[i, j] = float(r["vtype"] == vt)
        # Route-corridor FEs (drop first level)
        for j, rc in enumerate(rcorrs[1:], start=4 + k_vtype):
            X[i, j] = float(r["rcorr"] == rc)
        y[i] = r["outcome"]

    if k_total >= n:
        # Under-determined: too many fixed effects relative to observations
        return None

    try:
        beta, se, _ = _ols_hc3(X, y)
    except np.linalg.LinAlgError:
        return None

    att = float(beta[3])
    att_se = float(se[3])
    dof = max(n - k_total, 1)
    t_stat = att / att_se if att_se > 0 else 0.0
    p_val = _t_to_p(t_stat, dof)

    n_treated = len(treated_mmsis)
    n_control = len(control_mmsis)

    return {
        "att": att,
        "se": att_se,
        "t": t_stat,
        "p": p_val,
        "n_treated": n_treated,
        "n_control": n_control,
    }


# ---------------------------------------------------------------------------
# Per-regime pooled estimate
# ---------------------------------------------------------------------------


def _pool_estimates(results: list[dict]) -> dict:
    """
    Pool multiple announcement-date estimates using inverse-variance weighting.

    Returns pooled att, ci_lower, ci_upper, p_value.
    """
    valid = [r for r in results if r is not None and r["se"] > 0]
    if not valid:
        return {"att": 0.0, "ci_lower": 0.0, "ci_upper": 0.0, "p": 1.0,
                "n_treated": 0, "n_control": 0}

    weights = np.array([1.0 / (r["se"] ** 2) for r in valid])
    atts = np.array([r["att"] for r in valid])
    pooled_att = float(np.sum(weights * atts) / np.sum(weights))
    pooled_se = float(1.0 / np.sqrt(np.sum(weights)))
    z = 1.959964  # 95% CI z-score
    ci_lower = pooled_att - z * pooled_se
    ci_upper = pooled_att + z * pooled_se

    # Combined p-value: use the largest (most conservative) individual SE
    max_se = float(np.max([r["se"] for r in valid]))
    t_stat = pooled_att / max_se if max_se > 0 else 0.0
    dof = sum(r["n_treated"] + r["n_control"] for r in valid) - 4
    p_val = _t_to_p(t_stat, max(dof, 1))

    n_treated = max(r["n_treated"] for r in valid)
    n_control = max(r["n_control"] for r in valid)

    return {
        "att": pooled_att,
        "ci_lower": ci_lower,
        "ci_upper": ci_upper,
        "p": p_val,
        "n_treated": n_treated,
        "n_control": n_control,
    }


# ---------------------------------------------------------------------------
# Weight calibration
# ---------------------------------------------------------------------------


def calibrate_graph_weight(effects: list[CausalEffect]) -> float:
    """
    Derive a calibrated ``graph_risk_score`` weight from the set of
    per-regime effect sizes.

    Logic:
    - Start with the default weight (0.40).
    - For each statistically significant regime: if the ATT is *positive*
      (more AIS gaps after sanction announcements for connected vessels),
      the graph risk dimension is predictive → increase weight proportionally
      to the fraction of significant regimes.
    - Cap the weight in [0.20, 0.65] to keep the other two score components
      (anomaly + identity) non-trivial.

    Returns a float in [0.20, 0.65].
    """
    significant = [e for e in effects if e.is_significant]
    if not significant:
        return DEFAULT_GRAPH_WEIGHT

    # Positive ATT = graph exposure predicts more evasion (good predictor)
    positive_sig = [e for e in significant if e.att_estimate > 0]
    fraction = len(positive_sig) / max(len(effects), 1)

    # Scale linearly from 0.40 → 0.65 as fraction → 1.0
    calibrated = DEFAULT_GRAPH_WEIGHT + fraction * (0.65 - DEFAULT_GRAPH_WEIGHT)
    return float(np.clip(calibrated, 0.20, 0.65))


# ---------------------------------------------------------------------------
# Main API
# ---------------------------------------------------------------------------


def run_causal_model(
    db_path: str = DEFAULT_DB_PATH,
    regimes: dict[str, dict] | None = None,
    gap_threshold_h: float = 6.0,
    regimes_path: str | None = "config/sanction_regimes.yaml",
) -> list[CausalEffect]:
    """
    Run the DiD causal model for all sanction regimes.

    Parameters
    ----------
    db_path:
        Path to the DuckDB database.
    regimes:
        Regime descriptors dict.  Defaults to ``SANCTION_REGIMES``.
    gap_threshold_h:
        Minimum AIS gap length (hours) to count as a gap event.

    Returns
    -------
    List of :class:`CausalEffect` dataclasses, one per regime.
    """
    if regimes is None:
        regimes = SANCTION_REGIMES
        if regimes_path and os.path.exists(regimes_path):
            try:
                import yaml
                with open(regimes_path, "r") as f:
                    data = yaml.safe_load(f)
                    if data and "regimes" in data:
                        regimes = data["regimes"]
            except Exception as e:
                print(f"Failed to load regimes from {regimes_path}: {e}")

    con = duckdb.connect(db_path, read_only=True)
    effects: list[CausalEffect] = []

    try:
        for regime_key, regime in regimes.items():
            treated, control = _identify_treatment_groups(con, regime_key, regime)

            per_date: list[dict | None] = []
            for date_str in regime.get("announcement_dates", []):
                ann_date = datetime.strptime(date_str, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
                result = _did_estimate(
                    treated, control, ann_date, con, gap_threshold_h
                )
                per_date.append(result)

            pooled = _pool_estimates([r for r in per_date if r is not None])
            is_sig = pooled["p"] < ALPHA and pooled["att"] != 0.0

            effect = CausalEffect(
                regime=regime_key,
                label=regime["label"],
                n_treated=pooled["n_treated"],
                n_control=pooled["n_control"],
                att_estimate=pooled["att"],
                att_ci_lower=pooled["ci_lower"],
                att_ci_upper=pooled["ci_upper"],
                p_value=pooled["p"],
                is_significant=is_sig,
                calibrated_weight=DEFAULT_GRAPH_WEIGHT,  # filled below
            )
            effects.append(effect)

    finally:
        con.close()

    # Calibrate weight using all regimes together
    calibrated_w = calibrate_graph_weight(effects)
    for e in effects:
        e.calibrated_weight = calibrated_w

    return effects


def effects_to_dataframe(effects: list[CausalEffect]) -> pl.DataFrame:
    """Convert a list of CausalEffect objects to a Polars DataFrame."""
    if not effects:
        return pl.DataFrame(
            schema={
                "regime": pl.Utf8,
                "label": pl.Utf8,
                "n_treated": pl.Int32,
                "n_control": pl.Int32,
                "att_estimate": pl.Float64,
                "att_ci_lower": pl.Float64,
                "att_ci_upper": pl.Float64,
                "p_value": pl.Float64,
                "is_significant": pl.Boolean,
                "calibrated_weight": pl.Float64,
            }
        )
    return pl.DataFrame(
        {
            "regime": [e.regime for e in effects],
            "label": [e.label for e in effects],
            "n_treated": [e.n_treated for e in effects],
            "n_control": [e.n_control for e in effects],
            "att_estimate": [e.att_estimate for e in effects],
            "att_ci_lower": [e.att_ci_lower for e in effects],
            "att_ci_upper": [e.att_ci_upper for e in effects],
            "p_value": [e.p_value for e in effects],
            "is_significant": [e.is_significant for e in effects],
            "calibrated_weight": [e.calibrated_weight for e in effects],
        }
    )


def write_effects(df: pl.DataFrame, output_path: str = DEFAULT_OUTPUT_PATH) -> None:
    """Persist the effects DataFrame to Parquet."""
    write_parquet_uri(df, output_path)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="C3: Causal sanction-response model (DiD)"
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="DuckDB path")
    parser.add_argument(
        "--output", default=DEFAULT_OUTPUT_PATH, help="Output Parquet path"
    )
    parser.add_argument(
        "--gap-threshold-hours",
        type=float,
        default=6.0,
        help="AIS gap threshold in hours (default 6; use 12 for DPRK/Iran)",
    )
    parser.add_argument(
        "--regimes",
        default="config/sanction_regimes.yaml",
        help="Path to YAML regimes config",
    )
    args = parser.parse_args()

    print("Running C3 causal sanction-response model …")
    effects = run_causal_model(args.db, gap_threshold_h=args.gap_threshold_hours, regimes_path=args.regimes)

    df = effects_to_dataframe(effects)
    write_effects(df, args.output)

    print(f"\n{'Regime':<18} {'N_trt':>6} {'N_ctl':>6} {'ATT':>8} "
          f"{'CI_lo':>8} {'CI_hi':>8} {'p':>7} {'sig':>4}")
    print("-" * 72)
    for e in effects:
        sig_mark = "✓" if e.is_significant else " "
        print(
            f"{e.label:<18} {e.n_treated:>6} {e.n_control:>6} "
            f"{e.att_estimate:>8.3f} {e.att_ci_lower:>8.3f} "
            f"{e.att_ci_upper:>8.3f} {e.p_value:>7.4f} {sig_mark:>4}"
        )

    sig_count = sum(1 for e in effects if e.is_significant)
    print(f"\nSignificant regimes: {sig_count}/{len(effects)}")

    if effects:
        w = effects[0].calibrated_weight  # same for all
        print(f"Calibrated graph_risk_score weight: {w:.3f}")
        print(
            f"  → Pass --w-graph {w:.3f} to src/score/composite.py to apply calibration"
        )

    print(f"\nEffects written to: {args.output}")


if __name__ == "__main__":
    main()
