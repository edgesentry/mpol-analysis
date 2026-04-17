"""
Tests for C3: Causal Sanction-Response Model.

All tests run against a temporary in-memory DuckDB instance seeded with
synthetic AIS and vessel data, so they never touch the production database
and require no network access.
"""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import numpy as np
import polars as pl

from pipeline.src.score.causal_sanction import (
    SANCTION_REGIMES,
    CausalEffect,
    _did_estimate,
    _ols_hc3,
    _pool_estimates,
    calibrate_graph_weight,
    count_ais_gaps,
    effects_to_dataframe,
    run_causal_model,
    write_effects,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_causal_data(db_path: str) -> None:
    """
    Seed a test DuckDB with enough data to exercise the DiD pipeline.

    Vessels:
        'TR1' – 'TR4' : treated (sanctions_distance ≤ 2, flag IR)
        'CT1' – 'CT4' : control  (sanctions_distance = 99, flag SG)

    AIS data spans 2019-01-01 – 2019-07-31 to straddle the OFAC Iran
    announcement date 2019-05-08.  Treated vessels receive 10 extra gaps
    in the post period; control vessels have a flat 2 gaps in both periods.
    """
    con = duckdb.connect(db_path)
    try:
        # Vessel meta
        con.execute("""
            INSERT OR IGNORE INTO vessel_meta (mmsi, imo, name, flag, ship_type)
            VALUES
                ('TR1', 'IMO_TR1', 'TREATED_1', 'IR', 82),
                ('TR2', 'IMO_TR2', 'TREATED_2', 'IR', 82),
                ('TR3', 'IMO_TR3', 'TREATED_3', 'IR', 82),
                ('TR4', 'IMO_TR4', 'TREATED_4', 'IR', 80),
                ('CT1', 'IMO_CT1', 'CONTROL_1', 'SG', 82),
                ('CT2', 'IMO_CT2', 'CONTROL_2', 'SG', 82),
                ('CT3', 'IMO_CT3', 'CONTROL_3', 'SG', 82),
                ('CT4', 'IMO_CT4', 'CONTROL_4', 'SG', 82)
        """)

        # Vessel features
        con.execute("""
            INSERT OR IGNORE INTO vessel_features (
                mmsi, ais_gap_count_30d, ais_gap_max_hours, position_jump_count,
                sts_candidate_count, port_call_ratio, loitering_hours_30d,
                flag_changes_2y, name_changes_2y, owner_changes_2y,
                high_risk_flag_ratio, ownership_depth, sanctions_distance,
                cluster_sanctions_ratio, shared_manager_risk,
                shared_address_centrality, sts_hub_degree,
                route_cargo_mismatch, declared_vs_estimated_cargo_value
            )
            VALUES
                ('TR1', 10, 14.0, 2, 2, 0.1, 20.0, 2, 1, 1, 0.9, 3, 1, 0.5, 1, 3, 3, 1.0, 50000.0),
                ('TR2', 8,  12.0, 1, 1, 0.1, 18.0, 1, 1, 1, 0.8, 3, 2, 0.4, 2, 2, 2, 1.0, 40000.0),
                ('TR3', 9,  13.0, 2, 2, 0.1, 15.0, 2, 2, 1, 0.7, 2, 1, 0.6, 1, 2, 2, 1.0, 60000.0),
                ('TR4', 7,  11.0, 1, 1, 0.2, 10.0, 1, 1, 0, 0.8, 2, 2, 0.5, 2, 1, 1, 1.0, 30000.0),
                ('CT1', 1,  2.0,  0, 0, 0.9, 1.0,  0, 0, 0, 0.0, 1, 99, 0.0, 99, 0, 0, 0.0, 0.0),
                ('CT2', 1,  1.5,  0, 0, 0.9, 1.0,  0, 0, 0, 0.0, 1, 99, 0.0, 99, 0, 0, 0.0, 0.0),
                ('CT3', 2,  2.5,  0, 0, 0.8, 2.0,  0, 0, 0, 0.0, 1, 99, 0.0, 99, 0, 0, 0.0, 0.0),
                ('CT4', 1,  1.0,  0, 0, 0.9, 0.5,  0, 0, 0, 0.0, 1, 99, 0.0, 99, 0, 0, 0.0, 0.0)
        """)

        # AIS positions — generate realistic gap patterns
        # Base timestamps for control vessels (2 gaps each period, uniform)
        # for treated vessels: 2 gaps pre, 12 gaps post (large treatment effect)

        _pre_ann = datetime(2019, 4, 8, tzinfo=UTC)  # day before pre-window end
        _post_ann = datetime(2019, 5, 9, tzinfo=UTC)  # day after announcement

        def _insert_positions(mmsi: str, base: datetime, gap_hours: float, n_obs: int):
            """Insert n_obs observations with regular gap_hours spacing."""
            ts = base
            from datetime import timedelta

            for _ in range(n_obs):
                con.execute(
                    """
                    INSERT OR IGNORE INTO ais_positions
                        (mmsi, timestamp, lat, lon, sog, nav_status, ship_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [mmsi, ts.isoformat(), 26.5, 55.5, 0.5, 1, 82],
                )
                ts = ts + timedelta(hours=gap_hours)

        # Pre-period: all vessels, 2 gaps each (gap = 8h so each counts once)
        for mmsi in ["TR1", "TR2", "TR3", "TR4", "CT1", "CT2", "CT3", "CT4"]:
            _insert_positions(mmsi, datetime(2019, 4, 8, tzinfo=UTC), 8.0, 3)

        # Post-period: treated get 12 gaps, control get 2 gaps
        for mmsi in ["TR1", "TR2", "TR3", "TR4"]:
            _insert_positions(mmsi, datetime(2019, 5, 8, tzinfo=UTC), 7.0, 13)
        for mmsi in ["CT1", "CT2", "CT3", "CT4"]:
            _insert_positions(mmsi, datetime(2019, 5, 8, tzinfo=UTC), 8.0, 3)

    finally:
        con.close()


# ---------------------------------------------------------------------------
# Unit tests — OLS / HC3 math
# ---------------------------------------------------------------------------


class TestOlsHc3:
    def test_known_coefficients(self):
        """OLS should recover exact coefficients on noise-free data."""
        rng = np.random.default_rng(0)
        n = 50
        X = np.column_stack([np.ones(n), rng.standard_normal((n, 2))])
        true_beta = np.array([1.0, 2.0, -0.5])
        y = X @ true_beta
        beta, se, resid = _ols_hc3(X, y)
        np.testing.assert_allclose(beta, true_beta, atol=1e-8)

    def test_se_positive(self):
        """Standard errors should be non-negative."""
        rng = np.random.default_rng(42)
        n = 30
        X = np.column_stack([np.ones(n), rng.standard_normal((n, 2))])
        y = rng.standard_normal(n)
        _, se, _ = _ols_hc3(X, y)
        assert np.all(se >= 0)

    def test_residuals_sum_near_zero(self):
        """Residuals of OLS with intercept should sum to near zero."""
        rng = np.random.default_rng(7)
        n = 40
        X = np.column_stack([np.ones(n), rng.standard_normal(n)])
        y = rng.standard_normal(n)
        _, _, resid = _ols_hc3(X, y)
        assert abs(resid.sum()) < 1e-8


# ---------------------------------------------------------------------------
# Unit tests — AIS gap counting
# ---------------------------------------------------------------------------


def test_count_ais_gaps_empty(tmp_db):
    """Gap counter should return zeros for MMSIs with no AIS data."""
    result = count_ais_gaps(
        duckdb.connect(tmp_db, read_only=True),
        mmsis=["999999999"],
        start=datetime(2025, 1, 1, tzinfo=UTC),
        end=datetime(2025, 2, 1, tzinfo=UTC),
    )
    assert result == {"999999999": 0}


def test_count_ais_gaps_detects_gaps(tmp_db):
    """Gap counter should detect gaps above the threshold."""
    from datetime import timedelta

    con_rw = duckdb.connect(tmp_db)
    base = datetime(2025, 1, 1, tzinfo=UTC)
    # Insert 3 positions with 8-hour spacing (gap > 6h threshold)
    for i in range(3):
        ts = base + timedelta(hours=i * 8)
        con_rw.execute(
            "INSERT INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status) "
            "VALUES (?, ?, 1.0, 103.0, 0.5, 1)",
            ["888888888", ts.isoformat()],
        )
    con_rw.close()

    result = count_ais_gaps(
        duckdb.connect(tmp_db, read_only=True),
        mmsis=["888888888"],
        start=base,
        end=base + timedelta(hours=24),
    )
    # Two gaps of 8 hours each (both > 6h threshold)
    assert result["888888888"] == 2


def test_count_ais_gaps_below_threshold(tmp_db):
    """Gaps below the threshold should not be counted."""
    from datetime import timedelta

    con_rw = duckdb.connect(tmp_db)
    base = datetime(2025, 3, 1, tzinfo=UTC)
    # Insert 4 positions with 3-hour spacing (gap < 6h threshold)
    for i in range(4):
        ts = base + timedelta(hours=i * 3)
        con_rw.execute(
            "INSERT INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status) "
            "VALUES (?, ?, 1.5, 104.0, 5.0, 0)",
            ["777777777", ts.isoformat()],
        )
    con_rw.close()

    result = count_ais_gaps(
        duckdb.connect(tmp_db, read_only=True),
        mmsis=["777777777"],
        start=base,
        end=base + timedelta(hours=12),
    )
    assert result["777777777"] == 0


def test_count_ais_gaps_utc_consistency(tmp_db, monkeypatch):
    """Gap counter should return correct counts regardless of host machine timezone."""
    import time
    from datetime import timedelta

    # Mock host timezone to a non-UTC offset
    monkeypatch.setenv("TZ", "Asia/Tokyo")
    if hasattr(time, "tzset"):
        time.tzset()

    con_rw = duckdb.connect(tmp_db)
    # Force DuckDB session timezone
    con_rw.execute("SET TimeZone='Asia/Tokyo'")

    # Base date exactly at midnight UTC
    base = datetime(2025, 4, 1, 0, 0, 0, tzinfo=UTC)

    # Insert 2 positions with 10-hour spacing (gap = 10h > 6h threshold)
    for i in range(2):
        ts = base + timedelta(hours=i * 10)
        con_rw.execute(
            "INSERT INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status) "
            "VALUES (?, CAST(? AS TIMESTAMPTZ), 1.0, 103.0, 0.5, 1)",
            ["UTC_TEST", ts.isoformat()],
        )
    con_rw.close()

    # The query bounds in python timezone-aware datetimes
    start_window = base - timedelta(hours=1)
    end_window = base + timedelta(hours=12)

    result = count_ais_gaps(
        duckdb.connect(tmp_db, read_only=True),
        mmsis=["UTC_TEST"],
        start=start_window,
        end=end_window,
    )

    # Reset monkeypatch if needed, though pytest handles it.
    # Note: tzset is effective globally, but in a test run it's acceptable.

    # 1 gap of 10h should be detected
    assert result["UTC_TEST"] == 1


# ---------------------------------------------------------------------------
# Unit tests — DiD estimator
# ---------------------------------------------------------------------------


def test_did_estimate_insufficient_data(tmp_db):
    """DiD should return None when there are fewer than 2 vessels."""
    ann = datetime(2019, 5, 8, tzinfo=UTC)
    con = duckdb.connect(tmp_db, read_only=True)
    result = _did_estimate(["TR1"], [], ann, con)
    con.close()
    assert result is None


def test_did_estimate_with_seeded_data(tmp_db):
    """DiD should return a non-None result with sufficient seeded data."""
    _seed_causal_data(tmp_db)
    ann = datetime(2019, 5, 8, tzinfo=UTC)
    con = duckdb.connect(tmp_db, read_only=True)
    result = _did_estimate(
        ["TR1", "TR2", "TR3", "TR4"],
        ["CT1", "CT2", "CT3", "CT4"],
        ann,
        con,
    )
    con.close()
    assert result is not None
    assert "att" in result
    assert "p" in result
    assert 0.0 <= result["p"] <= 1.0


# ---------------------------------------------------------------------------
# Unit tests — pooling
# ---------------------------------------------------------------------------


class TestPoolEstimates:
    def test_empty_returns_defaults(self):
        pooled = _pool_estimates([])
        assert pooled["att"] == 0.0
        assert pooled["p"] == 1.0

    def test_single_result_passthrough(self):
        pooled = _pool_estimates(
            [{"att": 5.0, "se": 1.0, "t": 5.0, "p": 0.001, "n_treated": 10, "n_control": 10}]
        )
        assert abs(pooled["att"] - 5.0) < 0.01

    def test_inverse_variance_weighting(self):
        """More precise estimates should dominate the pooled result."""
        results = [
            {
                "att": 10.0,
                "se": 0.1,
                "t": 100.0,
                "p": 0.0,
                "n_treated": 5,
                "n_control": 5,
            },  # very precise
            {
                "att": 0.0,
                "se": 10.0,
                "t": 0.0,
                "p": 1.0,
                "n_treated": 5,
                "n_control": 5,
            },  # very noisy
        ]
        pooled = _pool_estimates(results)
        # Pooled ATT should be much closer to 10 than to 0
        assert pooled["att"] > 9.0


# ---------------------------------------------------------------------------
# Unit tests — weight calibration
# ---------------------------------------------------------------------------


class TestCalibrateGraphWeight:
    def test_no_significant_returns_default(self):
        effects = [
            CausalEffect("R1", "L1", 10, 10, 0.5, -0.1, 1.1, 0.2, False, 0.4),
            CausalEffect("R2", "L2", 10, 10, -0.3, -1.0, 0.4, 0.3, False, 0.4),
        ]
        assert calibrate_graph_weight(effects) == 0.40

    def test_all_positive_significant_increases_weight(self):
        effects = [
            CausalEffect("R1", "L1", 10, 10, 3.0, 1.0, 5.0, 0.01, True, 0.4),
            CausalEffect("R2", "L2", 10, 10, 2.0, 0.5, 3.5, 0.02, True, 0.4),
            CausalEffect("R3", "L3", 10, 10, 1.5, 0.2, 2.8, 0.04, True, 0.4),
        ]
        w = calibrate_graph_weight(effects)
        assert w > 0.40
        assert w <= 0.65

    def test_weight_clamped_in_valid_range(self):
        # Edge case: single significant effect with large ATT
        effects = [CausalEffect("R1", "L1", 100, 100, 50.0, 10.0, 90.0, 0.001, True, 0.4)]
        w = calibrate_graph_weight(effects)
        assert 0.20 <= w <= 0.65


# ---------------------------------------------------------------------------
# Integration tests — full pipeline
# ---------------------------------------------------------------------------


def test_run_causal_model_empty_db(tmp_db):
    """Full pipeline should not crash on an empty database; returns a list."""
    effects = run_causal_model(tmp_db)
    assert isinstance(effects, list)
    assert len(effects) == len(SANCTION_REGIMES)
    for e in effects:
        assert isinstance(e, CausalEffect)
        assert 0.0 <= e.p_value <= 1.0
        assert 0.20 <= e.calibrated_weight <= 0.65


def test_run_causal_model_with_data(tmp_db):
    """Full pipeline with seeded data should produce finite effect estimates."""
    _seed_causal_data(tmp_db)
    effects = run_causal_model(
        tmp_db,
        regimes={
            "OFAC_Iran": SANCTION_REGIMES["OFAC_Iran"],
        },
    )
    assert len(effects) == 1
    e = effects[0]
    assert np.isfinite(e.att_estimate)
    assert np.isfinite(e.att_ci_lower)
    assert np.isfinite(e.att_ci_upper)
    assert e.att_ci_lower <= e.att_ci_upper


def test_effects_to_dataframe_schema(tmp_db):
    """effects_to_dataframe should produce a Polars DataFrame with expected columns."""
    effects = run_causal_model(tmp_db)
    df = effects_to_dataframe(effects)
    required_cols = {
        "regime",
        "label",
        "n_treated",
        "n_control",
        "att_estimate",
        "att_ci_lower",
        "att_ci_upper",
        "p_value",
        "is_significant",
        "calibrated_weight",
    }
    assert required_cols <= set(df.columns)
    assert df.height == len(SANCTION_REGIMES)


def test_effects_to_dataframe_empty():
    """Empty effects list should return an empty DataFrame with correct schema."""
    df = effects_to_dataframe([])
    assert df.height == 0
    assert "att_estimate" in df.columns


def test_write_effects(tmp_db, tmp_path):
    """write_effects should persist a readable Parquet file."""
    effects = run_causal_model(tmp_db)
    df = effects_to_dataframe(effects)
    out = str(tmp_path / "causal_effects.parquet")
    write_effects(df, out)

    loaded = pl.read_parquet(out)
    assert loaded.height == len(SANCTION_REGIMES)
    assert "calibrated_weight" in loaded.columns


# ---------------------------------------------------------------------------
# Acceptance test — calibrated weight feeds composite.py
# ---------------------------------------------------------------------------


def test_calibrated_weight_feeds_composite(tmp_db):
    """
    The calibrated weight from run_causal_model should be usable as
    --w-graph in compute_composite_scores without error.
    """
    from pipeline.src.score.composite import compute_composite_scores
    from tests.test_scoring_pipeline import _seed_scoring_data

    _seed_scoring_data(tmp_db)
    _seed_causal_data(tmp_db)

    effects = run_causal_model(tmp_db)
    calibrated_w = effects[0].calibrated_weight

    # Redistribute remaining weight: keep anomaly + identity proportional
    remaining = 1.0 - calibrated_w
    w_anomaly = round(remaining * 0.67, 3)
    w_identity = round(remaining * 0.33, 3)
    # Ensure they sum to 1.0
    w_anomaly = round(1.0 - calibrated_w - w_identity, 3)

    composite = compute_composite_scores(
        tmp_db,
        w_anomaly=w_anomaly,
        w_graph=calibrated_w,
        w_identity=w_identity,
    )
    assert composite.height > 0
    assert composite["confidence"].is_sorted(descending=True)
    assert composite["confidence"].min() >= 0.0
    assert composite["confidence"].max() <= 1.0
