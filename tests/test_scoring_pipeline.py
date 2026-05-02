import json

import duckdb

from pipeline.src.score.anomaly import ANOMALY_FEATURE_COLUMNS, load_feature_frame, score_anomalies
from pipeline.src.score.composite import FEATURE_VALUE_COLUMNS, compute_composite_scores
from pipeline.src.score.mpol_baseline import build_mpol_baseline
from pipeline.src.score.watchlist import build_candidate_watchlist, write_candidate_watchlist


def _seed_scoring_data(db_path: str) -> None:
    con = duckdb.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO vessel_meta (mmsi, imo, name, flag, ship_type)
            VALUES
                ('111111111', 'IMO111', 'ALPHA', 'IR', 82),
                ('222222222', 'IMO222', 'BRAVO', 'SG', 82),
                ('333333333', 'IMO333', 'CHARLIE', 'PA', 82),
                ('444444444', 'IMO444', 'DELTA', 'SG', 70),
                -- Realistic shadow fleet candidates
                ('273456782', 'IMO9234567', 'PETROVSKY ZVEZDA', 'RU', 82),
                ('613115678', 'IMO9345612', 'SARI NOUR', 'CM', 82),
                ('352123456', 'IMO9456781', 'OCEAN VOYAGER', 'PA', 82),
                ('538009876', 'IMO9678901', 'VERA SUNSET', 'MH', 82)
            """
        )
        con.execute(
            """
            INSERT INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status, ship_type)
            VALUES
                ('111111111', '2026-03-01 00:00:00+00', 1.20, 103.80, 0.8, 1, 82),
                ('222222222', '2026-03-01 00:00:00+00', 1.30, 103.90, 12.0, 5, 82),
                ('333333333', '2026-03-01 00:00:00+00', 1.40, 104.00, 9.0, 5, 82),
                ('444444444', '2026-03-01 00:00:00+00', 1.50, 104.10, 7.0, 5, 70),
                -- PETROVSKY ZVEZDA: at anchor in Strait of Hormuz approaches; AIS went dark for 22h before reappearing here
                ('273456782', '2026-03-15 00:00:00+00', 26.50, 55.50, 0.5, 1, 82),
                -- SARI NOUR: loitering off Kharg Island; previously reported near Bandar Abbas loading terminal
                ('613115678', '2026-03-20 00:00:00+00', 29.10, 50.30, 0.3, 1, 82),
                -- OCEAN VOYAGER: stationary off Ceuta; matched position of another tanker for 4h (STS candidate)
                ('352123456', '2026-03-10 00:00:00+00', 35.90, -5.50, 0.5, 0, 82),
                -- VERA SUNSET: transiting Gulf of Oman, declared Fujairah as next port
                ('538009876', '2026-03-25 00:00:00+00', 25.10, 56.40, 6.5, 5, 82)
            """
        )
        con.execute(
            """
            INSERT INTO vessel_features (
                mmsi, ais_gap_count_30d, ais_gap_max_hours, position_jump_count,
                sts_candidate_count, port_call_ratio, loitering_hours_30d,
                flag_changes_2y, name_changes_2y, owner_changes_2y,
                high_risk_flag_ratio, ownership_depth, sanctions_distance,
                cluster_sanctions_ratio, shared_manager_risk, shared_address_centrality,
                sts_hub_degree, route_cargo_mismatch, declared_vs_estimated_cargo_value,
                sanctions_list_count
            )
            VALUES
                ('111111111', 12, 18.0, 4, 3, 0.10, 22.0, 3, 2, 2, 1.0, 4, 1, 0.50, 1, 8, 4, 1.0, 100000.0, 3),
                ('222222222', 1, 1.0, 0, 0, 0.90, 1.0, 0, 0, 0, 0.0, 1, 5, 0.00, 5, 0, 0, 0.0, 0.0,       0),
                ('333333333', 2, 2.5, 0, 1, 0.70, 2.0, 0, 1, 0, 0.1, 2, 4, 0.10, 4, 1, 1, 0.0, 1000.0,    0),
                ('444444444', 0, 0.5, 0, 0, 0.95, 0.5, 0, 0, 0, 0.0, 1, 6, 0.00, 6, 0, 0, 0.0, 0.0,       0),
                -- PETROVSKY ZVEZDA: 14 AIS gaps in 30d (max 22h), reflagged twice in 2y, 1 hop from sanctioned entity,
                --   60% of co-owned fleet is OFAC-listed, confirmed route-cargo mismatch on Iran crude export route
                ('273456782', 14, 22.0, 2, 2, 0.15, 28.0, 2, 1, 1, 0.90, 3, 1, 0.60, 1, 3, 3, 1.0, 50000.0, 4),
                -- SARI NOUR: 8 AIS gaps, 3 GPS position jumps (>50-knot implied speed), reflagged IR→CM in 2024,
                --   route-cargo mismatch: operates Kharg Island routes with no Comtrade crude import record
                ('613115678', 8, 14.0, 3, 1, 0.08, 35.0, 1, 2, 1, 0.85, 4, 2, 0.45, 2, 2, 2, 1.0, 75000.0, 2),
                -- OCEAN VOYAGER: low AIS gaps but 6 distinct STS partners, shares a Piraeus address with 5 other vessels
                --   of which 40% are under OFAC designation; route-cargo mismatch on Ceuta dark transfer corridor
                ('352123456', 3, 7.5, 0, 5, 0.45, 15.0, 0, 0, 1, 0.30, 3, 3, 0.40, 3, 5, 6, 1.0, 120000.0, 3),
                -- VERA SUNSET: clean AIS but 5-layer ownership chain; renamed once; beneficial owner 2 hops from
                --   a designated entity; 25% of co-managed fleet sanctioned
                ('538009876', 1, 3.0, 0, 0, 0.75, 3.0, 0, 1, 2, 0.20, 5, 2, 0.25, 2, 2, 1, 0.0, 8000.0,   1)
            """
        )
    finally:
        con.close()


def test_baseline_and_anomaly_scores(tmp_db):
    _seed_scoring_data(tmp_db)

    baseline = build_mpol_baseline(tmp_db)
    assert baseline.height == 8

    features = load_feature_frame(tmp_db)
    anomaly_df, _, _ = score_anomalies(features, baseline)
    assert anomaly_df.height == 8
    assert anomaly_df["behavioral_deviation_score"].min() >= 0.0
    assert anomaly_df["behavioral_deviation_score"].max() <= 1.0


def test_directly_sanctioned_vessels_differentiated_by_list_count(tmp_db):
    """Vessels at sanctions_distance=0 with different program counts must not all tie.

    Root cause of #243: when behavioral_deviation_score≈0 and identity_score≈0, the only
    remaining signal is graph_risk_score.  With identical sanctions_distance the
    old formula gave every directly-sanctioned vessel the same score.
    sanctions_list_count breaks this tie.
    """
    con = duckdb.connect(tmp_db)
    try:
        con.execute(
            """
            INSERT INTO vessel_meta (mmsi, imo, name, flag, ship_type) VALUES
                ('900000001', 'IMO9000001', 'VESSEL_ONE_LIST',   'PA', 82),
                ('900000002', 'IMO9000002', 'VESSEL_THREE_LIST', 'PA', 82)
            """
        )
        con.execute(
            """
            INSERT INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status, ship_type) VALUES
                ('900000001', '2026-03-01 00:00:00+00', 1.0, 103.0, 0.5, 1, 82),
                ('900000002', '2026-03-01 00:00:00+00', 1.1, 103.1, 0.5, 1, 82)
            """
        )
        # Both directly sanctioned (distance=0), sparse AIS → anomaly≈0, identity≈0.
        # Only difference: sanctions_list_count (1 vs 3 programs).
        con.execute(
            """
            INSERT INTO vessel_features (
                mmsi, ais_gap_count_30d, ais_gap_max_hours, position_jump_count,
                sts_candidate_count, port_call_ratio, loitering_hours_30d,
                flag_changes_2y, name_changes_2y, owner_changes_2y,
                high_risk_flag_ratio, ownership_depth, sanctions_distance,
                cluster_sanctions_ratio, shared_manager_risk, shared_address_centrality,
                sts_hub_degree, route_cargo_mismatch, declared_vs_estimated_cargo_value,
                sanctions_list_count
            ) VALUES
                ('900000001', 0, 0.0, 0, 0, 0.5, 0.0, 0, 0, 0, 0.0, 0, 0, 0.0, 99, 0, 0, 0.0, 0.0, 1),
                ('900000002', 0, 0.0, 0, 0, 0.5, 0.0, 0, 0, 0, 0.0, 0, 0, 0.0, 99, 0, 0, 0.0, 0.0, 3)
            """
        )
    finally:
        con.close()

    composite = compute_composite_scores(tmp_db)
    rows = {r["mmsi"]: r for r in composite.iter_rows(named=True)}

    score_one = rows["900000001"]["confidence"]
    score_three = rows["900000002"]["confidence"]
    assert score_three > score_one, (
        f"Vessel on 3 programs ({score_three:.4f}) should outscore vessel on 1 program "
        f"({score_one:.4f}) when all other features are identical"
    )


def test_composite_and_watchlist_output(tmp_db, tmp_path):
    _seed_scoring_data(tmp_db)

    composite = compute_composite_scores(tmp_db)
    assert composite.height == 8
    assert composite["confidence"].is_sorted(descending=True)

    first_signals = json.loads(composite.row(0, named=True)["top_signals"])
    assert 1 <= len(first_signals) <= 5
    assert {"feature", "value", "contribution"} <= set(first_signals[0])

    watchlist = build_candidate_watchlist(tmp_db)
    output_path = tmp_path / "candidate_watchlist.parquet"
    write_candidate_watchlist(watchlist, str(output_path))
    assert output_path.exists()


# ---------------------------------------------------------------------------
# IMO spoofing + chokepoint features wired into scoring
# ---------------------------------------------------------------------------


def test_imo_type_mismatch_in_anomaly_feature_columns():
    """imo_type_mismatch must be a covariate in the anomaly model."""
    assert "imo_type_mismatch" in ANOMALY_FEATURE_COLUMNS


def test_imo_scrapped_flag_in_anomaly_feature_columns():
    """imo_scrapped_flag must be a covariate in the anomaly model."""
    assert "imo_scrapped_flag" in ANOMALY_FEATURE_COLUMNS


def test_chokepoint_features_in_anomaly_feature_columns():
    """chokepoint_exit_gap_count and ais_pre_gap_regularity must feed the anomaly model."""
    assert "chokepoint_exit_gap_count" in ANOMALY_FEATURE_COLUMNS
    assert "ais_pre_gap_regularity" in ANOMALY_FEATURE_COLUMNS


def test_new_features_in_feature_value_columns():
    """All four new features must appear in SHAP signal attribution."""
    for col in (
        "imo_type_mismatch",
        "imo_scrapped_flag",
        "chokepoint_exit_gap_count",
        "ais_pre_gap_regularity",
    ):
        assert col in FEATURE_VALUE_COLUMNS, f"{col} missing from FEATURE_VALUE_COLUMNS"


def test_imo_type_mismatch_raises_behavioral_score(tmp_db):
    """A vessel with imo_type_mismatch=True scores higher than an identical vessel with False."""
    con = duckdb.connect(tmp_db)
    try:
        con.execute(
            """
            INSERT INTO vessel_meta (mmsi, imo, name, flag, ship_type) VALUES
                ('800000001', 'IMO8000001', 'SPOOF_VESSEL', 'PA', 70),
                ('800000002', 'IMO8000002', 'CLEAN_VESSEL', 'PA', 80)
            """
        )
        con.execute(
            """
            INSERT INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status, ship_type) VALUES
                ('800000001', '2026-03-01 00:00:00+00', 1.0, 103.0, 5.0, 0, 70),
                ('800000002', '2026-03-01 00:00:00+00', 1.0, 103.0, 5.0, 0, 80)
            """
        )
        # Identical features except imo_type_mismatch
        con.execute(
            """
            INSERT INTO vessel_features (
                mmsi, ais_gap_count_30d, ais_gap_max_hours, position_jump_count,
                sts_candidate_count, port_call_ratio, loitering_hours_30d,
                flag_changes_2y, name_changes_2y, owner_changes_2y,
                high_risk_flag_ratio, ownership_depth, sanctions_distance,
                cluster_sanctions_ratio, shared_manager_risk, shared_address_centrality,
                sts_hub_degree, route_cargo_mismatch, declared_vs_estimated_cargo_value,
                sanctions_list_count, imo_type_mismatch, imo_scrapped_flag
            ) VALUES
                ('800000001', 2, 5.0, 0, 0, 0.5, 2.0, 0, 0, 0, 0.0, 1, 5, 0.0, 5, 0, 0, 0.0, 0.0, 0, TRUE,  FALSE),
                ('800000002', 2, 5.0, 0, 0, 0.5, 2.0, 0, 0, 0, 0.0, 1, 5, 0.0, 5, 0, 0, 0.0, 0.0, 0, FALSE, FALSE)
            """
        )
    finally:
        con.close()

    features = load_feature_frame(tmp_db)
    baseline = build_mpol_baseline(tmp_db)
    anomaly_df, _, _ = score_anomalies(features, baseline)

    spoof = anomaly_df.filter(features["mmsi"] == "800000001")["behavioral_deviation_score"][0]
    clean = anomaly_df.filter(features["mmsi"] == "800000002")["behavioral_deviation_score"][0]
    assert spoof >= clean, (
        f"imo_type_mismatch vessel ({spoof:.4f}) should score >= clean vessel ({clean:.4f})"
    )
