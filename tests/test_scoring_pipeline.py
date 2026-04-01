import json

import duckdb

from src.score.anomaly import load_feature_frame, score_anomalies
from src.score.composite import compute_composite_scores
from src.score.mpol_baseline import build_mpol_baseline
from src.score.watchlist import build_candidate_watchlist, write_candidate_watchlist


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
                ('444444444', 'IMO444', 'DELTA', 'SG', 70)
            """
        )
        con.execute(
            """
            INSERT INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status, ship_type)
            VALUES
                ('111111111', '2026-03-01 00:00:00+00', 1.20, 103.80, 0.8, 1, 82),
                ('222222222', '2026-03-01 00:00:00+00', 1.30, 103.90, 12.0, 5, 82),
                ('333333333', '2026-03-01 00:00:00+00', 1.40, 104.00, 9.0, 5, 82),
                ('444444444', '2026-03-01 00:00:00+00', 1.50, 104.10, 7.0, 5, 70)
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
                sts_hub_degree, route_cargo_mismatch, declared_vs_estimated_cargo_value
            )
            VALUES
                ('111111111', 12, 18.0, 4, 3, 0.10, 22.0, 3, 2, 2, 1.0, 4, 1, 0.50, 1, 8, 4, 1.0, 100000.0),
                ('222222222', 1, 1.0, 0, 0, 0.90, 1.0, 0, 0, 0, 0.0, 1, 5, 0.00, 5, 0, 0, 0.0, 0.0),
                ('333333333', 2, 2.5, 0, 1, 0.70, 2.0, 0, 1, 0, 0.1, 2, 4, 0.10, 4, 1, 1, 0.0, 1000.0),
                ('444444444', 0, 0.5, 0, 0, 0.95, 0.5, 0, 0, 0, 0.0, 1, 6, 0.00, 6, 0, 0, 0.0, 0.0)
            """
        )
    finally:
        con.close()


def test_baseline_and_anomaly_scores(tmp_db):
    _seed_scoring_data(tmp_db)

    baseline = build_mpol_baseline(tmp_db)
    assert baseline.height == 4

    features = load_feature_frame(tmp_db)
    anomaly_df, _, _ = score_anomalies(features, baseline)
    assert anomaly_df.height == 4
    assert anomaly_df["anomaly_score"].min() >= 0.0
    assert anomaly_df["anomaly_score"].max() <= 1.0


def test_composite_and_watchlist_output(tmp_db, tmp_path):
    _seed_scoring_data(tmp_db)

    composite = compute_composite_scores(tmp_db)
    assert composite.height == 4
    assert composite["confidence"].is_sorted(descending=True)

    first_signals = json.loads(composite.row(0, named=True)["top_signals"])
    assert 1 <= len(first_signals) <= 3
    assert {"feature", "value", "contribution"} <= set(first_signals[0])

    watchlist = build_candidate_watchlist(tmp_db)
    output_path = tmp_path / "candidate_watchlist.parquet"
    write_candidate_watchlist(watchlist, str(output_path))
    assert output_path.exists()
