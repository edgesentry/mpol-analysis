import duckdb

from src.features.build_matrix import (
    CORE_COLUMNS,
    build_feature_matrix,
    validate_core_columns_non_null,
    write_vessel_features,
)


def _seed_minimal_data(db_path: str) -> None:
    con = duckdb.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status, ship_type)
            VALUES
                ('111111111', '2026-03-01 00:00:00+00', 1.0, 103.0, 0.8, 1, 82),
                ('111111111', '2026-03-01 07:00:00+00', 1.0, 103.0, 0.7, 1, 82),
                ('222222222', '2026-03-01 00:00:00+00', 2.0, 104.0, 8.0, 5, 70),
                ('222222222', '2026-03-01 00:30:00+00', 2.0, 104.0, 0.5, 1, 70)
            """
        )
        con.execute(
            """
            INSERT INTO vessel_meta (mmsi, flag, ship_type)
            VALUES ('111111111', 'IR', 82), ('222222222', 'SG', 70)
            """
        )
        con.execute(
            """
            INSERT INTO trade_flow (reporter, partner, hs_code, period, trade_value_usd, route_key)
            VALUES ('702', 'IR', '2709', '2024', 1000000, '702-IR-2709-2024')
            """
        )
    finally:
        con.close()


def test_build_feature_matrix_skip_neo4j(tmp_db):
    _seed_minimal_data(tmp_db)

    df = build_feature_matrix(db_path=tmp_db, window_days=3650, skip_graph=True)

    assert df.height == 2
    assert set(df["mmsi"].to_list()) == {"111111111", "222222222"}
    for col in CORE_COLUMNS:
        assert df.select(col).null_count().item() == 0


def test_write_vessel_features_and_validate_core_columns(tmp_db):
    _seed_minimal_data(tmp_db)

    df = build_feature_matrix(db_path=tmp_db, window_days=3650, skip_graph=True)
    validate_core_columns_non_null(df)

    written = write_vessel_features(tmp_db, df)
    assert written == 2

    con = duckdb.connect(tmp_db)
    try:
        count = con.execute("SELECT count(*) FROM vessel_features").fetchone()[0]
        assert count == 2

        nulls = con.execute(
            """
            SELECT
                SUM(CASE WHEN ais_gap_count_30d IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN position_jump_count IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN sts_candidate_count IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN loitering_hours_30d IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN flag_changes_2y IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN name_changes_2y IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN owner_changes_2y IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN sanctions_distance IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN cluster_sanctions_ratio IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN route_cargo_mismatch IS NULL THEN 1 ELSE 0 END)
            FROM vessel_features
            """
        ).fetchone()
        assert all(v == 0 for v in nulls)
    finally:
        con.close()
