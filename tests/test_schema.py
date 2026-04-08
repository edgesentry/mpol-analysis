import duckdb

from src.ingest.schema import init_schema


def test_all_tables_created(tmp_db):
    con = duckdb.connect(tmp_db)
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    con.close()
    assert tables == {
        "ais_positions",
        "vessel_meta",
        "sanctions_entities",
        "trade_flow",
        "vessel_features",
        "analyst_briefs",
        "chat_cache",
        "cleared_vessels",
        "vessel_reviews",
        "analyst_prelabels",
        "sar_detections",
    }


def test_ais_positions_primary_key(tmp_db):
    """Inserting a duplicate (mmsi, timestamp) must be silently ignored."""
    con = duckdb.connect(tmp_db)
    con.execute("""
        INSERT INTO ais_positions (mmsi, timestamp, lat, lon)
        VALUES ('123456789', '2024-01-15 10:00:00+00', 1.3, 103.8)
    """)
    # Duplicate — should not raise
    con.execute("""
        INSERT OR IGNORE INTO ais_positions (mmsi, timestamp, lat, lon)
        VALUES ('123456789', '2024-01-15 10:00:00+00', 1.3, 103.8)
    """)
    count = con.execute("SELECT count(*) FROM ais_positions").fetchone()[0]
    con.close()
    assert count == 1


def test_idempotent_schema_init(tmp_db):
    """Running init_schema a second time on an existing DB must not raise."""
    init_schema(tmp_db)  # second call — all CREATE TABLE IF NOT EXISTS


def test_vessel_features_columns(tmp_db):
    con = duckdb.connect(tmp_db)
    cols = {row[0] for row in con.execute("DESCRIBE vessel_features").fetchall()}
    con.close()
    assert "sanctions_distance" in cols
    assert "sts_hub_degree" in cols
    assert "shared_address_centrality" in cols
