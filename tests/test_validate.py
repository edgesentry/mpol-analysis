import json

import duckdb
import polars as pl

from src.score.validate import (
    compute_validation_metrics,
    label_watchlist_against_ofac,
    validate_watchlist,
)
from src.score.watchlist import write_candidate_watchlist


def _seed_validation_data(db_path: str) -> None:
    con = duckdb.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO sanctions_entities (entity_id, name, mmsi, imo, flag, type, list_source)
            VALUES
                ('ofac-1', 'ALPHA', '111111111', 'IMO111', 'IR', 'Vessel', 'ofac_sdn'),
                ('ofac-2', 'CHARLIE', '333333333', 'IMO333', 'PA', 'Vessel', 'ofac_sdn')
            """
        )
    finally:
        con.close()


def _sample_watchlist() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mmsi": ["111111111", "222222222", "333333333", "444444444"],
            "imo": ["IMO111", "IMO222", "IMO333", "IMO444"],
            "vessel_name": ["ALPHA", "BRAVO", "CHARLIE", "DELTA"],
            "vessel_type": ["Tanker", "Tanker", "Tanker", "Cargo"],
            "flag": ["IR", "SG", "PA", "SG"],
            "confidence": [0.99, 0.60, 0.55, 0.10],
            "anomaly_score": [0.95, 0.50, 0.45, 0.10],
            "graph_risk_score": [0.90, 0.40, 0.60, 0.10],
            "identity_score": [0.80, 0.30, 0.35, 0.05],
            "top_signals": ["[]", "[]", "[]", "[]"],
        }
    )


def test_label_watchlist_against_ofac(tmp_db):
    _seed_validation_data(tmp_db)
    labeled = label_watchlist_against_ofac(_sample_watchlist(), tmp_db)

    assert labeled["is_ofac_listed"].to_list() == [True, False, True, False]


def test_compute_validation_metrics():
    labeled = _sample_watchlist().with_columns(pl.Series("is_ofac_listed", [True, False, True, False]))
    metrics = compute_validation_metrics(labeled)

    assert metrics["candidate_count"] == 4
    assert metrics["positive_count"] == 2
    assert metrics["precision_at_50"] == 0.5
    assert metrics["recall_at_200"] == 1.0
    assert metrics["auroc"] is not None


def test_validate_watchlist_writes_metrics_file(tmp_db, tmp_path):
    _seed_validation_data(tmp_db)
    watchlist = _sample_watchlist()
    watchlist_path = tmp_path / "candidate_watchlist.parquet"
    metrics_path = tmp_path / "validation_metrics.json"

    write_candidate_watchlist(watchlist, str(watchlist_path))
    metrics = validate_watchlist(tmp_db, str(watchlist_path), str(metrics_path))

    assert metrics_path.exists()
    persisted = json.loads(metrics_path.read_text())
    assert persisted == metrics
    assert persisted["recall_at_200"] == 1.0
