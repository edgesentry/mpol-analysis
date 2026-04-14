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
                ('ofac-2', 'CHARLIE', '333333333', 'IMO333', 'PA', 'Vessel', 'ofac_sdn'),
                ('ofac-3', 'PETROVSKY ZVEZDA', '273456782', 'IMO9234567', 'RU', 'Vessel', 'ofac_sdn'),
                ('ofac-4', 'SARI NOUR', '613115678', 'IMO9345612', 'CM', 'Vessel', 'ofac_sdn')
            """
        )
    finally:
        con.close()


def _sample_watchlist() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mmsi": [
                "111111111",
                "222222222",
                "333333333",
                "444444444",
                "273456782",
                "613115678",
                "352123456",
                "538009876",
            ],
            "imo": [
                "IMO111",
                "IMO222",
                "IMO333",
                "IMO444",
                "IMO9234567",
                "IMO9345612",
                "IMO9456781",
                "IMO9678901",
            ],
            "vessel_name": [
                "ALPHA",
                "BRAVO",
                "CHARLIE",
                "DELTA",
                "PETROVSKY ZVEZDA",
                "SARI NOUR",
                "OCEAN VOYAGER",
                "VERA SUNSET",
            ],
            "vessel_type": [
                "Tanker",
                "Tanker",
                "Tanker",
                "Cargo",
                "Tanker",
                "Tanker",
                "Tanker",
                "Tanker",
            ],
            "flag": ["IR", "SG", "PA", "SG", "RU", "CM", "PA", "MH"],
            "confidence": [0.99, 0.60, 0.55, 0.10, 0.91, 0.87, 0.79, 0.72],
            "behavioral_deviation_score": [0.95, 0.50, 0.45, 0.10, 0.88, 0.84, 0.70, 0.55],
            "graph_risk_score": [0.90, 0.40, 0.60, 0.10, 0.92, 0.80, 0.75, 0.65],
            "identity_score": [0.80, 0.30, 0.35, 0.05, 0.75, 0.70, 0.25, 0.40],
            "top_signals": [
                "[]",
                "[]",
                "[]",
                "[]",
                # PETROVSKY ZVEZDA: AIS dark + near-OFAC + flag churn
                json.dumps(
                    [
                        {"feature": "ais_gap_count_30d", "value": 14, "contribution": 0.38},
                        {"feature": "sanctions_distance", "value": 1, "contribution": 0.28},
                        {"feature": "flag_changes_2y", "value": 2, "contribution": 0.15},
                    ]
                ),
                # SARI NOUR: route-cargo mismatch + GPS jumps + reflagged IR→CM
                json.dumps(
                    [
                        {"feature": "route_cargo_mismatch", "value": 1.0, "contribution": 0.42},
                        {"feature": "position_jump_count", "value": 3, "contribution": 0.25},
                        {"feature": "high_risk_flag_ratio", "value": 0.85, "contribution": 0.18},
                    ]
                ),
                # OCEAN VOYAGER: STS hub off Ceuta + shared address with sanctioned vessels
                json.dumps(
                    [
                        {"feature": "sts_hub_degree", "value": 6, "contribution": 0.30},
                        {"feature": "shared_address_centrality", "value": 5, "contribution": 0.22},
                        {"feature": "cluster_sanctions_ratio", "value": 0.40, "contribution": 0.18},
                    ]
                ),
                # VERA SUNSET: 5-layer ownership chain + 2 hops from designated entity + renamed
                json.dumps(
                    [
                        {"feature": "ownership_depth", "value": 5, "contribution": 0.28},
                        {"feature": "sanctions_distance", "value": 2, "contribution": 0.24},
                        {"feature": "name_changes_2y", "value": 1, "contribution": 0.12},
                    ]
                ),
            ],
        }
    )


def test_label_watchlist_against_ofac(tmp_db):
    _seed_validation_data(tmp_db)
    labeled = label_watchlist_against_ofac(_sample_watchlist(), tmp_db)

    assert labeled["is_ofac_listed"].to_list() == [
        True,
        False,
        True,
        False,
        True,
        True,
        False,
        False,
    ]


def test_compute_validation_metrics():
    labeled = _sample_watchlist().with_columns(
        pl.Series("is_ofac_listed", [True, False, True, False, True, True, False, False])
    )
    metrics = compute_validation_metrics(labeled)

    assert metrics["candidate_count"] == 8
    assert metrics["positive_count"] == 4
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
