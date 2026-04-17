"""Tests for src/score/prelabel_evaluation.py — analyst pre-label holdout evaluation."""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import polars as pl
import pytest

from pipeline.src.ingest.schema import init_schema
from pipeline.src.score.prelabel_evaluation import (
    PRE_LABEL_NEGATIVE,
    PRE_LABEL_POSITIVE,
    PRE_LABEL_UNCERTAIN,
    _disagreement_report,
    _label_watchlist,
    _ops_thresholds,
    _tier_breakdown,
    load_prelabels_from_csv,
    load_prelabels_from_db,
    run_prelabel_evaluation,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def prelabel_db(tmp_path):
    db_path = str(tmp_path / "prelabel.duckdb")
    init_schema(db_path)
    return db_path


@pytest.fixture
def watchlist_parquet(tmp_path):
    """Write a small fake watchlist parquet."""
    df = pl.DataFrame(
        {
            "mmsi": [
                "111111111",
                "222222222",
                "333333333",
                "444444444",
                "555555555",
                "666666666",
                "777777777",
                "888888888",
                "999999999",
                "000000001",
            ],
            "imo": ["IMO1", "IMO2", "IMO3", "IMO4", "IMO5", "IMO6", "IMO7", "IMO8", "IMO9", "IMO0"],
            "vessel_name": [f"VESSEL_{i}" for i in range(10)],
            "vessel_type": ["tanker"] * 10,
            "confidence": [0.95, 0.85, 0.75, 0.65, 0.55, 0.45, 0.35, 0.25, 0.15, 0.05],
        }
    )
    path = tmp_path / "watchlist.parquet"
    df.write_parquet(path)
    return str(path)


@pytest.fixture
def prelabel_csv(tmp_path):
    """Write a pre-label CSV with 6 entries across confidence tiers."""
    rows = [
        {
            "mmsi": "111111111",
            "imo": "IMO1",
            "pre_label": "suspected-positive",
            "confidence_tier": "high",
            "region": "singapore",
            "evidence_notes": "Dark ship detection",
            "source_urls": "https://example.test/1",
            "analyst_id": "analyst-a",
            "evidence_timestamp": "2025-10-01T00:00:00+00:00",
        },
        {
            "mmsi": "222222222",
            "imo": "IMO2",
            "pre_label": "suspected-positive",
            "confidence_tier": "medium",
            "region": "singapore",
            "evidence_notes": "STS event detected",
            "source_urls": "",
            "analyst_id": "analyst-a",
            "evidence_timestamp": "2025-10-05T00:00:00+00:00",
        },
        {
            "mmsi": "333333333",
            "imo": "IMO3",
            "pre_label": "suspected-positive",
            "confidence_tier": "weak",
            "region": "singapore",
            "evidence_notes": "Single AIS gap",
            "source_urls": "",
            "analyst_id": "analyst-b",
            "evidence_timestamp": "2025-10-10T00:00:00+00:00",
        },
        {
            "mmsi": "444444444",
            "imo": "IMO4",
            "pre_label": "analyst-negative",
            "confidence_tier": "high",
            "region": "singapore",
            "evidence_notes": "Cleared via MPA inspection",
            "source_urls": "https://example.test/4",
            "analyst_id": "analyst-a",
            "evidence_timestamp": "2025-09-15T00:00:00+00:00",
        },
        {
            "mmsi": "555555555",
            "imo": "IMO5",
            "pre_label": "analyst-negative",
            "confidence_tier": "medium",
            "region": "singapore",
            "evidence_notes": "Legitimate ferry route",
            "source_urls": "",
            "analyst_id": "analyst-b",
            "evidence_timestamp": "2025-09-20T00:00:00+00:00",
        },
        {
            "mmsi": "666666666",
            "imo": "IMO6",
            "pre_label": "uncertain",
            "confidence_tier": "medium",
            "region": "singapore",
            "evidence_notes": "Under review",
            "source_urls": "",
            "analyst_id": "analyst-a",
            "evidence_timestamp": "2025-10-20T00:00:00+00:00",
        },
    ]
    path = tmp_path / "prelabels.csv"
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return str(path)


def _seed_prelabels(db_path: str, rows: list[dict]) -> None:
    con = duckdb.connect(db_path)
    try:
        for row in rows:
            con.execute(
                "INSERT INTO analyst_prelabels "
                "(mmsi, imo, pre_label, confidence_tier, region, "
                " evidence_notes, source_urls_json, analyst_id, evidence_timestamp) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    row["mmsi"],
                    row.get("imo"),
                    row["pre_label"],
                    row["confidence_tier"],
                    row.get("region"),
                    row.get("evidence_notes"),
                    row.get("source_urls"),
                    row["analyst_id"],
                    row["evidence_timestamp"],
                ],
            )
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Unit: schema fixtures
# ---------------------------------------------------------------------------


def test_schema_creates_analyst_prelabels_table(prelabel_db):
    con = duckdb.connect(prelabel_db, read_only=True)
    try:
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    finally:
        con.close()
    assert "analyst_prelabels" in tables


# ---------------------------------------------------------------------------
# Unit: load_prelabels_from_csv
# ---------------------------------------------------------------------------


def test_load_csv_returns_all_rows(prelabel_csv):
    df, n_leaked = load_prelabels_from_csv(prelabel_csv)
    assert df.height == 6
    assert n_leaked == 0


def test_load_csv_leakage_filter(prelabel_csv):
    # end_date before all evidence timestamps → all dropped
    df, n_leaked = load_prelabels_from_csv(prelabel_csv, end_date="2025-09-01T00:00:00+00:00")
    assert df.height == 0
    assert n_leaked == 6


def test_load_csv_leakage_partial(prelabel_csv):
    # end_date keeps only rows with evidence_timestamp <= 2025-09-25
    df, n_leaked = load_prelabels_from_csv(prelabel_csv, end_date="2025-09-25T00:00:00+00:00")
    assert df.height == 2  # analyst-negative rows from Sep 15 and Sep 20
    assert n_leaked == 4


def test_load_csv_confidence_filter(prelabel_csv):
    df, _ = load_prelabels_from_csv(prelabel_csv, min_confidence_tier="high")
    assert all(row == "high" for row in df["confidence_tier"].to_list())


def test_load_csv_excludes_uncertain_only_in_metrics(prelabel_csv):
    # Uncertain rows are loaded — exclusion happens in _label_watchlist
    df, _ = load_prelabels_from_csv(prelabel_csv)
    uncertain = df.filter(pl.col("pre_label") == PRE_LABEL_UNCERTAIN)
    assert uncertain.height == 1


# ---------------------------------------------------------------------------
# Unit: load_prelabels_from_db
# ---------------------------------------------------------------------------


def test_load_db_empty(prelabel_db):
    df, n_leaked = load_prelabels_from_db(prelabel_db)
    assert df.height == 0
    assert n_leaked == 0


def test_load_db_returns_latest_per_mmsi(prelabel_db):
    _seed_prelabels(
        prelabel_db,
        [
            {
                "mmsi": "111111111",
                "pre_label": "suspected-positive",
                "confidence_tier": "medium",
                "analyst_id": "analyst-a",
                "evidence_timestamp": "2025-09-01T00:00:00+00:00",
            },
            {
                "mmsi": "111111111",
                "pre_label": "analyst-negative",
                "confidence_tier": "high",
                "analyst_id": "analyst-a",
                "evidence_timestamp": "2025-10-01T00:00:00+00:00",
            },
        ],
    )
    df, _ = load_prelabels_from_db(prelabel_db)
    assert df.height == 1
    assert df["pre_label"][0] == "analyst-negative"


# ---------------------------------------------------------------------------
# Unit: _label_watchlist
# ---------------------------------------------------------------------------


def test_label_watchlist_maps_positive():
    watchlist = pl.DataFrame({"mmsi": ["A", "B", "C"], "confidence": [0.9, 0.5, 0.1]})
    labels = pl.DataFrame(
        {
            "mmsi": ["A", "B"],
            "pre_label": [PRE_LABEL_POSITIVE, PRE_LABEL_NEGATIVE],
            "confidence_tier": ["high", "high"],
        }
    )
    result = _label_watchlist(watchlist, labels)
    y_true = {row["mmsi"]: row["y_true"] for row in result.iter_rows(named=True)}
    assert y_true["A"] == 1
    assert y_true["B"] == 0
    assert y_true["C"] is None  # uncertain / unlabeled


def test_label_watchlist_uncertain_excluded():
    watchlist = pl.DataFrame({"mmsi": ["X"], "confidence": [0.8]})
    labels = pl.DataFrame(
        {
            "mmsi": ["X"],
            "pre_label": [PRE_LABEL_UNCERTAIN],
            "confidence_tier": ["medium"],
        }
    )
    result = _label_watchlist(watchlist, labels)
    assert result["y_true"][0] is None


def test_label_watchlist_empty_prelabels():
    watchlist = pl.DataFrame({"mmsi": ["A"], "confidence": [0.9]})
    result = _label_watchlist(watchlist, pl.DataFrame())
    assert result["y_true"][0] is None


# ---------------------------------------------------------------------------
# Unit: _disagreement_report
# ---------------------------------------------------------------------------


def test_disagreement_report_finds_high_model_analyst_negative():
    df = pl.DataFrame(
        {
            "mmsi": ["A", "B", "C"],
            "confidence": [0.9, 0.8, 0.2],
            "y_true": pl.Series([0, 1, 1], dtype=pl.Int8),
        }
    )
    report = _disagreement_report(df, threshold=0.7, display_cols=["mmsi", "confidence", "y_true"])
    # A: model high (0.9 >= 0.7), analyst negative (y_true=0) → model_high_analyst_negative
    assert len(report["model_high_analyst_negative"]) == 1
    assert report["model_high_analyst_negative"][0]["mmsi"] == "A"


def test_disagreement_report_finds_low_model_analyst_positive():
    df = pl.DataFrame(
        {
            "mmsi": ["A", "B", "C"],
            "confidence": [0.9, 0.8, 0.2],
            "y_true": pl.Series([0, 1, 1], dtype=pl.Int8),
        }
    )
    report = _disagreement_report(df, threshold=0.7, display_cols=["mmsi", "confidence", "y_true"])
    # C: model low (0.2 < 0.7), analyst positive (y_true=1) → model_low_analyst_positive
    assert len(report["model_low_analyst_positive"]) == 1
    assert report["model_low_analyst_positive"][0]["mmsi"] == "C"


# ---------------------------------------------------------------------------
# Unit: _ops_thresholds
# ---------------------------------------------------------------------------


def test_ops_thresholds_basic():
    df = pl.DataFrame(
        {
            "confidence": [0.9, 0.8, 0.5, 0.3],
            "y_true": pl.Series([1, 0, 1, 0], dtype=pl.Int8),
        }
    )
    thresholds = _ops_thresholds(df, capacities=[2, 4])
    assert thresholds[0]["review_capacity"] == 2
    assert thresholds[1]["review_capacity"] == 4


# ---------------------------------------------------------------------------
# Unit: _tier_breakdown
# ---------------------------------------------------------------------------


def test_tier_breakdown_groups_by_tier():
    df = pl.DataFrame(
        {
            "confidence": [0.9, 0.8, 0.4],
            "y_true": pl.Series([1, 0, 1], dtype=pl.Int8),
            "confidence_tier": ["high", "high", "medium"],
        }
    )
    breakdown = _tier_breakdown(df)
    assert "high" in breakdown
    assert "medium" in breakdown
    assert breakdown["high"]["count"] == 2


# ---------------------------------------------------------------------------
# Integration: run_prelabel_evaluation (CSV mode)
# ---------------------------------------------------------------------------


def test_run_prelabel_evaluation_csv_produces_report(tmp_path, watchlist_parquet, prelabel_csv):
    output = str(tmp_path / "prelabel_eval.json")
    report = run_prelabel_evaluation(
        watchlist_path=watchlist_parquet,
        output_path=output,
        capacities=[5, 10],
        prelabels_csv=prelabel_csv,
        min_confidence_tier="weak",
    )

    assert Path(output).exists()
    assert "result" in report
    result = report["result"]
    assert "metrics" in result
    assert result["metrics"]["candidate_count"] == 10
    assert result["metrics"]["labeled_count"] >= 1
    assert "disagreement" in result
    assert "leakage_report" in result


def test_run_prelabel_evaluation_excludes_uncertain_from_metrics(
    tmp_path, watchlist_parquet, prelabel_csv
):
    report = run_prelabel_evaluation(
        watchlist_path=watchlist_parquet,
        output_path=str(tmp_path / "out.json"),
        capacities=[5],
        prelabels_csv=prelabel_csv,
    )
    # 3 positives + 2 negatives = 5 labeled (1 uncertain excluded)
    assert report["result"]["metrics"]["labeled_count"] == 5


def test_run_prelabel_evaluation_leakage_filter(tmp_path, watchlist_parquet, prelabel_csv):
    report = run_prelabel_evaluation(
        watchlist_path=watchlist_parquet,
        output_path=str(tmp_path / "out.json"),
        capacities=[5],
        prelabels_csv=prelabel_csv,
        end_date="2025-09-01T00:00:00+00:00",  # before all evidence
    )
    assert report["result"]["metrics"]["labeled_count"] == 0
    assert report["result"]["leakage_report"]["labels_dropped"] == 6


def test_run_prelabel_evaluation_raises_if_both_sources(
    tmp_path, watchlist_parquet, prelabel_csv, prelabel_db
):
    with pytest.raises(ValueError, match="not both"):
        run_prelabel_evaluation(
            watchlist_path=watchlist_parquet,
            output_path=str(tmp_path / "out.json"),
            capacities=[5],
            db_path=prelabel_db,
            prelabels_csv=prelabel_csv,
        )


def test_run_prelabel_evaluation_raises_if_no_source(tmp_path, watchlist_parquet):
    with pytest.raises(ValueError, match="required"):
        run_prelabel_evaluation(
            watchlist_path=watchlist_parquet,
            output_path=str(tmp_path / "out.json"),
            capacities=[5],
        )


# ---------------------------------------------------------------------------
# Integration: run_prelabel_evaluation (DB mode)
# ---------------------------------------------------------------------------


def test_run_prelabel_evaluation_db_mode(tmp_path, watchlist_parquet, prelabel_db):
    _seed_prelabels(
        prelabel_db,
        [
            {
                "mmsi": "111111111",
                "pre_label": "suspected-positive",
                "confidence_tier": "high",
                "analyst_id": "analyst-a",
                "evidence_timestamp": "2025-10-01T00:00:00+00:00",
            },
            {
                "mmsi": "444444444",
                "pre_label": "analyst-negative",
                "confidence_tier": "high",
                "analyst_id": "analyst-a",
                "evidence_timestamp": "2025-10-01T00:00:00+00:00",
            },
        ],
    )
    report = run_prelabel_evaluation(
        watchlist_path=watchlist_parquet,
        output_path=str(tmp_path / "out.json"),
        capacities=[5, 10],
        db_path=prelabel_db,
    )
    assert report["result"]["metrics"]["labeled_count"] == 2
    assert report["result"]["metrics"]["positive_count"] == 1


# ---------------------------------------------------------------------------
# Demo CSV: smoke test
# ---------------------------------------------------------------------------


def test_demo_prelabels_csv_is_valid():
    csv_path = Path(__file__).parents[1] / "data" / "demo" / "analyst_prelabels_demo.csv"
    assert csv_path.exists(), "Demo CSV not committed"

    df, _ = load_prelabels_from_csv(str(csv_path))
    assert df.height >= 50, "Expected at least 50 demo rows"

    valid_labels = {PRE_LABEL_POSITIVE, PRE_LABEL_NEGATIVE, PRE_LABEL_UNCERTAIN}
    assert set(df["pre_label"].to_list()).issubset(valid_labels)

    valid_tiers = {"high", "medium", "weak"}
    assert set(df["confidence_tier"].to_list()).issubset(valid_tiers)

    regions = set(df["region"].to_list())
    assert len(regions) >= 3, "Expected coverage of at least 3 regions"
