import json
from datetime import UTC, datetime

import duckdb
import polars as pl

from pipeline.src.ingest.schema import init_schema
from pipeline.src.score.review_feedback_evaluation import run_review_feedback_evaluation


def _write_watchlist(path, rows):
    pl.DataFrame(rows).write_parquet(path)


def test_review_feedback_report_generation(tmp_path):
    db_path = tmp_path / "mpol.duckdb"
    init_schema(str(db_path))

    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            INSERT INTO vessel_reviews (mmsi, review_tier, handoff_state, rationale, reviewed_by, reviewed_at)
            VALUES
                ('111', 'confirmed', 'handoff_completed', 'high confidence', 'analyst-1', '2026-01-10T00:00:00Z'),
                ('222', 'cleared', 'closed', 'false positive', 'analyst-2', '2026-01-10T00:10:00Z'),
                ('333', 'probable', 'handoff_recommended', 'pattern overlap', 'analyst-3', '2026-01-10T00:20:00Z'),
                ('444', 'inconclusive', 'in_review', 'need more evidence', 'analyst-4', '2026-01-10T00:30:00Z')
            """
        )
    finally:
        con.close()

    singapore_path = tmp_path / "singapore.parquet"
    japan_path = tmp_path / "japan.parquet"
    _write_watchlist(
        singapore_path,
        {
            "mmsi": ["111", "222", "333", "999"],
            "confidence": [0.92, 0.85, 0.81, 0.20],
        },
    )
    _write_watchlist(
        japan_path,
        {
            "mmsi": ["222", "333", "444"],
            "confidence": [0.83, 0.70, 0.66],
        },
    )

    output_path = tmp_path / "feedback_report.json"
    report = run_review_feedback_evaluation(
        db_path=str(db_path),
        output_path=str(output_path),
        capacities=[2, 3],
        watchlists={
            "singapore": str(singapore_path),
            "japan": str(japan_path),
        },
        as_of_utc=datetime(2026, 1, 11, tzinfo=UTC).isoformat(),
    )

    assert output_path.exists()
    assert report["summary"]["snapshot_review_count"] == 4
    assert report["summary"]["region_count"] == 2

    regions = {r["region"]: r for r in report["regions"]}
    sg = regions["singapore"]
    assert sg["status"] == "ok"
    assert sg["labeled_count"] == 3
    assert sg["positive_count"] == 2
    assert sg["threshold_recommendation"]["recommended_threshold"] is not None
    assert sg["threshold_recommendation"]["support"]["labeled_count"] == 3
    assert len(sg["ops_aware"]["ops_thresholds"]) == 2

    jp = regions["japan"]
    assert jp["status"] == "ok"
    assert jp["labeled_count"] == 2


def test_review_feedback_regression_checks(tmp_path):
    db_path = tmp_path / "mpol.duckdb"
    init_schema(str(db_path))

    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            INSERT INTO vessel_reviews (mmsi, review_tier, handoff_state, reviewed_by, reviewed_at)
            VALUES
                ('100', 'confirmed', 'handoff_completed', 'analyst', '2026-01-10T00:00:00Z'),
                ('200', 'cleared', 'closed', 'analyst', '2026-01-10T00:10:00Z')
            """
        )
    finally:
        con.close()

    region_path = tmp_path / "region.parquet"
    _write_watchlist(
        region_path,
        {
            "mmsi": ["200", "100"],
            "confidence": [0.95, 0.40],
        },
    )

    baseline = {
        "regions": [
            {
                "region": "singapore",
                "status": "ok",
                "ops_aware": {
                    "precision_at_primary_capacity": 1.0,
                    "recall_at_primary_capacity": 1.0,
                },
            }
        ]
    }
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(json.dumps(baseline))

    report = run_review_feedback_evaluation(
        db_path=str(db_path),
        output_path=str(tmp_path / "out.json"),
        capacities=[1],
        watchlists={"singapore": str(region_path)},
        as_of_utc="2026-01-11T00:00:00Z",
        baseline_report_path=str(baseline_path),
    )

    checks = report["drift_regression_checks"]
    assert checks["baseline_used"] is True
    assert checks["overall_pass"] is False
    assert any(
        c["metric"] == "precision_at_primary_capacity" and c["passed"] is False
        for c in checks["checks"]
    )
