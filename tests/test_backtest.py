import json

import polars as pl

from pipeline.src.score.backtest import run_backtest


def test_run_backtest_generates_report(tmp_path):
    watchlist_path = tmp_path / "watchlist.parquet"
    labels_path = tmp_path / "labels.csv"
    manifest_path = tmp_path / "manifest.json"
    output_path = tmp_path / "report.json"

    watchlist = pl.DataFrame(
        {
            "mmsi": ["111", "222", "333", "444", "555", "666"],
            "imo": ["IMO111", "IMO222", "IMO333", "IMO444", "IMO555", "IMO666"],
            "vessel_name": ["A", "B", "C", "D", "E", "F"],
            "vessel_type": ["Tanker", "Tanker", "Cargo", "Cargo", "Tanker", "Cargo"],
            "confidence": [0.95, 0.85, 0.75, 0.65, 0.35, 0.10],
        }
    )
    watchlist.write_parquet(watchlist_path)

    labels = pl.DataFrame(
        {
            "mmsi": ["111", "333", "999", "444", "666"],
            "imo": ["IMO111", "IMO333", "IMO999", "IMO444", "IMO666"],
            "label": ["positive", "positive", "positive", "negative", "negative"],
            "label_confidence": ["high", "medium", "high", "high", "medium"],
            "evidence_source": ["ofac_sdn", "un_list", "eu_list", "registry", "registry"],
            "evidence_url": [
                "https://ofac.example/111",
                "https://un.example/333",
                "https://eu.example/999",
                "https://registry.example/444",
                "https://registry.example/666",
            ],
        }
    )
    labels.write_csv(labels_path)

    manifest = {
        "schema_version": "1.0",
        "windows": [
            {
                "window_id": "2025q1",
                "watchlist_path": str(watchlist_path),
                "labels_path": str(labels_path),
                "start_date": "2025-01-01",
                "end_date": "2025-03-31",
                "region": "singapore",
            }
        ],
    }
    manifest_path.write_text(json.dumps(manifest))

    report = run_backtest(str(manifest_path), str(output_path), [2, 4])

    assert output_path.exists()
    assert report["schema_version"] == "1.0"

    windows = report["windows"]
    assert isinstance(windows, list) and len(windows) == 1
    w0 = windows[0]
    metrics = w0["metrics"]
    assert metrics["labeled_count"] == 4
    assert metrics["positive_count"] == 2
    assert metrics["precision_at_50"] >= 0.0
    assert metrics["precision_at_100"] >= 0.0
    assert "recommended_threshold" in w0

    source_cov = w0["source_positive_coverage"]
    assert source_cov["source_positive_total"] == 3
    assert source_cov["matched_total"] == 2
    assert source_cov["missed_total"] == 1
    assert source_cov["source_recall_in_watchlist"] > 0.0
    assert len(source_cov["matched_examples"]) == 2
    assert len(source_cov["missed_examples"]) == 1

    summary = report["summary"]
    assert summary["window_count"] == 1
    assert summary["precision_at_50"]["mean"] is not None
