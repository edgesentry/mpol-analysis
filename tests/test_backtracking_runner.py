from datetime import datetime, timedelta

import duckdb
import pyarrow as pa
import pytest

from pipeline.src.analysis.backtracking_runner import run_backtracking
from pipeline.src.graph.store import REL_SCHEMAS, write_tables
from pipeline.src.ingest.schema import init_schema


@pytest.fixture
def bt_db(tmp_path):
    db_path = str(tmp_path / "bt.duckdb")
    init_schema(db_path)
    return db_path


def _seed_confirmed(db_path: str, mmsi: str, reviewed_at: str) -> None:
    con = duckdb.connect(db_path)
    try:
        con.execute(
            "INSERT INTO vessel_reviews (mmsi, review_tier, handoff_state, reviewed_by, reviewed_at) "
            "VALUES (?, 'confirmed', 'handoff_completed', 'analyst', ?)",
            [mmsi, reviewed_at],
        )
    finally:
        con.close()


def _seed_ais(db_path: str, mmsi: str, base_dt: datetime, count: int = 5) -> None:
    con = duckdb.connect(db_path)
    try:
        for i in range(count):
            ts = base_dt - timedelta(days=30 * i)
            con.execute(
                "INSERT OR IGNORE INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status) "
                "VALUES (?, ?, 1.3, 103.8, 0.5, 0)",
                [mmsi, ts],
            )
    finally:
        con.close()


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_backtracking_empty_db(bt_db, tmp_path):
    output = str(tmp_path / "report.json")
    report = run_backtracking(bt_db, output)

    assert report["rewind"]["vessel_count"] == 0
    assert report["propagation"]["seed_count"] == 0
    assert report["regression_checks"]["pass"] is True
    assert (tmp_path / "report.json").exists()


def test_backtracking_single_confirmed_vessel(bt_db, tmp_path):
    _seed_confirmed(bt_db, "M11", "2026-03-01T00:00:00Z")
    output = str(tmp_path / "report.json")

    report = run_backtracking(bt_db, output, as_of_utc="2026-04-01T00:00:00Z", rewind_days=90)

    assert report["rewind"]["vessel_count"] == 1
    assert report["rewind"]["vessels"][0]["mmsi"] == "M11"
    assert report["propagation"]["seed_count"] == 1
    assert report["regression_checks"]["confirmed_vessel_count"] == 1
    assert report["regression_checks"]["rewind_vessel_count"] == 1
    assert report["regression_checks"]["pass"] is True


def test_backtracking_regression_check_pass(bt_db, tmp_path):
    _seed_confirmed(bt_db, "R01", "2026-02-01T00:00:00Z")
    _seed_confirmed(bt_db, "R02", "2026-02-15T00:00:00Z")
    output = str(tmp_path / "reg.json")

    report = run_backtracking(bt_db, output, as_of_utc="2026-04-01T00:00:00Z", rewind_days=30)

    rc = report["regression_checks"]
    assert rc["confirmed_vessel_count"] == 2
    assert rc["rewind_vessel_count"] == 2
    assert rc["pass"] is True


def test_backtracking_incremental_since_filter(bt_db, tmp_path):
    _seed_confirmed(bt_db, "OLD1", "2025-12-01T00:00:00Z")
    _seed_confirmed(bt_db, "NEW1", "2026-03-15T00:00:00Z")
    output = str(tmp_path / "inc.json")

    report = run_backtracking(
        bt_db,
        output,
        since_utc="2026-01-01T00:00:00Z",
        as_of_utc="2026-04-01T00:00:00Z",
        rewind_days=30,
    )

    # Only NEW1 should be in the rewind (confirmed after since_utc)
    rewound_mmsis = [v["mmsi"] for v in report["rewind"]["vessels"]]
    assert "NEW1" in rewound_mmsis
    assert "OLD1" not in rewound_mmsis
    # But propagation uses all confirmed labels (not filtered by since_utc)
    assert report["propagation"]["seed_count"] == 2


def test_backtracking_with_graph_propagation(bt_db, tmp_path):
    _seed_confirmed(bt_db, "G11", "2026-03-01T00:00:00Z")
    # vessel G22 shares owner with confirmed G11
    table = pa.table(
        {
            "src_id": ["G11", "G22"],
            "dst_id": ["company-G", "company-G"],
            "since": ["", ""],
            "until": ["", ""],
        },
        schema=REL_SCHEMAS["OWNED_BY"],
    )
    write_tables(bt_db, {"OWNED_BY": table})

    output = str(tmp_path / "graph_report.json")
    report = run_backtracking(bt_db, output, as_of_utc="2026-04-01T00:00:00Z", rewind_days=30)

    assert report["propagation"]["propagated_count"] == 1
    propagated = [v for v in report["propagation"]["vessels"] if v["hop"] > 0]
    assert len(propagated) == 1
    assert propagated[0]["mmsi"] == "G22"
    assert propagated[0]["evidence_type"] == "shared_owner"


def test_backtracking_markdown_output(bt_db, tmp_path):
    _seed_confirmed(bt_db, "MD1", "2026-03-01T00:00:00Z")
    output = str(tmp_path / "report.json")
    md_output = str(tmp_path / "report.md")

    run_backtracking(
        bt_db,
        output,
        md_output_path=md_output,
        as_of_utc="2026-04-01T00:00:00Z",
        rewind_days=30,
    )

    md_path = tmp_path / "report.md"
    assert md_path.exists()
    content = md_path.read_text()
    assert "# Backtracking Report" in content
    assert "MD1" in content
    assert "Regression Checks" in content


def test_backtracking_new_confirmed_mmsis_in_report(bt_db, tmp_path):
    _seed_confirmed(bt_db, "NC1", "2026-03-10T00:00:00Z")
    _seed_confirmed(bt_db, "NC2", "2026-03-20T00:00:00Z")
    output = str(tmp_path / "nc.json")

    report = run_backtracking(
        bt_db,
        output,
        since_utc="2026-03-01T00:00:00Z",
        as_of_utc="2026-04-01T00:00:00Z",
        rewind_days=30,
    )

    assert set(report["new_confirmed_mmsis"]) == {"NC1", "NC2"}
