"""Tests for src/analysis/monitor.py — drift monitor."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from pipeline.src.analysis.monitor import (
    SEVERITY_CRITICAL,
    SEVERITY_OK,
    SEVERITY_WARNING,
    DriftAlert,
    _make_alert,
    _severity,
    alerts_to_dict,
    check_ais_gap_rate,
    check_concept_drift_proxy,
    check_flag_distribution,
    check_watchlist_score_shift,
    run_drift_checks,
)
from pipeline.src.ingest.schema import init_schema


@pytest.fixture
def monitor_db(tmp_path):
    db_path = str(tmp_path / "monitor.duckdb")
    init_schema(db_path)
    return db_path


def _seed_ais(db_path: str, mmsi: str, timestamps: list[datetime]) -> None:
    con = duckdb.connect(db_path)
    try:
        for ts in timestamps:
            con.execute(
                "INSERT OR IGNORE INTO ais_positions (mmsi, timestamp, lat, lon) "
                "VALUES (?, ?, 1.3, 103.8)",
                [mmsi, ts],
            )
    finally:
        con.close()


def _seed_vessel_features(db_path: str, mmsi: str, high_risk_flag_ratio: float) -> None:
    con = duckdb.connect(db_path)
    try:
        con.execute("INSERT OR IGNORE INTO vessel_meta (mmsi, flag) VALUES (?, 'XX')", [mmsi])
        con.execute(
            "INSERT OR REPLACE INTO vessel_features (mmsi, high_risk_flag_ratio, sanctions_distance) "
            "VALUES (?, ?, 99)",
            [mmsi, high_risk_flag_ratio],
        )
    finally:
        con.close()


def _seed_reviews(db_path: str, entries: list[tuple[str, str, str]]) -> None:
    """Seed vessel_reviews. entries: [(mmsi, tier, reviewed_at_iso)]"""
    con = duckdb.connect(db_path)
    try:
        for mmsi, tier, reviewed_at in entries:
            con.execute(
                "INSERT INTO vessel_reviews (mmsi, review_tier, handoff_state, reviewed_by, reviewed_at) "
                "VALUES (?, ?, 'under_review', 'analyst', ?)",
                [mmsi, tier, reviewed_at],
            )
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------------


def test_severity_ok():
    assert _severity(0.05, 0.30, 0.60) == SEVERITY_OK


def test_severity_warning():
    assert _severity(0.35, 0.30, 0.60) == SEVERITY_WARNING


def test_severity_critical():
    assert _severity(0.65, 0.30, 0.60) == SEVERITY_CRITICAL


def test_severity_negative_change():
    # Large negative change should also trigger warning/critical
    assert _severity(-0.40, 0.30, 0.60) == SEVERITY_WARNING
    assert _severity(-0.70, 0.30, 0.60) == SEVERITY_CRITICAL


def test_make_alert_fields():
    alert = _make_alert(
        "test_check",
        "test_metric",
        current=1.3,
        reference=1.0,
        warn=0.10,
        crit=0.30,
    )
    assert alert.check_name == "test_check"
    assert alert.metric_name == "test_metric"
    assert alert.current_value == pytest.approx(1.3)
    assert alert.reference_value == pytest.approx(1.0)
    assert alert.severity == SEVERITY_WARNING  # 30% change == warning threshold
    assert isinstance(alert.checked_at, str)


def test_drift_alert_str_contains_severity():
    alert = DriftAlert(
        check_name="foo",
        severity=SEVERITY_WARNING,
        metric_name="bar",
        current_value=0.5,
        reference_value=0.4,
        relative_change=0.25,
        threshold_warning=0.1,
        threshold_critical=0.3,
        message="test",
        checked_at="2026-04-04T00:00:00+00:00",
    )
    assert "WARNING" in str(alert)
    assert "foo" in str(alert)


def test_alerts_to_dict_structure(monitor_db):
    as_of = datetime(2026, 4, 4, tzinfo=UTC)
    alerts = run_drift_checks(monitor_db, as_of=as_of)
    d = alerts_to_dict(alerts)
    assert "checked_at" in d
    assert "summary" in d
    assert "alerts" in d
    assert isinstance(d["summary"]["ok"], int)
    assert isinstance(d["alerts"], list)


# ---------------------------------------------------------------------------
# check_ais_gap_rate
# ---------------------------------------------------------------------------


def test_ais_gap_rate_empty_db(monitor_db):
    as_of = datetime(2026, 4, 4, tzinfo=UTC)
    con = duckdb.connect(monitor_db, read_only=True)
    try:
        alert = check_ais_gap_rate(con, as_of)
    finally:
        con.close()
    assert alert.check_name == "ais_gap_rate"
    assert alert.severity == SEVERITY_OK  # 0 vs 0 → no change


def test_ais_gap_rate_detects_recent_spike(monitor_db):
    as_of = datetime(2026, 4, 4, tzinfo=UTC)

    # Baseline: 2 gaps in 30–90 day window for 2 vessels
    # Recent: 10 gaps in last 30 days for same vessels
    baseline_base = as_of - timedelta(days=80)
    recent_base = as_of - timedelta(days=25)

    for mmsi in ["G1", "G2"]:
        baseline_ts = [baseline_base + timedelta(hours=8 * i) for i in range(3)]
        recent_ts = [recent_base + timedelta(hours=8 * i) for i in range(11)]
        _seed_ais(monitor_db, mmsi, baseline_ts + recent_ts)

    con = duckdb.connect(monitor_db, read_only=True)
    try:
        alert = check_ais_gap_rate(con, as_of)
    finally:
        con.close()

    assert alert.check_name == "ais_gap_rate"
    # Recent rate should be higher than baseline
    assert alert.current_value >= alert.reference_value


# ---------------------------------------------------------------------------
# check_flag_distribution
# ---------------------------------------------------------------------------


def test_flag_distribution_insufficient_data(monitor_db):
    con = duckdb.connect(monitor_db, read_only=True)
    try:
        alert = check_flag_distribution(con)
    finally:
        con.close()
    assert alert.severity == SEVERITY_OK
    assert "Insufficient" in alert.message


def test_flag_distribution_ok_ratio(monitor_db):
    # Seed vessels with high_risk_flag_ratio near reference (0.35)
    for i in range(6):
        _seed_vessel_features(monitor_db, f"V{i:02d}", high_risk_flag_ratio=0.35)
    con = duckdb.connect(monitor_db, read_only=True)
    try:
        alert = check_flag_distribution(con)
    finally:
        con.close()
    assert alert.check_name == "flag_distribution"
    assert alert.severity == SEVERITY_OK


def test_flag_distribution_critical_shift(monitor_db):
    # All vessels at ratio 0.80 → large deviation from 0.35 reference
    for i in range(6):
        _seed_vessel_features(monitor_db, f"W{i:02d}", high_risk_flag_ratio=0.80)
    con = duckdb.connect(monitor_db, read_only=True)
    try:
        alert = check_flag_distribution(con)
    finally:
        con.close()
    assert alert.severity in (SEVERITY_WARNING, SEVERITY_CRITICAL)


# ---------------------------------------------------------------------------
# check_watchlist_score_shift
# ---------------------------------------------------------------------------


def test_watchlist_score_shift_insufficient(monitor_db):
    as_of = datetime(2026, 4, 4, tzinfo=UTC)
    con = duckdb.connect(monitor_db, read_only=True)
    try:
        alert = check_watchlist_score_shift(con, as_of)
    finally:
        con.close()
    assert alert.severity == SEVERITY_OK
    assert "Insufficient" in alert.message


def test_watchlist_score_shift_stable(monitor_db):
    # Seed 10 confirmed reviews evenly spread
    as_of = datetime(2026, 4, 4, tzinfo=UTC)
    entries = [
        (f"M{i:02d}", "confirmed", (as_of - timedelta(days=180 - i * 10)).isoformat())
        for i in range(10)
    ]
    _seed_reviews(monitor_db, entries)
    con = duckdb.connect(monitor_db, read_only=True)
    try:
        alert = check_watchlist_score_shift(con, as_of)
    finally:
        con.close()
    assert alert.check_name == "watchlist_score_shift"
    # All confirmed → same score on both sides → ok
    assert alert.severity == SEVERITY_OK


def test_watchlist_score_shift_detects_drop(monitor_db):
    as_of = datetime(2026, 4, 4, tzinfo=UTC)
    # Early period: all confirmed (score 0.95)
    early_entries = [
        (f"E{i:02d}", "confirmed", (as_of - timedelta(days=200 - i * 5)).isoformat())
        for i in range(8)
    ]
    # Recent period: all cleared (score 0.10)
    recent_entries = [
        (f"R{i:02d}", "cleared", (as_of - timedelta(days=30 - i * 3)).isoformat()) for i in range(8)
    ]
    _seed_reviews(monitor_db, early_entries + recent_entries)
    con = duckdb.connect(monitor_db, read_only=True)
    try:
        alert = check_watchlist_score_shift(con, as_of)
    finally:
        con.close()
    assert alert.severity in (SEVERITY_WARNING, SEVERITY_CRITICAL)


# ---------------------------------------------------------------------------
# check_concept_drift_proxy
# ---------------------------------------------------------------------------


def test_concept_drift_insufficient_data(monitor_db):
    as_of = datetime(2026, 4, 4, tzinfo=UTC)
    con = duckdb.connect(monitor_db, read_only=True)
    try:
        alert = check_concept_drift_proxy(con, as_of)
    finally:
        con.close()
    assert alert.severity == SEVERITY_OK
    assert "Insufficient" in alert.message


def test_concept_drift_stable_precision(monitor_db):
    as_of = datetime(2026, 4, 4, tzinfo=UTC)
    # Both windows: mostly confirmed → stable hit rate
    entries = []
    for i in range(6):
        entries.append(
            (
                f"CD{i:02d}",
                "confirmed",
                (as_of - timedelta(days=150 + i * 10)).isoformat(),
            )
        )
    for i in range(6):
        entries.append(
            (
                f"CD{i + 10:02d}",
                "confirmed",
                (as_of - timedelta(days=50 + i * 10)).isoformat(),
            )
        )
    _seed_reviews(monitor_db, entries)
    con = duckdb.connect(monitor_db, read_only=True)
    try:
        alert = check_concept_drift_proxy(con, as_of)
    finally:
        con.close()
    assert alert.check_name == "concept_drift_proxy"
    assert alert.severity == SEVERITY_OK


def test_concept_drift_detects_precision_drop(monitor_db):
    as_of = datetime(2026, 4, 4, tzinfo=UTC)
    # Prior window: all confirmed
    prior_entries = [
        (f"CP{i:02d}", "confirmed", (as_of - timedelta(days=120 + i * 8)).isoformat())
        for i in range(6)
    ]
    # Recent window: mostly suspect/cleared
    recent_entries = [
        (f"CR{i:02d}", "suspect", (as_of - timedelta(days=10 + i * 8)).isoformat())
        for i in range(6)
    ]
    _seed_reviews(monitor_db, prior_entries + recent_entries)
    con = duckdb.connect(monitor_db, read_only=True)
    try:
        alert = check_concept_drift_proxy(con, as_of)
    finally:
        con.close()
    assert alert.severity in (SEVERITY_WARNING, SEVERITY_CRITICAL)


# ---------------------------------------------------------------------------
# run_drift_checks (integration)
# ---------------------------------------------------------------------------


def test_run_drift_checks_returns_four_alerts(monitor_db):
    as_of = datetime(2026, 4, 4, tzinfo=UTC)
    alerts = run_drift_checks(monitor_db, as_of=as_of)
    assert len(alerts) == 4
    check_names = {a.check_name for a in alerts}
    assert check_names == {
        "ais_gap_rate",
        "flag_distribution",
        "watchlist_score_shift",
        "concept_drift_proxy",
    }


def test_run_drift_checks_all_ok_on_empty_db(monitor_db):
    as_of = datetime(2026, 4, 4, tzinfo=UTC)
    alerts = run_drift_checks(monitor_db, as_of=as_of)
    for alert in alerts:
        assert alert.severity == SEVERITY_OK, f"Expected OK but got {alert}"
