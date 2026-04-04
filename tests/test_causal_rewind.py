from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from src.analysis.causal_rewind import (
    compute_monthly_snapshots,
    detect_precursor_signals,
    rewind_vessel,
    run_causal_rewind,
)
from src.ingest.schema import init_schema


@pytest.fixture
def rewind_db(tmp_path):
    db_path = str(tmp_path / "rewind.duckdb")
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


def _seed_ais(db_path: str, mmsi: str, timestamps: list[datetime]) -> None:
    con = duckdb.connect(db_path)
    try:
        for ts in timestamps:
            con.execute(
                "INSERT OR IGNORE INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status) "
                "VALUES (?, ?, 1.3, 103.8, 0.5, 0)",
                [mmsi, ts],
            )
    finally:
        con.close()


# ── Unit tests for compute_monthly_snapshots ─────────────────────────────────

def test_monthly_snapshots_empty_ais():
    import polars as pl

    confirmed_at = datetime(2026, 4, 1, tzinfo=UTC)
    empty_df = pl.DataFrame(schema={"mmsi": pl.Utf8, "timestamp": pl.Datetime("us", "UTC"),
                                    "lat": pl.Float64, "lon": pl.Float64,
                                    "sog": pl.Float32, "nav_status": pl.Int8})
    snapshots = compute_monthly_snapshots(empty_df, confirmed_at, rewind_days=60, interval_days=30)
    assert len(snapshots) == 2
    for s in snapshots:
        assert s["record_count"] == 0.0
        assert s["ais_gap_count"] == 0.0


def test_monthly_snapshots_count():
    import polars as pl

    confirmed_at = datetime(2026, 4, 1, tzinfo=UTC)
    # Create a DataFrame with 3 records spread over 90 days
    ts_list = [
        confirmed_at - timedelta(days=80),
        confirmed_at - timedelta(days=50),
        confirmed_at - timedelta(days=10),
    ]
    df = pl.DataFrame({
        "mmsi": ["X"] * 3,
        "timestamp": ts_list,
        "lat": [1.0] * 3,
        "lon": [103.0] * 3,
        "sog": [0.5] * 3,
        "nav_status": [0] * 3,
    }).with_columns(pl.col("timestamp").dt.convert_time_zone("UTC"))

    snapshots = compute_monthly_snapshots(df, confirmed_at, rewind_days=90, interval_days=30)
    assert len(snapshots) == 3
    total_records = sum(s["record_count"] for s in snapshots)
    assert total_records == 3.0


# ── Unit tests for detect_precursor_signals ───────────────────────────────────

def test_no_precursor_signals_uniform_data():
    # All windows have the same gap count — no uplift
    snapshots = [
        {"days_before_confirmation": d, "ais_gap_count": 2.0,
         "sts_candidate_proxy": 1.0, "low_sog_fraction": 0.2}
        for d in [300, 240, 180, 120, 90, 60, 30]
    ]
    signals = detect_precursor_signals(snapshots)
    assert signals == []


def test_precursor_signal_detected_on_gap_uplift():
    # Baseline (days > 90): 0 gaps. Precursor (days <= 90): high gap count.
    snapshots = (
        [{"days_before_confirmation": d, "ais_gap_count": 0.0,
          "sts_candidate_proxy": 0.0, "low_sog_fraction": 0.0}
         for d in [330, 300, 270, 240, 210, 180, 150, 120, 91]]
        +
        [{"days_before_confirmation": d, "ais_gap_count": 10.0,
          "sts_candidate_proxy": 0.0, "low_sog_fraction": 0.0}
         for d in [60, 30, 1]]
    )
    signals = detect_precursor_signals(snapshots)
    features_detected = {s["feature"] for s in signals}
    assert "ais_gap_count" in features_detected
    signal = next(s for s in signals if s["feature"] == "ais_gap_count")
    assert signal["recent_value"] == pytest.approx(10.0, abs=1e-3)
    assert signal["baseline_value"] == pytest.approx(0.0, abs=1e-3)
    assert signal["uplift_ratio"] > 1.5


def test_precursor_signal_not_triggered_below_threshold():
    # Uplift < 1.5 should not trigger
    snapshots = (
        [{"days_before_confirmation": d, "ais_gap_count": 2.0,
          "sts_candidate_proxy": 0.0, "low_sog_fraction": 0.0}
         for d in [200, 150, 100]]
        +
        [{"days_before_confirmation": d, "ais_gap_count": 2.5,
          "sts_candidate_proxy": 0.0, "low_sog_fraction": 0.0}
         for d in [60, 30]]
    )
    signals = detect_precursor_signals(snapshots)
    assert signals == []


# ── Integration tests for rewind_vessel ──────────────────────────────────────

def test_rewind_vessel_no_ais_data(rewind_db):
    confirmed_at = datetime(2026, 4, 1, tzinfo=UTC)
    result = rewind_vessel(rewind_db, "Z99", confirmed_at, rewind_days=90)

    assert result["mmsi"] == "Z99"
    assert result["ais_records_scanned"] == 0
    assert result["rewind_days"] == 90
    assert result["precursor_signals"] == []
    assert len(result["monthly_snapshots"]) == 3  # 90 days / 30-day windows


def test_rewind_vessel_detects_precursor_signal(rewind_db):
    confirmed_at = datetime(2026, 4, 1, tzinfo=UTC)

    # Baseline period (days -365 to -91): 1 record per window start → no gaps
    baseline_start = confirmed_at - timedelta(days=365)
    baseline_timestamps = [
        baseline_start + timedelta(days=30 * i)
        for i in range(9)
    ]

    # Precursor period (days -90 to 0): 3 records with 8-hour gaps per window
    precursor_start = confirmed_at - timedelta(days=90)
    precursor_timestamps = []
    for window_offset in range(3):
        window_base = precursor_start + timedelta(days=30 * window_offset)
        precursor_timestamps += [
            window_base,
            window_base + timedelta(hours=8),
            window_base + timedelta(hours=16),
        ]

    _seed_ais(rewind_db, "PREC1", baseline_timestamps + precursor_timestamps)
    result = rewind_vessel(rewind_db, "PREC1", confirmed_at, rewind_days=365)

    assert result["ais_records_scanned"] > 0
    features_flagged = {s["feature"] for s in result["precursor_signals"]}
    assert "ais_gap_count" in features_flagged


def test_rewind_vessel_12month_snapshot_count(rewind_db):
    confirmed_at = datetime(2026, 4, 1, tzinfo=UTC)
    result = rewind_vessel(rewind_db, "CNT1", confirmed_at, rewind_days=365)
    # 365 days / 30-day windows = 12 full windows + 1 partial (5 days)
    assert len(result["monthly_snapshots"]) == 13


# ── Integration tests for run_causal_rewind ───────────────────────────────────

def test_run_causal_rewind_empty_db(rewind_db, tmp_path):
    output = str(tmp_path / "rewind_out.json")
    report = run_causal_rewind(rewind_db, output)

    assert report["vessel_count"] == 0
    assert report["vessels"] == []
    assert (tmp_path / "rewind_out.json").exists()


def test_run_causal_rewind_writes_artifact(rewind_db, tmp_path):
    _seed_confirmed(rewind_db, "ART1", "2026-03-01T00:00:00Z")
    output = str(tmp_path / "rewind_art.json")
    report = run_causal_rewind(
        rewind_db, output, as_of_utc="2026-04-01T00:00:00Z", rewind_days=90
    )

    assert report["vessel_count"] == 1
    assert report["vessels"][0]["mmsi"] == "ART1"
    assert (tmp_path / "rewind_art.json").exists()


def test_run_causal_rewind_mmsis_filter(rewind_db, tmp_path):
    _seed_confirmed(rewind_db, "F11", "2026-02-01T00:00:00Z")
    _seed_confirmed(rewind_db, "F22", "2026-02-01T00:00:00Z")
    output = str(tmp_path / "filt.json")
    report = run_causal_rewind(
        rewind_db, output, mmsis=["F11"], as_of_utc="2026-04-01T00:00:00Z"
    )
    assert report["vessel_count"] == 1
    assert report["vessels"][0]["mmsi"] == "F11"


def test_run_causal_rewind_deterministic(rewind_db, tmp_path):
    """Same inputs must produce identical output (determinism requirement)."""
    _seed_confirmed(rewind_db, "DET1", "2026-03-15T00:00:00Z")
    confirmed_at = datetime(2026, 3, 15, tzinfo=UTC)
    ts_list = [confirmed_at - timedelta(days=30 * i) for i in range(10)]
    _seed_ais(rewind_db, "DET1", ts_list)

    out1 = str(tmp_path / "det1.json")
    out2 = str(tmp_path / "det2.json")
    r1 = run_causal_rewind(rewind_db, out1, as_of_utc="2026-04-01T00:00:00Z", rewind_days=365)
    r2 = run_causal_rewind(rewind_db, out2, as_of_utc="2026-04-01T00:00:00Z", rewind_days=365)

    assert r1["vessels"][0]["precursor_signals"] == r2["vessels"][0]["precursor_signals"]
    assert len(r1["vessels"][0]["monthly_snapshots"]) == len(r2["vessels"][0]["monthly_snapshots"])
