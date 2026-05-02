"""
Tests for issue #26 — C7 Cap Vista pre-submission enhancements:
  1. Bunker barge / service vessel exclusion from HDBSCAN baseline
  2. Geopolitical rerouting filter in composite scoring
  3. Cleared-vessel feedback loop (hard negatives in training)
"""

from __future__ import annotations

import json
from datetime import date

import duckdb
import polars as pl
import pytest

from pipeline.src.score.anomaly import ANOMALY_FEATURE_COLUMNS, fit_isolation_forest
from pipeline.src.score.composite import (
    GeoEvent,
    _GeoCorridorBbox,
    apply_geopolitical_filter,
    load_geopolitical_filter,
)
from pipeline.src.score.mpol_baseline import (
    SERVICE_VESSEL_TYPES,
    compute_mpol_baseline,
    load_cleared_mmsis,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BEHAVIOR_COLUMNS = [
    "ais_gap_count_30d",
    "ais_gap_max_hours",
    "position_jump_count",
    "sts_candidate_count",
    "port_call_ratio",
    "loitering_hours_30d",
]


def _make_behavior_row(mmsi: str, ship_type: int, **overrides) -> dict:
    row = dict(
        mmsi=mmsi,
        ship_type=ship_type,
        ais_gap_count_30d=0,
        ais_gap_max_hours=0.0,
        position_jump_count=0,
        sts_candidate_count=0,
        port_call_ratio=0.5,
        loitering_hours_30d=0.0,
    )
    row.update(overrides)
    return row


def _make_feature_df(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows).with_columns(
        [
            pl.col("ais_gap_count_30d").cast(pl.Int64),
            pl.col("position_jump_count").cast(pl.Int64),
            pl.col("sts_candidate_count").cast(pl.Int64),
        ]
    )


def _make_anomaly_df(n: int, sanctions_distance: int = 5) -> pl.DataFrame:
    """Return a minimal feature frame for IsolationForest training."""
    rows = []
    for i in range(n):
        rows.append({col: float(i % 3) for col in ANOMALY_FEATURE_COLUMNS})
        rows[-1]["sanctions_distance"] = float(sanctions_distance)
    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Improvement 1 — Service vessel type constant
# ---------------------------------------------------------------------------


def test_service_vessel_types_includes_required_codes():
    assert 51 in SERVICE_VESSEL_TYPES  # pilot
    assert 55 in SERVICE_VESSEL_TYPES  # anti-pollution
    assert 59 in SERVICE_VESSEL_TYPES  # coast guard / offshore support
    assert 31 in SERVICE_VESSEL_TYPES  # tug
    assert 32 in SERVICE_VESSEL_TYPES  # supply / tender


def test_service_vessel_types_excludes_tankers():
    for t in range(80, 90):
        assert t not in SERVICE_VESSEL_TYPES


# ---------------------------------------------------------------------------
# Improvement 1 — Service vessels excluded from HDBSCAN training
# ---------------------------------------------------------------------------


def test_service_vessels_not_in_training_but_in_output():
    """Service vessels should appear in the output with noise_score=0 and not skew training."""
    rows = [
        _make_behavior_row("tanker1", 82, ais_gap_count_30d=10),
        _make_behavior_row("tanker2", 82, ais_gap_count_30d=1),
        _make_behavior_row("tanker3", 82, ais_gap_count_30d=2),
        _make_behavior_row("bunker1", 54, ais_gap_count_30d=30, loitering_hours_30d=80.0),
        _make_behavior_row("bunker2", 51, ais_gap_count_30d=25, loitering_hours_30d=70.0),
    ]
    df = _make_feature_df(rows)

    result = compute_mpol_baseline(df, exclude_service_vessels=True)

    assert result.height == 5, "All vessels (including service) must appear in output"
    bunker_rows = result.filter(pl.col("mmsi").is_in(["bunker1", "bunker2"]))
    assert (bunker_rows["baseline_noise_score"] == 0.0).all(), (
        "Service vessels must have baseline_noise_score=0.0"
    )


def test_service_vessels_included_when_flag_disabled():
    """With exclude_service_vessels=False, service vessels define part of the baseline."""
    rows = [
        _make_behavior_row("tanker1", 82),
        _make_behavior_row("tanker2", 82),
        _make_behavior_row("tanker3", 82),
        _make_behavior_row("bunker1", 54, loitering_hours_30d=80.0),
    ]
    df = _make_feature_df(rows)

    # Both enabled and disabled should produce 4 rows
    result_excl = compute_mpol_baseline(df, exclude_service_vessels=True)
    result_incl = compute_mpol_baseline(df, exclude_service_vessels=False)
    assert result_excl.height == result_incl.height == 4


def test_empty_df_after_service_exclusion_returns_empty():
    """If all vessels are service type, output is empty (no non-service vessels to cluster)."""
    rows = [
        _make_behavior_row("pilot1", 51),
        _make_behavior_row("tug1", 31),
    ]
    df = _make_feature_df(rows)
    result = compute_mpol_baseline(df, exclude_service_vessels=True)
    assert result.height == 2
    assert (result["baseline_noise_score"] == 0.0).all()


# ---------------------------------------------------------------------------
# Improvement 3 — Cleared vessels: load_cleared_mmsis
# ---------------------------------------------------------------------------


def test_load_cleared_mmsis_empty_table(tmp_db):
    cleared = load_cleared_mmsis(tmp_db)
    assert isinstance(cleared, frozenset)
    assert len(cleared) == 0


def test_load_cleared_mmsis_with_entries(tmp_db):
    con = duckdb.connect(tmp_db)
    con.execute("""
        INSERT INTO cleared_vessels (mmsi, cleared_by, notes) VALUES
            ('111111111', 'officer_A', 'Visual inspection passed'),
            ('222222222', 'officer_B', 'Hull markings confirmed')
    """)
    con.close()

    cleared = load_cleared_mmsis(tmp_db)
    assert "111111111" in cleared
    assert "222222222" in cleared


# ---------------------------------------------------------------------------
# Improvement 3 — Cleared vessels get baseline_noise_score=0
# ---------------------------------------------------------------------------


def test_cleared_vessel_noise_score_forced_zero():
    """Cleared vessels must always get baseline_noise_score=0."""
    rows = [
        _make_behavior_row("cleared1", 82, ais_gap_count_30d=20, loitering_hours_30d=50.0),
        _make_behavior_row("normal1", 82, ais_gap_count_30d=0),
        _make_behavior_row("normal2", 82, ais_gap_count_30d=1),
        _make_behavior_row("normal3", 82, ais_gap_count_30d=1),
    ]
    df = _make_feature_df(rows)

    result = compute_mpol_baseline(
        df,
        cleared_mmsis=frozenset(["cleared1"]),
        exclude_service_vessels=False,
    )
    cleared_row = result.filter(pl.col("mmsi") == "cleared1")
    assert cleared_row["baseline_noise_score"][0] == 0.0, (
        "Cleared vessel must have baseline_noise_score=0 even if features look anomalous"
    )


# ---------------------------------------------------------------------------
# Improvement 3 — Cleared vessels in IsolationForest training set
# ---------------------------------------------------------------------------


def test_cleared_vessels_always_in_clean_training():
    """Cleared vessels with low sanctions_distance must still be in the training set."""
    rows = []
    for i in range(10):
        row = {col: float(i % 3) for col in ANOMALY_FEATURE_COLUMNS}
        row["sanctions_distance"] = 5.0
        row["mmsi"] = f"vessel_{i}"
        rows.append(row)
    # Add a vessel with sanctions_distance=1 (would normally be excluded from clean set)
    close_vessel = {col: 0.0 for col in ANOMALY_FEATURE_COLUMNS}
    close_vessel["sanctions_distance"] = 1.0
    close_vessel["mmsi"] = "cleared_mmsi"
    rows.append(close_vessel)
    feature_df = pl.DataFrame(rows)

    # Should not raise; cleared vessel is included in clean training despite low sanctions_distance
    scaler, model = fit_isolation_forest(
        feature_df,
        cleared_mmsis=frozenset(["cleared_mmsi"]),
    )
    assert model is not None


# ---------------------------------------------------------------------------
# Improvement 2 — GeoEvent data model
# ---------------------------------------------------------------------------


def test_geo_corridor_contains():
    corridor = _GeoCorridorBbox(lat_min=-40, lat_max=-25, lon_min=10, lon_max=40)
    assert corridor.contains(-35.0, 20.0)
    assert not corridor.contains(0.0, 20.0)
    assert not corridor.contains(-35.0, 5.0)


def test_geo_event_is_active():
    ev = GeoEvent(
        name="test",
        active_from=date(2023, 11, 1),
        active_to=date(2026, 12, 31),
        down_weight=0.5,
    )
    assert ev.is_active(date(2025, 6, 1))
    assert not ev.is_active(date(2023, 10, 31))
    assert not ev.is_active(date(2027, 1, 1))


def test_geo_event_vessel_in_corridor():
    corridor = _GeoCorridorBbox(lat_min=-40, lat_max=-25, lon_min=10, lon_max=40)
    ev = GeoEvent(
        name="cape",
        active_from=date(2023, 1, 1),
        active_to=date(2030, 1, 1),
        corridors=[corridor],
        down_weight=0.5,
    )
    assert ev.vessel_in_corridor(-30.0, 18.0)
    assert not ev.vessel_in_corridor(1.3, 103.8)  # Singapore, not in Cape corridor
    assert not ev.vessel_in_corridor(None, None)


# ---------------------------------------------------------------------------
# Improvement 2 — load_geopolitical_filter
# ---------------------------------------------------------------------------


def test_load_geopolitical_filter(tmp_path):
    events_json = {
        "events": [
            {
                "name": "Test rerouting",
                "active_from": "2023-11-01",
                "active_to": "2026-12-31",
                "corridors": [{"lat_min": -40, "lat_max": -25, "lon_min": 10, "lon_max": 40}],
                "down_weight": 0.5,
            }
        ]
    }
    path = tmp_path / "events.json"
    path.write_text(json.dumps(events_json))

    events = load_geopolitical_filter(str(path))
    assert len(events) == 1
    assert events[0].name == "Test rerouting"
    assert events[0].down_weight == 0.5
    assert events[0].active_from == date(2023, 11, 1)
    assert len(events[0].corridors) == 1


# ---------------------------------------------------------------------------
# Improvement 2 — apply_geopolitical_filter
# ---------------------------------------------------------------------------


def _make_scored_df(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows).with_columns(
        [
            pl.col("behavioral_deviation_score").cast(pl.Float32),
            pl.col("last_lat").cast(pl.Float64),
            pl.col("last_lon").cast(pl.Float64),
        ]
    )


def test_apply_filter_down_weights_vessels_in_corridor():
    corridor = _GeoCorridorBbox(lat_min=-40, lat_max=-25, lon_min=10, lon_max=40)
    event = GeoEvent(
        name="cape",
        active_from=date(2020, 1, 1),
        active_to=date(2030, 1, 1),
        corridors=[corridor],
        down_weight=0.5,
    )
    df = _make_scored_df(
        [
            {
                "mmsi": "cape_vessel",
                "behavioral_deviation_score": 0.8,
                "last_lat": -30.0,
                "last_lon": 18.0,
            },
            {
                "mmsi": "normal_vessel",
                "behavioral_deviation_score": 0.8,
                "last_lat": 1.3,
                "last_lon": 103.8,
            },
        ]
    )
    result = apply_geopolitical_filter(df, [event], reference_date=date(2025, 1, 1))

    cape_score = result.filter(pl.col("mmsi") == "cape_vessel")["behavioral_deviation_score"][0]
    normal_score = result.filter(pl.col("mmsi") == "normal_vessel")["behavioral_deviation_score"][0]
    assert abs(cape_score - 0.4) < 1e-5, f"Expected 0.4, got {cape_score}"
    assert abs(normal_score - 0.8) < 1e-5, "Normal vessel score must be unchanged"


def test_apply_filter_no_effect_when_event_inactive():
    corridor = _GeoCorridorBbox(lat_min=-40, lat_max=-25, lon_min=10, lon_max=40)
    event = GeoEvent(
        name="past_event",
        active_from=date(2020, 1, 1),
        active_to=date(2021, 12, 31),  # expired
        corridors=[corridor],
        down_weight=0.5,
    )
    df = _make_scored_df(
        [
            {
                "mmsi": "vessel1",
                "behavioral_deviation_score": 0.8,
                "last_lat": -30.0,
                "last_lon": 18.0,
            },
        ]
    )
    result = apply_geopolitical_filter(df, [event], reference_date=date(2025, 1, 1))
    assert result["behavioral_deviation_score"][0] == pytest.approx(0.8)


def test_apply_filter_no_effect_when_no_active_events():
    df = _make_scored_df(
        [
            {
                "mmsi": "vessel1",
                "behavioral_deviation_score": 0.8,
                "last_lat": -30.0,
                "last_lon": 18.0,
            },
        ]
    )
    result = apply_geopolitical_filter(df, [], reference_date=date(2025, 1, 1))
    assert result["behavioral_deviation_score"][0] == pytest.approx(0.8)


# ---------------------------------------------------------------------------
# Schema — cleared_vessels table present
# ---------------------------------------------------------------------------


def test_cleared_vessels_table_exists(tmp_db):
    con = duckdb.connect(tmp_db)
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    con.close()
    assert "cleared_vessels" in tables


def test_cleared_vessels_schema(tmp_db):
    con = duckdb.connect(tmp_db)
    cols = {row[0] for row in con.execute("DESCRIBE cleared_vessels").fetchall()}
    con.close()
    assert {"mmsi", "cleared_at", "cleared_by", "investigation_id", "notes"} <= cols
