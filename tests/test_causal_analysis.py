"""Tests for src/analysis/causal.py — unknown-unknown causal reasoner."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from pipeline.src.analysis.causal import (
    CausalEvidence,
    CausalSignal,
    UnknownUnknownCandidate,
    _compute_signal_score,
    score_unknown_unknowns,
)
from pipeline.src.ingest.schema import init_schema


@pytest.fixture
def causal_db(tmp_path):
    db_path = str(tmp_path / "causal.duckdb")
    init_schema(db_path)
    return db_path


def _seed_vessel(
    db_path: str, mmsi: str, sanctions_distance: int = 99, sts_count: int = 0, flag_changes: int = 0
) -> None:
    con = duckdb.connect(db_path)
    try:
        con.execute(
            "INSERT OR IGNORE INTO vessel_meta (mmsi, flag) VALUES (?, 'XX')",
            [mmsi],
        )
        con.execute(
            """
            INSERT OR REPLACE INTO vessel_features
            (mmsi, sanctions_distance, sts_candidate_count, flag_changes_2y,
             ais_gap_count_30d)
            VALUES (?, ?, ?, ?, 0)
            """,
            [mmsi, sanctions_distance, sts_count, flag_changes],
        )
    finally:
        con.close()


def _seed_ais_gaps(
    db_path: str, mmsi: str, as_of: datetime, n_recent: int = 0, n_baseline: int = 0
) -> None:
    """Seed AIS records that produce the specified number of >6h gaps."""
    con = duckdb.connect(db_path)
    try:
        timestamps = []
        # Recent window: n_recent gaps, each 8h apart
        recent_base = as_of - timedelta(days=25)
        for i in range(n_recent + 1):
            timestamps.append(recent_base + timedelta(hours=8 * i))
        # Baseline window (30–90 days ago): n_baseline gaps
        baseline_base = as_of - timedelta(days=80)
        for i in range(n_baseline + 1):
            timestamps.append(baseline_base + timedelta(hours=8 * i))
        for ts in timestamps:
            con.execute(
                "INSERT OR IGNORE INTO ais_positions (mmsi, timestamp, lat, lon) "
                "VALUES (?, ?, 1.3, 103.8)",
                [mmsi, ts],
            )
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------------


def test_compute_signal_score_empty():
    assert _compute_signal_score([]) == 0.0


def test_compute_signal_score_single_signal():
    signal = CausalSignal(
        feature="ais_gap_count", recent_value=10.0, baseline_value=1.0, uplift_ratio=10.0
    )
    score = _compute_signal_score([signal])
    assert 0.0 < score <= 1.0


def test_compute_signal_score_caps_at_one():
    signals = [
        CausalSignal(feature="f", recent_value=100.0, baseline_value=1.0, uplift_ratio=1000.0),
    ]
    assert _compute_signal_score(signals) == 1.0


def test_candidate_prompt_context_no_evidence():
    candidate = UnknownUnknownCandidate(
        mmsi="123456789",
        causal_score=0.5,
        matching_signals=[CausalSignal("ais_gap_count", 5.0, 0.5, 10.0)],
        causal_evidence=[],
    )
    ctx = candidate.prompt_context()
    assert "SHADOW SIGNAL" in ctx
    assert "AIS disappearances" in ctx
    assert "investigative lead" in ctx


def test_candidate_prompt_context_with_evidence():
    evidence = CausalEvidence(
        regime="OFAC_Russia",
        regime_label="OFAC Russia",
        att_estimate=1.23,
        att_ci_lower=0.5,
        att_ci_upper=2.0,
        p_value=0.03,
        is_significant=True,
    )
    candidate = UnknownUnknownCandidate(
        mmsi="123456789",
        causal_score=0.7,
        matching_signals=[CausalSignal("ais_gap_count", 8.0, 1.0, 8.0)],
        causal_evidence=[evidence],
    )
    ctx = candidate.prompt_context()
    assert "OFAC Russia" in ctx
    assert "ATT=+1.230" in ctx
    assert "significant" in ctx


def test_causal_evidence_prompt_context_not_significant():
    ev = CausalEvidence(
        regime="UN_DPRK",
        regime_label="UN DPRK",
        att_estimate=-0.5,
        att_ci_lower=-2.0,
        att_ci_upper=1.0,
        p_value=0.45,
        is_significant=False,
    )
    ctx = ev.to_prompt_context()
    assert "not-significant" in ctx
    assert "UN DPRK" in ctx


# ---------------------------------------------------------------------------
# Integration tests for score_unknown_unknowns
# ---------------------------------------------------------------------------


def test_score_empty_db(causal_db):
    candidates = score_unknown_unknowns(db_path=causal_db)
    assert candidates == []


def test_score_no_unsanctioned_vessels(causal_db):
    # All vessels are sanctioned (distance <= 2)
    _seed_vessel(causal_db, "AAA111", sanctions_distance=1)
    candidates = score_unknown_unknowns(db_path=causal_db)
    assert candidates == []


def test_score_unsanctioned_vessel_no_signals(causal_db):
    # Unsanctioned vessel but no elevated signals → not in output
    _seed_vessel(causal_db, "BBB222", sanctions_distance=99, sts_count=0, flag_changes=0)
    candidates = score_unknown_unknowns(db_path=causal_db, min_signals=1)
    assert all(c.mmsi != "BBB222" for c in candidates)


def test_score_sts_signal_surfaces_candidate(causal_db):
    # Vessel with ≥3 STS candidates should be surfaced
    _seed_vessel(causal_db, "STS001", sanctions_distance=99, sts_count=5)
    as_of = datetime(2026, 4, 4, tzinfo=UTC)
    candidates = score_unknown_unknowns(db_path=causal_db, as_of=as_of)
    mmsis = [c.mmsi for c in candidates]
    assert "STS001" in mmsis
    candidate = next(c for c in candidates if c.mmsi == "STS001")
    features = {s.feature for s in candidate.matching_signals}
    assert "sts_candidate_count" in features


def test_score_flag_change_signal(causal_db):
    # ≥2 flag changes → surfaced
    _seed_vessel(causal_db, "FLAG01", sanctions_distance=99, flag_changes=3)
    as_of = datetime(2026, 4, 4, tzinfo=UTC)
    candidates = score_unknown_unknowns(db_path=causal_db, as_of=as_of)
    mmsis = [c.mmsi for c in candidates]
    assert "FLAG01" in mmsis
    candidate = next(c for c in candidates if c.mmsi == "FLAG01")
    features = {s.feature for s in candidate.matching_signals}
    assert "flag_changes_2y" in features


def test_score_ais_gap_uplift(causal_db):
    # Vessel with high recent AIS gap rate vs baseline → surfaced
    as_of = datetime(2026, 4, 4, tzinfo=UTC)
    _seed_vessel(causal_db, "GAP001", sanctions_distance=99)
    # Seed: 5 gaps in recent window, 0 in baseline
    _seed_ais_gaps(causal_db, "GAP001", as_of, n_recent=5, n_baseline=0)
    candidates = score_unknown_unknowns(db_path=causal_db, as_of=as_of)
    mmsis = [c.mmsi for c in candidates]
    assert "GAP001" in mmsis
    candidate = next(c for c in candidates if c.mmsi == "GAP001")
    features = {s.feature for s in candidate.matching_signals}
    assert "ais_gap_count" in features


def test_score_sorted_descending(causal_db):
    as_of = datetime(2026, 4, 4, tzinfo=UTC)
    _seed_vessel(causal_db, "HIGH01", sanctions_distance=99, sts_count=10, flag_changes=5)
    _seed_vessel(causal_db, "LOW01", sanctions_distance=99, sts_count=3)
    candidates = score_unknown_unknowns(db_path=causal_db, as_of=as_of)
    scores = [c.causal_score for c in candidates]
    assert scores == sorted(scores, reverse=True)


def test_score_attaches_causal_evidence(causal_db):
    from pipeline.src.score.causal_sanction import CausalEffect

    _seed_vessel(causal_db, "EVI001", sanctions_distance=99, sts_count=5)
    as_of = datetime(2026, 4, 4, tzinfo=UTC)

    effect = CausalEffect(
        regime="OFAC_Iran",
        label="OFAC Iran",
        n_treated=10,
        n_control=50,
        att_estimate=2.5,
        att_ci_lower=0.8,
        att_ci_upper=4.2,
        p_value=0.02,
        is_significant=True,
        calibrated_weight=0.45,
    )
    candidates = score_unknown_unknowns(db_path=causal_db, causal_effects=[effect], as_of=as_of)
    evi_candidates = [c for c in candidates if c.mmsi == "EVI001"]
    assert len(evi_candidates) == 1
    assert len(evi_candidates[0].causal_evidence) == 1
    assert evi_candidates[0].causal_evidence[0].regime == "OFAC_Iran"


def test_score_negative_att_not_attached(causal_db):
    """Effects with negative ATT (sanction → fewer gaps) should not be attached."""
    from pipeline.src.score.causal_sanction import CausalEffect

    _seed_vessel(causal_db, "NEG001", sanctions_distance=99, sts_count=5)
    as_of = datetime(2026, 4, 4, tzinfo=UTC)

    effect = CausalEffect(
        regime="OFAC_Iran",
        label="OFAC Iran",
        n_treated=10,
        n_control=50,
        att_estimate=-1.0,
        att_ci_lower=-3.0,
        att_ci_upper=0.5,
        p_value=0.04,
        is_significant=True,
        calibrated_weight=0.35,
    )
    candidates = score_unknown_unknowns(db_path=causal_db, causal_effects=[effect], as_of=as_of)
    neg_candidates = [c for c in candidates if c.mmsi == "NEG001"]
    if neg_candidates:
        assert neg_candidates[0].causal_evidence == []
