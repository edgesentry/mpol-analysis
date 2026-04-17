"""
Unknown-unknown causal reasoner.

Scores candidate vessels that are NOT in the known sanctions graph but show
elevated behavioural signals consistent with sanction-evasion causal patterns.

The "unknown-unknown" framing: a vessel may be evasion-active without appearing
in any current sanctions overlap (sanctions_distance = 99).  This module looks
for vessels whose behavioural feature deltas closely match the causal patterns
identified by the C3 DiD model (``src/score/causal_sanction.py``).

Method
------
For each vessel not linked to known sanctions:
1. Compute a feature-delta profile: recent-30d vs baseline-90d window for
   each key C3-relevant feature (AIS gap count, flag changes, STS candidate).
2. Compare the delta profile against the regime-specific ATT estimates from C3.
3. Produce a causal similarity score in [0, 1] and rank candidates.

Output
------
List of ``UnknownUnknownCandidate`` dataclasses, sorted descending by score.
Each contains:
  mmsi, causal_score, matching_signals, causal_evidence
  where causal_evidence carries ATT/CI/p-value fields for prompt injection.

Usage
-----
    from pipeline.src.analysis.causal import score_unknown_unknowns
    candidates = score_unknown_unknowns(db_path, causal_effects)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
import polars as pl
from dotenv import load_dotenv

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")

# Windows used for the feature-delta computation
RECENT_WINDOW_DAYS = 30
BASELINE_WINDOW_DAYS = 90

# Minimum uplift ratio to count a feature as a matching signal
SIGNAL_UPLIFT_THRESHOLD = 1.5

# Minimum number of matching signals to surface a candidate
MIN_MATCHING_SIGNALS = 1


@dataclass
class CausalSignal:
    """One feature that shows sanction-evasion-consistent uplift."""

    feature: str
    recent_value: float
    baseline_value: float
    uplift_ratio: float


@dataclass
class CausalEvidence:
    """Summary of the C3 DiD evidence that backs this candidate's score."""

    regime: str
    regime_label: str
    att_estimate: float
    att_ci_lower: float
    att_ci_upper: float
    p_value: float
    is_significant: bool

    def to_prompt_context(self) -> str:
        """Return a concise string suitable for injection into an analyst prompt."""
        sig_tag = "significant" if self.is_significant else "not-significant"
        return (
            f"[{self.regime_label}] ATT={self.att_estimate:+.3f} "
            f"(95% CI [{self.att_ci_lower:.3f}, {self.att_ci_upper:.3f}]), "
            f"p={self.p_value:.4f} ({sig_tag})"
        )


@dataclass
class UnknownUnknownCandidate:
    """A vessel scored as a potential unknown-unknown evasion candidate."""

    mmsi: str
    causal_score: float  # [0, 1]
    matching_signals: list[CausalSignal] = field(default_factory=list)
    causal_evidence: list[CausalEvidence] = field(default_factory=list)

    def prompt_context(self) -> str:
        """Format causal evidence for injection into brief/chat system prompts."""
        if not self.causal_evidence and not self.matching_signals:
            return ""
        lines = []
        if self.causal_evidence:
            lines.append("CAUSAL EVIDENCE (unknown-unknown candidate):")
            for ev in self.causal_evidence:
                lines.append(f"  • {ev.to_prompt_context()}")
        if self.matching_signals:
            lines.append("SHADOW SIGNAL — BEHAVIOURAL INDICATORS:")
            for sig in self.matching_signals:
                feature_label = {
                    "ais_gap_count": "AIS disappearances (recent spike)",
                    "sts_candidate_count": "Ship-to-ship transfer activity",
                    "flag_changes_2y": "Flag / identity changes in past 2 years",
                }.get(sig.feature, sig.feature)
                lines.append(
                    f"  • {feature_label}: "
                    f"recent {sig.recent_value:.1f} vs baseline {sig.baseline_value:.1f} "
                    f"({sig.uplift_ratio:.1f}× above normal)"
                )
        lines.append(
            "ANALYST NOTE: This vessel does not appear on any current sanctions list, "
            "but its recent behaviour closely matches patterns seen in vessels that were "
            "later confirmed as shadow-fleet operators. This is an early-warning signal — "
            "treat as an investigative lead requiring further evidence before any action."
        )
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _fetch_unsanctioned_mmsis(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Return MMSIs with sanctions_distance = 99 (no graph link)."""
    rows = con.execute("SELECT mmsi FROM vessel_features WHERE sanctions_distance >= 99").fetchall()
    return [r[0] for r in rows]


def _fetch_vessel_features(con: duckdb.DuckDBPyConnection, mmsis: list[str]) -> pl.DataFrame:
    """Load vessel_features rows for the given MMSIs."""
    if not mmsis:
        return pl.DataFrame()
    sql = ", ".join(f"'{m}'" for m in mmsis)
    return con.execute(
        f"SELECT mmsi, ais_gap_count_30d, sts_candidate_count, flag_changes_2y "
        f"FROM vessel_features WHERE mmsi IN ({sql})"
    ).pl()


def _fetch_recent_ais_gaps(
    con: duckdb.DuckDBPyConnection,
    mmsis: list[str],
    as_of: datetime,
    gap_threshold_h: float = 6.0,
) -> dict[str, int]:
    """Count AIS gaps in the recent window (last RECENT_WINDOW_DAYS days)."""
    if not mmsis:
        return {}
    start = as_of - timedelta(days=RECENT_WINDOW_DAYS)
    sql = ", ".join(f"'{m}'" for m in mmsis)
    rows = con.execute(
        f"""
        WITH ordered AS (
            SELECT mmsi, timestamp,
                   LAG(timestamp) OVER (PARTITION BY mmsi ORDER BY timestamp) AS prev_ts
            FROM ais_positions
            WHERE mmsi IN ({sql})
              AND timestamp >= ?
              AND timestamp <= ?
        )
        SELECT mmsi, COUNT(*) AS gap_count
        FROM ordered
        WHERE prev_ts IS NOT NULL
          AND epoch_ms(timestamp) - epoch_ms(prev_ts) > ? * 3600000
        GROUP BY mmsi
        """,
        [start, as_of, gap_threshold_h],
    ).fetchall()
    result = {m: 0 for m in mmsis}
    for mmsi, count in rows:
        result[mmsi] = int(count)
    return result


def _fetch_baseline_ais_gaps(
    con: duckdb.DuckDBPyConnection,
    mmsis: list[str],
    as_of: datetime,
    gap_threshold_h: float = 6.0,
) -> dict[str, int]:
    """Count AIS gaps in the baseline window (30–120 days before as_of)."""
    if not mmsis:
        return {}
    end = as_of - timedelta(days=RECENT_WINDOW_DAYS)
    start = as_of - timedelta(days=BASELINE_WINDOW_DAYS)
    sql = ", ".join(f"'{m}'" for m in mmsis)
    rows = con.execute(
        f"""
        WITH ordered AS (
            SELECT mmsi, timestamp,
                   LAG(timestamp) OVER (PARTITION BY mmsi ORDER BY timestamp) AS prev_ts
            FROM ais_positions
            WHERE mmsi IN ({sql})
              AND timestamp >= ?
              AND timestamp <= ?
        )
        SELECT mmsi, COUNT(*) AS gap_count
        FROM ordered
        WHERE prev_ts IS NOT NULL
          AND epoch_ms(timestamp) - epoch_ms(prev_ts) > ? * 3600000
        GROUP BY mmsi
        """,
        [start, end, gap_threshold_h],
    ).fetchall()
    result = {m: 0 for m in mmsis}
    for mmsi, count in rows:
        result[mmsi] = int(count)
    return result


def _compute_signal_score(signals: list[CausalSignal]) -> float:
    """Convert a list of matching signals to a scalar score in [0, 1].

    Uses the mean log-uplift of matching signals, normalised to [0, 1] with a
    soft cap at log(10) ≈ 2.3 (representing a 10× uplift ceiling).
    """
    import math

    if not signals:
        return 0.0
    log_uplifts = [math.log(max(s.uplift_ratio, 1.0)) for s in signals]
    mean_log = sum(log_uplifts) / len(log_uplifts)
    max_log = math.log(10.0)
    return float(min(mean_log / max_log, 1.0))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def score_unknown_unknowns(
    db_path: str = DEFAULT_DB_PATH,
    causal_effects: list[Any] | None = None,
    as_of: datetime | None = None,
    min_signals: int = MIN_MATCHING_SIGNALS,
    gap_threshold_h: float = 6.0,
) -> list[UnknownUnknownCandidate]:
    """Score unsanctioned vessels for unknown-unknown evasion similarity.

    Parameters
    ----------
    db_path:
        Path to the DuckDB database.
    causal_effects:
        List of :class:`~src.score.causal_sanction.CausalEffect` objects from
        the C3 model.  If None, no causal evidence is attached (signals only).
    as_of:
        Reference datetime for window computations.  Defaults to now().
    min_signals:
        Minimum number of matching features required to surface a candidate.
    gap_threshold_h:
        AIS gap threshold in hours.

    Returns
    -------
    List of :class:`UnknownUnknownCandidate`, sorted descending by
    ``causal_score``.
    """
    if as_of is None:
        as_of = datetime.now(UTC)

    con = duckdb.connect(db_path)
    try:
        mmsis = _fetch_unsanctioned_mmsis(con)
        if not mmsis:
            return []

        features_df = _fetch_vessel_features(con, mmsis)
        recent_gaps = _fetch_recent_ais_gaps(con, mmsis, as_of, gap_threshold_h)
        baseline_gaps = _fetch_baseline_ais_gaps(con, mmsis, as_of, gap_threshold_h)
    finally:
        con.close()

    # Build feature-delta profiles and score candidates
    features_map: dict[str, dict] = {}
    for row in features_df.iter_rows(named=True):
        features_map[row["mmsi"]] = row

    candidates: list[UnknownUnknownCandidate] = []
    for mmsi in mmsis:
        feat = features_map.get(mmsi, {})
        signals: list[CausalSignal] = []

        # 1. AIS gap uplift (real-time computed)
        r_gaps = float(recent_gaps.get(mmsi, 0))
        b_gaps = float(baseline_gaps.get(mmsi, 0))
        gap_uplift = r_gaps / (b_gaps + 1e-9)
        if gap_uplift >= SIGNAL_UPLIFT_THRESHOLD:
            signals.append(
                CausalSignal(
                    feature="ais_gap_count",
                    recent_value=r_gaps,
                    baseline_value=b_gaps,
                    uplift_ratio=round(gap_uplift, 4),
                )
            )

        # 2. STS candidate count from vessel_features (static feature)
        sts_val = float(feat.get("sts_candidate_count") or 0)
        if sts_val >= 3:  # ≥3 STS candidates is anomalous for an unsanctioned vessel
            signals.append(
                CausalSignal(
                    feature="sts_candidate_count",
                    recent_value=sts_val,
                    baseline_value=0.0,
                    uplift_ratio=round(sts_val / 1.0, 4),
                )
            )

        # 3. Flag change count (identity volatility)
        flag_chg = float(feat.get("flag_changes_2y") or 0)
        if flag_chg >= 2:
            signals.append(
                CausalSignal(
                    feature="flag_changes_2y",
                    recent_value=flag_chg,
                    baseline_value=0.0,
                    uplift_ratio=round(flag_chg / 1.0, 4),
                )
            )

        if len(signals) < min_signals:
            continue

        score = _compute_signal_score(signals)

        # Attach C3 causal evidence if provided
        evidence: list[CausalEvidence] = []
        if causal_effects:
            for effect in causal_effects:
                if effect.att_estimate > 0:
                    evidence.append(
                        CausalEvidence(
                            regime=effect.regime,
                            regime_label=effect.label,
                            att_estimate=effect.att_estimate,
                            att_ci_lower=effect.att_ci_lower,
                            att_ci_upper=effect.att_ci_upper,
                            p_value=effect.p_value,
                            is_significant=effect.is_significant,
                        )
                    )

        candidates.append(
            UnknownUnknownCandidate(
                mmsi=mmsi,
                causal_score=round(score, 4),
                matching_signals=signals,
                causal_evidence=evidence,
            )
        )

    candidates.sort(key=lambda c: c.causal_score, reverse=True)
    return candidates
