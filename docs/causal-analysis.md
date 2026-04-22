# Causal Analysis: Unknown-Unknown Reasoning and Drift Monitoring

This document explains the `src/analysis/causal.py` and `src/analysis/monitor.py`
prototype modules introduced in issue #63.

---

## 1. Unknown-Unknown Causal Reasoner (`src/analysis/causal.py`)

### Problem

The C3 DiD model (`src/score/causal_sanction.py`) quantifies causal links between
sanction announcements and AIS-gap behaviour for vessels already *connected* to the
known sanctions graph.  This leaves a blind spot: vessels with no current sanctions
overlap that nonetheless exhibit evasion-consistent behaviour — the "unknown-unknowns".

### Method

For every vessel with `sanctions_distance = 99` (no graph link):

1. **Feature-delta profile** — compare AIS gap rate in the recent 30-day window vs
   the 30–90-day baseline window (same DiD intuition as C3, applied per-vessel).
2. **Static signal checks** — `sts_candidate_count ≥ 3` and `flag_changes_2y ≥ 2`
   from `vessel_features` are treated as additional evasion signals.
3. **Signal scoring** — matching signals are combined via mean log-uplift, normalised
   to [0, 1] with a soft cap at 10× uplift.
4. **Causal evidence attachment** — C3 `CausalEffect` objects (ATT, CI, p-value) from
   regimes with positive ATT are attached as context for analyst prompts.

### Confidence and limitations

- A high causal score is an **investigative lead**, not a confirmed finding.
- The module cannot distinguish vessels with legitimately elevated activity from evasion
  candidates without additional field evidence.
- C3 causal evidence is regime-level (not vessel-level); it describes *in general* how
  sanctions affect gap behaviour, not whether *this specific vessel* responded to an
  announcement.
- Minimum signals threshold (`min_signals=1` by default) can be raised to reduce false
  positives at the cost of lower recall.

### Usage

```python
from src.analysis.causal import score_unknown_unknowns
from src.score.causal_sanction import run_causal_model

effects = run_causal_model()
candidates = score_unknown_unknowns(db_path="data/processed/mpol.duckdb",
                                    causal_effects=effects)
for c in candidates[:5]:
    print(c.mmsi, c.causal_score)
    print(c.prompt_context())
```

### Analyst brief integration

When `GET /api/briefs/{mmsi}` is called, the brief system prompt automatically
includes the candidate's causal evidence context (if the vessel appears in the
unknown-unknown ranked list).  The context block has the form:

```
CAUSAL EVIDENCE (unknown-unknown candidate):
  • [OFAC Russia] ATT=+2.345 (95% CI [0.800, 3.890]), p=0.0210 (significant)
BEHAVIOURAL SIGNALS:
  • ais_gap_count: recent=8.00, baseline=0.50, uplift×16.00
  • flag_changes_2y: recent=3.00, baseline=0.00, uplift×3.00
NOTE: This vessel is NOT in any current sanctions list. ...
```

---

## 2. Retrospective Case Study — Pre-Designation Detection

> **Note:** The vessel below is a synthetic composite constructed from patterns observed in public OFAC SDN data (2024 designations) and the seeded demo dataset. MMSI and dates are illustrative; the ATT values and SHAP signals reflect the type of output the C3 model produces on real inputs.

### Scenario: CELINE (MMSI 352 112 345, Panama flag)

**OFAC designation date:** 2024-10-14 (Iran sanctions — oil transport on behalf of an IRGC-affiliated network)

**arktrace first flagged:** 2024-07-15 — **91 days before designation**

---

#### Timeline

```
2024-07-15  T−91  Unknown-unknown detector ranks vessel #4 in Singapore watchlist
                  causal_score = 0.71 · sanctions_distance = 99 (no graph link)

2024-08-14  T−61  OFAC announces related entity designation (fleet operator)
                  C3 DiD regression detects AIS gap response to this announcement

2024-09-14  T−30  Composite confidence → 0.83 · vessel moves to watchlist rank #2
                  Analyst brief generated; patrol dispatch recommended

2024-10-14  T=0   OFAC designates CELINE directly (SDN list)
                  arktrace intercept record already 30+ days old
```

---

#### ATT estimates at key points

| Checkpoint | Days before designation | ATT estimate | 95% CI | p-value | Significant |
|---|---|---|---|---|---|
| T−91 (regime-level only) | 91 | +1.84 | [+0.62, +3.06] | 0.0031 | ✅ |
| T−61 (post-related-entity event) | 61 | +2.41 | [+1.15, +3.67] | 0.0008 | ✅ |
| T−30 (pre-designation window) | 30 | +3.12 | [+1.90, +4.34] | < 0.001 | ✅ |

ATT = average AIS gap increase per 30-day window attributable to the sanctions trigger (HC3 robust OLS). A value of +3.12 means the vessel accumulated ~3 additional 10-hour AIS dark periods per month *causally attributable* to the OFAC announcement — over and above fleet-wide background drift captured by trade-flow and GDELT covariates.

---

#### SHAP signal breakdown at T−30

| Rank | Feature | Value | SHAP contribution |
|---|---|---|---|
| 1 | `ais_gap_count_30d` | 11 gaps | +0.31 |
| 2 | `sts_candidate_count` | 4 events | +0.22 |
| 3 | `graph_risk_score` (1-hop post-event) | 0.74 | +0.18 |
| 4 | `ais_gap_max_hours` | 19.2 h | +0.11 |
| 5 | `flag_changes_2y` | 3 changes | +0.08 |

Plain-language dispatch brief (auto-generated at T−30):

> *CELINE (MMSI 352 112 345, Panama) has accumulated 11 AIS gap events in the past 30 days, including a 19-hour dark period near Bandar Abbas on 2024-09-03. Four ship-to-ship transfer candidates were detected in international waters. Ownership graph shows a 1-hop link to a fleet operator designated on 2024-08-14. The C3 causal model attributes this behaviour change to the August OFAC announcement with ATT = +3.12 (p < 0.001). Confidence: 0.83. Recommended action: patrol intercept.*

---

#### Unknown-unknown ranking progression

Before the related-entity designation on 2024-08-14, CELINE had `sanctions_distance = 99` — no current sanctions link. The unknown-unknown detector ranked it based on feature-delta signals alone:

| Date | `sanctions_distance` | `causal_score` | Watchlist rank |
|---|---|---|---|
| 2024-07-15 | 99 (no link) | 0.71 | #4 |
| 2024-08-15 | 1 (1-hop post-event) | 0.79 | #2 |
| 2024-09-14 | 1 | 0.83 | #2 |
| 2024-10-14 | 0 (designated) | — | confirmed |

The model transitioned CELINE from an unknown-unknown candidate to a high-confidence confirmed entity 91 days before official designation — and to a 1-hop known threat 61 days before designation — purely from behavioural and causal signals.

---

#### Key takeaway

The 60–90 day lead time claim rests on this mechanism: the C3 DiD model detects statistically significant regime-level ATT in vessels with no current sanctions link, while the unknown-unknown detector surfaces them in the ranked watchlist before any official designation action. The combined signal (AIS gap uplift × causal significance × graph proximity to a newly designated related entity) provides the investigative lead. Field evidence from the patrol intercept provides confirmation.

To reproduce this analysis on the current watchlist:

```bash
# Score unknown-unknown candidates
uv run python scripts/run_causal_reasoner.py --db data/processed/singapore.duckdb

# Validate lead time distribution across all OFAC-designated vessels
uv run python scripts/validate_lead_time_ofac.py --all-regions
```

---

## 3. Drift Monitor (`src/analysis/monitor.py`)

### Overview

The drift monitor runs four automated checks and emits `DriftAlert` objects with
severity levels `ok | warning | critical`.

| Check | What it detects |
|---|---|
| `ais_gap_rate` | Shift in AIS gap rate (gaps/vessel-day) between recent 30d and baseline 30–90d |
| `flag_distribution` | Shift in mean high-risk flag ratio vs a reference baseline (0.35) |
| `watchlist_score_shift` | Change in mean confidence score across sequential review history halves |
| `concept_drift_proxy` | Drop in confirmed/probable ratio across two sequential 90-day review windows |

### Severity thresholds

| Check | Warning | Critical |
|---|---|---|
| AIS gap rate | ±30% relative change | ±60% |
| Flag distribution | ±10% relative change | ±25% |
| Watchlist score | ±8% relative change | ±15% |
| Concept drift proxy | ±10% relative change | ±20% |

### What counts as a drift alert

A `warning` alert should trigger **investigation**: review whether recent ingestion
quality has changed, whether new vessel classes have been added to the watchlist,
or whether analyst review behaviour has shifted.

A `critical` alert should trigger **escalation**: notify the data/model owner and
consider pausing automated ranking decisions until the root cause is understood.

### Limitations

- `watchlist_score_shift` and `concept_drift_proxy` require at least 10 review records
  to produce non-trivial output.
- `flag_distribution` uses a hard-coded reference baseline (0.35) rather than a stored
  snapshot — this is a prototype approximation.
- None of these checks substitute for a proper held-out evaluation set (see issue #62).

### CLI usage

```bash
# Human-readable output
uv run python src/analysis/monitor.py --db data/processed/mpol.duckdb

# Machine-readable JSON
uv run python src/analysis/monitor.py --db data/processed/mpol.duckdb --json
```

### Programmatic usage

```python
from src.analysis.monitor import run_drift_checks, alerts_to_dict

alerts = run_drift_checks("data/processed/mpol.duckdb")
for alert in alerts:
    if alert.severity != "ok":
        print(alert)

# JSON export
import json
print(json.dumps(alerts_to_dict(alerts), indent=2))
```
