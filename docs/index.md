# arktrace

**arktrace** identifies ships that are evading international sanctions, ranks them by risk, and hands the highest-priority vessels to patrol officers for physical inspection.

---

## The Problem

A shadow fleet vessel moves sanctioned oil or cargo while deliberately hiding what it is doing. It combines several techniques at once, which is why individual tracking tools miss it:

| Technique | How it works |
|---|---|
| Going dark (AIS off) | Switches off its tracking transponder during cargo transfers so no position record exists |
| GPS spoofing | Broadcasts a false position while the actual transfer happens elsewhere |
| Flag hopping | Changes its country of registration frequently to reset port inspection history |
| Name and identity change | Renames itself and re-registers under new shell companies to break continuity in watchlists |
| Ship-to-ship transfer at sea | Moves cargo vessel-to-vessel far from any port, leaving no port record |
| Shell company ownership | Buries beneficial ownership behind 4–6 layers of holding companies across multiple jurisdictions |

arktrace detects all of these simultaneously, fuses them with ownership network proximity to sanctioned entities and trade flow data, and produces a single ranked list of vessels most likely to be operating on behalf of sanctioned states.

The default area of interest is the Strait of Malacca and Singapore Strait — the world's busiest shipping lane. Five regions are supported: Singapore/Malacca Strait, Japan Sea, Middle East, Europe, and US Gulf.

---

## How It Works — Four Steps

**1. Ingest public data**
The pipeline pulls vessel tracking data (AIS), international sanctions lists (OFAC, EU, UN), vessel ownership records, and bilateral trade statistics from public sources. No proprietary data feeds or costly subscriptions are required.

**2. Compute 19 risk signals per vessel**
Each vessel is scored on four signal families: how anomalously it moves, how often it changes its identity, how close it sits in the ownership network to a sanctioned entity, and whether its declared trade routes match official trade records. A causal model additionally checks whether the vessel changed its behaviour specifically after major sanction announcements — distinguishing evasion from ordinary commercial route changes.

**3. Rank candidates on the watchlist**
The 19 signals are combined into a single confidence score. The dashboard shows a map and ranked table with a plain-English explanation of the three signals that drove each vessel's score — e.g. *"went dark 12 times last month, one ownership hop from an OFAC-listed company, changed flag 3 times in 2 years"* — so an analyst can immediately understand why a vessel was flagged.

**4. Hand off to a patrol officer**
High-scoring vessels are exported as a task file for the patrol vessel. The officer dispatches for close-range inspection. Results (confirmed, cleared, inconclusive) feed back into the model to reduce false positives in future ranking cycles.

---

## How Effective Is It?

| Capability | What to expect |
|---|---|
| **Detection rate** | Precision@50 target ≥ 0.60: at least 30 of the top-50 ranked candidates are confirmed OFAC-listed vessels. AUROC and Recall@200 are also tracked. |
| **Novel threats** | An unknown-unknown detector surfaces vessels with no current sanctions link but evasion-consistent behaviour — catching threats before they appear on any list. |
| **False positive reduction** | Geopolitical rerouting filter down-weights anomaly scores for vessels on declared diversion routes (e.g. Cape of Good Hope since 2023), reducing noise from legitimate commercial rerouting. |
| **Self-improvement** | Patrol outcomes feed back as hard examples. Cleared vessels are never re-flagged. Confirmed vessels trigger a graph-wide backtrack to surface connected threats not yet on any watchlist. |
| **Explainability** | Every score has a per-feature breakdown. Analysts know exactly what drove each result — there are no black-box verdicts. |

---

## How Efficient Is It?

| Resource | Requirement |
|---|---|
| **Hardware** | Standard laptop (4 vCPU / 8 GB RAM). No GPU, no cloud, no external server. |
| **Full pipeline run** | ~45 minutes from raw data to ranked watchlist |
| **Incremental re-score** | Under 60 seconds per batch during live monitoring |
| **Live alerting** | Real-time SSE alerts when a vessel crosses a configurable confidence threshold |
| **Software cost** | Fully open-source. No licensing fees. |
| **Regions** | Switch between Singapore, Japan Sea, Middle East, Europe, and US Gulf with a single CLI flag — no code changes |

---

## Screening and Physical Investigation

arktrace covers **Phase A — Screening** (this repository). Phase B — Physical Investigation — is the patrol vessel software suite implemented in [edgesentry-rs](https://github.com/edgesentry/edgesentry-rs) and edgesentry-app.

| Phase | What it does | Status |
|---|---|---|
| **A — Screening** | Ingest public data → compute risk signals → rank candidates → analyst dashboard | Working |
| **B — Physical Investigation** | Patrol vessel dispatch → OCR identity check → LiDAR hull scan → cryptographically signed evidence → VDES secure transmission | Design specification complete; implementation begins after trial contract award |

Phase A produces the watchlist. Phase B acts on it. Patrol outcomes flow back into Phase A to improve future rankings.

---

## Human Oversight

The model ranks candidates — humans decide what to do. No automated decision triggers legal or operational action.

- Every candidate is reviewed by an analyst before escalation. Review tiers: Confirmed, Probable, Suspect, Cleared, Inconclusive.
- "Confirmed" requires at least two independent high-credibility sources, or one official designation with a verified vessel identifier (MMSI/IMO match).
- Every review decision is recorded with a rationale, evidence references, and reviewer identity.

Full policy: [`docs/triage-governance.md`](triage-governance.md).

---

## Document Index

| To understand… | Read |
|---|---|
| Detection signals and scoring formula | [`docs/scoring-model.md`](scoring-model.md) |
| All 19 features and what each detects | [`docs/feature-engineering.md`](feature-engineering.md) |
| Causal reasoning and unknown-unknown detection | [`docs/causal-analysis.md`](causal-analysis.md) |
| Physical vessel investigation (Phase B) | [`docs/field-investigation.md`](field-investigation.md) |
| Human oversight, evidence policy, tier taxonomy | [`docs/triage-governance.md`](triage-governance.md) |
| Validation metrics and backtesting methodology | [`docs/backtesting-validation.md`](backtesting-validation.md) |
| Three operational scenarios (duty officer, analyst, patrol) | [`docs/scenarios.md`](scenarios.md) |
| Full roadmap (Phase A and Phase B) | [`docs/roadmap.md`](roadmap.md) |
| Deployment (local, Docker, cloud VM) | [`docs/deployment.md`](deployment.md) |
| Tech stack and algorithm details | [`docs/technical-solution.md`](technical-solution.md) |
| Pipeline operations reference | [`docs/pipeline-operations.md`](pipeline-operations.md) |
| Regional configuration (Singapore, Japan Sea, etc.) | [`docs/regional-playbooks.md`](regional-playbooks.md) |
