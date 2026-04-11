# arktrace

**arktrace** is a **Causal Inference Engine for Shadow Fleet Prediction**. It uses Difference-in-Differences (DiD) to identify vessels that *causally respond* to sanction announcements with evasion behaviour — detecting unknown-unknown threats **60–90 days before** they appear on public sanctions lists. AIS behaviour, ownership graph proximity, and trade flow data form the evidentiary substrate; causal inference and network-based backtracking propagation are the novel methodology.

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

Conventional tools detect individual techniques in isolation. arktrace goes further: it uses causal inference to identify vessels whose evasion behaviour was *triggered by* specific sanction events, separating genuine evasion from ordinary commercial route changes, and propagates signals through the ownership network to surface connected threats not yet on any list.

The default area of interest is the Strait of Malacca and Singapore Strait — the world's busiest shipping lane. Five regions are supported: Singapore/Malacca Strait, Japan Sea, Middle East, Europe, and US Gulf.

---

## How It Works — Four Steps

**1. Ingest public data**
The pipeline pulls vessel tracking data (AIS), international sanctions lists (OFAC, EU, UN), vessel ownership records, and bilateral trade statistics from public sources. No proprietary data feeds or costly subscriptions are required.

**2. Apply causal inference and compute 19 signals per vessel**
The core model (`src/score/causal_sanction.py`) runs a Difference-in-Differences regression for each vessel around every major sanction announcement, testing whether behavioural change was *causally driven* by the event rather than coincidental. This is the primary innovation. Four signal families — movement anomaly, identity churn, ownership network distance, and trade flow mismatch — serve as the evidentiary substrate that feeds the causal model and an unknown-unknown detector (`src/analysis/causal.py`), which surfaces vessels with no current sanctions link but evasion-consistent causal signatures.

**3. Rank candidates on the watchlist**
Causal scores and network propagation results are combined into a single confidence score. The dashboard shows a map and ranked table with a plain-English explanation of the top signals that drove each vessel's ranking — e.g. *"causal DiD response to 2024-10 OFAC announcement (p < 0.01), one ownership hop from a sanctioned entity, changed flag 3 times in 2 years"* — so an analyst can immediately understand the causal chain.

**4. Hand off to a patrol officer**
High-scoring vessels are exported as a task file for the patrol vessel. The officer dispatches for close-range inspection. Results (confirmed, cleared, inconclusive) feed back into the causal model as hard labels, tightening future DiD estimates and triggering graph-wide backtracking to surface connected threats.

---

## How Effective Is It?

| Capability | What to expect |
|---|---|
| **Pre-designation lead time** | 60–90 days before OFAC listing (backtested via DiD on historical sanction announcements). See [docs/scoring-model.md](scoring-model.md). |
| **Unknown-unknown detection** | Causal signatures surface vessels with no sanctions link whose behaviour pattern matches confirmed evaders — catching threats before they appear on any list. |
| **Detection rate** | Precision@50 target ≥ 0.60: at least 30 of the top-50 ranked candidates are confirmed OFAC-listed vessels. AUROC and Recall@200 are also tracked. |
| **False positive reduction** | Geopolitical rerouting filter down-weights DiD scores for vessels on declared diversion routes (e.g. Cape of Good Hope since 2023), reducing noise from legitimate commercial rerouting. |
| **Network propagation** | Confirmed vessels trigger graph-wide backtracking (`scripts/run_backtracking.py`) to surface ownership-connected threats not yet on any watchlist. |
| **Explainability** | Every score has a per-feature SHAP breakdown. Analysts see the exact causal and network signals that drove each result — no black-box verdicts. |

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
| Current Precision@50 status and path to 0.68 | [`docs/precision-improvement-plan.md`](precision-improvement-plan.md) |
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
