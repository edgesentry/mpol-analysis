# Cap Vista Accelerator — Solicitation 5.0
# Challenge 1: Maritime Security Data Analytics
# Proposal Draft

> **Instructions for PDF submission:**  
> Copy each section below into the corresponding field of the official Cap Vista proposal
> template PDF downloaded from https://accelerator.capvista.com.sg.  
> Fields marked **[FILL IN]** require information only the submitter knows (company name,
> contact details, etc.).  
> Deadline: **29 April 2026, 13:00 GMT+8**.

---

## Section 1 — Applicant Information

| Field | Value |
|---|---|
| Company / Organisation name | **[FILL IN]** |
| Country of incorporation | **[FILL IN]** |
| Primary contact name | **[FILL IN]** |
| Primary contact email | **[FILL IN]** |
| Website | https://github.com/edgesentry/arktrace |
| Challenge statement selected | Challenge 1 — Maritime Security Data Analytics |

---

## Section 2 — Solution Title

**arktrace: Causal Inference Engine for Shadow Fleet Prediction**

---

## Section 3 — Executive Summary / Abstract

*(Recommended: 150–250 words. Should answer: what is the innovation, what problem does it solve, what is the evidence it works.)*

arktrace is a **Causal Inference Engine for Shadow Fleet Prediction** that identifies vessels destined for sanctions designation **60–90 days before they appear on public sanctions lists**. The primary technical innovation is the C3 Causal Sanction-Response model — a Difference-in-Differences (DiD) regression framework that tests whether each vessel's AIS evasion behaviour was *causally triggered* by a specific sanction announcement, as opposed to a coincidental commercial route change. This separates genuine evaders from noise with statistical rigour (HC3-robust OLS, p-value reporting) that conventional anomaly detection cannot provide.

Built on top of the causal layer is an unknown-unknown detector that surfaces vessels with no current sanctions link but evasion-consistent causal signatures, followed by graph-based backtracking that propagates confirmed evaders through the ownership network to predict next designations.

The full pipeline runs on a standard laptop (4 vCPU / 8 GB RAM) with no GPU, no cloud dependency, and no proprietary data feeds — all sources are open-access. On the Singapore / Malacca Strait dataset, the pipeline achieves **Precision@50 = 0.62** (6× lift over random), already exceeding the ≥ 0.60 acceptance target. The software is fully open-source (Apache-2.0 / MIT) with zero licensing cost at any scale.

---

## Section 4 — Problem Statement

*(Describe the problem you are solving and why existing approaches are insufficient.)*

Shadow fleet vessels evade international sanctions through a combination of six simultaneous techniques: AIS transponder disabling ("going dark"), GPS position spoofing, frequent flag and name changes, ship-to-ship cargo transfers at sea, and layered shell-company ownership structures. Each technique alone is detectable; their combination is not, because existing tools treat them as independent signals.

More critically, existing maritime analytics platforms use vessel behaviour profiling, conventional sanctions screening, and anomaly detection as their primary discriminators. These approaches share a fundamental limitation: they can only flag vessels that have already accumulated sufficient behavioural evidence *after* evasion has begun. By the time a vessel scores high on an anomaly detector, it has typically been evading for 6–12 months and may already be known to intelligence services.

The gap that remains unaddressed is causal reasoning: *did this vessel change its behaviour specifically because of a sanction event?* A vessel that changes its AIS patterns within weeks of a new OFAC designation, even if not itself sanctioned, is exhibiting a causal response — not coincidental variation. Identifying that causal signal, and propagating it through the ownership network to connected vessels, enables pre-designation detection 60–90 days ahead of public announcement.

---

## Section 5 — Proposed Solution and Technical Approach

*(Describe your solution, focusing on novelty. Map clearly to "What We Are Looking For" in the challenge statement.)*

### 5.1 Primary Innovation: C3 DiD Causal Sanction-Response Model

The core contribution is `src/score/causal_sanction.py` — a Difference-in-Differences (DiD) causal inference model applied to AIS data around sanction announcement events.

**Methodology:**
- For each vessel × sanction regime combination, a DiD regression is run comparing the vessel's AIS gap rate in the 30-day post-announcement window against its 30–90-day pre-announcement baseline.
- The estimated Average Treatment Effect on the Treated (ATT) captures whether the vessel's evasion behaviour increased specifically because of the sanction event.
- Standard errors use HC3 robust estimation to account for heteroskedasticity in AIS telemetry.
- Output: ATT coefficient, 95% confidence interval, p-value, and significance flag per vessel × regime.

**Why this is not anomaly detection:** Anomaly detection flags statistical outliers — vessels that look unusual compared to a population distribution. DiD causal inference tests whether a *specific event caused a specific behavioural change* in a *specific vessel*. A vessel that has always been active at night does not trigger the causal model. A vessel whose nocturnal AIS activity increased by 3σ in the two weeks following an OFAC announcement does.

### 5.2 Unknown-Unknown Detection (`src/analysis/causal.py`)

Vessels with `sanctions_distance = 99` (no current ownership-graph link to any sanctioned entity) that nonetheless exhibit evasion-consistent causal signatures are scored by the unknown-unknown detector:

1. AIS gap rate: recent 30-day window vs. 30–90-day baseline (same DiD intuition as C3).
2. Static signal checks: STS candidate count ≥ 3, flag changes in past 2 years ≥ 2.
3. Signals combined via mean log-uplift, normalised to [0, 1].

This module surfaces threats before they accumulate any sanctions overlap — the primary mechanism for the 60–90 day pre-designation lead time.

### 5.3 Network Backtracking Propagation (`scripts/run_backtracking.py`)

Once a vessel is confirmed as a shadow fleet operator (by patrol outcome or sanctions listing), the ownership graph is traversed to surface entities connected by `CONTROLLED_BY` or `OWNED_BY` edges up to 4 hops away. Connected vessels with no current sanctions link are promoted to the watchlist as predicted next-designation candidates.

### 5.4 Evidentiary Substrate (Input to the Causal Model)

The following signals feed into the causal model and composite scoring engine. They are *not* the primary innovation — they are the feature set that the causal model operates on:

| Signal family | Features | Source |
|---|---|---|
| AIS movement anomaly | Gap count, max gap duration, position jump count, loitering hours | aisstream.io / Marine Cadastre |
| Identity churn | Flag changes (2y), name changes (2y), IMO/MMSI mismatches | Equasis |
| Ownership network | Sanctions distance (hops), ownership depth, sanctioned co-investors | Lance Graph + OpenSanctions |
| Trade flow mismatch | Declared vs. actual bilateral trade route consistency | UN Comtrade+ |

### 5.5 Scoring Pipeline

```
vessel_features (19 signals)
       │
       ├──► C3 DiD causal model          → causal_score ∈ [0, 1]
       ├──► Unknown-unknown detector      → uu_score ∈ [0, 1]
       ├──► HDBSCAN baseline (MPOL)       → baseline_noise_score ∈ {0, 1}
       └──► Isolation Forest anomaly      → anomaly_score ∈ [0, 1]
                     │
                     ▼
             Composite confidence score ∈ [0, 1]
                     │
                     ▼
             candidate_watchlist.parquet
             (SHAP-explained top signals per vessel)
```

### 5.6 Human-in-the-Loop Oversight

Every score carries a SHAP feature breakdown so analysts know *exactly* which signals drove the ranking. The FastAPI + HTMX dashboard provides:
- Ranked watchlist with map
- Plain-English explanation of top 3 signals per vessel (e.g. *"causal DiD response to 2024-10 OFAC announcement (p < 0.01), one ownership hop from a sanctioned entity, changed flag 3 times in 2 years"*)
- Analyst chat backed by a local LLM (Ollama / MLX) with the vessel's causal evidence injected into context — no external API calls
- Patrol dispatch workflow: confirmed outcomes feed back as hard labels; cleared vessels are permanently suppressed from future rankings

---

## Section 6 — Innovativeness and Relevance

*(Address: solution novelty level; effectiveness in addressing stated challenges.)*

**Novel methodology:** The application of Difference-in-Differences causal inference to AIS event data around sanction announcement windows is not present in any known commercial maritime intelligence platform. Windward, Pole Star, MarineTraffic Enterprise, and similar tools use supervised anomaly scoring and rule-based sanctions matching. None test for causal response to specific designation events.

**Addresses the challenge directly:**
- *"Novel techniques to accurately identify and predict possible shadow fleet vessels"* → C3 DiD causal model and unknown-unknown detector predict vessels 60–90 days before designation.
- *"Ingest and fuse data from various sources to identify anomalies and form predictions"* → Five independent data layers (AIS, sanctions, ownership graph, trade flow, geopolitical events) are fused into a single causal + anomaly composite score.
- *"Relevant proprietary datasets"* → All sources are open-access; no proprietary datasets are required, which maximises reproducibility and auditability.

**Does not rely on disqualified methodologies:** The primary discriminator is causal inference, not vessel behaviour profiling, risk scoring, geofencing, or off-the-shelf behavioural analytics. AIS anomaly signals and sanctions screening are input features to the causal model, not the output claim.

**Explainability:** Key evaluation metric per the challenge brief. Every watchlist entry carries SHAP-attributed signal decomposition. Analysts see the causal chain, not a black-box score.

---

## Section 7 — Tech Feasibility and Readiness

*(Address: implementation feasibility and readiness; scalability; commercial adoption history.)*

### 7.1 Readiness

arktrace is a working implementation, not a concept or academic prototype.

| Capability | Status |
|---|---|
| AIS ingestion (live + historical) | Working — aisstream.io WebSocket + Marine Cadastre loader |
| C3 DiD causal model | Working — `src/score/causal_sanction.py` |
| Unknown-unknown detector | Working — `src/analysis/causal.py` |
| Ownership graph (Lance Graph) | Working — `src/graph/` |
| Scoring engine (HDBSCAN + Isolation Forest) | Working — `src/score/` |
| FastAPI + HTMX dashboard | Working — `src/web/` |
| Local LLM analyst brief | Working — Ollama / MLX provider |
| Patrol handoff workflow | Working — `src/web/routes/dispatch.py` |
| Feedback loop (hard labels from outcomes) | Working — `src/score/prelabel_evaluation.py` |
| Docker Compose deployment | Working — `docker compose up` |

**Measured baseline (Singapore region, full pipeline run):** Precision@50 = **0.62**, exceeding the ≥ 0.60 acceptance criterion on first run.

### 7.2 Deployment

The full stack deploys with a single command:

```bash
docker compose up   # starts MinIO object store + FastAPI dashboard
```

- No cloud dependency during operation
- No external database server (DuckDB and Lance Graph are embedded in-process)
- No GPU required
- Works on a standard laptop (4 vCPU / 8 GB RAM), a port operations workstation, or an NVIDIA Jetson Orin edge node

### 7.3 Scalability

| Scope | Active vessels/month | Est. monthly infra cost |
|---|---|---|
| Single strait (e.g. Singapore / Malacca) | ~5k | ~$30–$80 cloud VM, or $0 on-prem |
| Regional (SE Asia, 3–5 chokepoints) | ~20k | ~$150–$400 cloud |
| Global (5 major regions) | ~100k | ~$400–$1,000 cloud |

Software cost at any scale: **$0** (fully open-source, Apache-2.0 / MIT). Multi-region scaling uses one DuckDB file per region with a shared global Lance Graph ownership dataset.

### 7.4 Commercial Context

arktrace is the screening layer (Phase A) of a two-phase system. Phase B — physical vessel investigation — is implemented in `edgesentry-rs` and `edgesentry-app`, targeting patrol vessel deployment with LiDAR hull scanning, OCR identity verification, and cryptographically signed evidence reporting over VDES. The combined system represents a full intelligence-to-action loop from watchlist generation to field confirmation.

---

## Section 8 — Trial Specification

*(Directly addresses the "Trial Specifications" section of Challenge 1.)*

### 8.1 Proposed Datasets and Formats

| Dataset | Source | Format | Cost |
|---|---|---|---|
| AIS positions (live) | aisstream.io WebSocket | JSON → DuckDB | Free (API key) |
| AIS positions (historical) | Marine Cadastre | Parquet | Free download |
| Sanctions entities | OFAC SDN, EU FSF, UN 1267 via OpenSanctions | JSONL | Free (CC0) |
| Vessel registry + ownership | Equasis | HTTP / CSV | Free (registration) |
| Bilateral trade statistics | UN Comtrade+ REST API | JSON | Free (500 req/day) |
| Geopolitical events | GDELT Project | CSV | Free / open |
| Bathymetric depth mask | GEBCO | GeoTIFF | Free download |

### 8.2 Platform Requirements

| Resource | Minimum | Recommended |
|---|---|---|
| CPU | 4 cores | 8 cores |
| RAM | 8 GB | 16 GB |
| Storage | 100 GB SSD | 500 GB SSD |
| GPU | Not required | Not required |
| OS | Linux / macOS | Ubuntu 22.04 LTS |
| Network | Internet (AIS stream + API pulls during setup) | — |

### 8.3 Trial Demonstration Strategy (30–60 days, Singapore / Malacca Strait)

**Week 1 — Setup:** Deploy via `docker compose up` on provided VM; ingest 6 months of historical AIS for Singapore / Malacca Strait; load sanctions data; build Lance ownership graph; run full pipeline; generate initial watchlist.

**Weeks 2–3 — Baseline validation:** Run held-out evaluation against OFAC-listed vessels in the Singapore AIS dataset. Target metrics:

| Metric | Target |
|---|---|
| Precision@50 | ≥ 0.60 (measured: 0.62) |
| Recall@200 | ≥ 0.40 |
| AUROC | ≥ 0.75 |

**Weeks 3–7 — Live monitoring:** Connect live aisstream.io WebSocket; run continuous re-scoring at 15-minute cadence; duty officers use dashboard for morning briefs and patrol dispatch.

**Weeks 5–9 — Patrol handoff (if Phase B hardware available):** Dispatch for top-N candidates; record confirmed / cleared outcomes; feed back as hard labels; rerun causal model calibration.

---

## Section 9 — Option Cost (Scaling)

*(Directly addresses the "Option Cost" section of Challenge 1.)*

### Software Cost: $0

All components are open-source with no per-vessel, per-user, or per-region licensing fees.

### Infrastructure Cost

| Deployment scope | Monthly infrastructure cost |
|---|---|
| Single strait | ~$30–$80 (cloud VM) or $0 (on-prem) |
| Regional (3–5 chokepoints) | ~$150–$400 (cloud) |
| Global (5 regions) | ~$400–$1,000 (cloud) |

Storage at global scale (~400 GB/month): ~$10/month on S3-standard; $0 on-prem MinIO.

### Edge Hardware (one-off, per patrol vessel)

| Configuration | Cost |
|---|---|
| Tier 1: Camera + OCR + GPS + tablet | ~$1,500 |
| Tier 2: + LiDAR + Jetson Orin | ~$4,000–$9,000 |
| Tier 3: + Thermal / multispectral | ~$13,000–$33,000 |

Recommended: Tier 1 + Tier 2 per patrol vessel (~$8,000–$10,000). Software cost remains $0.

### Comparison to Commercial Alternatives

| Platform | Annual licence | Causal inference | Edge-deployable |
|---|---|---|---|
| **arktrace** | **$0** | ✅ | ✅ |
| Windward | ~$100k+ | ✗ | ✗ |
| Pole Star | ~$50k+ | ✗ | ✗ |
| MarineTraffic Enterprise | ~$80k+ | ✗ | ✗ |

---

## Section 10 — Supporting Documentation

The following are available in the arktrace repository for reviewer reference:

| Document | Location |
|---|---|
| Architecture and system design | `docs/architecture.md` |
| Full technical solution | `docs/technical-solution.md` |
| C3 causal model detail | `docs/scoring-model.md` |
| Unknown-unknown detector | `docs/causal-analysis.md` |
| Evaluation metrics and baselines | `docs/evaluation-metrics.md` |
| Trial specification (full) | `docs/trial-specification.md` |
| Cost and scaling detail | `docs/cost-scaling.md` |
| Deployment instructions | `docs/deployment.md` |
| Backtesting validation | `docs/backtesting-validation.md` |
| Triage governance and KPI spec | `docs/triage-governance.md` |

Repository: https://github.com/edgesentry/arktrace

---

## Checklist Before Submission

- [ ] Company / contact information filled in (Section 1)
- [ ] All sections copied into official Cap Vista PDF template
- [ ] Supporting documentation attached or linked
- [ ] "Submit" button clicked on the Cap Vista portal before **29 April 2026, 13:00 GMT+8**
