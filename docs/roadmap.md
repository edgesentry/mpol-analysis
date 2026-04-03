# Roadmap

## Phase A — Shadow Fleet Candidate Screening (This Repo)

### A1 · Project Setup and AIS Ingestion

**Goal:** Running pipeline that ingests historical and live AIS for the area of interest.

- `pyproject.toml` with full dependency set (DuckDB, Polars, lance-graph, scikit-learn, SHAP, FastAPI, uvicorn)
- DuckDB schema initialisation (`src/ingest/schema.py`)
- Marine Cadastre annual archive download + DuckDB load (`src/ingest/marine_cadastre.py`) — US coastal waters only; takes `--year` flag (annual files); see [regional-playbooks.md](regional-playbooks.md) for non-US historical backfill options
- aisstream.io WebSocket ingestion with configurable bounding box (`src/ingest/ais_stream.py`) — supports `--bbox lat_min lon_min lat_max lon_max` override for multi-region deployment
- Lance Graph storage module (`src/graph/store.py`)
- End-to-end local test guide (`docs/local-e2e-test.md`)

**Acceptance:** DuckDB `ais_positions` table contains ≥ 6 months of AIS data for the configured area of interest (default: Malacca Strait / SG; see [regional-playbooks.md](regional-playbooks.md) for other regions) with no duplicate MMSI/timestamp rows.

---

### A2 · Sanctions and Registry Ingestion

**Goal:** Sanctions entities and vessel ownership graph loaded and queryable.

- OFAC SDN + EU + UN + OpenSanctions XML/Parquet → DuckDB `sanctions_entities` (`src/ingest/sanctions.py`)
- Equasis ownership chains → Lance Graph datasets (`src/ingest/vessel_registry.py`)
  - Node types: Vessel, Company, Country, VesselName
  - Relationship types: OWNED_BY, MANAGED_BY, REGISTERED_IN, ALIAS, SANCTIONED_BY

**Acceptance:** `uv run python src/ingest/vessel_registry.py` completes without error and the Lance Graph directory contains non-empty OWNED_BY and SANCTIONED_BY datasets for vessels with known OFAC exposure.

---

### A3 · Feature Engineering

**Goal:** Full feature matrix computed for all vessels in the area of interest.

- AIS behavioral features (Polars): `ais_gap_count_30d`, `position_jump_count`, `sts_candidate_count`, `loitering_hours_30d` (`src/features/ais_behavior.py`)
- Identity volatility features: `flag_changes_2y`, `name_changes_2y`, `owner_changes_2y` (`src/features/identity.py`)
- Ownership graph features (Lance Graph + Polars joins): `sanctions_distance`, `cluster_sanctions_ratio` (`src/features/ownership_graph.py`)
- Trade flow mismatch: `route_cargo_mismatch` (`src/features/trade_mismatch.py`)
- ~~GEBCO bathymetric mask (`src/features/bathymetric_mask.py`)~~ — **not implemented**; STS candidate detection uses a 5nm-from-port filter as a proxy; bathymetric masking deferred to C4

**Acceptance:** `vessel_features` table in DuckDB has one row per MMSI with no null values for core features; STS candidate count matches independently verified events from open-source maritime incident reports.

---

### A4 · Scoring Engine and Watchlist Output

**Goal:** Ranked candidate watchlist with SHAP explanations.

- HDBSCAN normal MPOL baseline (`src/score/mpol_baseline.py`)
- Isolation Forest anomaly score (`src/score/anomaly.py`)
- Composite score + SHAP attribution (`src/score/composite.py`)
- Output `candidate_watchlist.parquet` (`src/score/watchlist.py`)
- FastAPI + HTMX dashboard with map + ranked table + filters (`src/api/`) → http://localhost:8000

**Acceptance:** Precision@50 ≥ 0.6 (≥ 30 of top-50 candidates are OFAC-listed vessels); SHAP explanations are human-readable and match analyst intuition on manually inspected cases.

---

### A5 · Validation and Proposal Submission

**Goal:** Quantified accuracy metrics and proposal submitted to Cap Vista.

- Validation script against OFAC ground truth (`src/score/validate.py`)
- AUROC, Precision@50, Recall@200 reported in dashboard
- Proposal document submitted via Cap Vista platform (deadline: 29 April 2026)

---

## Phase B — Physical Vessel Investigation

> **Implementation note:** Phase B is implemented in [edgesentry-rs](https://github.com/edgesentry/edgesentry-rs) and edgesentry-app — not in this repository. This section describes the requirements and roadmap to guide that work.

### B1 · Watchlist Integration in edgesentry-app

- Load `candidate_watchlist.parquet` or patrol task JSON
- Display target vessel on chart with live AIS overlay (aisstream.io WebSocket)
- Duty officer go/no-go decision log (signed audit event via `edgesentry-audit`)

### B2 · Tier 1 — Camera + OCR Identity Verification

- Hi-res camera feed integration in edgesentry-app
- OpenCV preprocessing + Tesseract OCR: read IMO number, vessel name, call sign from hull
- Cross-check against DuckDB vessel registry (MMSI vs physical markings)
- Photo capture with GPS timestamp

**Hardware:** ~$500 (camera + GPS + ruggedised tablet)

### B3 · Tier 2 — LiDAR Hull Measurement

- LiDAR integration (Livox Mid-360 or Ouster OS0-32) via `trilink-core` point cloud pipeline
- `trilink-core::project_to_depth_map` + AI inference: detect hull markings, structural damage, waterline
- Deviation from registry hull reference (uses `trilink-core::scan_delta` — CP3 in trilink-core roadmap)
- Heatmap PNG + deviation report JSON

**Hardware:** ~$3k–$8k (LiDAR + NVIDIA Jetson Orin field PC)

### B4 · Cryptographic Evidence Capture (`edgesentry-rs`)

- Ed25519 signing of evidence bundle (device key in hardware secure element)
- BLAKE3 hash chain across all evidence records from a patrol sortie
- Sequence monotonicity: replay and deletion detectable
- `edgesentry-audit::IngestService` on shore side: re-verify signature + hash chain on receipt

### B5 · VDES Secure Reporting — Two-Phase On-Demand Transfer

**Phase B5a — Lightweight triage report (sent immediately):**
- JSON summary with key detections, watchlist confidence, and signed hash
- Asset manifest listing S3 keys and BLAKE3 hashes for all captured media
- VDES ASM frame serialisation of signed `AuditRecord` (fragmentation + ARQ)
- Transmit on ASM channels 24/84 (ship-to-shore, ~40nm)

**Phase B5b — On-demand asset retrieval:**
- Shore station reviews triage report; sends `FETCH_ASSETS` request specifying S3 keys
- Patrol vessel queues and transmits requested assets on demand
- Avoids blocking VDES channel when multiple patrol vessels report simultaneously
- Shore analysts can escalate/clear from triage JSON alone without waiting for full evidence bundle

**Why two phases:** Full evidence bundle (LiDAR scans, HD photos) blocks VDES for 30–130 seconds per report. On-demand transfer reserves scarce bandwidth for cases that genuinely need high-resolution evidence.

- VDES-SAT fallback for extended range (global)
- Shore VDES gateway: reassemble → ingest → S3 Object Lock (WORM)

**Fallback channels:** Iridium SBD (alert-only), LoRaWAN (harbour range), 4G/LTE (port area)

### B6 · Port Operations Centre Dashboard

- Real-time display of patrol evidence reports as they arrive via VDES
- Candidate watchlist rows annotated with investigation outcome (`confirmed`, `cleared`, `inconclusive`)
- Integration loop: confirmed vessels feed back into Phase A as positive labels to improve scoring
- `cleared` outcome writes MMSI to `cleared_vessels` DuckDB table; Phase A re-training uses cleared MMSIs as hard negatives (see C7 Improvement 3)

---

## Phase C — Pre-Submission Enhancements

Work on Phase C items has begun in parallel with Phase A/B ahead of the 29 April 2026 Cap Vista submission. Items marked **In Progress** have open GitHub issues.

### C1 · Dashboard Migration: FastAPI + HTMX *(Done — [#15](https://github.com/edgesentry/arktrace/issues/15))*

Replaced the Phase A Streamlit prototype with a production-grade FastAPI + HTMX dashboard (`src/api/`):

- **FastAPI** as the API layer — existing `src/ingest/`, `src/score/` modules are endpoints with no rewrite
- **HTMX** for partial updates — ranked watchlist table and map refresh independently; no full-page reload on filter changes
- **Server-Sent Events (SSE)** via `/alerts/sse` for `confidence > 0.75` alerts — one-directional, HTTP-native, load-balancer compatible
- **MapLibre GL JS** consuming `/api/vessels/geojson` for the vessel position layer
- `streamlit` and `pydeck` removed from dependencies

### C2 · Geopolitical Context Layer (GDELT + RAG)

SHAP `top_signals` explain *which features* drove a flag but not *why those features matter now*. A RAG layer adds geopolitical context:

- Daily GDELT event CSV ingested and indexed in a local vector store (e.g. ChromaDB)
- For each high-confidence candidate, retrieve relevant GDELT events (sanction announcements, port bans, incident reports) by IMO / flag state / ownership country
- Local LLM (Ollama) generates a one-paragraph analyst brief: "Vessel flagged due to 3 ownership changes in 6 months; last change coincides with OFAC designation of managing company on [date]"

This keeps the stack fully offline-capable (no cloud LLM dependency) and satisfies the Cap Vista explainability requirement at the human-analyst level.

### C4 · Multi-Region CLI Hardening

Several parameters currently require direct code edits when deploying to non-default regions (documented in [regional-playbooks.md](regional-playbooks.md)):

- ~~`--gap-threshold-hours` flag on `src/features/ais_behavior.py`~~ **Done** — flag added; default 6h, pass 12 for Japan Sea / Middle East
- ~~`--w-anomaly`, `--w-graph`, `--w-identity` weight flags on `src/score/composite.py`~~ **Done** — flags added; defaults 0.4 / 0.4 / 0.2
- `--bbox` on `src/ingest/marine_cadastre.py` CLI (bbox currently only settable via Python call for non-Singapore regions)
- GEBCO bathymetric mask (`src/features/bathymetric_mask.py`) — deferred from A3; provides higher-precision STS candidate filtering than the current 5nm-from-port heuristic

### C5 · Interactive Pipeline CLI *(Done — [#24](https://github.com/edgesentry/arktrace/issues/24))*

`scripts/run_pipeline.py` — a single interactive CLI that walks the user through region selection and executes all pipeline steps with region-specific defaults.

- Five built-in region presets (Singapore, Japan Sea, Middle East, Europe, US Gulf) with correct bbox, gap threshold, feature window, and composite weights pre-configured
- Interactive step-by-step progress display; failed steps prompt retry or skip
- Ctrl-C during AIS streaming stops the stream cleanly and continues to the next step
- `--region` + `--non-interactive` flags for CI/scripted use
- No new dependencies

### C3 · Causal Sanction-Response Model *(Done — [#21](https://github.com/edgesentry/arktrace/issues/21))*

Quantify the causal link between sanction events and observable AIS behaviour:

- **Instrument:** sanction announcement date × affected flag state / entity (`OFAC_Iran`, `OFAC_Russia`, `UN_DPRK`)
- **Outcome:** AIS gap frequency for vessels connected within 2 hops in the ownership graph in the 30-day post-announcement window
- **Method:** Difference-in-Differences (DiD) with vessel-type and route-corridor fixed effects; OLS with HC3 heteroskedasticity-robust standard errors (`src/score/causal_sanction.py`)
- **Output:** Per-sanction-regime ATT estimate + 95% CI written to `data/processed/causal_effects.parquet`; `calibrate_graph_weight()` derives a `graph_risk_score` weight in [0.20, 0.65] that is passed as `--w-graph` to `src/score/composite.py`

### C7 · Cap Vista Pre-Submission Enhancements *(Done — [#26](https://github.com/edgesentry/arktrace/issues/26))*

Four targeted improvements identified during pre-submission review:

**1 — Bunker barge exclusion from HDBSCAN baseline (`src/score/mpol_baseline.py`)**
AIS ship_type codes 51–59 (pilot, SAR, tug, fire-fighting, law enforcement) and 31–32 are excluded from the HDBSCAN training partition by default.  These service craft operate at low SOG and high loitering hours in busy port areas (e.g. Singapore Strait); including them compressed anomaly scores for genuine shadow-fleet STS events.  They are still scored by the Isolation Forest. Controlled by `--no-exclude-service-vessels` flag.  Exclusion list documented in `docs/regional-playbooks.md` Persona 1.

**2 — Geopolitical rerouting filter (`src/score/composite.py`)**
`--geopolitical-event-filter PATH` accepts a JSON file declaring active rerouting corridors.  Vessels whose last known position falls within a corridor during the declared window have their `anomaly_score` down-weighted before `confidence` is computed.  A sample file (`config/geopolitical_events.json`) covers the Red Sea / Cape of Good Hope rerouting since late 2023.  Passed through `scripts/run_pipeline.py` via `--geopolitical-event-filter`.

**3 — Cleared-vessel feedback loop (`src/ingest/schema.py`, `src/score/mpol_baseline.py`, `src/score/anomaly.py`)**
New `cleared_vessels` DuckDB table records MMSIs from Phase B investigations with outcome `cleared`.  `build_mpol_baseline()` and `fit_isolation_forest()` load cleared MMSIs and use them as hard negatives: `baseline_noise_score = 0.0` always (HDBSCAN), and always included in the IsolationForest clean-training subset.  Data flow edge: `B6 (cleared) ──► A4 (negative labels)`.

**4 — VDES two-phase on-demand transfer (roadmap B5, implemented in `edgesentry-rs`)**
Documented in B5 above.  Splits evidence transmission into a lightweight triage JSON (sent immediately) and on-demand asset retrieval (shore-triggered).  Avoids blocking VDES channel for 30–130 seconds per report when multiple patrol vessels are active simultaneously.

---

## Timeline

| Week | Phase A deliverable | Phase C (parallel) |
|---|---|---|
| Week 1 (Apr 1–7) | A1: Project setup + AIS ingestion | — |
| Week 2 (Apr 8–14) | A2: Sanctions + Lance Graph ownership graph | — |
| Week 3 (Apr 15–21) | A3: Full feature engineering pipeline | C1: FastAPI + HTMX dashboard (start) |
| Week 4 (Apr 22–28) | A4: Scoring + watchlist + Streamlit dashboard | C1: continued |
| Apr 29 | A5: Validate + submit proposal to Cap Vista | C1: targeted for submission alongside A5 |

Phase B timeline depends on Cap Vista trial contract award (expected within 60 days of submission deadline).

---

## Dependency Graph

```
A1 (AIS ingestion)
  └── A3 (feature engineering — behavioral)
        └── A4 (scoring + watchlist)
              └── A5 (validation + submission)

A2 (sanctions + registry)
  └── A3 (feature engineering — graph + identity)

A4 ──► B1 (watchlist in edgesentry-app)
B1 ──► B2 (Tier 1 camera)
B1 ──► B3 (Tier 2 LiDAR)
B2, B3 ──► B4 (evidence signing)
B4 ──► B5 (VDES transmission)
B5 ──► B6 (port dashboard)
B6 ──► A4 (confirmed labels loop back to improve scoring)

A4 ──► C1 (FastAPI + HTMX replaces Streamlit dashboard — parallel track)
A4 ──► C2 (GDELT + RAG analyst briefs consume watchlist candidates)
A3 ──► C3 (causal model uses AIS gap + sanctions event time-series)
A3 ──► C4 (CLI hardening exposes ais_behavior / composite / marine_cadastre params)
C3 ──► A4 (calibrated graph_risk_score weights feed back into composite scoring)
C1 ──► B6 (C1 dashboard replaces Phase A Streamlit in port ops centre)
```
