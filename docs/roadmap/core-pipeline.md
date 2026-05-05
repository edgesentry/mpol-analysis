# Roadmap

## Phase A — Shadow Fleet Candidate Screening (This Repo)

### A1 · Project Setup and AIS Ingestion ✅

**Goal:** Running pipeline that ingests historical and live AIS for the area of interest.

- `pyproject.toml` with full dependency set (DuckDB, Polars, lance-graph, scikit-learn, SHAP)
- DuckDB schema initialisation (`src/ingest/schema.py`)
- Marine Cadastre annual archive download + DuckDB load (`src/ingest/marine_cadastre.py`) — US coastal waters only; takes `--year` flag and `--bbox` argument.
- aisstream.io WebSocket ingestion with configurable bounding box (`src/ingest/ais_stream.py`) — supports `--bbox lat_min lon_min lat_max lon_max` override for multi-region deployment
- Lance Graph storage module (`src/graph/store.py`)
- End-to-end local test guide (`docs/.agents/skills/arktrace-run-tests/SKILL.md`)
- Interactive Pipeline CLI (`scripts/run_pipeline.py`) — interactive CLI with region presets (Singapore, Japan Sea, Middle East, Europe, US Gulf) and retry mechanics.

**Acceptance:** DuckDB `ais_positions` table contains ≥ 6 months of AIS data for the configured area of interest (default: Malacca Strait / SG; see [regional-playbooks.md](https://edgesentry.github.io/indago/regional-playbooks/) for other regions) with no duplicate MMSI/timestamp rows.

---

### A2 · Sanctions and Registry Ingestion ✅

**Goal:** Sanctions entities and vessel ownership graph loaded and queryable.

- OFAC SDN + EU + UN + OpenSanctions XML/Parquet → DuckDB `sanctions_entities` (`src/ingest/sanctions.py`)
- Equasis ownership chains → Lance Graph datasets (`src/ingest/vessel_registry.py`)
  - Node types: Vessel, Company, Country, VesselName
  - Relationship types: OWNED_BY, MANAGED_BY, REGISTERED_IN, ALIAS, SANCTIONED_BY
- Cleared-vessel feedback loop (`src/ingest/schema.py`): tracks MMSIs cleared by Phase B patrols as hard negatives.

**Acceptance:** `uv run python src/ingest/vessel_registry.py` completes without error and the Lance Graph directory contains non-empty OWNED_BY and SANCTIONED_BY datasets for vessels with known OFAC exposure.

---

### A3 · Feature Engineering ✅

**Goal:** Full feature matrix computed for all vessels in the area of interest.

- AIS behavioral features (Polars): `ais_gap_count_30d` (with configurable `--gap-threshold-hours`), `position_jump_count`, `sts_candidate_count`, `loitering_hours_30d` (`src/features/ais_behavior.py`)
- Identity volatility features: `name_changes_2y`, `owner_changes_2y` (`src/features/identity.py`)
- **[TODO — Phase C]** `flag_changes_2y` — count of flag-state changes over 24 months. Currently hardcoded to 0 (see `src/features/identity.py`). **Pre-requisite:** ingest historical flag-state records from an external source (e.g., VesselFinder/MarineTraffic historical flag API, or manual EQUASIS export). Once available, remove the `pl.lit(0)` hardcode. See #296.
- Ownership graph features (Lance Graph + Polars joins): `sanctions_distance`, `cluster_sanctions_ratio` (`src/features/ownership_graph.py`)
- Trade flow mismatch: `route_cargo_mismatch` (`src/features/trade_mismatch.py`)
- **[TODO]** GEBCO bathymetric mask (`src/features/bathymetric_mask.py`) — provides higher-precision STS candidate filtering than the current 5nm-from-port heuristic.
- **[TODO — requires GFW research token]** EO dark-vessel signal via GFW GAP events (`src/ingest/eo_gfw.py`): the `eo_dark_count_30d` and `eo_ais_mismatch_ratio` features are fully implemented and wired into the Isolation Forest, but produce zero signal today because the free-tier GFW token only returns `FISHING` events, not `GAP`/`GAP_START` (AIS transponder disabling). With a research-tier token, these features would provide a direct, satellite-corroborated observation of AIS manipulation — the only feature family that can detect a vessel that is physically present but invisible on AIS. **Pre-requisite:** request GFW research access at https://globalfishingwatch.org/data-access/ for a maritime security / sanctions compliance use case. Once granted, change `_GFW_EVENT_TYPES = ["GAP", "GAP_START"]` and `_GFW_SOURCE_LABEL = "gfw-gap"` in `src/ingest/eo_gfw.py`. See #201 for full investigation notes.

**Acceptance:** `vessel_features` table in DuckDB has one row per MMSI with no null values for core features; STS candidate count matches independently verified events from open-source maritime incident reports.

---

### A4 · Scoring Engine and Watchlist Output ✅

**Goal:** Ranked candidate watchlist with SHAP explanations and analyst dashboard.

- HDBSCAN normal MPOL baseline (`src/score/mpol_baseline.py`) with service vessel exclusion (AIS 51-59, 31-32) and cleared-vessel hard negatives.
- Isolation Forest anomaly score (`src/score/anomaly.py`) with cleared-vessel hard negatives.
- Causal Sanction-Response Model (`src/score/causal_sanction.py`): calibrates graph risk weights based on historical sanction announcement impacts (DiD regression).
- Composite score + SHAP attribution (`src/score/composite.py`), supporting `--geopolitical-event-filter` for route downweighting.
- Watchlist Output `candidate_watchlist.parquet` (`src/score/watchlist.py`)
- React + TypeScript + Vite SPA with MapLibre GL JS and ranked watchlist table (`app/`). In-browser OLAP via DuckDB-WASM; Parquet files fetched from Cloudflare R2 and cached in OPFS. Deployed to Cloudflare Pages.
- Human-in-the-Loop Triage System: Tier taxonomy, dashboard review UI, local DuckDB-WASM `vessel_reviews` table, push to R2 via CF Pages Function, server-side merge via CF Queue (`app/src/lib/reviews.ts`, `app/functions/api/reviews/push.ts`). Feedback-driven evaluation (`pipeline/src/score/review_feedback_evaluation.py`).
- Geopolitical Context Layer (GDELT + RAG): Daily GDELT event ingestion (`pipeline/src/ingest/gdelt.py`, Step 10 in `run_pipeline.py`); GDELT macro-event covariates wired into DiD regression controls; LLM-generated analyst dispatch briefs with interactive chat in the dashboard (`app/src/components/VesselDetail.tsx`).

**Acceptance:** Precision@50 ≥ 0.6 (≥ 30 of top-50 candidates are OFAC-listed vessels); SHAP explanations are human-readable and match analyst intuition on manually inspected cases; dashboard supports rapid review and pipeline re-training.

---

### A5 · Validation and Intelligence Loops ✅

**Goal:** Ensure correctness of the model through backtesting, and iteratively improve detection using intelligence loops.

- Validation script against OFAC ground truth (`src/score/validate.py`), tracking AUROC, Precision@50, Recall@200.
- Historical Backtesting Validation (`scripts/run_backtest.py`, `scripts/run_public_backtest_batch.py`) evaluating over frozen AIS windows.
- Analyst Pre-Label Holdout Set (`src/score/prelabel_evaluation.py`): evaluating against `analyst_prelabels` DB curated by human analysts.
- Delayed-Label Intelligence Loop (`src/analysis/backtracking_runner.py`): retroactive scan of newly confirmed entities, applying BFS graph label propagation (`label_propagation.py`) to discover precursor networks.

---

## Phase B — Physical Vessel Investigation

> **Implementation note:** Phase B is implemented in [edgesentry-rs](https://github.com/edgesentry/edgesentry-rs) and edgesentry-app — not in this repository. This section describes the requirements and roadmap to guide that work. Work begins after the Cap Vista submission (29 April 2026).

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
- `cleared` outcome writes MMSI to `cleared_vessels` DuckDB table; Phase A re-training uses cleared MMSIs as hard negatives.

---

## Timeline

| Week | Phase A deliverable |
|---|---|
| Week 1 (Apr 1–7) | A1: Project setup + AIS ingestion |
| Week 2 (Apr 8–14) | A2: Sanctions + Lance Graph ownership graph |
| Week 3 (Apr 15–21) | A3: Full feature engineering pipeline |
| Week 4 (Apr 22–28) | A4: Scoring + watchlist + HTMX dashboard + Intelligence Loops |
| Apr 29 | A5: Validate + submit proposal to Cap Vista |

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

A3 ──► A4 (causal model uses AIS gap + sanctions event time-series to calibrate weights)
A4 ──► A5 (backtesting evaluates historical model stability)
A4 ──► A5 (triage workflow captures labels from analysts)
A5 ──► A5 (backtracking extracts precursor patterns from new confirmed labels)
A5 ──► A4 (propagated entity labels feed forward to next screening cycle)
```
