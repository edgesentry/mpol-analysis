# Roadmap

## Phase A — Shadow Fleet Candidate Screening (This Repo)

### A1 · Project Setup and AIS Ingestion

**Goal:** Running pipeline that ingests historical and live AIS for the area of interest.

- `pyproject.toml` with full dependency set (DuckDB, Polars, Neo4j driver, scikit-learn, SHAP, Streamlit)
- DuckDB schema initialisation (`src/ingest/schema.py`)
- Marine Cadastre bulk Parquet download + DuckDB load (`src/ingest/marine_cadastre.py`)
- aisstream.io WebSocket ingestion with bounding box filter (`src/ingest/ais_stream.py`)

**Acceptance:** DuckDB `ais_positions` table contains ≥ 6 months of AIS data for the Malacca Strait / SG area with no duplicate MMSI/timestamp rows.

---

### A2 · Sanctions and Registry Ingestion

**Goal:** Sanctions entities and vessel ownership graph loaded and queryable.

- OFAC SDN + EU + UN + OpenSanctions XML/Parquet → DuckDB `sanctions_entities` (`src/ingest/sanctions.py`)
- Equasis ownership chains → Neo4j graph (`src/ingest/vessel_registry.py`)
  - Node types: Vessel, Company, Country, VesselName
  - Relationship types: OWNED_BY, MANAGED_BY, REGISTERED_IN, ALIAS, SANCTIONED_BY

**Acceptance:** Neo4j Cypher query `MATCH (v:Vessel)-[:OWNED_BY*1..3]->(:Company)-[:SANCTIONED_BY]->() RETURN v LIMIT 10` returns results for vessels with known OFAC exposure.

---

### A3 · Feature Engineering

**Goal:** Full feature matrix computed for all vessels in the area of interest.

- AIS behavioral features (Polars): `ais_gap_count_30d`, `position_jump_count`, `sts_candidate_count`, `loitering_hours_30d` (`src/features/ais_behavior.py`)
- Identity volatility features: `flag_changes_2y`, `name_changes_2y`, `owner_changes_2y` (`src/features/identity.py`)
- Ownership graph features (Neo4j GDS BFS): `sanctions_distance`, `cluster_sanctions_ratio` (`src/features/ownership_graph.py`)
- Trade flow mismatch: `route_cargo_mismatch` (`src/features/trade_mismatch.py`)

**Acceptance:** `vessel_features` table in DuckDB has one row per MMSI with no null values for core features; STS candidate count matches independently verified events from open-source maritime incident reports.

---

### A4 · Scoring Engine and Watchlist Output

**Goal:** Ranked candidate watchlist with SHAP explanations.

- HDBSCAN normal MPOL baseline (`src/score/mpol_baseline.py`)
- Isolation Forest anomaly score (`src/score/anomaly.py`)
- Composite score + SHAP attribution (`src/score/composite.py`)
- Output `candidate_watchlist.parquet` (`src/score/watchlist.py`)
- Streamlit dashboard with map + ranked table + filters (`src/viz/dashboard.py`)

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

### B5 · VDES Secure Reporting

- VDES ASM frame serialisation of signed `AuditRecord` (fragmentation + ARQ)
- Transmit on ASM channels 24/84 (ship-to-shore, ~40nm)
- VDES-SAT fallback for extended range (global)
- Shore VDES gateway: reassemble → ingest → S3 Object Lock (WORM)

**Fallback channels:** Iridium SBD (alert-only), LoRaWAN (harbour range), 4G/LTE (port area)

### B6 · Port Operations Centre Dashboard

- Real-time display of patrol evidence reports as they arrive via VDES
- Candidate watchlist rows annotated with investigation outcome (`confirmed`, `cleared`, `inconclusive`)
- Integration loop: confirmed vessels feed back into Phase A as positive labels to improve scoring

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
```

---

## Timeline

| Week | Phase A deliverable |
|---|---|
| Week 1 (Apr 1–7) | A1: Project setup + AIS ingestion |
| Week 2 (Apr 8–14) | A2: Sanctions + Neo4j ownership graph |
| Week 3 (Apr 15–21) | A3: Full feature engineering pipeline |
| Week 4 (Apr 22–28) | A4: Scoring + watchlist + Streamlit dashboard |
| Apr 29 | A5: Validate + submit proposal to Cap Vista |

Phase B timeline depends on Cap Vista trial contract award (expected within 60 days of submission deadline).
