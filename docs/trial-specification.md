# Trial Specification

This document addresses the Cap Vista Solicitation 5.0 Challenge 1 "Trial Specifications" requirement:

> *Propose the datasets and their formats. Platform technical requirements to perform the analysis.*

---

## 1. Datasets and Formats

No proprietary data feeds are required. All sources are open-access or free-tier API.

### Why open-source data is a design choice, not a limitation

The challenge statement asks for *"relevant proprietary datasets to detect MPOL of shadow fleet vessels."* arktrace deliberately uses no proprietary feeds. This is the reasoning:

| Argument | Detail |
|---|---|
| **Coverage** | OpenSanctions fuses OFAC SDN + EU FSF + UN 1267 — over 500,000 sanctioned entities from three independent regimes. No single commercial feed covers all three simultaneously with weekly update cadence. |
| **Timeliness** | OpenSanctions updates weekly. Many commercial vessel registry products update quarterly or annually — meaning a flag change or ownership transfer may not appear in a proprietary feed for months. |
| **Novel data fusion** | The combination of AIS + OpenSanctions ownership graph + Equasis vessel registry + UN Comtrade trade flow + GDELT geopolitical events is not available in any single commercial product. No competitor fuses all five at once. |
| **Cost and replicability** | $0 licensing cost enables deployment to any port authority globally, including those without enterprise procurement budgets. The Precision@50 = 0.62 result is reproducible by any analyst with an internet connection — no vendor lock-in. |
| **Demonstrated sufficiency** | The 6× lift over the ~0.10 random baseline (Precision@50 = 0.62) was achieved *without* any proprietary dataset. Adding Windward vessel intelligence scores or Lloyd's cargo manifests would be additive — but the open-source baseline already meets the ≥ 0.60 target. |

**What proprietary data would add:** Windward vessel intelligence scores provide enriched behavioural history for vessels outside AIS coverage (e.g., in AIS-sparse regions). Lloyd's cargo manifests enable direct cargo-level mismatch detection. These would increase recall at the margins. They are not required to meet the PoC success criteria and can be integrated as optional signal sources in a future scale-up phase.

| Dataset | Source | Format | Refresh cadence | Estimated volume |
|---|---|---|---|---|
| AIS positions (live) | aisstream.io WebSocket | JSON over WebSocket → DuckDB `ais_positions` | Real-time (continuous) | ~10M rows/month per region |
| AIS positions (historical, US waters) | Marine Cadastre | Parquet | Annual | ~50 GB/year |
| Sanctions entities | OFAC SDN, EU FSF, UN 1267 via OpenSanctions | JSONL | Weekly | ~500k entities |
| Vessel registry + ownership chains | Equasis | HTTP scrape / CSV export | Monthly | ~200k vessels |
| Bilateral trade statistics | UN Comtrade+ REST API | JSON | Monthly | ~500 req/month (free tier) |
| Geopolitical events (GDELT) | GDELT Project | CSV (zipped, 15-min batches) | Daily | ~100k events/day |
| Bathymetric depth mask | GEBCO | NetCDF / GeoTIFF | One-off | ~8 GB |

**Data storage:** AIS positions and sanctions entities are loaded into DuckDB (`data/processed/mpol.duckdb`). Vessel ownership chains are stored as a Lance Graph (`data/processed/mpol_graph/`). All output Parquet files are written to MinIO / S3-compatible object storage. See [docs/architecture.md](architecture.md) for the full storage design.

---

## 2. Platform Requirements

### Screening layer — Phase A (port operations centre or analyst workstation)

| Resource | Minimum | Recommended |
|---|---|---|
| CPU | 4 cores | 8 cores |
| RAM | 8 GB | 16 GB |
| Storage | 100 GB SSD | 500 GB SSD |
| GPU | Not required | Not required |
| OS | Linux / macOS | Ubuntu 22.04 LTS |
| Network | Internet (AIS stream + API pulls) | — |

The pipeline runs fully in-process — no external database server is required. DuckDB, LanceDB, and Lance Graph are all embedded, serverless libraries.

**Docker deployment (recommended):** `docker compose up` starts the full stack (MinIO object store + FastAPI dashboard) in two containers. See [docs/deployment.md](deployment.md).

### Edge deployment — Phase B (patrol vessel / UAV ground station)

| Component | Specification |
|---|---|
| Edge PC | NVIDIA Jetson Orin 8 GB (Tier 2/3) or ruggedised laptop (Tier 1) |
| Network | No internet required once databases are synced from S3 |
| AIS receiver | Standard Class B transponder or SDR-based AIS decoder |
| LiDAR (Tier 2) | Livox Mid-360 (~$1k) or Ouster OS0-32 (~$3k) |
| Thermal camera (Tier 3) | FLIR Boson+ |

See [docs/field-investigation.md](field-investigation.md) for the full Phase B sensor stack and hardware cost breakdown.

---

## 3. Trial Demonstration Strategy

Proposed trial scope: **30–60 days** on Singapore / Malacca Strait as the primary chokepoint region.

### Week 1 — Setup and baseline ingestion

- Deploy pipeline on provided VM or edge hardware using `docker compose up`
- Ingest 6 months of historical AIS for the Singapore / Malacca Strait bounding box
- Load OFAC SDN, EU FSF, and UN 1267 sanctions lists via OpenSanctions
- Pull vessel ownership chains from Equasis; build Lance Graph
- Run full feature engineering and scoring pipeline; generate initial `candidate_watchlist.parquet`

### Weeks 2–3 — Baseline validation

Run held-out evaluation against OFAC-listed vessels that appear in the Singapore AIS dataset:

| Metric | Target | Method |
|---|---|---|
| Precision@50 | ≥ 0.60 | Fraction of top-50 scored vessels that are OFAC-listed |
| Recall@200 | ≥ 0.40 | Fraction of known OFAC-listed vessels recovered in top 200 |
| AUROC | ≥ 0.75 | Area under ROC curve across full scored population |

Compare against naïve AIS-gap-only baseline to demonstrate additive value of the ownership graph and trade flow layers. See [docs/evaluation-metrics.md](evaluation-metrics.md) for full metric definitions and current measured baselines (Precision@50 = 0.62 on Singapore run).

### Weeks 3–7 — Live monitoring

- Connect live aisstream.io WebSocket for the Singapore / Malacca Strait bounding box
- Run continuous re-scoring at 15-minute cadence as new AIS batches arrive
- Duty officers use the FastAPI + HTMX dashboard for morning briefs and patrol dispatch decisions
- Analyst chat (C6) and GDELT-grounded analyst brief (C2) available for each flagged vessel

### Weeks 5–9 — Patrol handoff (Phase B integration, if hardware available)

- For top-N candidates selected by duty officers, dispatch patrol vessel with edgesentry-app (Phase B software)
- Record confirmed / cleared outcomes per vessel
- Feed confirmed positives back into Phase A as hard positive labels; cleared vessels become hard negatives
- Rerun causal sanction-response calibration (`src/score/causal_sanction.py`) with updated labels

---

## 4. Success Metrics

| Metric | Target | Measurement |
|---|---|---|
| Precision@50 | ≥ 0.60 | Fraction of top-50 candidates that are OFAC-listed or confirmed by patrol |
| Patrol hit rate | ≥ 0.50 | Fraction of dispatched patrols returning a confirmed or probable finding |
| False positive rate | ≤ 0.30 of reviewed | Fraction of reviewed vessels cleared with no suspicious finding |
| Dashboard page load | < 2 s | Time to load ranked watchlist in dashboard |
| Re-score cycle | < 60 s | Time for incremental score update after new AIS batch |
| Pipeline cold-start | < 45 min | Time to run full pipeline on 6-month historical dataset (Singapore region) |

The Precision@50 target of 0.60 represents a **6× lift** over the ~0.10 base rate of sanctioned vessels in the monitored population. The 0.62 figure measured on the Singapore pipeline run already meets this target. See [docs/evaluation-metrics.md](evaluation-metrics.md) for full validation methodology.

---

## Related documents

- [docs/architecture.md](architecture.md) — system design and storage layer
- [docs/deployment.md](deployment.md) — Docker and bare-metal setup instructions
- [docs/evaluation-metrics.md](evaluation-metrics.md) — metric definitions and validated baselines
- [docs/field-investigation.md](field-investigation.md) — Phase B patrol vessel sensor stack
- [docs/regional-playbooks.md](regional-playbooks.md) — per-region AIS bbox and weight tuning
