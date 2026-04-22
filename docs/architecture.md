# Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  PUBLIC DATA SOURCES                                            │
│                                                                 │
│  AIS (aisstream.io WebSocket with --bbox override;              │
│       Marine Cadastre Parquet for US waters only)               │
│  Sanctions (OFAC SDN, EU, UN, OpenSanctions CC0)                │
│  Vessel registry (Equasis, ITU MMSI)                            │
│  Trade flow (UN Comtrade API)                                   │
│  GDELT (public HTTP feed — news-signal enrichment)              │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────┼──────────────────────────────────────┐
│  PROPRIETARY FEEDS  [OPTIONAL — private bucket]  │              │
│  arktrace-private-capvista (Cloudflare R2)       │              │
│                                                  │              │
│  Only needed if you want custom data ingested    │              │
│  into the CI pipeline.  Skip this bucket and     │              │
│  CI runs on public data only (--seed-dummy).     │              │
│                                                  │              │
│  App owner & CI: read + write access (same key). │              │
│                                                  │              │
│  AIS CSV / NMEA ─────────────────────────────────┤              │
│  SAR detections ──  push: sync_r2.py push-custom-feeds         │
│  Cargo manifests ─  pull: sync_r2.py pull-custom-feeds         │
│  Custom sanctions ──  (skipped gracefully if bucket absent)     │
└──────────────────────────┼──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER  (src/ingest/)                                 │
│                                                                 │
│  AIS positions ──────────────────► DuckDB (ais_positions table) │
│  Sanctions entities ─────────────► DuckDB (sanctions_entities)  │
│  Vessel ownership chains ────────► Lance Graph (on-disk files)  │
│  Trade flow by route ────────────► DuckDB (trade_flow table)    │
│  Custom feeds (step 5) ──────────► DuckDB (auto-detected type)  │
│    · AIS CSV → ais_positions                                    │
│    · SAR CSV → sar_detections                                   │
│    · Cargo CSV → trade_flow                                     │
│    · Sanctions CSV → sanctions_entities                         │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  FEATURE ENGINEERING  (src/features/)                           │
│                                                                 │
│  AIS behavioral features  ───────► Polars DataFrame             │
│    · gap count / max gap hours                                  │
│    · position jump count (spoofing)                             │
│    · STS candidate events                                       │
│    · port call ratio                                            │
│                                                                 │
│  Identity volatility features ───► Polars DataFrame             │
│    · flag_changes_2y                                            │
│    · name_changes_2y                                            │
│    · owner_changes_2y                                           │
│                                                                 │
│  Ownership graph features ───────► Lance Graph (Polars joins)   │
│    · sanctions_distance (min hops to sanctioned entity)         │
│    · cluster_sanctions_ratio                                    │
│                                                                 │
│  Trade mismatch features ────────► Polars + DuckDB              │
│    · route_cargo_mismatch                                       │
└──────────────────────────┬──────────────────────────────────────┘
                           │  combined feature matrix (Polars)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  SCORING ENGINE  (src/score/)                                   │
│                                                                 │
│  HDBSCAN ── normal MPOL baseline (per vessel type / route)      │
│  Isolation Forest ── anomaly_score ∈ [0,1]                      │
│  Lance Graph ── graph_risk_score ∈ [0,1]                        │
│  C3 DiD model ─ calibrate graph_risk_score weight (→ composite) │
│  Composite ── confidence = w_a·anomaly + w_g·graph              │
│                           + w_i·identity_volatility             │
│              (weights calibrated by causal_sanction.py)         │
│  SHAP ── top_signals JSON per vessel                            │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  CI DATA PUBLISHING  (data-publish.yml — weekly + on-merge)     │
│                                                                 │
│  Runs backtest on active regions (seed mode + custom feeds)      │
│  then pushes artifacts to arktrace-public (Cloudflare R2):      │
│                                                                 │
│  · <timestamp>.zip  ── generation zip (all region artifacts)    │
│  · demo.zip         ── lightweight bundle (no DuckDB)           │
│  · public_eval.duckdb ── OpenSanctions DB (integration tests)   │
│  · gdelt.lance.zip  ── GDELT corpus (analyst brief / chat)      │
│                                                                 │
│  arktrace-public is fully public — no credentials to download.  │
└──────────────────────────┬──────────────────────────────────────┘
                           │  app users pull via sync_r2.py
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  OUTPUT  (Cloudflare Pages — React SPA, edge-first)             │
│                                                                 │
│  arktrace-public (R2) ──► browser downloads Parquet via manifest│
│  OPFS cache ────────────► DuckDB-WASM queries locally           │
│  React SPA (app/) ──────► Watchlist + Map + VesselDetail        │
│                                                                 │
│  Analyst reviews ──► CF Pages Function ──► R2 ──► CF Queue     │
│                       (POST /api/reviews/push)   (merge job)    │
│                                                                 │
│  LLM brief ─────────► OpenAI-compatible endpoint (browser call) │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼  handoff
┌─────────────────────────────────────────────────────────────────┐
│  PHYSICAL INVESTIGATION  (edgesentry-app / edgesentry-rs)       │
│  (out of scope for this repo — see roadmap.md)                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  STORAGE LAYER  (cross-cutting)                                 │
│                                                                 │
│  OLAP  — DuckDB + Parquet                                       │
│    · local:  data/processed/<region>.duckdb                     │
│    · Parquet outputs: data/processed/*.parquet                  │
│              or  s3://arktrace/processed/  (MinIO / S3)         │
│                                                                 │
│  Graph — Lance (embedded, serverless)                           │
│    · local:  data/processed/<region>_graph/                     │
│              data/processed/gdelt.lance                         │
│    · remote: s3://arktrace/mpol_graph/                          │
│              s3://arktrace/gdelt.lance  (MinIO / S3)            │
│                                                                 │
│  Object store — Cloudflare R2  (production distribution)        │
│    · arktrace-public    — public read; app users pull from here │
│    · arktrace-private-capvista — auth; proprietary custom feeds │
│                                                                 │
│  Object store — MinIO  localhost:9000  (local dev / Docker)     │
│    · bucket: arktrace  (created by minio_init on first run)     │
│    · console: localhost:9001                                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Storage Design

### DuckDB (`data/processed/mpol.duckdb`)

DuckDB is the primary analytical store. It runs in-process with no server and queries Parquet files directly. Multi-region deployments use separate DuckDB files per region (e.g. `data/processed/europe.duckdb`) — every script accepts a `--db` flag to target the correct file. See [regional-playbooks.md](regional-playbooks.md) for per-region paths and bbox values.

**Parquet persistence:** all pipeline output files (watchlist, causal effects, validation metrics) are written by `src/storage/config.py`. When `S3_BUCKET` is set, output goes to `s3://<bucket>/processed/<filename>` via MinIO or any S3-compatible store; otherwise it writes to `data/processed/<filename>` on the local filesystem. No code changes are required to switch between the two modes.

| Table | Key columns | Source |
|---|---|---|
| `ais_positions` | `mmsi, timestamp, lat, lon, sog, cog, nav_status, ship_type` | aisstream.io (all regions); Marine Cadastre Parquet (US waters only) |
| `sanctions_entities` | `entity_id, name, mmsi, imo, flag, type, list_source` | OFAC, EU, UN, OpenSanctions |
| `trade_flow` | `reporter, partner, hs_code, period, trade_value_usd, route_key` | UN Comtrade |
| `vessel_meta` | `mmsi, imo, name, flag, ship_type, gross_tonnage` | Equasis + ITU MMSI |
| `vessel_features` | one row per MMSI, all engineered features | Computed by `src/features/` |

### Lance Graph (`data/processed/mpol_graph/`)

Lance Graph stores the vessel ownership graph as columnar Lance datasets — no external server or Docker container required. The graph directory is written by `src/ingest/vessel_registry.py` and read by `src/features/ownership_graph.py` and `src/features/identity.py`.

**Storage backend:** `src/storage/config.py` exposes `lance_graph_uri(stem)` and `lance_db_uri()` which resolve to local paths (`data/processed/mpol_graph/`, `data/processed/gdelt.lance`) when running without S3, and to `s3://arktrace/mpol_graph/` / `s3://arktrace/gdelt.lance` when `S3_BUCKET` is set. Lance's built-in object store support handles the S3 read/write transparently.

**Node datasets** (one Lance file each):
- `Vessel {mmsi, imo, name}`
- `Company {id, name, country}`
- `Country {code}`
- `VesselName {name}`
- `Address {address_id, street}`
- `SanctionsRegime {name}`

**Relationship datasets** (src_id → dst_id plus edge properties):
- `OWNED_BY` — `(Vessel.mmsi) → (Company.id)` with `{since, until}`
- `MANAGED_BY` — `(Vessel.mmsi) → (Company.id)` with `{since, until}`
- `REGISTERED_IN` — `(Company.id) → (Country.code)`
- `CONTROLLED_BY` — `(Company.id) → (Company.id)` — beneficial ownership layers
- `ALIAS` — `(Vessel.mmsi) → (VesselName.name)` with `{date}`
- `SANCTIONED_BY` — `(Vessel.mmsi | Company.id) → (SanctionsRegime.name)` with `{list, date}`
- `REGISTERED_AT` — `(Company.id) → (Address.address_id)` — shared-address clustering
- `STS_CONTACT` — `(Vessel.mmsi) → (Vessel.mmsi)` — co-location events

**Key graph queries** (implemented as Polars joins in `src/features/`):
```python
# Minimum BFS distance from vessel to any sanctioned company
# 0 = directly sanctioned, 1 = 1-hop owner/manager, 2 = 2-hop via CONTROLLED_BY, 99 = none
```

---

## Data Distribution (Cloudflare R2)

CI generates pre-built artifacts and publishes them to Cloudflare R2 after every
weekly pipeline run so app users can start the dashboard without running the
pipeline locally.

### Two-bucket model

| Bucket | Access | Contents | Who reads/writes | Required? |
|---|---|---|---|---|
| `arktrace-public` | Anonymous read (no credentials) | Generation zips, demo bundle, `public_eval.duckdb`, `gdelt.lance.zip` | CI writes; app users read; app owner writes | **Yes** |
| `arktrace-private-capvista` | Authenticated read/write | Proprietary custom feed CSVs (AIS, SAR, cargo, sanctions) | App owner reads/writes; CI reads | **Optional** — only needed to feed custom data into the CI pipeline. Without it, CI runs on public data only (`--seed-dummy`). |

`arktrace-private-capvista` is **private and optional**. If the bucket or its credentials
are absent, `pull-custom-feeds` exits silently and the pipeline continues with public data.
No code changes are required — the step uses `continue-on-error: true` in CI.

When the private bucket is used, a single R2 API token (Cloudflare Dashboard → R2 →
Manage R2 API Tokens) is scoped to both buckets and shared between app owner and CI.
CI maps `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` repository secrets to
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.

### CI pipeline (data-publish.yml)

Triggers: weekly cron (Monday 02:00 UTC) and after every successful
`Public Backtest Integration` run on `main`.

1. Pull custom feed CSVs from `arktrace-private-capvista` → `_inputs/custom_feeds/` (`continue-on-error`; forks skip gracefully).
2. Run the full 9-step pipeline for all five regions in seed mode (`--seed-dummy`). Custom feeds are ingested at step 5 and their signals appear in all downstream scoring.
3. Push generation zip (`<timestamp>.zip`) to `arktrace-public`; delete previous generation (`--keep 1`).
4. Push `demo.zip` (lightweight bundle for quick developer setup).
5. Push `public_eval.duckdb` (OpenSanctions DB).

### App user pull options

```bash
# Pull one region — starts dashboard immediately
uv run python scripts/sync_r2.py pull --region singapore

# Lightweight demo bundle (no DuckDB, ~10–50 MB)
uv run python scripts/sync_r2.py pull-demo

# OpenSanctions DB for integration tests
uv run python scripts/sync_r2.py pull-sanctions-db
```

No credentials required. See [r2-data-layout.md](r2-data-layout.md) for the full
bucket layout, actor responsibilities, and credential model.

---

## Feature Design

### AIS Behavioral Features

Computed with Polars over a rolling 30-day window per MMSI.

| Feature | Definition | Shadow fleet signal |
|---|---|---|
| `ais_gap_count_30d` | Gaps > 10h in AIS signal while in open sea | STS transfer or deliberate dark period (10h threshold avoids normal 8–12h anchorage waits) |
| `ais_gap_max_hours` | Longest single gap | Severity indicator |
| `position_jump_count` | Consecutive positions implying > 50 knots | GPS spoofing |
| `sts_candidate_count` | Co-located drift events (2 vessels within 0.5nm, both drifting, at sea) | Illicit STS transfer |
| `port_call_ratio` | AIS-declared port calls ÷ detected anchorage events | Port declaration fraud |
| `loitering_hours_30d` | Hours at < 2 knots outside port boundaries | Waiting for STS opportunity |

### Identity Volatility Features

Computed from Equasis historical data via Lance Graph datasets.

| Feature | Definition |
|---|---|
| `flag_changes_2y` | Number of flag state changes in rolling 2 years |
| `name_changes_2y` | Number of name changes in rolling 2 years (from ALIAS dataset) |
| `owner_changes_2y` | Number of registered owner changes (from OWNED_BY dataset) |
| `high_risk_flag_ratio` | Fraction of time under flags with weak PSC oversight |
| `ownership_depth` | Number of beneficial ownership layers to natural person |

### Ownership Graph Features

Computed by Polars joins over Lance Graph datasets.

| Feature | Definition |
|---|---|
| `sanctions_distance` | Min BFS hops from vessel to any sanctioned entity (0 = vessel itself sanctioned) |
| `cluster_sanctions_ratio` | Fraction of vessels in same ownership cluster that are sanctioned |
| `shared_manager_risk` | Max sanctions_distance among all vessels sharing the same manager |
| `shared_address_centrality` | Number of distinct vessels sharing the same registered address as any company in this vessel's ownership chain |
| `sts_hub_degree` | Number of distinct vessels this vessel has been co-located with (STS_CONTACT degree) — identifies laundering hubs |

### Trade Flow Mismatch Features

Computed by joining AIS route segments to UN Comtrade flow data.

| Feature | Definition |
|---|---|
| `route_cargo_mismatch` | Declared cargo type vs modal cargo on detected origin→destination route |
| `declared_vs_estimated_cargo_value` | AIS-implied cargo volume vs UN Comtrade flow value for that route/period |

---

## Scoring Design

### MPOL Baseline (HDBSCAN)

HDBSCAN clusters vessels by behavioral profile (speed pattern, route regularity, gap frequency) stratified by vessel type and route corridor. The resulting cluster labels define "normal" MPOL for each segment. Vessels that fall outside all clusters (noise points) are assigned higher anomaly weight.

### Anomaly Score (Isolation Forest)

Isolation Forest is trained on the full feature matrix of vessels with `sanctions_distance ≥ 3` (assumed clean) to learn normal behavior. The resulting anomaly scores are calibrated to `[0,1]`.

### C3 · Causal Sanction-Response Model (DiD)

`src/score/causal_sanction.py` quantifies whether AIS gap frequency *causally increases* after sanction announcements for vessels connected (within 2 graph hops) to sanctioned entities. This is used to calibrate the `graph_risk_score` weight in the composite formula.

For each regime (OFAC Iran, OFAC Russia, UN DPRK) the model fits a Difference-in-Differences (DiD) regression:

```
outcome_{it} = β₀ + β₁·treated_i + β₂·post_t + β₃·(treated_i × post_t)
             + vessel_type FEs + route_corridor FEs + ε_{it}
```

where **β₃ (ATT)** is the sanction-attributable increase in AIS gaps per 30 days. OLS is estimated with HC3 heteroskedasticity-robust standard errors. Multiple announcement dates per regime are pooled via inverse-variance weighting.

**Weight calibration:** `calibrate_graph_weight(effects)` maps the fraction of positive-significant ATT estimates to a `w_graph` value in **[0.20, 0.65]**. Pass it to `compute_composite_scores()` via `--w-graph`:

```bash
# Calibrate then score
uv run python src/score/causal_sanction.py --output data/processed/causal_effects.parquet
uv run python src/score/composite.py --w-graph <calibrated_value>
```

Outputs: `data/processed/causal_effects.parquet` — regime, n_treated, n_control, ATT estimate, 95% CI, p-value, is_significant, calibrated_weight.

**Dashboard exposure:** the file is served via `GET /api/causal-effects` and rendered in the vessel review panel as per-regime ATT badges:

```
⚡ OFAC Iran    ATT = +0.42   95% CI [+0.31, +0.53]   p < 0.001
⚡ OFAC Russia  ATT = +0.15   95% CI [-0.02, +0.32]   p = 0.09   n.s.
```

Significant regimes (p < 0.05) are highlighted in indigo; non-significant regimes are rendered in grey. Returns `{"available": false}` if the file does not yet exist (e.g. before the first pipeline run).

### Composite Score

```
confidence = w_anomaly × anomaly_score
           + w_graph   × graph_risk_score
           + w_identity × identity_volatility_score
```

Standalone `composite.py` defaults: `w_anomaly = 0.35`, `w_graph = 0.55`, `w_identity = 0.10`. The pipeline (`scripts/run_pipeline.py`) applies region-specific presets before C3 auto-calibration overrides `w_graph` — see [regional-playbooks.md](regional-playbooks.md) for per-region values. All three weights are configurable via `--w-anomaly`, `--w-graph`, `--w-identity` CLI flags. The C3 causal model provides a data-driven `w_graph` calibration (see section above and [roadmap.md](roadmap.md) Phase C, C3).

Per-region weight tuning recommendations are in [regional-playbooks.md](regional-playbooks.md).

### Explainability (SHAP)

SHAP TreeExplainer computes per-feature contributions to the anomaly score for each vessel. The top 5 contributing features are serialised as `top_signals` JSON in the watchlist output and served via `GET /api/vessels/{mmsi}/signals`. The review panel renders them as a mini-table (Feature / Value / SHAP contribution / bar) so a duty officer can understand *why* a vessel was flagged without reading raw feature values.

---

## LLM Integration

The LLM converts a deterministic, structured risk assessment into readable English for the analyst. All scoring decisions are made before the LLM is called; the model receives a pre-computed context window and cannot modify scores or access external data.

**Use cases:**

| Code | Input | Output |
|------|-------|--------|
| C2 — Analyst brief | Vessel profile + SHAP `top_signals` + 3 GDELT events | One-paragraph risk summary per vessel |
| C6 — Analyst chat | Fleet overview + optional vessel detail + analyst question | Grounded factual answer |

**Provider selection:** controlled by `LLM_PROVIDER` environment variable.

| Value | Backend |
|-------|---------|
| `llamacpp` *(default)* | Bundled llamacpp server — no external process required |
| `ollama` | Ollama local server |
| `anthropic` | Anthropic API (requires `LLM_API_KEY`) |
| `gemini` | Google Gemini API (requires `LLM_API_KEY`) |
| `openai` | Any OpenAI-compatible endpoint |

Recommended local model: **Gemma 4 4B Instruct (Q4_K_M)** via llamacpp — downloaded automatically on first `docker compose up`. Context window fits within ~1 200 tokens; no GPU required.

**No cloud dependency:** inference runs entirely on-device by default. The LLM has no tool access, no function calling, and no internet connectivity during inference. Context is injected via the context window only.

See [docs/local-llm-setup.md](local-llm-setup.md) for model recommendations, hardware requirements, and setup instructions.
