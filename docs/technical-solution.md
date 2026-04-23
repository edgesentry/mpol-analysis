# Technical Solution

## Primary Innovation

arktrace is a **Causal Inference Engine for Shadow Fleet Prediction**. The primary technical contribution is the C3 Causal Sanction-Response model (`src/score/causal_sanction.py`) and the unknown-unknown detector (`src/analysis/causal.py`).

**What this is not:** real-time vessel monitoring, anomaly detection on raw AIS data, conventional sanctions screening, or off-the-shelf behavioural analytics.

**What this is:** A Difference-in-Differences (DiD) framework that tests, for each vessel, whether behavioural change was *causally triggered* by a specific sanction announcement — using HC3-robust OLS to distinguish genuine evasion responses from normal commercial variation. Vessels that pass the causal filter are then propagated through the ownership graph to surface connected unknown-unknown threats before any designation occurs.

**Decision-to-Dispatch flow**

```
DATA INGESTION
  AIS · Vessel registry · Sanctions lists · Trade flows · GDELT · EO/SAR
          │
          ▼
LAYER 1 — BEHAVIOURAL SUBSTRATE
  HDBSCAN baseline · Isolation Forest · Lance Graph BFS · SHAP attribution
          │
          ▼
LAYER 2 — C3 CAUSAL INFERENCE ENGINE
  DiD OLS · HC3 robust SE · ATT ± 95% CI · p < 0.05 (configurable)
          │
          ├─► Known fleet: ranked watchlist by causal confidence
          └─► Unknown-unknown detector: pre-designation evasion signatures
          │
          ▼
ANALYST DASHBOARD  →  PATROL DISPATCH BRIEF
```

| Model component | Implementation | Role |
|---|---|---|
| C3 DiD causal model | `src/score/causal_sanction.py` | Tests causal response to sanction events; primary innovation |
| Unknown-unknown detector | `src/analysis/causal.py` | Surfaces non-sanctioned vessels with evasion-consistent causal signatures |
| Backtracking propagation | `scripts/run_backtracking.py` | Graph-walks ownership network from confirmed evaders to predict next designations |
| AIS behaviour signals | `src/features/` | Input substrate for the causal model; not the primary discriminator |
| Sanctions screening | `src/data/sanctions.py` | Input substrate; frames the event timeline for DiD windows |

The 60–90 day pre-designation lead time (backtested — see [docs/scoring-model.md](scoring-model.md)) is a direct result of the causal model detecting evasion *responses* before those vessels accumulate enough evidence for a formal OFAC designation.

---

## Challenge Alignment — Shadow Fleet Behaviours

Cap Vista Solicitation 5.0, Challenge 1 names three specific shadow fleet behaviours:

> *"sophisticated AIS spoofing, frequent name/flag changes, and illicit ship-to-ship (STS) transfers to bypass international sanctions"*

arktrace maps directly to each:

| Challenge behaviour | arktrace feature(s) | Implementation |
|---|---|---|
| **AIS spoofing** | `position_jump_count` (implied speed > 50 kts indicates GPS broadcast spoofing) | `src/features/movement.py` |
| **AIS dark periods** | `ais_gap_count_30d`, `ais_gap_max_hours` | `src/features/movement.py` |
| **Frequent name changes** | `name_changes_2y` | `src/features/identity.py` |
| **Frequent flag changes** | `flag_changes_2y`, `high_risk_flag_ratio` | `src/features/identity.py` |
| **Ship-to-ship (STS) transfers** | `sts_candidate_count`, `sts_hub_degree` | `src/features/sts.py` |

These five features are the **evidentiary substrate** that feeds the C3 DiD causal model — not the final output claim. The model tests whether AIS gap counts, identity churn, and STS event frequency *increased causally in response to a specific sanction announcement*, separating deliberate evasion from ordinary commercial variation.

### Geographic Scope

The challenge specifies *"major shipping lanes up to 1,600 nm from Singapore to water depth of 200 m below mean sea level."* arktrace's default Singapore / Malacca Strait bounding box (`−5°N 92°E → 22°N 122°E`) covers this area. A **200 m bathymetric depth mask** (GEBCO) is applied during STS candidate detection: co-locations in water shallower than 200 m are excluded, removing false positives from port anchorages and shallow straits (the Malacca Strait narrows to ~25 m in some sections). Illicit STS transfers occur in international open water, which is uniformly deep. See [docs/regional-playbooks.md](regional-playbooks.md) for bbox details.

---

## Tech Stack

| Layer | Tool | Version | Rationale |
|---|---|---|---|
| Analytical store | **DuckDB** | ≥ 1.1 | In-process columnar OLAP; queries Parquet natively; no server; edge-deployable |
| DataFrame / feature engineering | **Polars** | ≥ 1.0 | Lazy evaluation; fast AIS window operations; Arrow-native |
| Graph DB | **Lance Graph** | ≥ 0.5 | Embedded in-process graph engine; stores ownership graph as Lance columnar files; local path or S3-compatible (`s3://`) |
| Object store | **MinIO** | RELEASE.2025-09-07 | S3-compatible local object store; persists Parquet and Lance datasets; port 9000 (API) / 9001 (console) |
| ML / clustering | **scikit-learn** | ≥ 1.5 | HDBSCAN, Isolation Forest; no GPU required |
| Explainability | **SHAP** | ≥ 0.46 | TreeExplainer for Isolation Forest; per-vessel feature attribution |
| Dashboard | **React + TypeScript + Vite** | 18 / 5 / — | Edge-first SPA; DuckDB-WASM for in-browser OLAP; OPFS for local Parquet cache; MapLibre GL JS for vessel map |
| Local LLM | **Ollama / MLX / LM Studio** | — | Local inference for analyst briefs and chat; no cloud dependency; provider selected via `LLM_PROVIDER` env var |
| AIS streaming | **websockets** + **httpx** | — | aisstream.io WebSocket; Marine Cadastre HTTP download |
| Causal inference | **numpy / scipy** (built-in) | — | DiD OLS with HC3 robust SEs; no external causal library required |
| Language | **Python 3.12** | — | Best ecosystem fit for all above |
| Packaging | **uv** | — | Fast lockfile-based dependency management |

---

## Local-First Deployment

The full stack runs on a single machine — a field laptop, a shipboard server, or a detached tactical edge node — with no cloud dependency during operation. All data remains on-device. Cloud connectivity is optional and used only for upstream AIS streaming or report export.

```
┌─────────────────────────────────────────────────────────────────┐
│  PIPELINE HOST  (server / CI / developer laptop)                │
│                                                                 │
│  AIS stream (aisstream.io WebSocket)                            │
│  SAR / EO imagery (offline batch or USB import)                 │
│         │                                                       │
│         ▼                                                       │
│  Scoring Engine (DuckDB + Polars + HDBSCAN + Isolation Forest)  │
│         │                      │                               │
│         │              Lance Graph (ownership network)          │
│         │                      │                               │
│         ▼                      ▼                               │
│  Parquet artifacts  →  sync_r2.py push  →  arktrace-public (R2) │
└─────────────────────────────────────────────────────────────────┘
          │  browser fetches Parquet via manifest
          ▼
┌─────────────────────────────────────────────────────────────────┐
│  ANALYST BROWSER  (Cloudflare Pages SPA)                        │
│                                                                 │
│  OPFS cache  ←──  manifest diff  ←──  arktrace-public (R2)     │
│                                                                 │
│  DuckDB-WASM  ←──  registered Parquet files  ←──  OPFS         │
│         │                                                       │
│         ▼                                                       │
│  React SPA  →  Watchlist + Map + VesselDetail                   │
│         │                                                       │
│         ↕  (context window injection — no external calls)       │
│         │                                                       │
│  OpenAI-compatible endpoint  →  Analyst brief / chat response   │
│         │                                                       │
│         ▼                                                       │
│  Duty Officer review  →  CF Pages Function  →  R2  →  CF Queue │
└─────────────────────────────────────────────────────────────────┘
```

| Component | Runtime | Default path / URL |
|---|---|---|
| OLAP store (pipeline) | DuckDB in-process | `data/processed/mpol.duckdb` |
| Parquet outputs | Polars → local or S3 | `data/processed/*.parquet` / `s3://arktrace/processed/` |
| Ownership graph | Lance embedded | `data/processed/mpol_graph/` |
| Geopolitical index | Lance embedded | `data/processed/gdelt.lance` |
| Object store (local dev) | MinIO `localhost:9000` | `minio_data` Docker volume |
| Object store (production) | Cloudflare R2 | `arktrace-public` (anonymous read) |
| Web app | Cloudflare Pages (prod) / Vite dev server (local) | `https://arktrace.edgesentry.io` / `localhost:5173` |
| In-browser OLAP | DuckDB-WASM + OPFS | Browser origin private file system |
| LLM inference | OpenAI-compatible endpoint (browser call) | Configured via `LLM_PROVIDER` env var |

**Storage backend selection** is automatic: when `S3_BUCKET` is set in the environment, `src/storage/config.py` routes all Parquet and Lance I/O to `s3://<bucket>/…` via MinIO (or any S3-compatible store). When unset, everything writes to local `data/processed/` paths. No code changes are required to switch between the two modes.

---

## Data Sources

### AIS Data

| Source | Coverage | Format | Cost |
|---|---|---|---|
| [aisstream.io](https://aisstream.io) | Real-time global AIS WebSocket | JSON over WS | Free (API key) |
| [Marine Cadastre](https://marinecadastre.gov/ais/) | Historical US waters AIS, 2015–present | CSV / Parquet | Free download |
| [AIS Hub](https://www.aishub.net) | Near-real-time aggregated AIS | NMEA / JSON | Free tier available |

aisstream.io supports all regions via the `--bbox lat_min lon_min lat_max lon_max` flag. The default bbox is the Singapore / Malacca Strait (`−5 92 22 122`). For other regions, pass `--bbox` with the appropriate coordinates and `--db` to write to a region-specific DuckDB file. Marine Cadastre is used only for US coastal regions (Gulf of Mexico, US West Coast). For non-US historical backfill (Japan Sea, Europe, Middle East), use AISHub or MarineTraffic CSV exports loaded via `load_csv_to_duckdb()` with a custom bbox. See [regional-playbooks.md](regional-playbooks.md) for per-region configuration.

**S-AIS / provider-agnostic ingestion:** arktrace is AIS-provider agnostic. Any Satellite AIS (S-AIS) provider (Spire Maritime, exactEarth, ORBCOMM, etc.) can substitute or supplement aisstream.io without code changes. Two ingestion paths are supported:

- **NMEA feed:** pipe raw NMEA sentences to `src/ingest/ais_stream.py` — sentences are decoded and written to the same DuckDB schema as the WebSocket path.
- **CSV export:** place any S-AIS CSV export in `_inputs/custom_feeds/` with a `.columnmap.json` sidecar mapping provider column names to the arktrace schema. The auto-detector picks it up on the next pipeline run (`step_custom_feeds`). See [pipeline-operations.md](pipeline-operations.md#custom-feed-drop-ins) for the full drop-in interface.

Switching providers or adding a secondary S-AIS feed requires no architectural changes — only a column mapping file.

### Sanctions & Registry Data

| Source | Data | Format | Cost |
|---|---|---|---|
| [OFAC SDN](https://ofac.treas.gov/sanctions-list-service) | US sanctions: vessels, companies, individuals | XML | Free |
| [EU Financial Sanctions](https://webgate.ec.europa.eu/fsd/fsf) | EU consolidated sanctions list | XML / CSV | Free |
| [UN Consolidated List](https://scsanctions.un.org) | UN Security Council sanctions | XML | Free |
| [OpenSanctions](https://www.opensanctions.org) | Merged sanctions + PEP dataset | JSON / Parquet | Free (CC0) |
| [Equasis](https://www.equasis.org) | Vessel ownership, flag, class history | Web (scraper) | Free (registration) |
| [ITU MMSI database](https://www.itu.int/online/mms/mars/ship_search.sh) | MMSI → vessel mapping | CSV download | Free |

### Trade Flow Data

| Source | Data | Format | Cost |
|---|---|---|---|
| [UN Comtrade+](https://comtradeplus.un.org) | Bilateral trade by HS code, port, period | REST API → JSON | Free (500 req/day) |

### Geospatial Reference Data

| Source | Data | Format | Cost |
|---|---|---|---|
| [GEBCO](https://www.gebco.net/) | Global bathymetric grid (water depth) | NetCDF / GeoTIFF | Free download |

GEBCO is used to build a **200 m depth mask** as an H3 resolution-8 hexagon set. The mask is pre-computed once via `scripts/build_gebco_mask.py` (downloads GEBCO 2024 via WCS for each region bbox, converts deep-water pixels to H3 cells, writes `{region}_deep_cells.parquet`). During STS candidate detection, co-locations in H3 cells shallower than 200 m are excluded — these are typically port anchorages, shallow straits, or coastal water where innocent vessel proximity is common. Only open-ocean deep-water co-locations are scored as STS candidates.

### Geopolitical Event Data

| Source | Data | Format | Cost |
|---|---|---|---|
| [GDELT Project](https://www.gdeltproject.org/) | Global news events: sanctions, conflicts, corporate actions | CSV (daily) | Free |

GDELT event records (EventCode, Actor1, Actor2, GoldsteinScale) are ingested as a time-series alongside AIS data. The primary use is correlating sanction announcement dates with AIS gap spikes in the area of interest — providing geopolitical context for anomaly scoring rather than acting as a primary detection signal.

---

## Key Algorithms

### AIS Gap Detection (Polars)

```python
# Identify gaps > 6h per MMSI, sorted by timestamp
df.sort(["mmsi", "timestamp"]) \
  .with_columns(
      pl.col("timestamp").diff().over("mmsi").alias("gap")
  ) \
  .filter(pl.col("gap") > pl.duration(hours=6))
```

Gaps are then aggregated per MMSI over a rolling 30-day window.

### Position Jump Detection (Polars)

Consecutive AIS positions are checked for implied speed:

```
implied_speed = haversine(pos_t, pos_{t+1}) / delta_t
```

Values > 50 knots between two non-gap positions indicate spoofed coordinates.

### STS Candidate Detection

Two-vessel co-location is detected by:
1. Spatial join: pairs of vessels within 0.5nm at the same timestamp
2. Filter: both vessels have `nav_status` ∈ {drifting, at anchor} AND position is > 5nm from any port
3. Duration filter: co-location persists > 2 hours

Implemented as a DuckDB spatial query (using `h3` or ST_Distance on lat/lon).

### Ownership Graph (Lance Graph + Polars)

Vessel ownership chains are stored as Lance columnar datasets on disk (no external server). Graph features are computed by Polars joins over these datasets in `src/features/ownership_graph.py` and `src/features/identity.py`.

```python
# BFS shortest path from vessel to nearest sanctioned entity
# 0 = directly sanctioned, 1 = 1-hop owner/manager, 2 = 2-hop via CONTROLLED_BY, 99 = none
tables = load_tables(db_path)
vessel_companies = pl.concat([OWNED_BY, MANAGED_BY]).unique()
one_hop = vessel_companies.filter(pl.col("dst_id").is_in(sanctioned_ids))["src_id"]
```

Cluster sanctions ratio is computed by self-joining the OWNED_BY dataset on `company_id`; `cluster_sanctions_ratio` is the fraction of co-owned vessels that are directly sanctioned.

```python
# Hub vessel detection: STS contact degree
sts_hub = STS_CONTACT.group_by("src_id").agg(
    pl.col("dst_id").n_unique().alias("sts_hub_degree")
)

# Shared-address clustering
vessel_address = vessel_company.join(REGISTERED_AT, on="company")
shared = vessel_address.join(vessel_address, on="address") \
    .filter(pl.col("vessel") != pl.col("peer")) \
    .group_by("vessel").agg(pl.col("peer").n_unique())
```

### HDBSCAN Normal Behavior Baseline

HDBSCAN clusters vessels by their behavioral feature vector (gap frequency, speed variance, route entropy, loitering ratio), stratified by `ship_type`. Clusters with high internal consistency represent well-understood normal MPOL patterns (e.g. regular container feeders on fixed schedules). Vessels assigned to noise (`cluster = -1`) receive a baseline anomaly weight of 1.0.

### Isolation Forest Scoring

Trained on the subset of vessels with `sanctions_distance ≥ 3` (proxy for "clean"). The decision function is calibrated to `[0,1]` using a sigmoid fit against the OFAC-listed vessel validation set.

### C3 · Causal Sanction-Response Model (DiD)

Implemented in `src/score/causal_sanction.py`. Quantifies the *causal* effect of sanction announcement events on AIS gap frequency for vessels connected within 2 hops in the Lance Graph ownership graph.

**Model specification** (for each regime × announcement date):

```
outcome_{it} = β₀ + β₁·treated_i + β₂·post_t + β₃·(treated_i × post_t)
             + γ_v (vessel-type fixed effects)
             + δ_r (route-corridor fixed effects)
             + ε_{it}
```

| Term | Meaning |
|---|---|
| `treated_i` | 1 if vessel has `sanctions_distance ≤ 2` |
| `post_t` | 1 if observation is in the 30-day window *after* the announcement date |
| **β₃ (ATT)** | **Average Treatment Effect on Treated: extra AIS gaps per 30 days attributable to the announcement** |
| `vessel-type FEs` | One dummy per AIS `ship_type` bucket (tanker, cargo, passenger, other) |
| `route-corridor FEs` | One dummy per geographic corridor (Malacca, Persian Gulf, Red Sea, North Sea, …) |

OLS is estimated with **HC3 heteroskedasticity-robust standard errors** (implemented in pure numpy—no statsmodels dependency). Multiple announcement dates per regime are pooled via **inverse-variance weighting**.

**Output:** Per-regime ATT estimate + 95% CI. `calibrate_graph_weight(effects)` converts the fraction of positive-significant estimates into a `w_graph` value ∈ [0.20, 0.65] suitable for `--w-graph` in `src/score/composite.py`.

**Supported regimes:**

| Regime key | Label | Announcement dates |
|---|---|---|
| `OFAC_Iran` | OFAC Iran | 2012-03-15, 2019-05-08, 2020-01-10 |
| `OFAC_Russia` | OFAC Russia | 2022-02-24, 2022-09-15, 2023-02-24 |
| `UN_DPRK` | UN DPRK | 2017-08-05, 2017-09-11, 2017-12-22 |

---

## Verifiable AI & Anti-Hallucination Grounding Pipeline

Defense and intelligence stakeholders require that AI-generated assessments be **auditable, reproducible, and traceable to primary evidence** — not black-box text. Arktrace addresses this through a strict two-phase architecture: all risk decisions are made by deterministic algorithms first; the LLM is only permitted to synthesise text from a pre-computed, structured context window.

### Two-phase architecture

```
Phase 1 — Deterministic scoring (no LLM)
─────────────────────────────────────────────────────────────────────
 AIS + registry + trade data
       │
       ├─► HDBSCAN MPOL baseline      → cluster_id, baseline_noise_score
       ├─► Isolation Forest           → anomaly_score  (sigmoid-calibrated)
       ├─► SHAP TreeExplainer         → top_signals[]  (feature, value, contribution)
       ├─► Lance Graph BFS            → sanctions_distance  (0 = direct, 99 = none)
       └─► C3 Causal DiD (β₃ ATT)    → causal_weight, att_estimate, p_value, 95% CI
                │
                ▼
       Composite score (weighted blend)  →  candidate_watchlist.parquet
                │
                ▼  structured context injected into prompt
Phase 2 — LLM text synthesis (bounded)
─────────────────────────────────────────────────────────────────────
 System prompt: vessel profile + SHAP signals + GDELT events + ATT evidence
       │
       ▼
 LLM role: one-paragraph brief, citing specific field values and event dates
       │
       ▼
 Analyst brief / chat response  →  cached in DuckDB (deterministic replay)
```

### Grounding mechanisms

**SHAP attribution prevents unsupported claims.**
Every feature contribution is computed by `shap.TreeExplainer` against the calibrated Isolation Forest. The `top_signals` field written to the watchlist Parquet carries the raw feature value alongside its SHAP contribution score. The LLM system prompt receives these as structured triples `(feature, value, contribution)` — not prose — so every claim in the generated brief can be traced back to a specific observable AIS or registry measurement.

```json
[
  {"feature": "ais_gap_count_30d", "value": 14, "contribution": 0.34},
  {"feature": "sanctions_distance", "value": 1,  "contribution": 0.29},
  {"feature": "flag_changes_2y",   "value": 3,   "contribution": 0.18}
]
```

**C3 Causal ATT provides statistically verifiable claims.**
The DiD model produces a point estimate (β₃ ATT) with 95% confidence intervals and a two-tailed p-value for each sanctions regime. These are injected verbatim into the brief system prompt as `causal_evidence`. The LLM can cite that "AIS gaps increased by X gaps/30-day window (ATT=X.X, 95% CI [X.X, X.X], p<0.05) following the OFAC Iran announcement of 2019-05-08" — a claim that is reproducible from the raw AIS data by any analyst running `src/score/causal_sanction.py`.

**Lance Graph distance makes ownership chains auditable.**
`sanctions_distance` is a BFS hop count through the Lance Graph ownership tables (`OWNED_BY`, `MANAGED_BY`, `CONTROLLED_BY`). A value of 1 means the vessel's direct registered owner appears in the OFAC SDN or EU/UN sanctions list. The path is materialized as rows in the Lance columnar store and can be queried directly — it is not an inference.

**The LLM system prompt enforces citation and prohibits fabrication.**
Both the brief endpoint (`src/api/routes/briefs.py`) and the analyst chat endpoint (`src/api/routes/chat.py`) inject the same structured context and instruct the LLM explicitly:

> *"Cite specific field values, GDELT event IDs/dates, or ownership chain hops to ground every claim."*

The LLM has no access to external tools, no internet, and no retrieval beyond what is injected in the context window. It cannot add vessels to the watchlist, change scores, or assert risk signals not already in the structured inputs.

**GDELT RAG anchors geopolitical context to dated, sourced news events.**
Geopolitical context is retrieved from a Lance columnar GDELT index by flag country and vessel name (`src/ingest/gdelt.py`). Each event record carries `event_date`, `actor1_name`, `actor2_name`, `action_geo`, and `source_url`. The LLM is required to name the date and URL when citing geopolitical context — preventing the substitution of plausible-sounding but unfounded geopolitical narrative.

**DuckDB cache enforces deterministic replay.**
Completed briefs are written to the `analyst_briefs` table keyed on `(mmsi, watchlist_version)`. Identical inputs always return the same cached output. This means a brief generated before a regulatory review can be reproduced verbatim from the same pipeline run, supporting chain-of-custody requirements.

### What the LLM cannot do

| Prohibited action | Enforcement mechanism |
|---|---|
| Assert a risk signal not in `top_signals` | Structured prompt — only listed signals are present in context |
| Claim a causal relationship without statistical support | ATT + CI + p-value required in prompt; LLM instructed to cite them |
| Modify a vessel's confidence score | Score is computed deterministically before LLM is called; LLM receives it read-only |
| Retrieve information from the internet | No tool access; local-first LLM (llamacpp / Ollama / LM Studio) or API with no browsing |
| Produce a different answer for the same inputs | DuckDB cache enforces identical output for identical `(mmsi, watchlist_version)` |

### Summary

Arktrace uses the LLM for one task only: converting a deterministic, structured risk assessment into readable English for the analyst. Every claim in a generated brief has a traceable origin — a SHAP contribution, a graph hop count, a DiD ATT estimate, or a dated GDELT source URL. The scoring pipeline can be re-run independently of the LLM and will produce identical numeric outputs, satisfying audit and chain-of-custody requirements for defence and regulatory applications.

---

## Output Schema

`data/processed/candidate_watchlist.parquet`

| Column | Type | Description |
|---|---|---|
| `mmsi` | `str` | MMSI number |
| `imo` | `str` | IMO number (if known) |
| `vessel_name` | `str` | Current name |
| `vessel_type` | `str` | Ship type |
| `flag` | `str` | Current flag state |
| `confidence` | `f32` | Composite score 0.0–1.0 |
| `anomaly_score` | `f32` | Isolation Forest score |
| `graph_risk_score` | `f32` | Normalised sanctions graph distance |
| `identity_score` | `f32` | Identity volatility score |
| `top_signals` | `str` (JSON) | Top 3 SHAP-attributed features |
| `last_lat` | `f64` | Last known latitude |
| `last_lon` | `f64` | Last known longitude |
| `last_seen` | `datetime` | Last AIS timestamp |
| `ais_gap_count_30d` | `i32` | AIS gaps > 6h in last 30 days |
| `ais_gap_max_hours` | `f32` | Longest gap in hours |
| `position_jump_count` | `i32` | Spoofing indicators |
| `sts_candidate_count` | `i32` | Co-location events |
| `flag_changes_2y` | `i32` | Flag changes in 2 years |
| `name_changes_2y` | `i32` | Name changes in 2 years |
| `owner_changes_2y` | `i32` | Ownership changes |
| `sanctions_distance` | `i32` | BFS hops to nearest sanctioned entity |
| `shared_address_centrality` | `i32` | Vessels sharing the same registered address in ownership chain |
| `sts_hub_degree` | `i32` | Distinct vessels contacted in STS co-location events |

`data/processed/causal_effects.parquet` (written by `src/score/causal_sanction.py`)

| Column | Type | Description |
|---|---|---|
| `regime` | `str` | Regime key (`OFAC_Iran`, `OFAC_Russia`, `UN_DPRK`) |
| `label` | `str` | Human-readable regime label |
| `n_treated` | `i32` | Treated vessel count |
| `n_control` | `i32` | Control vessel count |
| `att_estimate` | `f64` | Pooled ATT (extra AIS gaps / 30 days) |
| `att_ci_lower` | `f64` | 95% CI lower bound |
| `att_ci_upper` | `f64` | 95% CI upper bound |
| `p_value` | `f64` | Two-tailed p-value |
| `is_significant` | `bool` | True if p < 0.05 |
| `calibrated_weight` | `f64` | Suggested `w_graph` for `composite.py` |

### Example `top_signals` field

```json
[
  {"feature": "ais_gap_count_30d", "value": 14, "contribution": 0.34},
  {"feature": "sanctions_distance", "value": 1,  "contribution": 0.29},
  {"feature": "flag_changes_2y",   "value": 3,   "contribution": 0.18}
]
```

### Explainability Worked Example

**Vessel: PACIFIC GHOST** (MMSI 477123456) — Confidence: 0.87

SHAP `TreeExplainer` decomposes the composite score into per-feature contributions:

```
Feature                  Value   SHAP contribution   Meaning
──────────────────────── ─────── ─────────────────── ────────────────────────────────────────────────────
ais_gap_count_30d          14    +0.34               14 dark periods in 30 days (avg fleet: 1.2)
sanctions_distance          1    +0.31               direct owner on OFAC SDN list (1 BFS hop)
sts_hub_degree              6    +0.18               contacted 6 distinct vessels during co-location events
flag_changes_2y             3    +0.12               changed flag state 3 times in 2 years
position_jump_count         2    +0.09               2 AIS positions requiring implied speed > 50 kts
causal_weight           0.71    +0.06               statistically significant DiD response to
                                                     OFAC Iran 2024-10 announcement (ATT=3.1, p<0.01)
name_changes_2y             0    −0.03               no name changes — mild negative contribution
trade_flow_mismatch      0.12    +0.02               minor mismatch between declared and estimated volume
──────────────────────── ─────── ─────────────────── ────────────────────────────────────────────────────
Composite score                   0.87
```

**How to read this:** the SHAP contribution column shows exactly how much each feature *pushed the score up or down* from the baseline. AIS dark periods (+0.34) and direct ownership proximity to a sanctioned entity (+0.31) are the dominant signals. The causal DiD weight (+0.06) confirms the evasion behaviour intensified specifically after the October 2024 OFAC announcement — distinguishing deliberate evasion from routine commercial rerouting.

The analyst sees this breakdown directly in the dashboard alongside the confidence badge. No black-box verdict: every flagging decision is traceable to specific, dated, observable events.

**Dashboard rendering:** the FastAPI + HTMX dashboard renders `top_signals` as a horizontal bar chart (one bar per feature, scaled by SHAP contribution) alongside the vessel map pin and confidence badge. Each bar links to the raw data source (AIS position table, Equasis ownership record, or GDELT event) for one-click audit.

---

## Validation Against Ground Truth

Known OFAC-listed vessels (those already on the SDN list at time of analysis) are used as a positive label set for validation:

- **Precision@50**: fraction of top-50 candidates that are OFAC-listed
- **Recall@200**: fraction of all OFAC-listed vessels captured in top-200
- **AUROC**: area under ROC curve across all scored vessels

This validation is run in `src/score/validate.py` and reported in the FastAPI + HTMX dashboard.

---

## Sensor Fusion and Electro-Optics

### Phase A — open-source data fusion (screening layer)

The screening layer fuses four independent open-source data streams rather than relying on a single signal:

| Signal | Source | What it detects |
|---|---|---|
| AIS behaviour | aisstream.io / Marine Cadastre | Dark periods, spoofing, STS events, loitering |
| Ownership graph | Equasis + OpenSanctions | Proximity to sanctioned entities, shell-company layers |
| Trade flow | UN Comtrade+ | Route/cargo mismatch, declared vs. estimated volume |
| Geopolitical events | GDELT | Sanction announcements, flag-state risk changes |
| **EO vessel detections** | **Global Fishing Watch Vessel Presence API** | **"Dark" vessels visible from space with no AIS transmitting — `eo_dark_count_30d` and `eo_ais_mismatch_ratio` features; identifies vessels going dark deliberately vs. AIS receiver gaps** |

Open-source EO detections from the Global Fishing Watch Vessel Presence API are fused at the screening layer via two features: `eo_dark_count_30d` (satellite-detected vessel presences with no matching AIS in the 30-day window) and `eo_ais_mismatch_ratio` (fraction of EO detections with no AIS counterpart). This directly addresses the solicitation's requirement for *"fusion of various sources (open-source, Electro Optics)"* — identifying dark vessels visible from space that have deliberately switched off their AIS transponder. The GFW integration is fully implemented and pipeline-wired; live activation requires a GFW research-tier token for GAP/GAP_START event access (application submitted), or Cap Vista's MPOL EO feed via the Proprietary Fusion Gateway. Open-source AIS and ownership data alone already deliver a **6× lift** (Precision@50 = 0.62 vs. base rate ≈ 0.10); EO fusion adds an incremental lift by confirming dark periods with space-based detection.

### Phase B — EO sensors at close range (investigation layer)

Once Phase A identifies a high-confidence candidate, Phase B deploys tiered electro-optical sensors from a patrol vessel:

| Tier | EO sensor | Capability |
|---|---|---|
| Tier 1 | Hi-res camera (Sony RX100 / GoPro) | Vessel identity: IMO number, name, flag OCR |
| Tier 2 | LiDAR (Livox Mid-360 / Ouster OS0-32) | Hull shape deviation, waterline / draught, 3D point cloud |
| Tier 3 | FLIR Boson+ thermal + hyperspectral | Engine heat signature, night operation, cargo type proxy |

Phase B sensor output feeds the `edgesentry-app` evidence bundle — GPS-tagged, Ed25519-signed, BLAKE3 hash-chained — and is transmitted to the Port Operations Centre via VDES. See [docs/field-investigation.md](field-investigation.md) for full hardware specifications and cost breakdown.

### Roadmap — commercial satellite SAR integration

GFW open-source EO detections are already fused at the screening layer (see Phase A above). The next EO tier — wide-area persistent **commercial satellite SAR** (e.g. ICEYE) — is tracked in issue [#84](https://github.com/edgesentry/arktrace/issues/84). Commercial SAR adds continuous wide-area coverage independent of AIS receiver range, confirming dark-ship behaviour rather than inferring it. The intended integration point is the same Phase A feature engineering pipeline: SAR-derived vessel detections map directly to the existing `sar_detections` table and feed `eo_dark_count_30d` via the Proprietary Fusion Gateway with no code changes.

Commercial SAR is not required to meet the Precision@50 ≥ 0.60 PoC target. It becomes most valuable at global scale where AIS receiver coverage is sparse (open ocean, polar regions) and GFW coverage is thinner.

---

## Computational Requirements

The full pipeline (historical AIS + scoring) runs on a standard laptop:

| Step | Runtime (est.) | Memory |
|---|---|---|
| AIS Parquet load (12 months) | ~5 min | ~4 GB |
| Feature engineering (Polars) | ~10 min | ~2 GB |
| Lance Graph build | ~15 min | ~1 GB |
| HDBSCAN + Isolation Forest | ~5 min | ~1 GB |
| C3 causal DiD model | ~1 min | ~0.5 GB |
| SHAP attribution | ~10 min | ~2 GB |
| **Total** | **~46 min** | **~4 GB peak** |

For live streaming (aisstream.io), the incremental update pipeline runs in under 60 seconds per batch.

**Edge gateway benchmark (measured):** Re-scoring 5,000 vessels — feature matrix (`build_matrix.py`) + composite scoring (HDBSCAN + Isolation Forest + SHAP) + watchlist output — completes in **5.75 seconds** on a 14-core Apple M-series laptop. On a constrained 4-core / 4 GB edge gateway (Raspberry Pi 4 / NVIDIA Jetson Nano class), the same pipeline is well within the 30-second target given the pipeline is CPU-bound on the HDBSCAN and Isolation Forest steps which scale sub-linearly with vessel count. See `scripts/benchmark_rescore.py` and `docs/deployment.md` for the full benchmark command and reproduction instructions.

**Temporal granularity — 15-minute re-score cadence:** Because a full re-score completes in under 30 seconds, the pipeline is scheduled to run every 15 minutes against the live AIS stream. This cadence directly enables detection of *sudden evasion*: a vessel that switches off its AIS transponder immediately after a sanctions announcement will appear in the re-ranked watchlist within 15 minutes — before it clears the patrol area. Competing approaches that aggregate AIS over 24-hour or 7-day windows cannot surface this intra-day signal. The 15-minute interval was chosen to match the typical AIS reporting frequency for vessels in constrained waterways (Malacca/Singapore Strait transit ~2–4 hours), giving patrol commanders multiple re-score cycles before the vessel exits the operational zone.

---

## Open-Source Model and IP Protection

arktrace is published under Apache 2.0. The base algorithm — causal model architecture, feature engineering pipeline, scoring engine, and dashboard — is fully public. This is a deliberate design choice: open review strengthens trust with a government counterparty and allows Cap Vista to independently audit the methodology.

**Public layer (Apache 2.0, always open):**
- Causal model architecture (DiD ATT framework, `src/score/causal_sanction.py`)
- Feature engineering pipeline (`src/features/`)
- Composite scoring engine (`src/score/composite.py`)
- SHAP explainability layer
- Dashboard and analyst workflow

**Protected layer (Cap Vista's operational parameters, never published):**
- Calibrated scoring thresholds derived from Cap Vista's operational environment
- Patrol-derived feedback labels (cleared / confirmed vessels)
- Cap Vista-specific `regimes.yaml` weight tuning
- Any proprietary feed column mappings in `_inputs/custom_feeds/`

Cap Vista's calibrated model is operationally distinct from the public baseline even though it runs on the same open-source codebase. A competitor who forks the repository gets the algorithm — not Cap Vista's 7-week calibration or patrol-derived ground truth. The protected parameters are stored outside the repository and are never committed to version control.
