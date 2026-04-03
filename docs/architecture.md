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
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER  (src/ingest/)                                 │
│                                                                 │
│  AIS positions ──────────────────► DuckDB (ais_positions table) │
│  Sanctions entities ─────────────► DuckDB (sanctions_entities)  │
│  Vessel ownership chains ────────► Lance Graph (on-disk files)  │
│  Trade flow by route ────────────► DuckDB (trade_flow table)    │
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
│  OUTPUT                                                         │
│                                                                 │
│  data/processed/candidate_watchlist.parquet                     │
│  FastAPI + HTMX dashboard  (src/api/)  → http://localhost:8000  │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼  handoff
┌─────────────────────────────────────────────────────────────────┐
│  PHYSICAL INVESTIGATION  (edgesentry-app / edgesentry-rs)       │
│  (out of scope for this repo — see roadmap.md)                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Storage Design

### DuckDB (`data/processed/mpol.duckdb`)

DuckDB is the primary analytical store. It runs in-process with no server and queries Parquet files directly. Multi-region deployments use separate DuckDB files per region (e.g. `data/processed/europe.duckdb`) — every script accepts a `--db` flag to target the correct file. See [regional-playbooks.md](regional-playbooks.md) for per-region paths and bbox values.

| Table | Key columns | Source |
|---|---|---|
| `ais_positions` | `mmsi, timestamp, lat, lon, sog, cog, nav_status, ship_type` | aisstream.io (all regions); Marine Cadastre Parquet (US waters only) |
| `sanctions_entities` | `entity_id, name, mmsi, imo, flag, type, list_source` | OFAC, EU, UN, OpenSanctions |
| `trade_flow` | `reporter, partner, hs_code, period, trade_value_usd, route_key` | UN Comtrade |
| `vessel_meta` | `mmsi, imo, name, flag, ship_type, gross_tonnage` | Equasis + ITU MMSI |
| `vessel_features` | one row per MMSI, all engineered features | Computed by `src/features/` |

### Lance Graph (`data/processed/mpol_graph/`)

Lance Graph stores the vessel ownership graph as columnar Lance datasets on disk — no external server or Docker container required. The graph directory sits alongside the DuckDB file and is written by `src/ingest/vessel_registry.py`, read by `src/features/ownership_graph.py` and `src/features/identity.py`.

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

## Feature Design

### AIS Behavioral Features

Computed with Polars over a rolling 30-day window per MMSI.

| Feature | Definition | Shadow fleet signal |
|---|---|---|
| `ais_gap_count_30d` | Gaps > 6h in AIS signal while in open sea | STS transfer or deliberate dark period |
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

### Composite Score

```
confidence = w_anomaly × anomaly_score
           + w_graph   × graph_risk_score
           + w_identity × identity_volatility_score
```

Default weights: `w_anomaly = 0.4`, `w_graph = 0.4`, `w_identity = 0.2`. All three are configurable via `--w-anomaly`, `--w-graph`, `--w-identity` CLI flags on `src/score/composite.py`. The C3 causal model provides a data-driven `w_graph` calibration (see section above and [roadmap.md](roadmap.md) Phase C, C3).

Per-region weight tuning recommendations are in [regional-playbooks.md](regional-playbooks.md).

### Explainability (SHAP)

SHAP TreeExplainer computes per-feature contributions to the anomaly score for each vessel. The top 3 contributing features are serialised as `top_signals` JSON in the watchlist output, enabling a duty officer to understand *why* a vessel was flagged without reading raw feature values.
