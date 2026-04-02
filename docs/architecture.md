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
│  Vessel ownership chains ────────► Neo4j  (graph DB)            │
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
│  Ownership graph features ───────► Neo4j GDS (BFS)              │
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
│  Neo4j BFS ── graph_risk_score ∈ [0,1]                          │
│  Composite ── confidence = 0.4·anomaly + 0.4·graph              │
│                           + 0.2·identity_volatility             │
│  SHAP ── top_signals JSON per vessel                            │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  OUTPUT                                                         │
│                                                                 │
│  data/processed/candidate_watchlist.parquet                     │
│  Streamlit dashboard  (src/viz/dashboard.py)                    │
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

### Neo4j (Community Edition, Docker)

Neo4j holds the ownership graph. Cypher + GDS plugin enable BFS path queries and community detection that would be expensive to express in SQL.

**Node types:**
- `Vessel {mmsi, imo, name}`
- `Company {id, name, country}`
- `Country {code, name}`
- `VesselName {name, date_from, date_to}`
- `Address {address_id, street, city, country}` — registered address (P.O. box or physical)
- `Person {person_id, name, nationality}` — directors, nominees, beneficial owners

**Relationship types:**
- `(Vessel)-[:OWNED_BY {since, until}]->(Company)`
- `(Vessel)-[:MANAGED_BY {since, until}]->(Company)`
- `(Company)-[:REGISTERED_IN]->(Country)`
- `(Company)-[:CONTROLLED_BY]->(Company)` — beneficial ownership layers
- `(Vessel)-[:ALIAS {date}]->(VesselName)`
- `(Company)-[:SANCTIONED_BY {list, date}]->(SanctionsRegime)`
- `(Company)-[:REGISTERED_AT]->(Address)` — enables shared-address clustering
- `(Company)-[:DIRECTED_BY {since, until}]->(Person)` — nominee/director network
- `(Vessel)-[:STS_CONTACT {timestamp, lat, lon, duration_h}]->(Vessel)` — co-location events recorded as graph edges

**Key GDS queries:**
```cypher
// Minimum BFS distance from vessel to any sanctioned company
MATCH (v:Vessel {mmsi: $mmsi})
CALL gds.shortestPath.dijkstra.stream(...)
YIELD totalCost AS sanctions_distance
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

Computed from Equasis historical data via Neo4j.

| Feature | Definition |
|---|---|
| `flag_changes_2y` | Number of flag state changes in rolling 2 years |
| `name_changes_2y` | Number of name changes in rolling 2 years |
| `owner_changes_2y` | Number of registered owner changes |
| `high_risk_flag_ratio` | Fraction of time under flags with weak PSC oversight |
| `ownership_depth` | Number of beneficial ownership layers to natural person |

### Ownership Graph Features

Computed by Neo4j GDS.

| Feature | Definition |
|---|---|
| `sanctions_distance` | Min BFS hops from vessel to any sanctioned entity (0 = vessel itself sanctioned) |
| `cluster_sanctions_ratio` | Fraction of vessels in same Neo4j community that are sanctioned |
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

### Composite Score

```
confidence = 0.4 × anomaly_score
           + 0.4 × graph_risk_score
           + 0.2 × identity_volatility_score
```

Default weights emphasise behavioral and graph signals equally, with identity as a tiebreaker. Per-region weight tuning is documented in [regional-playbooks.md](regional-playbooks.md) — currently requires a direct edit to `src/score/composite.py:185` (no CLI flag yet).

### Explainability (SHAP)

SHAP TreeExplainer computes per-feature contributions to the anomaly score for each vessel. The top 3 contributing features are serialised as `top_signals` JSON in the watchlist output, enabling a duty officer to understand *why* a vessel was flagged without reading raw feature values.
