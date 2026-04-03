# Analytics Overview

End-to-end data flow for the arktrace Maritime Pattern of Life (MPOL) shadow fleet screening pipeline.

## Pipeline stages

```
┌─────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                           │
│  aisstream.io WebSocket · Marine Cadastre Parquet · OpenSanctions       │
│  Equasis vessel registry · UN Comtrade API · GDELT event CSV            │
└────────────────────────────┬────────────────────────────────────────────┘
                             │  src/ingest/
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STORAGE                                                                │
│  DuckDB      ──  ais_positions, vessel_meta, sanctions_entities,        │
│                  trade_flow, vessel_features, analyst_briefs,           │
│                  chat_cache, cleared_vessels                            │
│  Lance Graph ──  Vessel · Company · Country · VesselName datasets       │
│                  (ownership relationships as columnar Lance files)       │
│  LanceDB     ──  GDELT event vectors (RAG context)                     │
└────────────────────────────┬────────────────────────────────────────────┘
                             │  src/features/
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  FEATURE ENGINEERING  (19 features, one row per MMSI)                  │
│  AIS Behavioral   · Identity Volatility                                 │
│  Ownership Graph  · Trade Flow Mismatch                                 │
└────────────────────────────┬────────────────────────────────────────────┘
                             │  src/score/
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  SCORING ENGINE                                                         │
│  HDBSCAN baseline  →  Isolation Forest  →  Composite confidence score  │
│  causal_sanction.py calibrates graph_risk weight (C3)                  │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  OUTPUTS                                                                │
│  candidate_watchlist.parquet  ·  composite_scores.parquet               │
│  anomaly_scores.parquet       ·  causal_effects.parquet                 │
└────────────────────────────┬────────────────────────────────────────────┘
                             │  src/api/
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  DASHBOARD  (FastAPI + HTMX + MapLibre GL)                              │
│  http://localhost:8000                                                   │
│  /api/vessels/geojson  ·  /api/watchlist/top  ·  /api/alerts/sse       │
│  /api/briefs/{mmsi}    ·  /api/chat                                     │
└─────────────────────────────────────────────────────────────────────────┘
```

## Data sources

| Source | Module | Protocol | Content |
|---|---|---|---|
| **aisstream.io** | `src/ingest/ais_stream.py` | WebSocket | Live AIS positions for a configurable bounding box |
| **Marine Cadastre** | `src/ingest/marine_cadastre.py` | Parquet files | Annual US-coastal AIS archive (2022–2025); useful for Gulf of Mexico historical backfill |
| **OpenSanctions** | `src/ingest/sanctions.py` | JSONL | Merged OFAC SDN, EU, UN consolidated lists |
| **Equasis** | `src/ingest/vessel_registry.py` | HTTP scrape / CSV | Vessel ownership chains → Lance Graph |
| **UN Comtrade** | `src/features/trade_mismatch.py` | REST API | Bilateral crude oil trade statistics (HS 2709) |
| **GDELT** | `src/ingest/gdelt.py` | CSV | Geopolitical event stream; indexed in LanceDB for RAG |

## Storage layer

### DuckDB tables

| Table | Key columns | Purpose |
|---|---|---|
| `ais_positions` | mmsi, timestamp, lat, lon, sog | Raw AIS tracks; primary key (mmsi, timestamp) |
| `vessel_meta` | mmsi, imo, name, flag, ship_type | Static registry data |
| `sanctions_entities` | entity_id, name, mmsi, imo, list_source | Merged sanctions lists |
| `trade_flow` | reporter, partner, hs_code, period, trade_value_usd | Comtrade bilateral statistics |
| `vessel_features` | mmsi + 19 feature columns | Computed feature matrix (one row per vessel) |
| `analyst_briefs` | mmsi, watchlist_version, brief | Cached LLM-generated analyst briefings |
| `chat_cache` | cache_key, question_hash, response | Cached analyst Q&A responses |
| `cleared_vessels` | mmsi, cleared_at, cleared_by | Phase B inspection outcomes (cleared) |

Each region runs its own DuckDB file (e.g. `singapore.duckdb`, `japansea.duckdb`) so data is fully isolated between deployment areas.

### Lance Graph (`<region>_graph/`)

Stores the vessel ownership graph as Lance columnar datasets on disk — no external server required. Written by `src/ingest/vessel_registry.py`, read by `src/features/ownership_graph.py` and `src/features/identity.py`.

Node datasets: `Vessel`, `Company`, `Country`, `VesselName`, `Address`, `SanctionsRegime`

Relationship datasets (src_id → dst_id):

| Dataset | Meaning |
|---|---|
| `OWNED_BY` | Vessel → Company (registered owner) with `{since, until}` |
| `MANAGED_BY` | Vessel → Company (technical manager) with `{since, until}` |
| `REGISTERED_IN` | Company → Country (registration flag) |
| `ALIAS` | Vessel → VesselName (historical name) with `{date}` |
| `SANCTIONED_BY` | Vessel or Company → SanctionsRegime with `{list, date}` |
| `REGISTERED_AT` | Company → Address (shared-address clustering) |
| `CONTROLLED_BY` | Company → Company (beneficial ownership layers) |
| `STS_CONTACT` | Vessel → Vessel (co-location events) |

BFS traversal depth up to 3 hops is used to compute `sanctions_distance` and related graph features (implemented as Polars joins over the Lance datasets).

### LanceDB (GDELT)

Daily GDELT event rows are embedded and stored in a local LanceDB vector store. The analyst chat and brief generation endpoints retrieve the top-k most relevant events for a given vessel's flag state, ownership country, and watchlist confidence to construct geopolitical context.

## Orchestration

`scripts/run_pipeline.py` runs the full 9-step pipeline in order:

1. Schema initialisation
2. Marine Cadastre historical backfill (optional)
3. Live AIS streaming
4. Sanctions loading
5. Ownership graph computation (vessel_registry → Lance Graph datasets)
6. Feature engineering (AIS → identity → trade mismatch → build matrix)
7. Scoring (C3 causal calibration → MPOL baseline → anomaly → composite → watchlist)
8. GDELT ingestion
9. Dashboard launch

See [Pipeline Operations](pipeline-operations.md) for per-region configuration and [Regional Playbooks](regional-playbooks.md) for analyst-persona-specific guidance.
