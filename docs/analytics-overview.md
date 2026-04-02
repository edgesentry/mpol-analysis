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
│  DuckDB  ──  ais_positions, vessel_meta, sanctions_entities,            │
│              trade_flow, vessel_features, analyst_briefs, chat_cache,   │
│              cleared_vessels                                            │
│  Neo4j   ──  Vessel · Company · Country · VesselName graph             │
│  LanceDB ──  GDELT event vectors (RAG context)                         │
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
| **Equasis** | `src/ingest/vessel_registry.py` | HTTP scrape / CSV | Vessel ownership chains → Neo4j |
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

### Neo4j graph

Node types: `Vessel`, `Company`, `Country`, `VesselName`

Relationship types:

| Relationship | Meaning |
|---|---|
| `OWNED_BY` | Vessel → Company (registered owner) |
| `MANAGED_BY` | Vessel → Company (technical manager) |
| `REGISTERED_IN` | Vessel or Company → Country (flag/registration) |
| `ALIAS` | Vessel → VesselName (historical name) |
| `SANCTIONED_BY` | Company → sanctions list entity |

BFS traversal depth up to 3 hops is used to compute `sanctions_distance` and related graph features.

### LanceDB (GDELT)

Daily GDELT event rows are embedded and stored in a local LanceDB vector store. The analyst chat and brief generation endpoints retrieve the top-k most relevant events for a given vessel's flag state, ownership country, and watchlist confidence to construct geopolitical context.

## Orchestration

`scripts/run_pipeline.py` runs the full 10-step pipeline in order:

1. Schema initialisation
2. Marine Cadastre historical backfill (optional)
3. Neo4j startup
4. Live AIS streaming
5. Sanctions loading
6. Ownership graph computation
7. Feature engineering (AIS → identity → trade mismatch → build matrix)
8. Scoring (C3 causal calibration → MPOL baseline → anomaly → composite → watchlist)
9. GDELT ingestion
10. Dashboard launch

See [Pipeline Operations](pipeline-operations.md) for per-region configuration and [Regional Playbooks](regional-playbooks.md) for analyst-persona-specific guidance.
