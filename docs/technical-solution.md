# Technical Solution

## Tech Stack

| Layer | Tool | Version | Rationale |
|---|---|---|---|
| Analytical store | **DuckDB** | ≥ 1.1 | In-process columnar OLAP; queries Parquet natively; no server; edge-deployable |
| DataFrame / feature engineering | **Polars** | ≥ 1.0 | Lazy evaluation; fast AIS window operations; Arrow-native |
| Graph DB | **Neo4j Community** | ≥ 5.x | Cypher + GDS plugin for BFS, PageRank, community detection; Docker deployment |
| ML / clustering | **scikit-learn** | ≥ 1.5 | HDBSCAN, Isolation Forest; no GPU required |
| Explainability | **SHAP** | ≥ 0.46 | TreeExplainer for Isolation Forest; per-vessel feature attribution |
| PoC dashboard | **Streamlit** | ≥ 1.35 | Rapid map + table UI; no frontend build toolchain |
| AIS streaming | **websockets** + **httpx** | — | aisstream.io WebSocket; Marine Cadastre HTTP download |
| Language | **Python 3.12** | — | Best ecosystem fit for all above |
| Packaging | **uv** | — | Fast lockfile-based dependency management |

---

## Data Sources

### AIS Data

| Source | Coverage | Format | Cost |
|---|---|---|---|
| [aisstream.io](https://aisstream.io) | Real-time global AIS WebSocket | JSON over WS | Free (API key) |
| [Marine Cadastre](https://marinecadastre.gov/ais/) | Historical US waters AIS, 2015–present | CSV / Parquet | Free download |
| [AIS Hub](https://www.aishub.net) | Near-real-time aggregated AIS | NMEA / JSON | Free tier available |

For the area of interest (SG + Malacca Strait), aisstream.io with a bounding box filter covers the required 1,600nm radius. Historical backfill uses Marine Cadastre for vessels that transited US-monitored zones plus supplementary data from AIS Hub.

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

### Ownership Graph (Neo4j + GDS)

Vessel ownership chains are loaded into Neo4j. The Graph Data Science (GDS) plugin computes:

```cypher
// BFS shortest path from vessel to nearest sanctioned node
CALL gds.shortestPath.dijkstra.stream('ownership-graph', {
  sourceNode: vesselNodeId,
  targetNodes: sanctionedNodeIds,
  relationshipWeightProperty: null
})
YIELD nodeId, totalCost
RETURN min(totalCost) AS sanctions_distance
```

Community detection (Louvain) identifies ownership clusters; `cluster_sanctions_ratio` is computed per cluster.

### HDBSCAN Normal Behavior Baseline

HDBSCAN clusters vessels by their behavioral feature vector (gap frequency, speed variance, route entropy, loitering ratio), stratified by `ship_type`. Clusters with high internal consistency represent well-understood normal MPOL patterns (e.g. regular container feeders on fixed schedules). Vessels assigned to noise (`cluster = -1`) receive a baseline anomaly weight of 1.0.

### Isolation Forest Scoring

Trained on the subset of vessels with `sanctions_distance ≥ 3` (proxy for "clean"). The decision function is calibrated to `[0,1]` using a sigmoid fit against the OFAC-listed vessel validation set.

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

### Example `top_signals` field

```json
[
  {"feature": "ais_gap_count_30d", "value": 14, "contribution": 0.34},
  {"feature": "sanctions_distance", "value": 1,  "contribution": 0.29},
  {"feature": "flag_changes_2y",   "value": 3,   "contribution": 0.18}
]
```

---

## Validation Against Ground Truth

Known OFAC-listed vessels (those already on the SDN list at time of analysis) are used as a positive label set for validation:

- **Precision@50**: fraction of top-50 candidates that are OFAC-listed
- **Recall@200**: fraction of all OFAC-listed vessels captured in top-200
- **AUROC**: area under ROC curve across all scored vessels

This validation is run in `src/score/validate.py` and reported in the Streamlit dashboard.

---

## Computational Requirements

The full pipeline (historical AIS + scoring) runs on a standard laptop:

| Step | Runtime (est.) | Memory |
|---|---|---|
| AIS Parquet load (12 months) | ~5 min | ~4 GB |
| Feature engineering (Polars) | ~10 min | ~2 GB |
| Neo4j graph build | ~15 min | ~1 GB |
| HDBSCAN + Isolation Forest | ~5 min | ~1 GB |
| SHAP attribution | ~10 min | ~2 GB |
| **Total** | **~45 min** | **~4 GB peak** |

For live streaming (aisstream.io), the incremental update pipeline runs in under 60 seconds per batch.
