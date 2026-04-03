# Technical Solution

## Tech Stack

| Layer | Tool | Version | Rationale |
|---|---|---|---|
| Analytical store | **DuckDB** | ≥ 1.1 | In-process columnar OLAP; queries Parquet natively; no server; edge-deployable |
| DataFrame / feature engineering | **Polars** | ≥ 1.0 | Lazy evaluation; fast AIS window operations; Arrow-native |
| Graph DB | **Lance Graph** | ≥ 0.5 | Cypher-capable graph engine built in Rust with Python bindings; embedded in-process, serverless, stores data as Lance columnar files |
| ML / clustering | **scikit-learn** | ≥ 1.5 | HDBSCAN, Isolation Forest; no GPU required |
| Explainability | **SHAP** | ≥ 0.46 | TreeExplainer for Isolation Forest; per-vessel feature attribution |
| Dashboard | **FastAPI + HTMX** | ≥ 0.115 / — | Production-grade API layer + partial-page updates; SSE alerts; MapLibre GL JS |
| AIS streaming | **websockets** + **httpx** | — | aisstream.io WebSocket; Marine Cadastre HTTP download |
| Causal inference | **numpy / scipy** (built-in) | — | DiD OLS with HC3 robust SEs; no external causal library required |
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

aisstream.io supports all regions via the `--bbox lat_min lon_min lat_max lon_max` flag. The default bbox is the Singapore / Malacca Strait (`−5 92 22 122`). For other regions, pass `--bbox` with the appropriate coordinates and `--db` to write to a region-specific DuckDB file. Marine Cadastre is used only for US coastal regions (Gulf of Mexico, US West Coast). For non-US historical backfill (Japan Sea, Europe, Middle East), use AISHub or MarineTraffic CSV exports loaded via `load_csv_to_duckdb()` with a custom bbox. See [regional-playbooks.md](regional-playbooks.md) for per-region configuration.

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

GEBCO is used to build a **200m-depth boundary mask** as an H3 hexagon set. STS candidate detection filters to events within this mask (shallow draught tankers cannot operate in deeper open ocean), reducing false positives from legitimate vessel interactions.

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

---

## Validation Against Ground Truth

Known OFAC-listed vessels (those already on the SDN list at time of analysis) are used as a positive label set for validation:

- **Precision@50**: fraction of top-50 candidates that are OFAC-listed
- **Recall@200**: fraction of all OFAC-listed vessels captured in top-200
- **AUROC**: area under ROC curve across all scored vessels

This validation is run in `src/score/validate.py` and reported in the FastAPI + HTMX dashboard.

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
