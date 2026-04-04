# Scoring Model

The arktrace scoring engine is a three-stage ML pipeline that converts the 19-feature vessel matrix into a single `confidence` score in [0, 1]. Higher scores indicate stronger shadow fleet candidacy.

## Pipeline overview

```
vessel_features (19 cols)
        │
        ├──► HDBSCAN Baseline  (mpol_baseline.py)
        │         │  baseline_noise_score ∈ {0.0, 1.0}
        │         ▼
        └──► Isolation Forest  (anomaly.py)
                  │  anomaly_score ∈ [0, 1]
                  ▼
             Composite Score   (composite.py)
                  │  confidence ∈ [0, 1]
                  ▼
             Watchlist Output  (watchlist.py)
                  candidate_watchlist.parquet
```

---

## Stage 1 — HDBSCAN baseline (`src/score/mpol_baseline.py`)

### Purpose

HDBSCAN clusters vessels by behavioural features to define what "normal MPOL behaviour" looks like in the area of interest. Vessels assigned to cluster -1 (HDBSCAN noise) receive `baseline_noise_score = 1.0`; all others receive `0.0`.

### Features used

Six AIS behavioral features: `ais_gap_count_30d`, `ais_gap_max_hours`, `position_jump_count`, `sts_candidate_count`, `port_call_ratio`, `loitering_hours_30d`.

### Clustering design

- Data is partitioned by `ship_type` before clustering. Tankers (type 80–89) form separate clusters from cargo vessels (70–79), so the baseline captures type-specific normality.
- Service vessel types (AIS 51–59, 31–32 — bunker barges, pilot tenders, SAR craft) are **excluded from HDBSCAN training** by default (`exclude_service_vessels=True`). These craft have high loitering and low SOG that would otherwise compress anomaly scores for shadow fleet STS events near busy anchorages (e.g. Singapore Strait). They are still scored by the Isolation Forest.
- `min_cluster_size` scales automatically with group size: `max(2, min(10, n // 2))`.

### Cleared vessel hard negatives

Vessels physically inspected and cleared by a Phase B patrol (recorded in the `cleared_vessels` table) always receive `baseline_noise_score = 0.0`, preventing confirmed-normal vessels from inflating false-positive rates in future scoring cycles.

### Hyperparameters

| Parameter | Value |
|---|---|
| Algorithm | `sklearn.cluster.HDBSCAN` |
| `min_samples` | 1 |
| `allow_single_cluster` | True |
| Preprocessing | `StandardScaler` per group |

---

## Stage 2 — Isolation Forest anomaly scoring (`src/score/anomaly.py`)

### Purpose

The Isolation Forest assigns each vessel a continuous anomaly score in [0, 1]. It is trained on the "clean" vessel population (vessels with no proximity to sanctioned entities) and then scored on all vessels, including the suspicious ones.

### Features used

All 19 features from `vessel_features`.

### Training set selection

The model trains only on vessels with `sanctions_distance ≥ 3` — far enough from any sanctioned entity to serve as a reasonable clean baseline. If this subset is too small (< 4 vessels), the full feature frame is used as a fallback. **Cleared vessels** from the `cleared_vessels` table are always included in the clean training subset as hard negatives, regardless of their `sanctions_distance`.

### Anomaly score formula

```python
raw = -model.decision_function(scaled)   # higher = more anomalous
anomaly_score = 0.75 * norm(raw) + 0.25 * baseline_noise_score
```

The isolation forest raw score is min-max normalised to [0, 1], then blended 75/25 with the HDBSCAN baseline noise score. The blend ensures that a vessel whose behaviour is globally anomalous (Isolation Forest) AND locally anomalous relative to its ship-type peer group (HDBSCAN noise) receives the highest possible anomaly score.

### Hyperparameters

| Parameter | Value |
|---|---|
| Algorithm | `sklearn.ensemble.IsolationForest` |
| `n_estimators` | 200 |
| `contamination` | "auto" |
| `random_state` | 42 |
| Preprocessing | `StandardScaler` fit on clean subset |

---

## Stage 3 — Composite confidence score (`src/score/composite.py`)

### Purpose

The composite score combines three independent signal families into a single `confidence` value used to rank vessels on the watchlist.

### Formula

```
confidence = w_anomaly × anomaly_score
           + w_graph   × graph_risk_score
           + w_identity × identity_score
```

Default weights: `w_anomaly = 0.40`, `w_graph = 0.40`, `w_identity = 0.20`.

Weights are region-configurable (see `--w-anomaly`, `--w-graph`, `--w-identity` flags) and automatically calibrated by the C3 causal model (see below).

### Component scores

#### `graph_risk_score`

Weighted combination of three ownership graph signals:

```python
graph_risk = 0.6 × sanctions_component
           + 0.3 × cluster_component
           + 0.1 × manager_component
```

- `sanctions_component = clip(1 − sanctions_distance / 5, 0, 1)` — maps distance 0 → 1.0, distance 5+ → 0.0
- `cluster_component = cluster_sanctions_ratio` — direct [0, 1] value
- `manager_component = clip(1 − shared_manager_risk / 5, 0, 1)` — same mapping as sanctions_component

#### `identity_score`

Weighted combination of identity volatility signals:

```python
identity = 0.30 × clip(flag_changes_2y / 5, 0, 1)
         + 0.25 × clip(name_changes_2y / 5, 0, 1)
         + 0.20 × clip(owner_changes_2y / 5, 0, 1)
         + 0.15 × clip(high_risk_flag_ratio, 0, 1)
         + 0.10 × clip(ownership_depth / 6, 0, 1)
```

#### `anomaly_score`

Output of Stage 2.

### Geopolitical rerouting filter

Before computing `confidence`, an optional geopolitical filter can down-weight `anomaly_score` for vessels in declared rerouting corridors (e.g. Cape of Good Hope diversion since 2023). This reduces false positives from legitimate commercial rerouting. Pass `--geopolitical-event-filter config/geopolitical_events.json`.

See `config/geopolitical_events.json` for the sample file format.

---

## C3 causal weight calibration (`src/score/causal_sanction.py`)

The default `w_graph = 0.40` is calibrated automatically by the C3 Difference-in-Differences model before each scoring cycle. 

The pipeline auto-calibrates `w_graph` on every run via `_calibrate_graph_weight()`.
Calling `src.score.composite` standalone still requires `--w-graph` (or the new `--auto-calibrate` flag).
The calibrated value is printed at the end of Step 8 for reference.

The model estimates the Average Treatment Effect on the Treated (ATT) for three sanction regimes (by default):

| Regime | Announcement dates used |
|---|---|
| OFAC Iran | 2012-03-15, 2019-05-08, 2020-01-10 |
| OFAC Russia | 2022-02-24, 2022-09-15, 2023-02-24 |
| UN DPRK | 2017-08-05, 2017-09-11, 2017-12-22 |

If the ATT is positive and statistically significant (p < 0.05) for a regime, the graph risk dimension is predictive → `w_graph` is increased proportionally, up to a cap of 0.65. The remaining weight is redistributed proportionally between `w_anomaly` and `w_identity`.

The calibrated weight and per-regime effect sizes are written to `<region>_causal_effects.parquet`.

### Adding a new sanction regime

Sanction regimes are configured dynamically in `config/sanction_regimes.yaml`. To add a new regime (e.g. EU 14th sanctions package against Russia) without modifying source code, add a new entry to the `regimes` dictionary with the following required fields:
- `label`: Human-readable name.
- `list_source_substr`: The string to match in the `list_source` column of the entities table.
- `flag_filter`: Fallback list of flag strings (e.g. `["RU", ""]`).
- `announcement_dates`: List of ISO-8601 string dates.

---

## SHAP explainability (`src/score/composite.py → _compute_top_signals`)

Each row on the watchlist includes a `top_signals` JSON array identifying the 3 features that most influenced the vessel's anomaly score, using SHAP TreeExplainer values from the Isolation Forest:

```json
[
  {"feature": "ais_gap_count_30d",  "value": 14,   "contribution": 0.42},
  {"feature": "sanctions_distance", "value": 1,    "contribution": 0.31},
  {"feature": "sts_hub_degree",     "value": 6,    "contribution": 0.18}
]
```

Contributions are normalised to sum to 1.0 across the top 3. This satisfies the Cap Vista explainability requirement and is rendered in the dashboard alongside the confidence badge.

---

## Validation metrics (`src/score/validate.py`)

The pipeline computes validation metrics against a holdout set of OFAC-listed vessels as positive labels:

| Metric | Target | Meaning |
|---|---|---|
| Precision@50 | ≥ 0.60 | ≥ 30 of the top-50 candidates are confirmed OFAC-listed |
| Recall@200 | — | Fraction of all OFAC-listed vessels recovered in the top 200 |
| AUROC | — | Area under the ROC curve across the full ranked list |

Metrics are written to `data/processed/validation_metrics.json` and surfaced at `/api/metrics` in the dashboard.

---

## Watchlist output (`src/score/watchlist.py`)

`candidate_watchlist.parquet` contains one row per vessel, sorted by `confidence` descending. Key columns:

| Column | Type | Description |
|---|---|---|
| mmsi | str | Maritime Mobile Service Identity |
| imo | str | IMO number (from vessel_meta) |
| vessel_name | str | Last known vessel name |
| vessel_type | str | Human-readable type label (Tanker, Cargo, …) |
| confidence | float32 | Composite score ∈ [0, 1] |
| anomaly_score | float32 | Stage 2 output |
| graph_risk_score | float32 | Ownership graph component |
| identity_score | float32 | Identity volatility component |
| top_signals | JSON str | Top-3 SHAP contributing features |
| last_lat / last_lon | float64 | Most recent AIS position |
| last_seen | timestamptz | Most recent AIS timestamp |
