# Pipeline Operations

How to run the arktrace MPOL screening pipeline for each supported region, configure parameters, and interpret outputs.

If you need an operations-level summary of pipeline types (purpose, run timing, expected results), see [Pipeline Catalog (Operations View)](pipeline-catalog.md).

## Quick start

```bash
# Interactive — prompts for region selection, retries failed steps
uv run python scripts/run_pipeline.py

# Non-interactive — Singapore region, fails fast
uv run python scripts/run_pipeline.py --region singapore --non-interactive
```

Prerequisites: a valid `AISSTREAM_API_KEY` in `.env` and Python 3.12+.

### Interactive operations shell

Run a menu-driven shell that lets you execute common operations jobs and prints a result summary after each run.

```bash
bash scripts/run_operations_shell.sh
```

Available menu jobs:
- Full Screening
- Review-Feedback Evaluation
- Historical Backtesting + Public Integration Batch
- Demo/Smoke
- SAR Feature Smoke Test (job 11)
- EO Feature Smoke Test (job 12)
- Ingest EO Detections from CSV (job 13) — load a local EO CSV into `eo_detections` without a GFW API token, then optionally run feature matrix + scoring to verify in the dashboard

### Delayed-label intelligence loop (backtracking)

Run after a confirmed label is submitted to surface precursor signals and uplift related entities.

```bash
# Full pass
uv run python scripts/run_backtracking.py

# Incremental — only labels confirmed since a checkpoint
uv run python scripts/run_backtracking.py --since 2026-04-01T00:00:00Z
```

See [Backtracking Runbook](backtracking-runbook.md) for full options, output format, and demo scenario.

---

## Region presets

| Region | Flag | DuckDB | Gap threshold | Feature window | Weights (A / G / I) |
|---|---|---|---|---|---|
| Singapore / Malacca | `singapore` | `singapore.duckdb` | 6 h | 30 d | 0.40 / 0.40 / 0.20 |
| Japan Sea / DPRK | `japan` | `japansea.duckdb` | 12 h | 60 d | 0.40 / 0.40 / 0.20 |
| Middle East | `middleeast` | `middleeast.duckdb` | 12 h | 60 d | 0.40 / 0.40 / 0.20 |
| Europe / Baltic | `europe` | `europe.duckdb` | 6 h | 45 d | 0.35 / 0.35 / 0.30 |
| US Gulf | `gulf` | `gulf.duckdb` | 6 h | 14 d | 0.50 / 0.30 / 0.20 |

The preset weights are starting points. The pipeline auto-calibrates `w_graph` on every run via `_calibrate_graph_weight()`. Calling `src.score.composite` standalone still requires `--w-graph` (or the new `--auto-calibrate` flag). The calibrated value is printed at the end of Step 8 for reference.

---

## Pipeline steps

The pipeline runs 9 steps in sequence. Each step prints a status line; in interactive mode, failed steps prompt retry or skip.

| # | Step | Key modules |
|---|---|---|
| 1 | Schema initialisation | `src/ingest/schema.py` |
| 2 | Marine Cadastre historical backfill | `src/ingest/marine_cadastre.py` |
| 3 | Live AIS streaming | `src/ingest/ais_stream.py` |
| 4 | Sanctions loading | `src/ingest/sanctions.py` |
| 5 | Ownership graph | `src/ingest/vessel_registry.py` (→ Lance Graph) + `src/features/ownership_graph.py` |
| 6 | Feature engineering | `src/features/ais_behavior.py` + `identity.py` + `trade_mismatch.py` + `build_matrix.py` |
| 7 | Scoring | `src/score/causal_sanction.py` + `mpol_baseline.py` + `anomaly.py` + `composite.py` + `watchlist.py` |
| 8 | GDELT ingestion | `src/ingest/gdelt.py` |
| 9 | Dashboard | `src/api/main.py` (uvicorn) |

---

## Common CLI flags

### Live AIS streaming (`--stream-duration`)

```bash
# Collect 300 seconds of live AIS before moving on
uv run python scripts/run_pipeline.py --region singapore \
  --non-interactive --stream-duration 300
```

Without `--stream-duration`, non-interactive mode skips live streaming entirely. In interactive mode, streaming runs until Ctrl-C.

### Historical Marine Cadastre backfill (`--marine-cadastre-year`)

Only available for US-covered regions (Gulf of Mexico and US coastal waters). Repeat for multiple years.

```bash
uv run python scripts/run_pipeline.py --region gulf \
  --non-interactive \
  --marine-cadastre-year 2022 \
  --marine-cadastre-year 2023
```

### Geopolitical rerouting filter (`--geopolitical-event-filter`)

Down-weights `anomaly_score` for vessels in declared rerouting corridors to reduce false positives. Supply the path to a JSON event file.

```bash
uv run python scripts/run_pipeline.py --region middleeast \
  --non-interactive \
  --geopolitical-event-filter config/geopolitical_events.json
```

`config/geopolitical_events.json` covers:
- Red Sea / Cape of Good Hope rerouting (2023-11-01 → ongoing), down_weight 0.5
- Taiwan Strait GPS spoofing zone (2024-01-01 → ongoing), down_weight 0.7

Add new events to the JSON file without code changes. Format:

```json
{
  "events": [
    {
      "name": "Description of event",
      "active_from": "YYYY-MM-DD",
      "active_to": "YYYY-MM-DD",
      "corridors": [
        {"lat_min": -40, "lat_max": -25, "lon_min": 10, "lon_max": 40}
      ],
      "down_weight": 0.5
    }
  ]
}
```

### Dummy vessel seeding (`--seed-dummy`)

Injects four realistic shadow fleet vessels (PETROVSKY ZVEZDA, SARI NOUR, OCEAN VOYAGER, VERA SUNSET) into the DB after feature engineering so they appear on the dashboard during demos without requiring real AIS data.

```bash
uv run python scripts/run_pipeline.py --region singapore \
  --non-interactive --seed-dummy
```

---

## Running steps individually

Each module can be run standalone. Useful for re-running a single step after a failure without re-ingesting all data.

```bash
# Schema
uv run python -m src.ingest.schema --db data/processed/singapore.duckdb

# AIS feature engineering (30-day window, 6h gap threshold)
uv run python -m src.features.ais_behavior \
  --db data/processed/singapore.duckdb --window 30 --gap-threshold-hours 6

# Build full feature matrix
uv run python -m src.features.build_matrix --db data/processed/singapore.duckdb

# MPOL baseline (service vessel exclusion on by default)
uv run python -m src.score.mpol_baseline --db data/processed/singapore.duckdb

# Anomaly scoring
uv run python -m src.score.anomaly --db data/processed/singapore.duckdb

# C3 causal calibration (produces causal_effects.parquet, prints calibrated w_graph)
uv run python -m src.score.causal_sanction \
  --db data/processed/singapore.duckdb \
  --output data/processed/singapore_causal_effects.parquet

# Composite scoring with calibrated weight
uv run python -m src.score.composite \
  --db data/processed/singapore.duckdb \
  --w-graph 0.52 \
  --geopolitical-event-filter config/geopolitical_events.json

# Watchlist output
uv run python -m src.score.watchlist --db data/processed/singapore.duckdb
```

---

## Environment variables

All configurable paths are also settable via environment variables (useful in Docker Compose):

| Variable | Default | Description |
|---|---|---|
| `DB_PATH` | `data/processed/mpol.duckdb` | Active DuckDB path |
| `AISSTREAM_API_KEY` | — | Required for live AIS ingestion |
| `LLM_PROVIDER` | `llamacpp` | LLM client: `llamacpp`, `openai`, `anthropic`, `gemini`, `ollama` |
| `LLM_BASE_URL` | — | OpenAI-compatible base URL (required for `gemini`, `ollama`, custom endpoints) |
| `LLM_MODEL` | — | Model identifier (e.g., `gpt-4o-mini`, `llama3.2:3b`) |
| `WATCHLIST_OUTPUT_PATH` | `data/processed/candidate_watchlist.parquet` | Watchlist parquet output |
| `COMPOSITE_SCORES_PATH` | `data/processed/composite_scores.parquet` | Composite scores output |
| `CAUSAL_EFFECTS_PATH` | `data/processed/causal_effects.parquet` | C3 output |
| `VALIDATION_METRICS_PATH` | `data/processed/validation_metrics.json` | Metrics for dashboard |


---

## Docker Compose

```bash
# Full pipeline (non-interactive, Singapore preset)
PIPELINE_REGION=singapore docker compose run --rm pipeline

# With historical backfill
PIPELINE_REGION=gulf docker compose run --rm pipeline \
  uv run python scripts/run_pipeline.py \
  --region gulf --non-interactive \
  --marine-cadastre-year 2023

# Dashboard only (after pipeline has run)
docker compose up dashboard
```

---

## Cleared vessel feedback

When a Phase B physical inspection returns `outcome = cleared`, record the MMSI in the `cleared_vessels` table. On the next scoring cycle, the vessel will be used as a hard negative in HDBSCAN and Isolation Forest training, lowering false positive rates for similar vessels.

```bash
# Add a cleared vessel (replace values as appropriate)
duckdb data/processed/singapore.duckdb << 'SQL'
INSERT INTO cleared_vessels (mmsi, cleared_by, investigation_id, notes)
VALUES ('123456789', 'officer_kim', 'INV-2026-042',
        'Boarded 2026-04-15; IMO confirmed, cargo documents valid');
SQL
```

---

## Output files

| File | Description |
|---|---|
| `data/processed/<region>.duckdb` | All ingested and computed data for the region |
| `data/processed/<region>_causal_effects.parquet` | C3 causal model per-regime ATT estimates |
| `data/processed/composite_scores.parquet` | Full scored vessel frame (all vessels) |
| `data/processed/candidate_watchlist.parquet` | Top candidates, sorted by confidence |
| `data/processed/validation_metrics.json` | Precision@50, Recall@200, AUROC |
| `data/processed/review_feedback_evaluation.json` | Tier-aware metrics and threshold recommendations |
| `data/processed/backtracking_report.json` | Delayed-label intelligence loop report |
| `data/processed/backtracking_report.md` | Human-readable backtracking summary |

---

## Troubleshooting

**Step 3 (AIS stream) — 0 rows inserted**
Check `AISSTREAM_API_KEY` in `.env` and verify the bounding box covers an area with vessel traffic.

**Step 5 (Ownership graph) — vessel_registry fails**
Run `uv run python src/ingest/vessel_registry.py --db <db_path>` to rebuild the Lance Graph datasets manually. Alternatively, pass `--skip-graph` to `build_matrix.py` to run without graph features (graph features default to safe values).

**Step 7 (Scoring) — composite returns empty DataFrame**
`vessel_features` is empty. Re-run step 6 (feature engineering) and confirm `build_matrix.py` completed without errors.

**Dashboard shows no vessels**
Confirm `WATCHLIST_OUTPUT_PATH` points to the correct parquet file and that it contains rows (`polars.read_parquet(path).height > 0`).
