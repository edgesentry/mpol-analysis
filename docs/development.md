# Development Guide

Project overview, repo layout, commands, coding conventions, and links to full documentation.

## What This Repo Is

A shadow fleet candidate screening pipeline. Ingests public AIS, sanctions, vessel registry, and trade flow data → produces a ranked watchlist of candidate shadow fleet vessels with SHAP-explained confidence scores.

For context on the problem and full architecture, read [`docs/index.md`](index.md) and [`docs/architecture.md`](architecture.md).

## Repo Layout

```
_inputs/        Challenge docs (Cap Vista Solicitation 5.0 — do not edit)
docs/           Project documentation (source of truth for design decisions)
scripts/        Operator-facing CLI tools (run_pipeline.py, sync_r2.py, …)
pipeline/
  src/
    graph/      Lance Graph storage layer (store.py — node/relationship schemas, read/write)
    ingest/     Data ingestion scripts (AIS, sanctions, registry, trade flow)
    features/   Feature engineering (Polars + Lance Graph)
    score/      Scoring engine (HDBSCAN, Isolation Forest, SHAP, composite, causal DiD)
    analysis/   Post-confirmation intelligence (label_propagation, causal_rewind, backtracking_runner)
    api/        FastAPI pipeline API (POST /api/reviews/merge — called by CF Queue consumer)
app/            React + TypeScript + Vite SPA (the analyst dashboard)
  src/
    components/ KpiBar, WatchlistTable, VesselDetail, VesselMap, …
    lib/        duckdb.ts, opfs.ts, push.ts, reviews.ts, auth.ts, …
  functions/    Cloudflare Pages Functions (POST /api/reviews/push)
workers/
  review-merge-consumer/   CF Queue consumer Worker (triggers merge-reviews after push)
data/
  raw/          Downloaded raw data (gitignored)
  processed/    DuckDB files, Parquet outputs, Lance Graph datasets (<region>_graph/)
tests/
pyproject.toml
```

## Key References

- **Architecture and feature design:** [`docs/architecture.md`](architecture.md)
- **Tech stack and data sources:** [`docs/technical-solution.md`](technical-solution.md)
- **Implementation steps (A1–A5):** [`docs/roadmap.md`](roadmap.md)
- **Regional deployment playbooks:** [`docs/regional-playbooks.md`](regional-playbooks.md)
- **Field investigation design (edgesentry OSS):** [`docs/field-investigation.md`](field-investigation.md)
- **Human-in-the-loop triage governance:** [`docs/triage-governance.md`](triage-governance.md)
- **Backtesting and feedback evaluation:** [`docs/backtesting-validation.md`](backtesting-validation.md)
- **Delayed-label intelligence loop:** [`docs/backtracking-runbook.md`](backtracking-runbook.md)

## Procedures

### Run the full screening pipeline

The easiest way to run the full pipeline is the interactive CLI, which handles region selection and passes all flags automatically:

```bash
uv run python scripts/run_pipeline.py                          # interactive region selection
uv run python scripts/run_pipeline.py --region singapore --non-interactive
uv run python scripts/run_pipeline.py --region japan --non-interactive
```

Available regions: `singapore`, `japan`, `middleeast`, `europe`, `persiangulf`, `gulfofguinea`, `gulfofaden`, `gulfofmexico`, `blacksea`. See [`regional-playbooks.md`](regional-playbooks.md) for per-region parameter details.

Alternatively, run each step manually:

```bash
uv run python -m pipeline.src.ingest.schema             # initialise DuckDB schema
uv run python -m pipeline.src.ingest.marine_cadastre    # load historical AIS
uv run python -m pipeline.src.ingest.sanctions          # load sanctions entities
uv run python -m pipeline.src.ingest.vessel_registry    # load Equasis + ITU MMSI → Lance Graph
uv run python -m pipeline.src.ingest.eo_gfw --bbox 95,1,110,6 --days 30  # EO detections (requires GFW_API_TOKEN in .env)
uv run python -m pipeline.src.ingest.eo_gfw --csv data/raw/eo_detections_sample.csv  # EO detections via local CSV (no token needed)
uv run python -m pipeline.src.features.ais_behavior     # compute AIS behavioral features
uv run python -m pipeline.src.features.identity         # identity volatility features (Lance Graph)
uv run python -m pipeline.src.features.ownership_graph  # Lance Graph ownership features
uv run python -m pipeline.src.features.trade_mismatch   # trade flow mismatch features
uv run python -m pipeline.src.score.mpol_baseline       # HDBSCAN baseline
uv run python -m pipeline.src.score.anomaly             # Isolation Forest scoring
uv run python -m pipeline.src.score.causal_sanction     # C3: DiD causal model → calibrated w_graph
uv run python -m pipeline.src.score.composite           # composite score + SHAP (pass --w-graph from above)
uv run python -m pipeline.src.score.watchlist           # output candidate_watchlist.parquet
```

### Run the dashboard

The analyst dashboard is a React SPA that runs entirely in the browser (DuckDB-WASM + OPFS). In production it is deployed to Cloudflare Pages. For local development:

```bash
cd app && npm install   # first time only
cd app && npm run dev   # http://localhost:5173
```

The dev server fetches Parquet files from Cloudflare R2 (same as production). No local server process is needed — the browser queries data directly via DuckDB-WASM.

### Run the operations shell (menu-driven jobs)

```bash
bash scripts/run_operations_shell.sh
```

Covers Full Screening, Review-Feedback Evaluation, Historical Backtesting, and Demo/Smoke. See [`pipeline-operations.md`](pipeline-operations.md).

### Run the delayed-label intelligence loop (backtracking)

```bash
# Full pass (all confirmed labels):
uv run python scripts/run_backtracking.py --db data/processed/mpol.duckdb

# Incremental (only labels confirmed since a checkpoint):
uv run python scripts/run_backtracking.py --since 2026-04-01T00:00:00Z
```

See [`backtracking-runbook.md`](backtracking-runbook.md) for full options and output format.

### Run tests

```bash
# Python — pipeline unit and integration tests
uv run pytest tests/

# Frontend — Vitest unit tests (jsdom environment)
cd app && npm test

# Frontend — ESLint static analysis
cd app && npx eslint src/
```

## Coding Conventions

- **Polars:** use the lazy API (`pl.scan_parquet`, `.lazy()`, `.collect()`) for all large AIS queries; avoid `.to_pandas()`.
- **DuckDB:** use parameterised queries; never interpolate user-supplied strings into SQL.
- **Lance Graph:** read datasets via `src.graph.store.load_tables(db_path)`; write via `write_tables(db_path, tables)`. Graph features are implemented as Polars joins — no external graph server.
- **Output:** all intermediate outputs are Parquet in `data/processed/`; no CSV outputs.
- **Secrets:** API keys (aisstream.io, Equasis, GFW) go in `.env` (gitignored); read via `python-dotenv`. For EO fusion without a GFW token, pass `--skip-eo` or use `--csv` with a local detections file.

## Out of Scope

Do not implement physical vessel inspection, edge sensor measurement, or VDES communication in this repo. Those belong in edgesentry-rs / edgesentry-app. If you need to reference those requirements, see [`field-investigation.md`](field-investigation.md).
