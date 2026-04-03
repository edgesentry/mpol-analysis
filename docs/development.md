# Development Guide

Project overview, repo layout, commands, coding conventions, and links to full documentation.

## What This Repo Is

A shadow fleet candidate screening pipeline. Ingests public AIS, sanctions, vessel registry, and trade flow data → produces a ranked watchlist of candidate shadow fleet vessels with SHAP-explained confidence scores.

For context on the problem and full architecture, read [`docs/index.md`](index.md) and [`docs/architecture.md`](architecture.md).

## Repo Layout

```
_inputs/        Challenge docs (Cap Vista Solicitation 5.0 — do not edit)
docs/           Project documentation (source of truth for design decisions)
scripts/        Operator-facing CLI tools (run_pipeline.py)
src/
  graph/        Lance Graph storage layer (store.py — node/relationship schemas, read/write)
  ingest/       Data ingestion scripts (AIS, sanctions, registry, trade flow)
  features/     Feature engineering (Polars + Lance Graph)
  score/        Scoring engine (HDBSCAN, Isolation Forest, SHAP, composite, causal DiD)
  api/          FastAPI + HTMX dashboard (src/api/main.py → http://localhost:8000)
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

## Procedures

### Run the full screening pipeline

The easiest way to run the full pipeline is the interactive CLI, which handles region selection and passes all flags automatically:

```bash
uv run python scripts/run_pipeline.py                          # interactive region selection
uv run python scripts/run_pipeline.py --region singapore --non-interactive
uv run python scripts/run_pipeline.py --region japan --non-interactive
```

Available regions: `singapore`, `japan`, `middleeast`, `europe`, `gulf`. See [`regional-playbooks.md`](regional-playbooks.md) for per-region parameter details.

Alternatively, run each step manually:

```bash
uv run python src/ingest/schema.py             # initialise DuckDB schema
uv run python src/ingest/marine_cadastre.py    # load historical AIS
uv run python src/ingest/sanctions.py          # load sanctions entities
uv run python src/ingest/vessel_registry.py    # load Equasis + ITU MMSI → Lance Graph
uv run python src/features/ais_behavior.py     # compute AIS behavioral features
uv run python src/features/identity.py         # identity volatility features (Lance Graph)
uv run python src/features/ownership_graph.py  # Lance Graph ownership features
uv run python src/features/trade_mismatch.py   # trade flow mismatch features
uv run python src/score/mpol_baseline.py       # HDBSCAN baseline
uv run python src/score/anomaly.py             # Isolation Forest scoring
uv run python src/score/causal_sanction.py     # C3: DiD causal model → calibrated w_graph
uv run python src/score/composite.py           # composite score + SHAP (pass --w-graph from above)
uv run python src/score/watchlist.py           # output candidate_watchlist.parquet
```

### Run the dashboard

```bash
uv run uvicorn src.api.main:app --reload
# open http://localhost:8000
```

### Run tests

```bash
uv run pytest tests/
```

## Coding Conventions

- **Polars:** use the lazy API (`pl.scan_parquet`, `.lazy()`, `.collect()`) for all large AIS queries; avoid `.to_pandas()`.
- **DuckDB:** use parameterised queries; never interpolate user-supplied strings into SQL.
- **Lance Graph:** read datasets via `src.graph.store.load_tables(db_path)`; write via `write_tables(db_path, tables)`. Graph features are implemented as Polars joins — no external graph server.
- **Output:** all intermediate outputs are Parquet in `data/processed/`; no CSV outputs.
- **Secrets:** API keys (aisstream.io, Equasis) go in `.env` (gitignored); read via `python-dotenv`.

## Out of Scope

Do not implement physical vessel inspection, edge sensor measurement, or VDES communication in this repo. Those belong in edgesentry-rs / edgesentry-app. If you need to reference those requirements, see [`field-investigation.md`](field-investigation.md).
