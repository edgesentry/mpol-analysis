# AGENTS.md

Agent instructions for working in this repository.

## What This Repo Is

A shadow fleet candidate screening pipeline. Ingests public AIS, sanctions, vessel registry, and trade flow data → produces a ranked watchlist of candidate shadow fleet vessels with SHAP-explained confidence scores.

For context on the problem and full architecture, read [`docs/index.md`](docs/index.md) and [`docs/architecture.md`](docs/architecture.md).

## Repo Layout

```
_inputs/        Challenge docs (Cap Vista Solicitation 5.0 — do not edit)
docs/           Project documentation (source of truth for design decisions)
src/
  ingest/       Data ingestion scripts (AIS, sanctions, registry, trade flow)
  features/     Feature engineering (Polars + Neo4j)
  score/        Scoring engine (HDBSCAN, Isolation Forest, SHAP, composite)
  viz/          Streamlit dashboard
data/
  raw/          Downloaded raw data (gitignored)
  processed/    DuckDB files, Parquet outputs, Neo4j database
tests/
pyproject.toml
```

## Key References

- **Architecture and feature design:** [`docs/architecture.md`](docs/architecture.md)
- **Tech stack and data sources:** [`docs/technical-solution.md`](docs/technical-solution.md)
- **Implementation steps (A1–A5):** [`docs/roadmap.md`](docs/roadmap.md)
- **Field investigation design (edgesentry OSS):** [`docs/field-investigation.md`](docs/field-investigation.md)

## Procedures

### Run the full screening pipeline

```bash
uv run python src/ingest/schema.py             # initialise DuckDB schema
uv run python src/ingest/marine_cadastre.py    # load historical AIS
uv run python src/ingest/sanctions.py          # load sanctions entities
uv run python src/ingest/vessel_registry.py    # load Equasis + ITU MMSI → Neo4j
uv run python src/features/ais_behavior.py     # compute AIS behavioral features
uv run python src/features/identity.py         # identity volatility features
uv run python src/features/ownership_graph.py  # Neo4j BFS graph features
uv run python src/features/trade_mismatch.py   # trade flow mismatch features
uv run python src/score/mpol_baseline.py       # HDBSCAN baseline
uv run python src/score/anomaly.py             # Isolation Forest scoring
uv run python src/score/composite.py           # composite score + SHAP
uv run python src/score/watchlist.py           # output candidate_watchlist.parquet
```

### Run the dashboard

```bash
uv run streamlit run src/viz/dashboard.py
```

### Run tests

```bash
uv run pytest tests/
```

### Start Neo4j (Docker)

```bash
docker run -d \
  --name neo4j-mpol \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/password \
  -e NEO4J_PLUGINS='["graph-data-science"]' \
  neo4j:5-community
```

## Coding Conventions

- **Polars:** use the lazy API (`pl.scan_parquet`, `.lazy()`, `.collect()`) for all large AIS queries; avoid `.to_pandas()`.
- **DuckDB:** use parameterised queries; never interpolate user-supplied strings into SQL.
- **Neo4j:** use parameterised Cypher (`$param`); close driver sessions explicitly.
- **Output:** all intermediate outputs are Parquet in `data/processed/`; no CSV outputs.
- **Secrets:** API keys (aisstream.io, Equasis) go in `.env` (gitignored); read via `python-dotenv`.

## Out of Scope

Do not implement physical vessel inspection, edge sensor measurement, or VDES communication in this repo. Those belong in edgesentry-rs / edgesentry-app. If you need to reference those requirements, see [`docs/field-investigation.md`](docs/field-investigation.md).
