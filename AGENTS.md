# AGENTS

Shadow fleet candidate screening application. Consumes edgesentry-rs primitives and indago data; domain-specific business logic lives here.

## External dependency map

Before modifying scoring logic or debugging unexpected scores, identify which layer owns the problem:

| Symptom | Likely owner | Where to look |
|---|---|---|
| Wrong distance / TTC / zone membership | edgesentry-rs physics engine | `crates/edgesentry-compute/` in edgesentry-rs |
| AuditRecord fails to verify | edgesentry-rs audit chain | `crates/edgesentry-audit/` in edgesentry-rs |
| AIS feature values wrong (gap counts, STS, loitering) | indago feature pipeline | `pipelines/features/ais_behavior.py` in indago |
| Ownership graph distance wrong | indago identity pipeline | `pipelines/features/identity.py` in indago |
| Composite score unexpected | this repo scoring engine | `pipeline/src/score/composite.py` |
| SHAP attribution wrong | this repo SHAP layer | `pipeline/src/score/composite.py` |
| Parquet schema mismatch | indago → arktrace contract | `indago` R2 bucket schema; `pipeline/src/ingest/schema.py` |
| Dashboard renders wrong data | this repo React SPA | `app/src/` |

## Directory map

| Path | Purpose |
|---|---|
| `pipeline/src/ingest/` | AIS, sanctions, vessel registry, EO detection ingestion |
| `pipeline/src/features/` | Feature engineering (AIS behaviour, identity, ownership graph, trade mismatch) |
| `pipeline/src/score/` | Scoring: HDBSCAN baseline, Isolation Forest, DiD causal model, composite + SHAP |
| `pipeline/src/analysis/` | Post-confirmation: label propagation, causal rewind, backtracking |
| `pipeline/src/graph/` | Lance Graph storage (node/relationship schemas, read/write) |
| `pipeline/src/api/` | FastAPI endpoint (`POST /api/reviews/merge`) |
| `app/src/` | React SPA — KpiBar, WatchlistTable, VesselDetail, VesselMap |
| `app/src/lib/` | DuckDB-WASM, OPFS, push/pull, auth |
| `app/functions/` | Cloudflare Pages Functions (`POST /api/reviews/push`) |
| `workers/` | CF Queue consumer Worker (review-merge) |
| `scripts/` | Operator CLI: `run_pipeline.py`, `sync_r2.py`, `run_operations_shell.sh` |
| `data/processed/` | DuckDB files, Parquet outputs, Lance Graph datasets (gitignored) |
| `tests/` | Pipeline unit and integration tests |
| `docs/` | Reference docs (`ref-`), use-case scenarios (`feature-`), integration specs (`integration-`), UI specs (`ui-`) |

## Key files

- Pipeline entry point: `scripts/run_pipeline.py`
- Scoring output: `data/processed/candidate_watchlist.parquet`
- Schema: `pipeline/src/ingest/schema.py`
- Dashboard entry: `app/src/main.tsx`

## Coding conventions

- **Polars:** use lazy API (`pl.scan_parquet`, `.lazy()`, `.collect()`) for large AIS queries
- **DuckDB:** parameterised queries only — never interpolate user strings into SQL
- **Lance Graph:** read via `src.graph.store.load_tables()`; write via `write_tables()`
- **Output:** all intermediate outputs are Parquet in `data/processed/`; no CSV outputs
- **Secrets:** API keys in `.env` (gitignored); read via `python-dotenv`

## Commit convention

Conventional Commits (`fix:`, `feat:`, `feat!:`)

## Docs

- Pipeline and data flow design: `docs/ref-architecture.md`
- Problem context and shadow fleet background: `docs/ref-background.md`
- Tech stack, algorithms, data sources: `docs/ref-technical-solution.md`
- LLM anti-hallucination design: `docs/ref-llm-grounding.md`
- End-to-end use case flows: `docs/feature-scenarios.md`
- Custom AIS/data feed integration: `docs/integration-custom-feeds.md`
- Roadmap: `docs/roadmap/index.md`

## Agent Skills

```bash
npx skills add edgesentry/arktrace
```

| Skill | Trigger |
|---|---|
| `/arktrace-run-pipeline` | When asked to "update the watchlist" or "score vessels for a region"; when `candidate_watchlist.parquet` is stale; when a pipeline step fails mid-run |
| `/arktrace-run-dashboard` | When developing or debugging the React frontend; when verifying a UI change locally; when `npm run dev` output is needed |
| `/arktrace-run-tests` | Before every commit; when CI fails on `pytest` or `eslint`; after modifying pipeline scoring logic or frontend components |
| `/arktrace-deploy` | When setting up Cloudflare Pages or R2 for a new environment; when updating production env vars or CI publish job |
| `/arktrace-llm-setup` | When Dispatch Brief generation is failing; when switching from OpenAI to a local model; when `LLM_PROVIDER` is not set |
| `/arktrace-demo-data` | When `data/processed/` is empty and the full pipeline cannot be run; when demoing to a reviewer without credentials |
