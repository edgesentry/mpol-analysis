# Local End-to-End Test

Step-by-step guide to run the full screening pipeline on your local machine and verify the output.

## Prerequisites

| Requirement | Check |
|---|---|
| Docker (with Colima or Docker Desktop) | `docker info` |
| Docker Compose v2 | `docker compose version` |
| aisstream.io API key in `.env` | `AISSTREAM_API_KEY=<key>` |
| LLM provider key in `.env` (for analyst briefs) | `LLM_API_KEY=<key>` |

Clone the repo and create your `.env`:

```bash
git clone https://github.com/edgesentry/arktrace.git
cd arktrace
cp .env.example .env
```

---

## API key setup

### aisstream.io (AIS data)

1. Register at https://aisstream.io and create an API key.
2. Add it to `.env`:

```env
AISSTREAM_API_KEY=your_key_here
```

Without this key the AIS streaming step is skipped (sanctions, ownership graph, and GDELT still run). Set `PIPELINE_STREAM_DURATION` to a non-zero value to actually collect live AIS.

---

### LLM provider (analyst briefs)

The dashboard uses an LLM to generate analyst briefs for flagged vessels. Pick one provider and add the corresponding block to `.env`. The `LLM_PROVIDER` value controls which client is used.

**OpenAI**

```env
LLM_PROVIDER=openai
LLM_BASE_URL=https://api.openai.com/v1
LLM_API_KEY=sk-...
LLM_MODEL=gpt-4o-mini
```

Get an API key at https://platform.openai.com/api-keys.

**Anthropic Claude**

```env
LLM_PROVIDER=anthropic
LLM_API_KEY=sk-ant-...
LLM_MODEL=claude-haiku-4-5-20251001
```

Get an API key at https://console.anthropic.com.

**Google Gemini**

```env
LLM_PROVIDER=gemini
LLM_BASE_URL=https://generativelanguage.googleapis.com/v1beta/openai/
LLM_API_KEY=your_gemini_api_key
LLM_MODEL=gemini-2.5-flash-lite
```

Get an API key at https://aistudio.google.com/app/apikey.

> **LLM is optional.** If no key is configured the dashboard loads normally — clicking **Get Brief** on a vessel displays "Brief unavailable" instead of streaming a brief.

---

### Local LLM (macOS)

For macOS users, you can also run inference locally using **MLX LM** or **Ollama**. This allows you to generate analyst briefs without API keys or external costs.

See [Local LLM Setup (macOS)](local-llm-setup.md) for a detailed configuration guide.

---

## Step 1 — Start the dashboard

```bash
docker compose up -d
```

This starts:

- **dashboard** — FastAPI app at `http://localhost:8000`

---

## Step 2 — Ingest data

`scripts/run_pipeline.py` handles everything in one command: schema init → Marine Cadastre (optional) → AIS streaming → sanctions → ownership graph (Lance Graph) → feature engineering → scoring → GDELT geopolitical context.

```bash
docker compose run --rm pipeline
```

Defaults to the **Singapore / Malacca Strait** region with no live AIS streaming and 3 days of GDELT context. Four dummy shadow fleet candidates are injected automatically via `--seed-dummy`, so the dashboard is populated even without live AIS.

**Key flags:**

| Env var / flag | Default | Description |
|---|---|---|
| `PIPELINE_REGION` | `singapore` | Region preset: `singapore`, `japan`, `middleeast`, `europe`, `gulf` |
| `PIPELINE_STREAM_DURATION` | _(unset)_ | Seconds of live AIS to collect |
| `--gdelt-days N` | `3` | Days of GDELT events to ingest |
| `--marine-cadastre-year YEAR` | _(unset)_ | Load a historical Marine Cadastre year (repeatable; uses region bbox automatically) |
| `--seed-dummy` | on | Inject realistic dummy vessels after feature engineering |

Examples:

```bash
# Different region
PIPELINE_REGION=japan docker compose run --rm pipeline

# Collect 5 minutes of live AIS
PIPELINE_REGION=singapore PIPELINE_STREAM_DURATION=300 docker compose run --rm pipeline

# Gulf region with 2023 historical Marine Cadastre backfill
PIPELINE_REGION=gulf docker compose run --rm pipeline \
  uv run python scripts/run_pipeline.py \
  --region gulf --non-interactive --marine-cadastre-year 2023
```

See `docs/regional-playbooks.md` for per-region configuration details.

A successful run ends with:

```
[ 7/9] Scoring...                                  ✓  precision_at_50=0.62
[ 8/9] Ingesting GDELT context (3d)...             ✓  Total events ingested: 5423
[ 9/9] Launching dashboard...                      (skipped in non-interactive mode)
```

> **Note on composite weights:** Step 7 automatically runs the C3 causal sanction-response model before `composite.py`. If enough AIS data is present to estimate a statistically significant treatment effect, the `w_graph` used will differ from the preset value shown in the region summary above. The calibrated value is logged to `data/processed/<region>_causal_effects.parquet`.

Output files written to `./data/processed/`:

- `<region>.duckdb` — DuckDB database for that region's raw data
- `<region>_graph/` — Lance Graph datasets (Vessel, Company, ownership relationships)
- `candidate_watchlist.parquet` — ranked candidate watchlist (read by the dashboard)
- `<region>_causal_effects.parquet` — per-regime DiD ATT estimates and calibrated `w_graph` (C3)
- `gdelt.lance/` — LanceDB vector store for analyst briefs

---

## Step 3 — Verify the dashboard

Open `http://localhost:8000`. Verify:

- Map shows candidate vessels colour-coded by confidence (green < 0.4, amber 0.4–0.7, red ≥ 0.7)
- Ranked table updates independently when you change filters (HTMX partial refresh — visible in browser network tab)
- KPI bar shows candidate count, high-confidence count, avg confidence, and validation metrics
- Sidebar filters (minimum confidence, vessel type, top N) → click **Apply**

**Validation metrics** — check acceptance criterion (`precision_at_50 >= 0.6`):

```bash
cat data/processed/validation_metrics.json
```

**Analyst briefs (C2):** click any map marker → **Get Brief**. A one-paragraph brief citing recent GDELT events streams into the popup. Requires LLM credentials in `.env`. Best-effort — displays "Brief unavailable" if no LLM is reachable.

**Causal effects (C3):** the pipeline writes `data/processed/<region>_causal_effects.parquet` during Step 7. To verify it manually:

1. Open the file in DuckDB CLI or any Parquet viewer:
   ```bash
   duckdb -c "SELECT label, n_treated, n_control, round(att_estimate,3) AS att, round(p_value,4) AS p, is_significant, round(calibrated_weight,3) AS w_graph FROM 'data/processed/singapore_causal_effects.parquet';"
   ```
2. Expect **3 rows** — one each for OFAC Iran, OFAC Russia, and UN DPRK.
3. With sparse AIS data (no live streaming), `n_treated` / `n_control` will be small and `is_significant = false` — the pipeline correctly falls back to the preset `w_graph`. With ≥ 30 days of real AIS data, significant positive ATT estimates raise `w_graph` above the 0.40 default (up to 0.65).
4. `calibrated_weight` must be the same value in all three rows — it is a single pipeline-level scalar.

To run the automated structural checks against the output file:

```bash
uv run pytest tests/test_causal_effects_output.py -v
```

The test skips automatically if the pipeline has not been run yet.

---

## Unit tests

```bash
docker compose run --rm pipeline uv run pytest tests/ -v
```

All tests should pass. Any warnings from sklearn are harmless.

---

## Teardown

```bash
docker compose down
```
