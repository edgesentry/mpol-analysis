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

## Step 1 — Start Neo4j and the dashboard

```bash
docker compose up -d
```

This starts two services:

- **neo4j** — waits until the healthcheck passes before the dashboard connects
- **dashboard** — FastAPI app at `http://localhost:8000`

Verify Neo4j is up: open `http://localhost:7474` and log in with `neo4j / mpol-password`.

---

## Step 2 — Ingest data

The pipeline script handles all ingestion and scoring in sequence: schema init → AIS streaming → sanctions → ownership graph → feature engineering → scoring → GDELT geopolitical context.

```bash
docker compose run --rm pipeline
```

Defaults to the **Singapore / Malacca Strait** region with no live AIS streaming and 3 days of GDELT context.

**Options:**

| Env var / flag | Default | Description |
|---|---|---|
| `PIPELINE_REGION` | `singapore` | Region preset: `singapore`, `japan`, `middleeast`, `europe`, `gulf` |
| `PIPELINE_STREAM_DURATION` | _(unset)_ | Seconds of live AIS to collect before continuing |
| `--gdelt-days N` | `3` | Days of GDELT events to ingest |

Examples:

```bash
# Different region
PIPELINE_REGION=japan docker compose run --rm pipeline

# Collect 5 minutes of live AIS
PIPELINE_REGION=singapore PIPELINE_STREAM_DURATION=300 docker compose run --rm pipeline

# More GDELT history
docker compose run --rm pipeline uv run python scripts/run_pipeline.py \
  --region singapore --non-interactive --gdelt-days 7
```

A successful run ends with:

```
[7/9] Scoring...                                   ✓  precision_at_50=0.62
[8/9] Ingesting GDELT context (3d)...              ✓  Total events ingested: 5423
[9/9] Launching dashboard...                       (skipped in non-interactive mode)
```

Output files are written to `./data/processed/` on the host:

- `<region>.duckdb` — DuckDB database
- `<region>_watchlist.parquet` — ranked candidate watchlist
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

---

## Unit tests

```bash
docker compose run --rm pipeline uv run pytest tests/ -v
```

Expected: **87 passed**, 3 warnings (sklearn FutureWarning, harmless).

---

## Teardown

```bash
docker compose down
```

To also remove the persisted Neo4j data volume:

```bash
docker compose down -v
```
