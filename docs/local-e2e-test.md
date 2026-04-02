# Local End-to-End Test

Step-by-step guide to run the full screening pipeline on your local machine and verify the output.

## Prerequisites

| Requirement | Check |
|---|---|
| Python 3.12 | `python3 --version` |
| uv | `uv --version` |
| Docker (with Colima or Docker Desktop) | `docker info` |
| aisstream.io API key in `.env` | `AISSTREAM_API_KEY=<key>` |

Clone the repo and install dependencies:

```bash
git clone https://github.com/edgesentry/mpol-analysis.git
cd mpol-analysis
uv sync --all-extras --group dev
cp .env.example .env   # then fill in AISSTREAM_API_KEY
```

---

## Step 1 — Start Neo4j

```bash
bash scripts/start_neo4j.sh
```

The script is idempotent. It waits until Neo4j is ready before returning.

Verify: open `http://localhost:7474` in a browser and log in with `neo4j / password`.

---

## Step 2 — A1: Initialise the database and stream live AIS

```bash
uv run python src/ingest/schema.py
```

Expected output:
```
Schema initialised at data/processed/mpol.duckdb
```

The primary AIS source is **aisstream.io** (live WebSocket), which is pre-configured to the Singapore + Malacca Strait bounding box (`5°S–22°N, 92°E–122°E`). Run it for a few minutes to collect position reports, then interrupt with `Ctrl-C`:

```bash
uv run python src/ingest/ais_stream.py
```

| Argument | Default | Description |
|---|---|---|
| `--batch-size` | `200` | Number of messages to accumulate before flushing to DuckDB |
| `--flush-interval` | `60` | Max seconds between flushes regardless of batch size |
| `--db` | `data/processed/mpol.duckdb` | DuckDB path |

Expected output (repeating until interrupted):
```
Connecting to wss://stream.aisstream.io/v0/stream …
Subscribed — bbox [[-5.0, 92.0], [22.0, 122.0]], batch_size=200, flush_interval=60s
  Flushed 200 records → 198 inserted (total 198)
  Flushed 200 records → 195 inserted (total 393)
  …
^C
Shutdown signal received — flushing final batch …
Ingestion complete. Total inserted: <N>
```

> **Marine Cadastre is not used for Singapore data.** It covers US coastal waters only and is not applicable to this pipeline's area of interest. Ignore `src/ingest/marine_cadastre.py` for local testing.

---

## Step 3 — A2: Load sanctions and vessel registry

```bash
uv run python src/ingest/sanctions.py
```

Expected output:
```
Sanctions entities loaded: <N> rows
```

```bash
uv run python src/ingest/vessel_registry.py
```

Expected output:
```
Vessel nodes merged: <N>
Sanctions relationships created: <N>
```

---

## Step 4 — A3: Feature engineering

Run the four feature scripts in any order (they all write into `vessel_features`):

```bash
uv run python src/features/ais_behavior.py
uv run python src/features/identity.py
uv run python src/features/ownership_graph.py
uv run python src/features/trade_mismatch.py
```

Verify the feature matrix is populated:

```bash
uv run python - <<'EOF'
import duckdb, os
con = duckdb.connect(os.getenv("DB_PATH", "data/processed/mpol.duckdb"), read_only=True)
print(con.execute("SELECT COUNT(*) FROM vessel_features").fetchone())
con.close()
EOF
```

Expected: a non-zero row count.

---

## Step 5 — A4: Scoring and watchlist

```bash
uv run python src/score/mpol_baseline.py
uv run python src/score/anomaly.py
uv run python src/score/composite.py
uv run python src/score/watchlist.py
```

Expected final output:
```
Watchlist rows written: <N>
```

Verify the output file:

```bash
uv run python - <<'EOF'
import polars as pl
df = pl.read_parquet("data/processed/candidate_watchlist.parquet")
print(df.select(["mmsi", "vessel_name", "confidence", "top_signals"]).head(5))
EOF
```

Expected: a ranked table with `confidence` descending, each row containing a JSON `top_signals` array.

---

## Step 6 — A5: Validation metrics

Validation runs automatically as part of the pipeline. Check the output:

```bash
cat data/processed/validation_metrics.json
```

Expected shape:

```json
{
  "precision_at_50": 0.62,
  "recall_at_200": 0.41,
  "auroc": 0.78
}
```

Acceptance criterion: `precision_at_50 >= 0.6`.

---

## Step 7 — Dashboard

### FastAPI dashboard (default)

```bash
uv run uvicorn src.api.main:app --reload
```

Open `http://localhost:8000`. Verify:

- Map shows candidate vessels colour-coded by confidence (green < 0.4, amber 0.4–0.7, red > 0.7)
- Ranked table updates independently when you change filters (HTMX partial refresh — visible in browser network tab)
- KPI bar shows candidate count, high-confidence count, avg confidence, and validation metrics
- Sidebar filters (minimum confidence, vessel type, top N) → click **Apply**

### Streamlit dashboard (development fallback)

```bash
uv run streamlit run src/viz/dashboard.py
```

Open `http://localhost:8501`. The Streamlit dashboard is the Phase A prototype — use it for quick single-user iteration. The FastAPI dashboard (`src/api/`) is the production path (see [deployment.md](deployment.md)).

---

## Step 8 — Unit tests

```bash
uv run pytest tests/ -v
```

Expected: **48 passed**, 3 warnings (sklearn FutureWarning, harmless).

---

## Teardown

```bash
bash scripts/stop_neo4j.sh
```
