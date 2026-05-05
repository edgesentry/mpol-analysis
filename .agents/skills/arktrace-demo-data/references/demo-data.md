# Demo Data — R2 Distribution

arktrace ships a lightweight **demo bundle** to Cloudflare R2 after every
successful data-publish CI run.  Developers can download it with a single
command, no credentials required, to explore the app without running the full
AIS ingestion and scoring pipeline locally.

---

## Data directory

Files are stored in **`~/.arktrace/data/`** by default — a user-level location
that persists across repo updates and works for both installed and source builds.

| Override | Description |
|----------|-------------|
| `ARKTRACE_DATA_DIR=<path>` | Use a custom data directory |
| `DB_PATH=<path>` | Full path to the region DuckDB (overrides everything; dev/CI use) |

Repo contributors working from source will find `data/processed/` is used
automatically when it already exists (the resolver checks for it first).

---

## Region selection

Default region is **singapore**. Change it with the `ARKTRACE_REGION` env var
or the `--region` flag on `fetch_demo_data.sh`:

| Region | Value |
|--------|-------|
| Singapore (default) | `singapore` |
| Sea of Japan | `japan` |
| Middle East | `middleeast` |
| Europe | `europe` |
| Gulf of Mexico | `gulf` |

---

## For developers — pulling the demo bundle

```bash
# Option A: convenience shell script (recommended)
bash scripts/fetch_demo_data.sh                        # Singapore (default)
bash scripts/fetch_demo_data.sh --region middleeast    # Middle East

# Option B: Python sync script
uv run python scripts/sync_r2.py pull-demo             # uses resolved data dir
uv run python scripts/sync_r2.py pull-demo --data-dir ~/.arktrace/data

# Option C: set region via env var
ARKTRACE_REGION=gulf bash scripts/fetch_demo_data.sh
```

Both commands download `demo.zip` from the public R2 bucket and extract:

| File | Contents |
|------|----------|
| `candidate_watchlist.parquet` | Top candidate shadow-fleet vessels, scored and ranked |
| `composite_scores.parquet` | Full composite score table for all vessels |
| `causal_effects.parquet` | C3 DiD causal uplift estimates |
| `validation_metrics.json` | Backtest metrics (AUROC, Recall@200, P@50) |

After pulling, start the API:

```bash
# Singapore (default)
uv run uvicorn src.api.main:app --reload
open http://localhost:8000

# Different region
ARKTRACE_REGION=japan uv run uvicorn src.api.main:app --reload
```

### Auto-pull on startup

The app checks on every startup whether local files are **missing or stale**
(older than the R2 latest snapshot). If so, it re-downloads automatically —
no manual intervention needed after the first pull.

```
AUTO_PULL=0  # disable auto-pull (offline / air-gapped environments)
```

### Optional extras

```bash
# OpenSanctions DuckDB — needed for integration tests, not the dashboard
uv run python scripts/sync_r2.py pull-sanctions-db

# Full pipeline snapshot — includes regional DuckDBs and Lance graphs (~500 MB)
uv run python scripts/sync_r2.py pull --region singapore
```

---

## For app owners — generating fresh demo data and pushing to R2

The CI job (`data-publish.yml`) pushes the demo bundle automatically after
every weekly pipeline run.  If you need to push a manually generated batch:

### 1. Run the pipeline locally

```bash
# Singapore is the primary demo region
uv run python scripts/run_pipeline.py --region singapore --non-interactive

# Optional: also run other regions
uv run python scripts/run_pipeline.py --region japan,middleeast --non-interactive
```

### 2. Run the backtest to generate validation_metrics.json

```bash
uv run python scripts/run_public_backtest_batch.py \
  --regions singapore \
  --skip-pipeline \
  --max-known-cases 200
```

### 3. Push the demo bundle to R2

```bash
# Requires R2 write credentials in .env:
#   AWS_ACCESS_KEY_ID=<key>
#   AWS_SECRET_ACCESS_KEY=<secret>
uv run python scripts/sync_r2.py push-demo
```

This overwrites `demo.zip` in R2 with the current `data/processed/` outputs.

---

## R2 credentials setup

R2 credentials are only required for **push** commands.  Pull commands work
without credentials because the `arktrace-public` bucket has public access
enabled.

1. Create an R2 API token at Cloudflare Dashboard → R2 → Manage R2 API Tokens
   with **Object Read & Write** permission scoped to **both** `arktrace-public`
   and `arktrace-private-capvista`.
2. Add to `.env`:

```dotenv
AWS_ACCESS_KEY_ID=<your-r2-access-key-id>
AWS_SECRET_ACCESS_KEY=<your-r2-secret-access-key>
AWS_REGION=auto
S3_ENDPOINT=https://b8a0b09feb89390fb6e8cf4ef9294f48.r2.cloudflarestorage.com
S3_BUCKET=arktrace-public
```

The same token is used for both the public and private buckets.  See
[R2 data layout](https://edgesentry.github.io/indago/r2-data-layout/#private-bucket--arktrace-private-capvista)
for the full credential model.

---

## CI integration

The `data-publish.yml` workflow pushes the demo bundle automatically after
every weekly pipeline run (Monday 02:00 UTC) and after each successful
public-backtest-integration run.

See also: [r2-data-layout.md](https://edgesentry.github.io/indago/r2-data-layout/) for the full R2 bucket structure.
