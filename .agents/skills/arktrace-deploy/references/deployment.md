# Deployment

## Architecture overview

The dashboard is a **React SPA deployed to Cloudflare Pages**. There is no application server — the browser fetches Parquet files from Cloudflare R2, caches them in OPFS, and queries them locally via DuckDB-WASM. The Docker image and pipeline CLI are for running the data pipeline, not for serving the dashboard.

| Concern | How |
|---|---|
| Dashboard (production) | Cloudflare Pages — auto-deployed from `main` via GitHub Actions |
| Dashboard (local dev) | `cd app && npm run dev` — Vite dev server on `localhost:5173` |
| Data pipeline | Docker / uv — `run_pipeline.py` generates Parquet, `sync_r2.py push` publishes to R2 |
| Auth (private mode) | Cloudflare Access on the private proxy Worker |
| Review push | POST `/api/reviews/push` → Cloudflare Pages Function → R2 → CF Queue → `merge-reviews` |

---

## Dashboard — local development

```bash
git clone https://github.com/edgesentry/arktrace && cd arktrace
cd app && npm install
cd app && npm run dev   # http://localhost:5173
```

The dev server fetches live data from `arktrace-public` R2 — no local data needed. To point at a local fixture instead, set `VITE_PUBLIC_BUCKET_URL` in `app/.env.local`.

---

## Dashboard — production deployment (Cloudflare Pages)

The dashboard is deployed automatically by `.github/workflows/deploy-app.yml` on every push to `main`. Manual deploy:

```bash
cd app && npm run build
npx wrangler pages deploy app/dist --project-name arktrace
```

Required Pages environment variables (set in CF dashboard or via `wrangler pages env`):

| Variable | Description |
|---|---|
| `VITE_PUBLIC_BUCKET_URL` | Public R2 URL, e.g. `https://arktrace-public.edgesentry.io` |
| `VITE_PRIVATE_MANIFEST_URL` | Private proxy URL for authenticated mode (optional) |

---

## Pipeline — Docker quickstart

The Docker image runs the **data pipeline** (`run_pipeline.py`), not the dashboard. Use it to generate Parquet artifacts and push them to R2.

```bash
docker run --name arktrace-pipeline \
  -v arktrace-data:/root/.arktrace/data \
  --env-file .env \
  ghcr.io/edgesentry/arktrace:latest
```

To change region:

```bash
docker run -v arktrace-data:/root/.arktrace/data \
  -e PIPELINE_REGION=japan \
  --env-file .env \
  ghcr.io/edgesentry/arktrace:latest
```

### Update to a new version

```bash
docker pull ghcr.io/edgesentry/arktrace:latest
```

The named volume `arktrace-data` persists data across restarts.

---

## Pipeline — native (developer path)

```bash
git clone https://github.com/edgesentry/arktrace && cd arktrace
uv sync
uv run python scripts/run_pipeline.py --region singapore --non-interactive
uv run python scripts/sync_r2.py push   # publish artifacts to R2
```

See [development.md](development.md) for the full list of pipeline steps.

---

## Docker image — published to GHCR

The multi-arch image (`linux/amd64` + `linux/arm64`) is published automatically:

| Tag | When |
|---|---|
| `ghcr.io/edgesentry/arktrace:latest` | Every push to `main` |
| `ghcr.io/edgesentry/arktrace:v1.2.3` | Release tags |

Source: `.github/workflows/docker-publish.yml`.

---

## Local — Docker Compose (pipeline + MinIO)

Run the pipeline with a local MinIO S3-compatible store for development:

```bash
docker compose up
```

This starts:

| Container | Port | Purpose |
|---|---|---|
| `mpol-minio` | 9000 / 9001 | MinIO S3-compatible object store (API / console) |

Open the MinIO console at `http://localhost:9001` (user: `minioadmin` / password: `minioadmin`) to browse stored objects.

Stop everything:

```bash
docker compose down
```

---

## Cloud — single VM (e.g. AWS EC2, GCP Compute Engine, DigitalOcean Droplet)

Minimal setup for running the data pipeline on a cloud host so CI-generated data is published to R2 on a schedule.

### 1. Provision the VM

Recommended spec: **4 vCPU / 8 GB RAM / 50 GB SSD**, Ubuntu 24.04 LTS.

### 2. Install Docker

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# log out and back in
```

### 3. Clone and configure

```bash
git clone https://github.com/edgesentry/arktrace.git
cd arktrace
cp .env.example .env
# Edit .env — set AISSTREAM_API_KEY, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
```

### 4. Run the pipeline

```bash
docker run -v arktrace-data:/root/.arktrace/data --env-file .env \
  ghcr.io/edgesentry/arktrace:latest
```

### 5. Automate re-scoring (optional)

```bash
uv run python scripts/run_pipeline.py --region singapore --non-interactive --cadence 900
```

This runs the full initial pipeline once, then loops `step_features` + `step_score` every 900 seconds (15 minutes). Press Ctrl-C to stop.

---

## AIS providers

arktrace is AIS-provider agnostic. Three ingestion paths are available — switch or combine them without code changes.

### Live WebSocket (aisstream.io)

Default for real-time feeds. Requires `AISSTREAM_API_KEY` in `.env`.

```bash
uv run python -m pipeline.src.ingest.ais_stream --bbox -5 92 22 122
```

### CSV file (any provider)

Accepts any CSV with configurable column mapping. Default layout matches MarineCadastre (NOAA):

```bash
# MarineCadastre / NOAA (default column names):
uv run python -m pipeline.src.ingest.ais_csv --file data/raw/ais_2024.csv

# Spire / exactEarth / Orbcomm — map provider columns to internal schema:
uv run python -m pipeline.src.ingest.ais_csv --file spire_feed.csv \
    --column-map mmsi=vessel_id,lat=latitude,lon=longitude,timestamp=time_utc,sog=speed,cog=course
```

### NMEA 0183 sentence file (S-AIS raw feed)

```bash
uv run python -m pipeline.src.ingest.ais_csv --file feed.nmea --nmea
```

---

## Edge gateway benchmark

The incremental re-score pipeline (feature matrix + HDBSCAN + Isolation Forest + SHAP + watchlist output) is optimised for low-power environments.

| Metric | Value |
|---|---|
| Vessels | 5,000 |
| Feature matrix (`build_matrix.py`) | 0.28 s |
| Composite scoring (`composite.py`) | 5.46 s |
| Watchlist output (`watchlist.py`) | 0.01 s |
| **Pipeline total** | **5.75 s** |
| Host | Apple M-series, 14 cores |
| Target (4-core / 4 GB edge gateway) | < 30 s ✓ |

Reproduce locally:

```bash
uv run python scripts/benchmark_rescore.py --vessels 5000
```
