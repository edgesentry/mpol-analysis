# Deployment

## Platform quick-reference

| Platform | Recommended path | LLM inference |
|---|---|---|
| Windows | Docker | CPU (works out of the box) |
| Linux | Docker or native (`run_app.sh`) | CPU (Docker) or NVIDIA CUDA (native) |
| macOS (Apple Silicon) | Docker or native (`run_app.sh`) | CPU (Docker) or Metal GPU (native) |

---

## Docker quickstart — Windows, Linux, macOS (zero prerequisites)

The fastest path. No Python, no uv, no repo clone required.

```bash
docker run --name arktrace -p 8000:8000 \
  -v arktrace-data:/root/.arktrace/data \
  ghcr.io/edgesentry/arktrace:latest
```

Open **http://localhost:8000**. Demo data is pulled from R2 automatically on first run. The named volume `arktrace-data` persists data across restarts.

To change region:

```bash
docker run -p 8000:8000 \
  -v arktrace-data:/root/.arktrace/data \
  -e ARKTRACE_REGION=japan \
  ghcr.io/edgesentry/arktrace:latest
```

### Enable analyst briefs (optional)

Analyst briefs are disabled by default in Docker (CPU inference is slower and the model is large). Two options:

**Option A — Anthropic API (recommended for Docker):**

```bash
docker run -p 8000:8000 \
  -v arktrace-data:/root/.arktrace/data \
  -e LLM_PROVIDER=anthropic \
  -e LLM_API_KEY=sk-ant-... \
  -e LLM_MODEL=claude-haiku-4-5-20251001 \
  ghcr.io/edgesentry/arktrace:latest
```

**Option B — Native with GPU (macOS Metal or Linux CUDA):**

The Docker image does not include `llama-server`. For local model inference with GPU acceleration, use the native path:

```bash
bash scripts/run_app.sh   # macOS: Metal GPU; Linux: CUDA if llama.cpp built with CUDA
```

**Linux with NVIDIA GPU (Docker):**

```bash
docker run --gpus all -p 8000:8000 \
  -v "$(pwd)/models:/models:ro" \
  -e LLM_MODEL=Qwen2.5-7B-Instruct-Q4_K_M.gguf \
  ghcr.io/edgesentry/arktrace:latest
```

### Update to a new version

```bash
docker pull ghcr.io/edgesentry/arktrace:latest
```

Then stop the running container (Ctrl+C or `docker stop arktrace`) and re-run the same `docker run` command. The `arktrace-data` volume is preserved — no data is lost on update.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `ARKTRACE_REGION` | `singapore` | Region to serve |
| `AUTO_PULL` | `1` | Set to `0` to disable automatic R2 data pull on startup |
| `LLM_PROVIDER` | _(unset — briefs disabled)_ | `anthropic` or `openai` (local llama-server) |
| `LLM_API_KEY` | _(unset)_ | API key for Anthropic or remote OpenAI-compatible endpoint |
| `LLM_MODEL` | `Qwen2.5-7B-Instruct-Q4_K_M.gguf` | GGUF filename in `/models` or Anthropic model ID |

---

## Native — all platforms (developer path)

For developers running from a cloned repo with native GPU acceleration.

### Prerequisites

- Python 3.12+ and [uv](https://docs.astral.sh/uv/getting-started/installation/)
- llama.cpp (optional — for analyst briefs):

| Platform | Install |
|---|---|
| macOS | `brew install llama.cpp` |
| Linux | Download binary from [github.com/ggml-org/llama.cpp/releases/latest](https://github.com/ggml-org/llama.cpp/releases/latest) (`llama-<tag>-bin-ubuntu-x64.zip`) and add to `PATH` |
| Windows | Use Docker (recommended). Or download the Windows binary (`llama-<tag>-bin-win-avx2-x64.zip`) and run via Git Bash or WSL2 |

### Start

```bash
git clone https://github.com/edgesentry/arktrace && cd arktrace
bash scripts/run_app.sh                    # Singapore (default)
bash scripts/run_app.sh --region japan     # different region
bash scripts/run_app.sh --no-llm           # skip llama-server (briefs disabled)
bash scripts/run_app.sh --provider anthropic  # use Anthropic API for briefs
```

Data is pulled from R2 automatically if not already present. Press **Ctrl+C** to stop everything (uvicorn + llama-server).

See [docs/local-llm-setup.md](local-llm-setup.md) for model selection and LLM provider options.

---

## Docker image — published to GHCR

The multi-arch image (`linux/amd64` + `linux/arm64`) is published automatically:

| Tag | When |
|---|---|
| `ghcr.io/edgesentry/arktrace:latest` | Every push to `main` |
| `ghcr.io/edgesentry/arktrace:v1.2.3` | Release tags |

Source: `.github/workflows/docker-publish.yml`.

---

## Local — single command (Docker Compose)

The fastest way to run the dashboard locally without touching Python:

```bash
docker compose up
```

This starts:

| Container | Port | Purpose |
|---|---|---|
| `mpol-minio` | 9000 / 9001 | MinIO S3-compatible object store (API / console) |
| `mpol-dashboard` | 8000 | FastAPI + HTMX dashboard |

MinIO is pre-configured with a bucket named `arktrace`. The pipeline and dashboard write Lance Graph datasets, LanceDB, and output Parquet files to it. Open the MinIO console at `http://localhost:9001` (user: `minioadmin` / password: `minioadmin`) to browse stored objects.

Open `http://localhost:8000`.

The `data/` directory is bind-mounted for the DuckDB working file only. All derived artifacts (Parquet outputs, graph datasets, GDELT vector store) go to MinIO.

Stop everything:

```bash
docker compose down
```

---

## Local — uv only (no Docker for the app)

If you are running the scoring pipeline on your host and only want the dashboard process:

```bash
# 1. Run the ingestion + scoring pipeline (Steps 1–5 from local-e2e-test.md)

# 2. Start the dashboard
uv run uvicorn src.api.main:app --reload
```

Open `http://localhost:8000`. The `--reload` flag watches `src/` for code changes.

Environment variables (all optional — defaults shown):

| Variable | Default | Description |
|---|---|---|
| `WATCHLIST_OUTPUT_PATH` | `data/processed/candidate_watchlist.parquet` | Watchlist input |
| `VALIDATION_METRICS_PATH` | `data/processed/validation_metrics.json` | Metrics input |
| `ALERT_CONFIDENCE_THRESHOLD` | `0.75` | Confidence level that triggers SSE toast |
| `ALERT_POLL_INTERVAL` | `60` | Seconds between watchlist polls for alerts |

### Object storage (S3)

When `S3_BUCKET` is set, all derived artifacts are written to S3-compatible storage instead of local disk. Omit `S3_BUCKET` to use local paths (default).

| Variable | Local default | Description |
|---|---|---|
| `S3_BUCKET` | _(unset — local mode)_ | Bucket name; setting this enables S3 mode |
| `S3_ENDPOINT` | — | Custom endpoint URL for MinIO or R2 (omit for real AWS S3) |
| `AWS_ACCESS_KEY_ID` | — | Access key |
| `AWS_SECRET_ACCESS_KEY` | — | Secret key |
| `AWS_REGION` | `us-east-1` | Region (ignored for MinIO) |

What goes to S3: Lance Graph datasets (`<region>_graph/`), LanceDB GDELT store (`gdelt.lance`), output Parquet files (`processed/`). The DuckDB `.duckdb` working file always stays local.

---

## Cloud — single VM (e.g. AWS EC2, GCP Compute Engine, DigitalOcean Droplet)

Minimal setup for a single-node port operations deployment.

### 1. Provision the VM

Recommended spec: **4 vCPU / 8 GB RAM / 50 GB SSD**, Ubuntu 24.04 LTS.

Open inbound ports: `22` (SSH), `80` (HTTP), `443` (HTTPS if using TLS).

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
# Edit .env — set AISSTREAM_API_KEY, LLM_API_KEY, and S3 credentials
```

For production, point to a real S3 bucket instead of MinIO:

```env
S3_BUCKET=your-bucket-name
# S3_ENDPOINT=               # leave blank for AWS S3
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
AWS_REGION=ap-southeast-1
```

### 4. Build and start

```bash
docker compose up -d
```

The dashboard is now reachable on port 8000. To expose it on port 80 via nginx:

```bash
sudo apt install nginx -y
sudo tee /etc/nginx/sites-available/mpol <<'EOF'
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://localhost:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        # Required for SSE (disable buffering)
        proxy_buffering off;
        proxy_cache off;
    }
}
EOF
sudo ln -sf /etc/nginx/sites-available/mpol /etc/nginx/sites-enabled/mpol
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx
```

> **SSE note:** `proxy_buffering off` is required for the `/api/alerts/stream` endpoint. Without it, nginx will buffer the event stream and toasts will not appear in real time.

### 5. Run the pipeline

The scoring pipeline runs on the host (outside Docker), writing into the bind-mounted `data/` directory:

```bash
cd arktrace
uv sync
uv run python src/ingest/schema.py
uv run python src/ingest/ais_stream.py &    # background ingestion
# ... remaining pipeline steps (see local-e2e-test.md)
```

The dashboard container reads from `data/processed/` via the bind mount and reflects updates within one poll interval (default 60 s).

### 6. Automate re-scoring (optional)

**Preferred — built-in cadence loop:**

```bash
uv run python scripts/run_pipeline.py --region strait --non-interactive --cadence 900
```

This runs the full initial pipeline once, then loops `step_features` + `step_score` every 900 seconds (15 minutes). Press Ctrl-C to stop.

**Alternative — cron (if you need OS-level scheduling):**

```bash
crontab -e
# Add:
*/15 * * * * cd /home/ubuntu/arktrace && uv run python src/score/watchlist.py >> /var/log/mpol-score.log 2>&1
```

---

## AIS providers

arktrace is AIS-provider agnostic. Three ingestion paths are available — switch or combine them without code changes.

### Live WebSocket (aisstream.io)

Default for real-time feeds. Requires `AISSTREAM_API_KEY` in `.env`.

```bash
uv run python src/ingest/ais_stream.py --bbox -5 92 22 122
```

### CSV file (any provider)

Accepts any CSV with configurable column mapping. Default layout matches MarineCadastre (NOAA):

```bash
# MarineCadastre / NOAA (default column names):
uv run python src/ingest/ais_csv.py --file data/raw/ais_2024.csv

# Spire / exactEarth / Orbcomm — map provider columns to internal schema:
uv run python src/ingest/ais_csv.py --file spire_feed.csv \
    --column-map mmsi=vessel_id,lat=latitude,lon=longitude,timestamp=time_utc,sog=speed,cog=course

# With bounding-box filter (lat_min lon_min lat_max lon_max):
uv run python src/ingest/ais_csv.py --file feed.csv --bbox -5 92 22 122
```

**Default CSV column mapping (MarineCadastre layout):**

| Internal field | Default provider column |
|---|---|
| `mmsi` | `MMSI` |
| `timestamp` | `BaseDateTime` |
| `lat` | `LAT` |
| `lon` | `LON` |
| `sog` | `SOG` |
| `cog` | `COG` |
| `nav_status` | `Status` |
| `ship_type` | `VesselType` |

Override any field with `--column-map key=ProviderColumnName,...`. Unspecified fields use the defaults above.

### NMEA 0183 sentence file (S-AIS raw feed)

Parses VDM/VDO sentences — AIS message types 1, 2, 3 (Class A) and 18 (Class B). Multi-part sentences are assembled automatically.

```bash
# NMEA file from any S-AIS provider:
uv run python src/ingest/ais_csv.py --file feed.nmea --nmea

# With bounding-box filter:
uv run python src/ingest/ais_csv.py --file feed.nmea --nmea --bbox -5 92 22 122
```

All three paths write to the same `ais_positions` table in DuckDB, so downstream feature engineering and scoring are unaffected by which provider supplies the data.

---

## Edge gateway benchmark

The incremental re-score pipeline (feature matrix + HDBSCAN + Isolation Forest + SHAP + watchlist output) is optimised for low-power environments.

### Measured result

| Metric | Value |
|---|---|
| Vessels | 5,000 |
| Feature matrix (`build_matrix.py`) | 0.28 s |
| Composite scoring (`composite.py`) | 5.46 s |
| Watchlist output (`watchlist.py`) | 0.01 s |
| **Pipeline total** | **5.75 s** |
| Host | Apple M-series, 14 cores |
| Target (4-core / 4 GB edge gateway) | < 30 s ✓ |

### Reproduce locally

```bash
uv run python scripts/benchmark_rescore.py --vessels 5000
```

### Reproduce with hardware constraints (Docker)

Simulates a 4-core / 4 GB Raspberry Pi 4 or NVIDIA Jetson Nano:

```bash
docker run --rm --cpus 4 --memory 4g \
    -v $(pwd):/app -w /app \
    ghcr.io/edgesentry/mpol-dashboard:latest \
    uv run python scripts/benchmark_rescore.py --vessels 5000
```

The benchmark seeds a temporary DuckDB with synthetic AIS data, runs the full pipeline, and prints a pass/fail result against the 30-second target. Seeding time (~46 s for 50,000 AIS fixes) is excluded from the pipeline measurement.

---

## Cloud — container registry (CI/CD path)

Build and push the dashboard image to a registry, then deploy on any container platform (ECS, Cloud Run, Fly.io, etc.).

```bash
docker build -t ghcr.io/edgesentry/mpol-dashboard:latest .
docker push ghcr.io/edgesentry/mpol-dashboard:latest
```

The image exposes port `8000`. Pass S3 credentials as environment variables so the dashboard reads Parquet outputs and Lance datasets from the shared bucket — no volume mount required:

```bash
docker run -p 8000:8000 \
  -e S3_BUCKET=your-bucket \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  -e AWS_REGION=ap-southeast-1 \
  ghcr.io/edgesentry/mpol-dashboard:latest
```
