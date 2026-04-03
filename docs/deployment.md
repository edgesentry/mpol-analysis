# Deployment

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

Add a cron job to re-score every 15 minutes:

```bash
crontab -e
# Add:
*/15 * * * * cd /home/ubuntu/arktrace && uv run python src/score/watchlist.py >> /var/log/mpol-score.log 2>&1
```

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
