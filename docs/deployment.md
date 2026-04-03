# Deployment

## Local — single command (Docker Compose)

The fastest way to run the dashboard locally without touching Python:

```bash
docker compose up
```

This starts:

| Container | Port | Purpose |
|---|---|---|
| `mpol-dashboard` | 8000 | FastAPI + HTMX dashboard |

Open `http://localhost:8000`.

The `data/` directory is bind-mounted into the container, so the scoring pipeline (`uv run python src/score/watchlist.py`) can still be run on the host and the dashboard will pick up changes immediately.

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
# Edit .env — set AISSTREAM_API_KEY and LLM_API_KEY
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

The image exposes port `8000` and reads `WATCHLIST_OUTPUT_PATH` / `VALIDATION_METRICS_PATH` from environment variables. Mount the processed data directory as a volume or copy the parquet/json files into the image at build time for a fully self-contained read-only demo.
