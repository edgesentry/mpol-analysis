# ── builder: compiles Rust/maturin packages (lance-graph) ─────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    protobuf-compiler \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:${PATH}"

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./

# Export pinned requirements then install into /install so it can be copied cleanly
RUN uv export --no-dev --frozen --no-emit-project -o /tmp/requirements.txt \
    && pip install --no-cache-dir --prefix /install -r /tmp/requirements.txt

# ── runtime: lean image without build tools ────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# Copy the pre-built site-packages and scripts from the builder
COPY --from=builder /install /usr/local

# Copy source
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY data/ ./data/

ENV WATCHLIST_OUTPUT_PATH=data/processed/candidate_watchlist.parquet
ENV VALIDATION_METRICS_PATH=data/processed/validation_metrics.json

EXPOSE 8000

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
