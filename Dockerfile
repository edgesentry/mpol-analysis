# syntax=docker/dockerfile:1

# ── builder: compiles Rust/maturin packages (lance-graph) ─────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    curl \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    libprotobuf-dev \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:${PATH}"
# Limit cargo parallelism to avoid OOM during Rust release builds.
# deltalake-core and similar crates are very memory-hungry at opt-level=3.
ENV CARGO_BUILD_JOBS=2

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./

# Cache mounts keep cargo registry and uv wheel cache across builds,
# so Rust only recompiles when lance-graph itself changes.
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --frozen && \
    uv pip install --no-cache "llama-cpp-python>=0.3" "huggingface-hub>=0.24"

# ── runtime: lean image without build tools ────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# Copy the pre-built virtualenv from the builder
COPY --from=builder /app/.venv /app/.venv

# uv is needed at runtime because docker-compose commands use `uv run`
RUN pip install --no-cache-dir uv

COPY src/ ./src/
COPY scripts/ ./scripts/

ENV PATH="/app/.venv/bin:${PATH}"
ENV WATCHLIST_OUTPUT_PATH=data/processed/candidate_watchlist.parquet
ENV VALIDATION_METRICS_PATH=data/processed/validation_metrics.json

EXPOSE 8000

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
