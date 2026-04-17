# syntax=docker/dockerfile:1
#
# Multi-stage build:
#   builder  — installs Python deps (compiles Rust/lance-graph)
#   runtime  — lean final image: Python app
#
# LLM inference in Docker:
#   Option A (recommended): set LLM_PROVIDER=anthropic + LLM_API_KEY
#   Option B: mount a GGUF model volume and install llama-server on the host,
#             or use native run_app.sh for Metal/CUDA GPU acceleration.

# ── builder: Python deps (Rust/maturin for lance-graph) ───────────────────────
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
ENV CARGO_BUILD_JOBS=2

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --frozen

# ── runtime: lean final image ─────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Python virtualenv from builder
COPY --from=builder /app/.venv /app/.venv

# Application source
COPY pipeline/ ./pipeline/
COPY docker/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENV PATH="/app/.venv/bin:${PATH}"

# Data dir inside the container — override with ARKTRACE_DATA_DIR
ENV ARKTRACE_DATA_DIR=/root/.arktrace/data

# Model volume mount point — mount a GGUF model here to enable analyst briefs
VOLUME ["/models"]

EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]
