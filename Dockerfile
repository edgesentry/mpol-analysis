FROM python:3.12-slim

WORKDIR /app

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install production dependencies (no dev extras)
RUN uv sync --no-dev --frozen

# Copy source
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY data/ ./data/

ENV WATCHLIST_OUTPUT_PATH=data/processed/candidate_watchlist.parquet
ENV VALIDATION_METRICS_PATH=data/processed/validation_metrics.json

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
