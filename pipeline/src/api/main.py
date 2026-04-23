"""Pipeline API server.

Exposes endpoints consumed by the arktrace SPA.

Start with:
    uv run uvicorn pipeline.src.api.main:app --host 0.0.0.0 --port 8000 --reload

Environment variables:
    DB_PATH  Path to the DuckDB file (default: data/processed/singapore.duckdb).
"""

from __future__ import annotations

from fastapi import FastAPI


def create_app() -> FastAPI:
    app = FastAPI(title="arktrace pipeline API", version="0.1.0", docs_url="/api/docs")
    return app


app = create_app()
