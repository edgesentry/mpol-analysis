"""FastAPI application factory for the MPOL watchlist dashboard."""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from pipeline.src.api.routes.alerts import router as alerts_router
from pipeline.src.api.routes.briefs import router as briefs_router
from pipeline.src.api.routes.chat import router as chat_router
from pipeline.src.api.routes.reviews import router as reviews_router
from pipeline.src.api.routes.vessels import router as vessels_router
from pipeline.src.ingest.schema import DEFAULT_DB_PATH, init_schema
from pipeline.src.storage.bootstrap import maybe_pull

_TEMPLATE_DIR = Path(__file__).parent.parent / "viz" / "templates"


def create_app() -> FastAPI:
    app = FastAPI(title="MPOL Watchlist", version="0.1.0", docs_url="/api/docs")

    @app.on_event("startup")
    def _init_db() -> None:
        maybe_pull()
        db_path = os.getenv("DB_PATH", DEFAULT_DB_PATH)
        init_schema(db_path)

    app.include_router(vessels_router)
    app.include_router(alerts_router)
    app.include_router(briefs_router)
    app.include_router(chat_router)
    app.include_router(reviews_router)

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        html = (_TEMPLATE_DIR / "index.html").read_text()
        return HTMLResponse(html)

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("pipeline.src.api.main:app", host="0.0.0.0", port=8000, reload=True)
