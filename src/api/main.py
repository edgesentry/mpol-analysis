"""FastAPI application factory for the MPOL watchlist dashboard."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from src.api.routes.alerts import router as alerts_router
from src.api.routes.briefs import router as briefs_router
from src.api.routes.vessels import router as vessels_router

_TEMPLATE_DIR = Path(__file__).parent.parent / "viz" / "templates"


def create_app() -> FastAPI:
    app = FastAPI(title="MPOL Watchlist", version="0.1.0", docs_url="/api/docs")

    app.include_router(vessels_router)
    app.include_router(alerts_router)
    app.include_router(briefs_router)

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        html = (_TEMPLATE_DIR / "index.html").read_text()
        return HTMLResponse(html)

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)
