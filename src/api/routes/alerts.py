"""Server-Sent Events endpoint for real-time confidence alerts."""

from __future__ import annotations

import asyncio
import json
import os

import polars as pl
from fastapi import APIRouter
from fastapi.responses import StreamingResponse

DEFAULT_WATCHLIST_PATH = os.getenv("WATCHLIST_OUTPUT_PATH", "data/processed/candidate_watchlist.parquet")
ALERT_THRESHOLD = float(os.getenv("ALERT_CONFIDENCE_THRESHOLD", "0.75"))
POLL_INTERVAL_SECONDS = int(os.getenv("ALERT_POLL_INTERVAL", "60"))

router = APIRouter()


async def _event_stream():
    """Poll watchlist every POLL_INTERVAL_SECONDS and emit alerts for high-confidence vessels."""
    seen: set[str] = set()
    while True:
        try:
            path = DEFAULT_WATCHLIST_PATH
            if os.path.exists(path):
                df = pl.read_parquet(path)
                if not df.is_empty():
                    high = df.filter(pl.col("confidence") >= ALERT_THRESHOLD).with_columns(
                        pl.col("last_seen").cast(pl.Utf8)
                    )
                    for row in high.to_dicts():
                        key = f"{row['mmsi']}:{row.get('last_seen', '')}"
                        if key not in seen:
                            seen.add(key)
                            payload = json.dumps({
                                "mmsi": row["mmsi"],
                                "vessel_name": row["vessel_name"],
                                "confidence": row["confidence"],
                                "flag": row["flag"],
                            })
                            yield f"data: {payload}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'error': str(exc)})}\n\n"

        # heartbeat to keep connection alive
        yield ": heartbeat\n\n"
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


@router.get("/api/alerts/stream")
def alerts_stream() -> StreamingResponse:
    return StreamingResponse(
        _event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
