"""Analyst brief generation endpoint — streaming LLM briefs with GDELT context."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import polars as pl
from fastapi import APIRouter
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from src.analysis.causal import score_unknown_unknowns
from src.api.db import get_conn
from src.api.llm import get_llm_client
from src.ingest.gdelt import DEFAULT_LANCE_PATH, query_gdelt_context
from src.storage.config import output_uri
from src.storage.config import read_parquet as read_parquet_uri

DEFAULT_WATCHLIST_PATH = os.getenv("WATCHLIST_OUTPUT_PATH") or output_uri(
    "candidate_watchlist.parquet"
)
_DEFAULT_DB_PATH = "data/processed/mpol.duckdb"
BRIEF_CONFIDENCE_THRESHOLD = float(os.getenv("BRIEF_CONFIDENCE_THRESHOLD", "0.7"))

logger = logging.getLogger(__name__)

router = APIRouter()

_SYSTEM_TEMPLATE = """\
You are a maritime intelligence analyst specializing in shadow fleet vessel detection. \
Produce a concise one-paragraph analyst brief for the vessel below, citing at least one \
of the geopolitical events listed. Be specific: name the event, its date, and explain \
how it connects to the vessel's risk profile.

VESSEL PROFILE:
Name: {vessel_name} | MMSI: {mmsi} | IMO: {imo}
Flag: {flag} | Type: {vessel_type}
Confidence Score: {confidence:.2f}

TOP RISK SIGNALS:
{signals_text}

RECENT GEOPOLITICAL CONTEXT:
{gdelt_text}
{causal_context}"""

_USER_TEMPLATE = (
    "Write a one-paragraph analyst brief for {vessel_name} (MMSI {mmsi}). "
    "Cite at least one geopolitical event above."
)


def _load_vessel(mmsi: str) -> dict | None:
    df = read_parquet_uri(DEFAULT_WATCHLIST_PATH)
    if df is None:
        return None
    df = df.filter(pl.col("mmsi") == mmsi)
    if df.is_empty():
        return None
    return df.row(0, named=True)


def _format_signals(top_signals_json: str | None) -> str:
    if not top_signals_json:
        return "No signal data available."
    try:
        signals = json.loads(top_signals_json)
        lines = []
        for s in signals[:3]:
            feature = s.get("feature", "?")
            value = s.get("value", "?")
            contrib = s.get("contribution", 0)
            lines.append(f"  • {feature}: {value} (contribution {contrib:.2f})")
        return "\n".join(lines) or "No signals."
    except Exception:
        return str(top_signals_json)[:200]


def _fetch_causal_context(mmsi: str, db_path: str | None = None) -> str:
    """Return causal evidence prompt context for a vessel, or empty string."""
    if db_path is None:
        db_path = os.getenv("DB_PATH", _DEFAULT_DB_PATH)
    if not os.path.exists(db_path):
        return ""
    try:
        candidates = score_unknown_unknowns(db_path=db_path)
        for candidate in candidates:
            if candidate.mmsi == mmsi:
                ctx = candidate.prompt_context()
                return f"\n{ctx}\n" if ctx else ""
    except Exception:
        logger.debug("Causal context unavailable for mmsi=%s", mmsi)
    return ""


def _format_gdelt(events: list[dict]) -> str:
    if not events:
        return "No recent geopolitical events retrieved."
    lines = []
    for ev in events:
        date = ev.get("event_date", "")
        if len(date) == 8:
            date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        actor1 = ev.get("actor1_name", "?")
        actor2 = ev.get("actor2_name", "?")
        geo = ev.get("action_geo", "")
        url = ev.get("source_url", "")
        lines.append(f"  • [{date}] {actor1} → {actor2} in {geo}. {url}")
    return "\n".join(lines)


def _watchlist_version() -> str:
    """Return a cache key based on watchlist file modification time."""
    path = DEFAULT_WATCHLIST_PATH
    try:
        return str(int(Path(path).stat().st_mtime))
    except OSError:
        return "0"


def _read_cached_brief(mmsi: str, version: str, db_path: str | None = None) -> str | None:
    try:
        with get_conn() as con:
            if con is None:
                return None
            rows = con.execute(
                "SELECT brief FROM analyst_briefs WHERE mmsi = ? AND watchlist_version = ?",
                [mmsi, version],
            ).fetchall()
            return rows[0][0] if rows else None
    except Exception:
        return None


def _write_cached_brief(mmsi: str, version: str, brief: str, db_path: str | None = None) -> None:
    try:
        with get_conn() as con:
            if con is None:
                return
            con.execute(
                """
                INSERT OR REPLACE INTO analyst_briefs (mmsi, watchlist_version, brief)
                VALUES (?, ?, ?)
                """,
                [mmsi, version, brief],
            )
    except Exception:
        logger.exception(
            "Failed to write cached brief to DuckDB (mmsi=%s, version=%s)",
            mmsi,
            version,
        )


async def _generate_brief_tokens(vessel: dict) -> list[str]:
    """Generate brief tokens and return them for caching."""
    flag = str(vessel.get("flag") or "")
    vessel_name = str(vessel.get("vessel_name") or vessel.get("mmsi", ""))

    gdelt_events = query_gdelt_context(
        flag_country=flag,
        vessel_name=vessel_name,
        n=3,
        lance_path=DEFAULT_LANCE_PATH,
    )

    system = _SYSTEM_TEMPLATE.format(
        vessel_name=vessel_name,
        mmsi=vessel.get("mmsi", ""),
        imo=vessel.get("imo", ""),
        flag=flag,
        vessel_type=vessel.get("vessel_type", "Unknown"),
        confidence=float(vessel.get("confidence", 0)),
        signals_text=_format_signals(vessel.get("top_signals")),
        gdelt_text=_format_gdelt(gdelt_events),
        causal_context=_fetch_causal_context(vessel.get("mmsi", "")),
    )
    user = _USER_TEMPLATE.format(
        vessel_name=vessel_name,
        mmsi=vessel.get("mmsi", ""),
    )

    llm = get_llm_client()
    tokens: list[str] = []
    async for token in llm.chat(system, user):
        tokens.append(token)
    return tokens


@router.get("/api/briefs/{mmsi}")
async def vessel_brief(mmsi: str) -> StreamingResponse:
    """Stream an analyst brief for a vessel as server-sent events.

    Returns SSE lines of the form ``data: <token>\\n\\n``.
    Sends ``data: [DONE]\\n\\n`` when complete.
    Caches the completed brief in DuckDB keyed on (mmsi, watchlist_version).
    """
    vessel = _load_vessel(mmsi)
    if vessel is None:

        async def _not_found():
            yield "data: Vessel not found in watchlist.\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(
            _not_found(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    version = _watchlist_version()
    cached = _read_cached_brief(mmsi, version)
    if cached:

        async def _cached_stream():
            # Stream cached brief word by word so the UI sees progressive output
            words = cached.split(" ")
            for i, word in enumerate(words):
                chunk = word if i == 0 else " " + word
                yield f"data: {chunk}\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(
            _cached_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    async def _stream():
        tokens: list[str] = []
        try:
            flag = str(vessel.get("flag") or "")
            vessel_name = str(vessel.get("vessel_name") or vessel.get("mmsi", ""))

            gdelt_events = query_gdelt_context(
                flag_country=flag,
                vessel_name=vessel_name,
                n=3,
                lance_path=DEFAULT_LANCE_PATH,
            )

            system = _SYSTEM_TEMPLATE.format(
                vessel_name=vessel_name,
                mmsi=vessel.get("mmsi", ""),
                imo=vessel.get("imo", ""),
                flag=flag,
                vessel_type=vessel.get("vessel_type", "Unknown"),
                confidence=float(vessel.get("confidence", 0)),
                signals_text=_format_signals(vessel.get("top_signals")),
                gdelt_text=_format_gdelt(gdelt_events),
                causal_context=_fetch_causal_context(vessel.get("mmsi", "")),
            )
            user = _USER_TEMPLATE.format(
                vessel_name=vessel_name,
                mmsi=vessel.get("mmsi", ""),
            )

            llm = get_llm_client()
            async for token in llm.chat(system, user):
                tokens.append(token)
                yield f"data: {token}\n\n"
        except Exception as exc:
            yield f"data: Brief unavailable — {exc}\n\n"
        finally:
            yield "data: [DONE]\n\n"
            full = "".join(tokens)
            if (
                full
                and not full.startswith("LLM not configured")
                and not full.startswith("Brief unavailable")
            ):
                _write_cached_brief(mmsi, version, full)

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class _ChatMsg(BaseModel):
    role: str
    content: str


class _ChatRequest(BaseModel):
    messages: list[_ChatMsg]


def _build_vessel_system(vessel: dict) -> str:
    flag = str(vessel.get("flag") or "")
    vessel_name = str(vessel.get("vessel_name") or vessel.get("mmsi", ""))
    gdelt_events = query_gdelt_context(
        flag_country=flag,
        vessel_name=vessel_name,
        n=3,
        lance_path=DEFAULT_LANCE_PATH,
    )
    return _SYSTEM_TEMPLATE.format(
        vessel_name=vessel_name,
        mmsi=vessel.get("mmsi", ""),
        imo=vessel.get("imo", ""),
        flag=flag,
        vessel_type=vessel.get("vessel_type", "Unknown"),
        confidence=float(vessel.get("confidence", 0)),
        signals_text=_format_signals(vessel.get("top_signals")),
        gdelt_text=_format_gdelt(gdelt_events),
        causal_context=_fetch_causal_context(vessel.get("mmsi", "")),
    )


@router.post("/api/briefs/{mmsi}/chat")
async def vessel_chat(mmsi: str, body: _ChatRequest) -> StreamingResponse:
    """Stream an LLM response for a multi-turn analyst conversation about a vessel."""
    vessel = _load_vessel(mmsi)

    async def _not_found():
        yield "data: Vessel not found in watchlist.\n\n"
        yield "data: [DONE]\n\n"

    if vessel is None:
        return StreamingResponse(
            _not_found(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    system = _build_vessel_system(vessel)
    messages = [{"role": m.role, "content": m.content} for m in body.messages]

    async def _stream():
        try:
            llm = get_llm_client()
            async for token in llm.stream_messages(system, messages):
                yield f"data: {token}\n\n"
        except Exception as exc:
            yield f"data: Brief unavailable — {exc}\n\n"
        finally:
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/briefs/{mmsi}/cached")
def vessel_brief_cached(mmsi: str) -> JSONResponse:
    """Return the cached analyst brief for a vessel, if available."""
    version = _watchlist_version()
    cached = _read_cached_brief(mmsi, version)
    if cached is None:
        return JSONResponse({"available": False, "mmsi": mmsi})
    return JSONResponse({"available": True, "mmsi": mmsi, "brief": cached})
