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

from pipeline.src.analysis.causal import score_unknown_unknowns
from pipeline.src.api.db import get_conn
from pipeline.src.api.llm import get_llm_client
from pipeline.src.ingest.gdelt import DEFAULT_LANCE_PATH, query_gdelt_context
from pipeline.src.storage.config import _canonical_data_dir, output_uri, watchlist_uri
from pipeline.src.storage.config import read_parquet as read_parquet_uri

_DEFAULT_DB_PATH = str(Path(_canonical_data_dir()) / "singapore.duckdb")
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

_DISPATCH_SYSTEM_TEMPLATE = """\
You are a maritime patrol officer producing a verbal dispatch brief to relay to a commander. \
Output exactly one paragraph in the following structure — no headings, no bullet points, no markdown:

"Vessel [VESSEL_NAME] (MMSI [MMSI]) is recommended for priority dispatch. \
It went dark [DARK_COUNT] times in the past 30 days, is [HOP_DISTANCE] ownership hop(s) from a designated entity, \
and changed flag [FLAG_CHANGES] time(s) in the past 2 years. \
Causal analysis confirms its evasion behaviour began within the window of a sanction event \
(ATT = [ATT_ESTIMATE], p < [P_VALUE]) — this is not coincidental route variation. \
Confidence score: [CONFIDENCE_SCORE]."

Replace the bracketed placeholders with the values below. Do not add any text outside this format.

VESSEL DATA:
Name: {vessel_name} | MMSI: {mmsi}
AIS dark periods (30d): {dark_count}
Ownership hops to sanctioned entity: {hop_distance}
Flag changes (2y): {flag_changes}
ATT estimate: {att_estimate} | p-value: {p_value}
Confidence score: {confidence:.2f}"""

_DISPATCH_USER_TEMPLATE = (
    "Generate the officer-to-commander dispatch brief for {vessel_name} (MMSI {mmsi})."
)


def _load_vessel(mmsi: str) -> dict | None:
    df = read_parquet_uri(watchlist_uri())
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
    path = watchlist_uri()
    try:
        return str(int(Path(path).stat().st_mtime))
    except OSError:
        return "0"


def _read_cached_brief(
    mmsi: str, version: str, table: str = "analyst_briefs", db_path: str | None = None
) -> str | None:
    try:
        with get_conn() as con:
            if con is None:
                return None
            rows = con.execute(
                f"SELECT brief FROM {table} WHERE mmsi = ? AND watchlist_version = ?",
                [mmsi, version],
            ).fetchall()
            return rows[0][0] if rows else None
    except Exception:
        return None


def _write_cached_brief(
    mmsi: str, version: str, brief: str, table: str = "analyst_briefs", db_path: str | None = None
) -> None:
    try:
        with get_conn() as con:
            if con is None:
                return
            con.execute(
                f"""
                INSERT OR REPLACE INTO {table} (mmsi, watchlist_version, brief)
                VALUES (?, ?, ?)
                """,
                [mmsi, version, brief],
            )
    except Exception:
        logger.exception(
            "Failed to write cached brief to DuckDB (table=%s, mmsi=%s, version=%s)",
            table,
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


def _extract_dispatch_fields(vessel: dict) -> dict:
    """Extract numeric fields needed for the officer-to-commander dispatch brief."""
    dark_count = int(vessel.get("ais_gap_count_30d") or 0)
    flag_changes = int(vessel.get("flag_changes_2y") or 0)

    # sanctions_distance is the shortest ownership path to a sanctioned entity (0 = direct match)
    raw_dist = vessel.get("sanctions_distance")
    if raw_dist is None:
        hop_distance = "unknown"
    else:
        hops = int(raw_dist)
        hop_distance = str(hops) if hops > 0 else "direct"

    # Pull the strongest significant causal regime
    att_estimate: float | None = None
    p_value: float | None = None
    effects_df = read_parquet_uri(
        os.getenv("CAUSAL_EFFECTS_OUTPUT_PATH") or output_uri("causal_effects.parquet")
    )
    if effects_df is not None and not effects_df.is_empty():
        import polars as _pl

        sig = effects_df.filter(_pl.col("is_significant")).sort("att_estimate", descending=True)
        if not sig.is_empty():
            er = sig.row(0, named=True)
            att_estimate = float(er.get("att_estimate", 0.0))
            p_value = float(er.get("p_value", 1.0))

    att_str = f"{att_estimate:+.2f}" if att_estimate is not None else "n/a"
    p_str = f"{p_value:.4f}" if p_value is not None else "n/a"

    return {
        "dark_count": dark_count,
        "hop_distance": hop_distance,
        "flag_changes": flag_changes,
        "att_estimate": att_str,
        "p_value": p_str,
    }


@router.get("/api/briefs/{mmsi}/dispatch")
async def vessel_dispatch_brief_stream(mmsi: str) -> StreamingResponse:
    """Stream an officer-to-commander dispatch brief for a vessel.

    Produces a single paragraph in a fixed verbal format suitable for relaying
    directly from a patrol officer to a commander without referencing raw data.
    Returns SSE lines of the form ``data: <token>\\n\\n``.
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
    cached = _read_cached_brief(mmsi, version, table="dispatch_briefs")
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

    dispatch = _extract_dispatch_fields(vessel)
    vessel_name = str(vessel.get("vessel_name") or vessel.get("mmsi", ""))

    system = _DISPATCH_SYSTEM_TEMPLATE.format(
        vessel_name=vessel_name,
        mmsi=vessel.get("mmsi", ""),
        dark_count=dispatch["dark_count"],
        hop_distance=dispatch["hop_distance"],
        flag_changes=dispatch["flag_changes"],
        att_estimate=dispatch["att_estimate"],
        p_value=dispatch["p_value"],
        confidence=float(vessel.get("confidence", 0)),
    )
    user = _DISPATCH_USER_TEMPLATE.format(
        vessel_name=vessel_name,
        mmsi=vessel.get("mmsi", ""),
    )

    async def _stream():
        tokens: list[str] = []
        try:
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
                _write_cached_brief(mmsi, version, full, table="dispatch_briefs")

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


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
    cached = _read_cached_brief(mmsi, version, table="analyst_briefs")
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
                _write_cached_brief(mmsi, version, full, table="analyst_briefs")

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
    cached = _read_cached_brief(mmsi, version, table="analyst_briefs")
    if cached is None:
        return JSONResponse({"available": False, "mmsi": mmsi})
    return JSONResponse({"available": True, "mmsi": mmsi, "brief": cached})


@router.get("/api/briefs/{mmsi}/dispatch/cached")
def vessel_dispatch_brief_cached(mmsi: str) -> JSONResponse:
    """Return the cached officer-to-commander dispatch brief for a vessel, if available."""
    version = _watchlist_version()
    cached = _read_cached_brief(mmsi, version, table="dispatch_briefs")
    if cached is None:
        return JSONResponse({"available": False, "mmsi": mmsi})
    return JSONResponse({"available": True, "mmsi": mmsi, "brief": cached})
