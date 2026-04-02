"""Interactive analyst chat — POST /api/chat with DuckDB response caching.

Accepts a natural-language question, optional vessel MMSI, and multi-turn
history.  Builds an LLM context window from:
  - Vessel feature row + SHAP top_signals (if MMSI provided)
  - Neo4j 2-hop ownership subgraph (if MMSI provided, graceful fallback)
  - GDELT geopolitical events via RAG (if MMSI provided)
  - Fleet overview: top watchlist candidates (always, for cross-vessel Q&A)

Responses are cached in DuckDB keyed on (mmsi, question_hash, watchlist_version)
so repeated identical questions never re-call the LLM.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path

import duckdb
import polars as pl
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src.api.llm import get_llm_client
from src.ingest.gdelt import DEFAULT_LANCE_PATH, query_gdelt_context

DEFAULT_WATCHLIST_PATH = os.getenv("WATCHLIST_OUTPUT_PATH", "data/processed/candidate_watchlist.parquet")
_DEFAULT_DB_PATH = "data/processed/mpol.duckdb"

logger = logging.getLogger(__name__)
router = APIRouter()

# ── prompt templates ────────────────────────────────────────────────────────

_SYSTEM_TEMPLATE = """\
You are a maritime intelligence analyst specializing in shadow fleet vessel \
detection. Answer the analyst's questions using the data provided below. \
Cite specific field values, GDELT event IDs/dates, or ownership chain hops \
to ground every claim.

FLEET OVERVIEW — TOP WATCHLIST CANDIDATES:
{fleet_context}
{vessel_section}\
{ownership_section}\
{gdelt_section}\
"""

_VESSEL_SECTION = """\

VESSEL UNDER ANALYSIS:
Name: {vessel_name} | MMSI: {mmsi} | IMO: {imo}
Flag: {flag} | Type: {vessel_type} | Confidence: {confidence:.2f}

TOP RISK SIGNALS:
{signals_text}
"""

_OWNERSHIP_SECTION = """\

OWNERSHIP NETWORK (2-hop):
{ownership_text}
"""

_GDELT_SECTION = """\

RECENT GEOPOLITICAL CONTEXT:
{gdelt_text}
"""


# ── request / response models ───────────────────────────────────────────────

class _Msg(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str
    mmsi: str | None = None
    history: list[_Msg] = []


# ── helpers ─────────────────────────────────────────────────────────────────

def _watchlist_version() -> str:
    try:
        return str(int(Path(DEFAULT_WATCHLIST_PATH).stat().st_mtime))
    except OSError:
        return "0"


def _question_hash(message: str) -> str:
    return hashlib.sha256(message.lower().strip().encode()).hexdigest()[:16]


def _cache_key(mmsi: str | None, q_hash: str, version: str) -> str:
    return f"{mmsi or 'global'}:{q_hash}:{version}"


def _db_path() -> str:
    return os.getenv("DB_PATH", _DEFAULT_DB_PATH)


def _read_cache(key: str) -> str | None:
    path = _db_path()
    if not os.path.exists(path):
        return None
    try:
        con = duckdb.connect(path, read_only=True)
        rows = con.execute(
            "SELECT response FROM chat_cache WHERE cache_key = ?", [key]
        ).fetchall()
        con.close()
        return rows[0][0] if rows else None
    except Exception:
        return None


def _write_cache(key: str, mmsi: str | None, q_hash: str, version: str, response: str) -> None:
    path = _db_path()
    if not os.path.exists(path):
        return
    try:
        con = duckdb.connect(path)
        con.execute(
            """
            INSERT OR REPLACE INTO chat_cache
                (cache_key, mmsi, question_hash, watchlist_version, response)
            VALUES (?, ?, ?, ?, ?)
            """,
            [key, mmsi, q_hash, version, response],
        )
        con.close()
    except Exception:
        logger.exception("Failed to write chat_cache (key=%s)", key)


def _load_watchlist() -> pl.DataFrame:
    if not os.path.exists(DEFAULT_WATCHLIST_PATH):
        return pl.DataFrame()
    return pl.read_parquet(DEFAULT_WATCHLIST_PATH)


def _fleet_context(df: pl.DataFrame) -> str:
    if df.is_empty():
        return "  No watchlist data available."
    top = df.sort("confidence", descending=True).head(10)
    lines: list[str] = []
    for row in top.with_columns(pl.col("last_seen").cast(pl.Utf8)).to_dicts():
        try:
            sigs = json.loads(row.get("top_signals") or "[]")
            top_sig = sigs[0]["feature"] if sigs else "—"
        except Exception:
            top_sig = "—"
        lines.append(
            f"  • {row['vessel_name']} (MMSI {row['mmsi']}, flag {row['flag']}, "
            f"conf {row['confidence']:.2f}, top signal: {top_sig})"
        )
    return "\n".join(lines)


def _format_signals(top_signals_json: str | None) -> str:
    if not top_signals_json:
        return "  No signal data."
    try:
        sigs = json.loads(top_signals_json)
        return "\n".join(
            f"  • {s.get('feature','?')}: {s.get('value','?')} "
            f"(contribution {s.get('contribution', 0):.2f})"
            for s in sigs[:3]
        ) or "  No signals."
    except Exception:
        return str(top_signals_json)[:200]


def _format_gdelt(events: list[dict]) -> str:
    if not events:
        return "  No recent geopolitical events retrieved."
    lines: list[str] = []
    for ev in events:
        date = ev.get("event_date", "")
        if len(date) == 8:
            date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        lines.append(
            f"  • [{date}] {ev.get('actor1_name','?')} → {ev.get('actor2_name','?')} "
            f"in {ev.get('action_geo','')}. {ev.get('source_url','')}"
        )
    return "\n".join(lines)


def _query_neo4j_ownership(mmsi: str) -> str:
    """Return a text summary of the 2-hop ownership subgraph. Fails gracefully."""
    try:
        from neo4j import GraphDatabase  # optional dependency

        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "password")
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            rows = session.run(
                """
                MATCH (v:Vessel {mmsi: $mmsi})
                OPTIONAL MATCH (v)-[:OWNED_BY|MANAGED_BY*1..2]->(c:Company)
                OPTIONAL MATCH (c)-[:SANCTIONED_BY]->(s)
                RETURN c.name AS company, c.country AS country,
                       s.list_source AS sanction_list
                LIMIT 20
                """,
                mmsi=mmsi,
            ).data()
        driver.close()
        if not rows:
            return "  No ownership records in graph for this vessel."
        lines: list[str] = []
        for r in rows:
            sanction = f" [SANCTIONED: {r['sanction_list']}]" if r.get("sanction_list") else ""
            lines.append(f"  • {r.get('company') or '?'} ({r.get('country','?')}){sanction}")
        return "\n".join(lines)
    except Exception as exc:
        return f"  Ownership graph unavailable ({exc})."


def _build_system(vessel: dict | None, df: pl.DataFrame) -> str:
    fleet = _fleet_context(df)

    if vessel is None:
        return _SYSTEM_TEMPLATE.format(
            fleet_context=fleet,
            vessel_section="",
            ownership_section="",
            gdelt_section="",
        )

    flag = str(vessel.get("flag") or "")
    vessel_name = str(vessel.get("vessel_name") or vessel.get("mmsi", ""))
    mmsi = str(vessel.get("mmsi", ""))

    vessel_section = _VESSEL_SECTION.format(
        vessel_name=vessel_name,
        mmsi=mmsi,
        imo=vessel.get("imo", ""),
        flag=flag,
        vessel_type=vessel.get("vessel_type", "Unknown"),
        confidence=float(vessel.get("confidence", 0)),
        signals_text=_format_signals(vessel.get("top_signals")),
    )

    ownership_text = _query_neo4j_ownership(mmsi)
    ownership_section = _OWNERSHIP_SECTION.format(ownership_text=ownership_text)

    gdelt_events = query_gdelt_context(
        flag_country=flag,
        vessel_name=vessel_name,
        n=3,
        lance_path=DEFAULT_LANCE_PATH,
    )
    gdelt_section = _GDELT_SECTION.format(gdelt_text=_format_gdelt(gdelt_events))

    return _SYSTEM_TEMPLATE.format(
        fleet_context=fleet,
        vessel_section=vessel_section,
        ownership_section=ownership_section,
        gdelt_section=gdelt_section,
    )


# ── endpoint ────────────────────────────────────────────────────────────────

@router.post("/api/chat")
async def analyst_chat(body: ChatRequest) -> StreamingResponse:
    """Stream an LLM response for an analyst question with optional vessel context.

    - If ``mmsi`` is provided the context window includes vessel features,
      Neo4j 2-hop ownership, and GDELT events for that vessel.
    - Without ``mmsi`` the response draws on the fleet overview to answer
      cross-vessel questions such as "which vessels share the same owner network?".
    - Responses are cached in DuckDB; duplicate questions within the same
      pipeline run are answered from cache without calling the LLM.
    """
    version = _watchlist_version()
    q_hash = _question_hash(body.message)
    key = _cache_key(body.mmsi, q_hash, version)

    cached = _read_cache(key)
    if cached:
        async def _cached():
            words = cached.split(" ")
            for i, w in enumerate(words):
                chunk = w if i == 0 else " " + w
                yield f"data: {chunk}\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(
            _cached(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    df = _load_watchlist()

    vessel: dict | None = None
    if body.mmsi:
        if not df.is_empty():
            rows = df.filter(pl.col("mmsi") == body.mmsi)
            if not rows.is_empty():
                vessel = rows.row(0, named=True)

    system = _build_system(vessel, df)
    messages = [{"role": m.role, "content": m.content} for m in body.history]
    messages.append({"role": "user", "content": body.message})

    async def _stream():
        tokens: list[str] = []
        try:
            llm = get_llm_client()
            async for token in llm.stream_messages(system, messages):
                tokens.append(token)
                yield f"data: {token}\n\n"
        except Exception as exc:
            yield f"data: Answer unavailable — {exc}\n\n"
        finally:
            yield "data: [DONE]\n\n"
            if tokens:
                _write_cache(key, body.mmsi, q_hash, version, "".join(tokens))

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
