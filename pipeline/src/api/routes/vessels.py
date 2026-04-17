"""Vessel data endpoints: GeoJSON, watchlist table fragment, metrics, vessel types."""

from __future__ import annotations

import json
import os
from datetime import UTC
from pathlib import Path

import polars as pl
from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse, JSONResponse

from pipeline.src.analysis.causal import score_unknown_unknowns
from pipeline.src.storage.config import _canonical_data_dir, output_uri, watchlist_uri
from pipeline.src.storage.config import read_parquet as read_parquet_uri

DEFAULT_VALIDATION_PATH = os.getenv(
    "VALIDATION_METRICS_PATH",
    str(Path(_canonical_data_dir()) / "validation_metrics.json"),
)
DEFAULT_CAUSAL_EFFECTS_PATH = os.getenv("CAUSAL_EFFECTS_PATH") or output_uri(
    "causal_effects.parquet"
)

router = APIRouter()


def _load_watchlist() -> pl.DataFrame:
    df = read_parquet_uri(watchlist_uri())
    if df is None:
        return pl.DataFrame()
    return df


def _load_metrics() -> dict | None:
    p = Path(DEFAULT_VALIDATION_PATH)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


@router.get("/api/vessels/geojson")
def vessels_geojson(
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
    vessel_types: list[str] = Query(default=[]),
) -> JSONResponse:
    df = _load_watchlist()
    if df.is_empty():
        return JSONResponse({"type": "FeatureCollection", "features": []})

    filtered = df.filter(pl.col("confidence") >= min_confidence)
    if vessel_types:
        filtered = filtered.filter(pl.col("vessel_type").is_in(vessel_types))

    filtered = filtered.filter(
        pl.col("last_lat").is_not_null() & pl.col("last_lon").is_not_null()
    ).with_columns(pl.col("last_seen").cast(pl.Utf8))

    features = []
    for row in filtered.select(
        [
            "mmsi",
            "vessel_name",
            "flag",
            "vessel_type",
            "confidence",
            "last_lat",
            "last_lon",
            "last_seen",
        ]
    ).to_dicts():
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [row["last_lon"], row["last_lat"]]},
                "properties": {
                    "mmsi": row["mmsi"],
                    "vessel_name": row["vessel_name"],
                    "flag": row["flag"],
                    "vessel_type": row["vessel_type"],
                    "confidence": row["confidence"],
                    "last_seen": row["last_seen"],
                },
            }
        )

    return JSONResponse({"type": "FeatureCollection", "features": features})


@router.get("/api/watchlist/top", response_class=HTMLResponse)
def watchlist_top(
    min_confidence: float = Query(0.4, ge=0.0, le=1.0),
    vessel_types: list[str] = Query(default=[]),
    top_n: int = Query(50, ge=1, le=500),
) -> HTMLResponse:
    df = _load_watchlist()
    if df.is_empty():
        return HTMLResponse("<tr><td colspan='9'>No data — run watchlist.py first.</td></tr>")

    filtered = df.filter(pl.col("confidence") >= min_confidence)
    if vessel_types:
        filtered = filtered.filter(pl.col("vessel_type").is_in(vessel_types))

    rows_html = []
    for row in filtered.head(top_n).with_columns(pl.col("last_seen").cast(pl.Utf8)).to_dicts():
        conf = row["confidence"]
        badge_class = (
            "badge-red" if conf >= 0.7 else "badge-yellow" if conf >= 0.4 else "badge-green"
        )
        vessel_name = str(row["vessel_name"])
        safe_name_attr = vessel_name.replace("'", "&#39;")
        try:
            signals = json.loads(row.get("top_signals") or "[]")
            signals_text = ", ".join(f"{s['feature']}" for s in signals[:2]) if signals else "—"
        except Exception:
            signals_text = str(row.get("top_signals", "—"))[:60]
        safe_signals_attr = str(signals_text).replace("'", "&#39;")
        safe_type_attr = str(row.get("vessel_type", "")).replace("'", "&#39;")
        safe_flag_attr = str(row.get("flag", "")).replace("'", "&#39;")
        safe_last_seen_attr = str(row.get("last_seen", "")).replace("'", "&#39;")

        lat = row.get("last_lat") or ""
        lon = row.get("last_lon") or ""
        rows_html.append(
            f"<tr class='watchlist-row' data-mmsi='{row['mmsi']}' data-lat='{lat}' data-lon='{lon}' "
            f"data-name='{safe_name_attr}' data-type='{safe_type_attr}' data-flag='{safe_flag_attr}' "
            f"data-confidence='{conf:.4f}' data-last-seen='{safe_last_seen_attr}' data-signals='{safe_signals_attr}'>"
            f"<td>{row['mmsi']}</td>"
            f"<td>{vessel_name}</td>"
            f"<td>{row['vessel_type']}</td>"
            f"<td>{row['flag']}</td>"
            f"<td><span class='badge {badge_class}'>{conf:.2f}</span></td>"
            f"<td class='signals'>{signals_text}</td>"
            f"<td class='causal-col' data-mmsi='{row['mmsi']}'></td>"
            f"<td class='review-tier' data-mmsi='{row['mmsi']}'>—</td>"
            f"<td class='review-handoff' data-mmsi='{row['mmsi']}'>—</td>"
            f"<td><button class='review-btn' onclick=\"event.stopPropagation(); openReviewPanel('{row['mmsi']}', '{safe_name_attr}');\">Review</button></td>"
            f"</tr>"
        )

    return HTMLResponse("\n".join(rows_html))


@router.get("/api/metrics")
def metrics() -> JSONResponse:
    m = _load_metrics()
    if m is None:
        return JSONResponse({"available": False})
    return JSONResponse(
        {
            "available": True,
            "precision_at_50": m.get("precision_at_50"),
            "recall_at_200": m.get("recall_at_200"),
            "auroc": m.get("auroc"),
        }
    )


@router.get("/api/vessels/causal-candidates")
def causal_candidates() -> JSONResponse:
    """Return the set of unknown-unknown candidate MMSIs and their scores.

    Intended for the watchlist table to badge rows without per-row API calls.
    Response: { "candidates": [{ "mmsi": "...", "causal_score": 0.0 }, ...] }
    """
    db_path = os.getenv("DB_PATH", str(Path(_canonical_data_dir()) / "singapore.duckdb"))
    try:
        candidates = score_unknown_unknowns(db_path=db_path, min_signals=1)
    except Exception:
        return JSONResponse({"candidates": []})
    return JSONResponse(
        {"candidates": [{"mmsi": c.mmsi, "causal_score": c.causal_score} for c in candidates]}
    )


@router.get("/api/vessels/{mmsi}/causal")
def vessel_causal(mmsi: str) -> JSONResponse:
    """Return the unknown-unknown causal score and matching signals for a vessel.

    Response shape:
      { "mmsi": "...", "causal_score": 0.0, "is_candidate": false, "signals": [] }
    where signals is a list of { feature, recent_value, baseline_value, uplift_ratio }.
    Returns causal_score=0 and is_candidate=false if the vessel is in the sanctions
    graph or does not meet the minimum signal threshold.
    """
    db_path = os.getenv("DB_PATH", str(Path(_canonical_data_dir()) / "singapore.duckdb"))
    try:
        candidates = score_unknown_unknowns(db_path=db_path, min_signals=1)
    except Exception:
        return JSONResponse(
            {"mmsi": mmsi, "causal_score": 0.0, "is_candidate": False, "signals": []}
        )

    for candidate in candidates:
        if candidate.mmsi == mmsi:
            return JSONResponse(
                {
                    "mmsi": mmsi,
                    "causal_score": candidate.causal_score,
                    "is_candidate": True,
                    "signals": [
                        {
                            "feature": s.feature,
                            "recent_value": s.recent_value,
                            "baseline_value": s.baseline_value,
                            "uplift_ratio": s.uplift_ratio,
                        }
                        for s in candidate.matching_signals
                    ],
                }
            )

    return JSONResponse({"mmsi": mmsi, "causal_score": 0.0, "is_candidate": False, "signals": []})


@router.get("/api/vessels/{mmsi}/signals")
def vessel_signals(mmsi: str) -> JSONResponse:
    """Return the top SHAP signals for a vessel.

    Response shape:
      { "mmsi": "...", "confidence": 0.87, "signals": [
          { "feature": "ais_gap_count_30d", "value": 14, "contribution": 0.42 }, ...
      ]}
    Signals are sorted by contribution descending (up to 5).
    Returns an empty signals list if the vessel is not found.
    """
    df = _load_watchlist()
    if df.is_empty():
        return JSONResponse({"mmsi": mmsi, "confidence": None, "signals": []})

    rows = df.filter(pl.col("mmsi") == mmsi)
    if rows.is_empty():
        return JSONResponse({"mmsi": mmsi, "confidence": None, "signals": []})

    row = rows.row(0, named=True)
    try:
        signals = json.loads(row.get("top_signals") or "[]")
    except Exception:
        signals = []

    return JSONResponse(
        {
            "mmsi": mmsi,
            "confidence": row.get("confidence"),
            "signals": signals,
        }
    )


@router.get("/api/vessel-types")
def vessel_types() -> JSONResponse:
    df = _load_watchlist()
    if df.is_empty():
        return JSONResponse([])
    return JSONResponse(sorted(df["vessel_type"].unique().to_list()))


@router.get("/api/causal-effects")
def causal_effects() -> JSONResponse:
    """Return per-regime DiD ATT estimates from causal_effects.parquet.

    Response shape:
      { "available": true, "regimes": [
          {
            "regime": "OFAC Iran",
            "att_estimate": 0.42,
            "att_ci_lower": 0.31,
            "att_ci_upper": 0.53,
            "p_value": 0.0003,
            "is_significant": true,
            "n_treated": 18,
            "n_control": 142,
            "calibrated_weight": 0.55
          }, ...
      ]}
    Returns { "available": false } if the file does not exist or cannot be read.
    """
    df = read_parquet_uri(DEFAULT_CAUSAL_EFFECTS_PATH)
    if df is None or df.is_empty():
        return JSONResponse({"available": False, "regimes": []})

    required = {
        "regime",
        "att_estimate",
        "att_ci_lower",
        "att_ci_upper",
        "p_value",
        "is_significant",
    }
    if not required.issubset(set(df.columns)):
        return JSONResponse({"available": False, "regimes": []})

    regimes = []
    for row in df.to_dicts():
        regimes.append(
            {
                "regime": str(row.get("regime", "")),
                "att_estimate": float(row.get("att_estimate", 0.0)),
                "att_ci_lower": float(row.get("att_ci_lower", 0.0)),
                "att_ci_upper": float(row.get("att_ci_upper", 0.0)),
                "p_value": float(row.get("p_value", 1.0)),
                "is_significant": bool(row.get("is_significant", False)),
                "n_treated": int(row.get("n_treated", 0)),
                "n_control": int(row.get("n_control", 0)),
                "calibrated_weight": float(row.get("calibrated_weight", 0.0)),
            }
        )

    return JSONResponse({"available": True, "regimes": regimes})


def _ais_history(mmsi: str, limit: int = 10) -> list[dict]:
    """Return last N AIS positions for a vessel from DuckDB. Fails gracefully."""
    try:
        from pipeline.src.api.db import get_conn

        with get_conn() as con:
            if con is None:
                return []
            rows = con.execute(
                """
                SELECT timestamp, lat, lon, sog, cog, nav_status
                FROM ais_positions
                WHERE mmsi = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                [mmsi, limit],
            ).fetchall()
        return [
            {
                "timestamp": str(r[0]),
                "lat": r[1],
                "lon": r[2],
                "sog": r[3],
                "cog": r[4],
                "nav_status": r[5],
            }
            for r in rows
        ]
    except Exception:
        return []


def _ownership_chain(mmsi: str) -> list[dict]:
    """Return structured ownership chain (up to 3 hops). Fails gracefully."""
    try:
        import polars as pl

        from pipeline.src.graph.store import load_tables

        db_path = os.getenv("DB_PATH", str(Path(_canonical_data_dir()) / "singapore.duckdb"))
        tables = load_tables(db_path)

        ob: pl.DataFrame = pl.from_arrow(tables["OWNED_BY"])  # type: ignore[assignment]
        mb: pl.DataFrame = pl.from_arrow(tables["MANAGED_BY"])  # type: ignore[assignment]
        sb: pl.DataFrame = pl.from_arrow(tables["SANCTIONED_BY"])  # type: ignore[assignment]
        co: pl.DataFrame = pl.from_arrow(tables["Company"])  # type: ignore[assignment]

        sanctioned_ids: set[str] = set(sb["src_id"].to_list()) if len(sb) else set()
        chain: list[dict] = []

        def _add_companies(ids: list[str], hop: int) -> None:
            if not ids or hop > 3:
                return
            info = co.filter(pl.col("id").is_in(ids))
            next_ids: list[str] = []
            for row in info.iter_rows(named=True):
                chain.append(
                    {
                        "id": row["id"],
                        "name": row.get("name") or "?",
                        "country": row.get("country") or "?",
                        "hop": hop,
                        "sanctioned": row["id"] in sanctioned_ids,
                    }
                )
                # next hop via CONTROLLED_BY
                if "CONTROLLED_BY" in tables:
                    cb: pl.DataFrame = pl.from_arrow(tables["CONTROLLED_BY"])  # type: ignore[assignment]
                    next_ids += cb.filter(pl.col("src_id") == row["id"])["dst_id"].to_list()
            _add_companies(list(set(next_ids) - {r["id"] for r in chain}), hop + 1)

        direct: list[str] = []
        if len(ob):
            direct += ob.filter(pl.col("src_id") == mmsi)["dst_id"].to_list()
        if len(mb):
            direct += mb.filter(pl.col("src_id") == mmsi)["dst_id"].to_list()
        direct = list(set(direct))
        _add_companies(direct, 1)
        return chain
    except Exception:
        return []


@router.get("/api/vessels/{mmsi}/dispatch-brief")
def vessel_dispatch_brief(mmsi: str) -> JSONResponse:
    """Assemble a patrol dispatch brief for a vessel.

    Response shape:
      {
        "mmsi": "...",
        "identity": { vessel_name, flag, vessel_type, imo, confidence, confidence_tier,
                      last_lat, last_lon, last_seen },
        "signals": [ {feature, value, contribution}, ... ],   # top-5 SHAP
        "causal": { att_estimate, att_ci_lower, att_ci_upper, p_value,
                    is_significant, regime } | null,          # strongest significant regime
        "ownership_chain": [ {id, name, country, hop, sanctioned}, ... ],
        "ais_history": [ {timestamp, lat, lon, sog, cog, nav_status}, ... ],
        "generated_at": "ISO timestamp"
      }
    Returns 404 if vessel not in watchlist.
    """
    from datetime import datetime

    df = _load_watchlist()
    if df.is_empty():
        return JSONResponse({"error": "watchlist unavailable"}, status_code=404)

    rows = df.filter(pl.col("mmsi") == mmsi)
    if rows.is_empty():
        return JSONResponse({"error": "vessel not found"}, status_code=404)

    row = rows.row(0, named=True)
    conf = float(row.get("confidence") or 0.0)
    confidence_tier = "High" if conf >= 0.7 else "Medium" if conf >= 0.4 else "Low"

    try:
        signals = json.loads(row.get("top_signals") or "[]")
    except Exception:
        signals = []

    # Best significant causal regime
    causal: dict | None = None
    effects_df = read_parquet_uri(DEFAULT_CAUSAL_EFFECTS_PATH)
    if effects_df is not None and not effects_df.is_empty():
        sig = effects_df.filter(pl.col("is_significant")).sort("att_estimate", descending=True)
        if not sig.is_empty():
            er = sig.row(0, named=True)
            causal = {
                "regime": str(er.get("regime", "")),
                "att_estimate": float(er.get("att_estimate", 0.0)),
                "att_ci_lower": float(er.get("att_ci_lower", 0.0)),
                "att_ci_upper": float(er.get("att_ci_upper", 0.0)),
                "p_value": float(er.get("p_value", 1.0)),
                "is_significant": True,
            }

    return JSONResponse(
        {
            "mmsi": mmsi,
            "generated_at": datetime.now(UTC).isoformat(),
            "identity": {
                "vessel_name": str(row.get("vessel_name") or ""),
                "flag": str(row.get("flag") or ""),
                "vessel_type": str(row.get("vessel_type") or ""),
                "imo": str(row.get("imo") or ""),
                "confidence": conf,
                "confidence_tier": confidence_tier,
                "last_lat": row.get("last_lat"),
                "last_lon": row.get("last_lon"),
                "last_seen": str(row.get("last_seen") or ""),
            },
            "signals": signals,
            "causal": causal,
            "ownership_chain": _ownership_chain(mmsi),
            "ais_history": _ais_history(mmsi),
        }
    )
