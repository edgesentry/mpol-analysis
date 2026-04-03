"""Vessel data endpoints: GeoJSON, watchlist table fragment, metrics, vessel types."""

from __future__ import annotations

import json
import os
from pathlib import Path

import polars as pl
from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse, JSONResponse

from src.storage.config import output_uri
from src.storage.config import read_parquet as read_parquet_uri

DEFAULT_WATCHLIST_PATH = os.getenv("WATCHLIST_OUTPUT_PATH") or output_uri("candidate_watchlist.parquet")
DEFAULT_VALIDATION_PATH = os.getenv("VALIDATION_METRICS_PATH", "data/processed/validation_metrics.json")

router = APIRouter()


def _load_watchlist() -> pl.DataFrame:
    df = read_parquet_uri(DEFAULT_WATCHLIST_PATH)
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
        ["mmsi", "vessel_name", "flag", "vessel_type", "confidence", "last_lat", "last_lon", "last_seen"]
    ).to_dicts():
        features.append({
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
        })

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
        badge_class = "badge-red" if conf >= 0.7 else "badge-yellow" if conf >= 0.4 else "badge-green"
        vessel_name = str(row["vessel_name"])
        safe_name_attr = vessel_name.replace("'", "&#39;")
        try:
            signals = json.loads(row.get("top_signals") or "[]")
            signals_text = ", ".join(f"{s['feature']}" for s in signals[:2]) if signals else "—"
        except Exception:
            signals_text = str(row.get("top_signals", "—"))[:60]

        lat = row.get("last_lat") or ""
        lon = row.get("last_lon") or ""
        rows_html.append(
            f"<tr class='watchlist-row' data-mmsi='{row['mmsi']}' data-lat='{lat}' data-lon='{lon}' data-name='{safe_name_attr}'>"
            f"<td>{row['mmsi']}</td>"
            f"<td>{vessel_name}</td>"
            f"<td>{row['vessel_type']}</td>"
            f"<td>{row['flag']}</td>"
            f"<td><span class='badge {badge_class}'>{conf:.2f}</span></td>"
            f"<td class='signals'>{signals_text}</td>"
            f"<td class='review-tier' data-mmsi='{row['mmsi']}'>—</td>"
            f"<td class='review-handoff' data-mmsi='{row['mmsi']}'>—</td>"
            f"<td><button class='brief-btn review-btn' onclick=\"event.stopPropagation(); openReviewPanel('{row['mmsi']}', '{safe_name_attr}');\">Review</button></td>"
            f"</tr>"
        )

    return HTMLResponse("\n".join(rows_html))


@router.get("/api/metrics")
def metrics() -> JSONResponse:
    m = _load_metrics()
    if m is None:
        return JSONResponse({"available": False})
    return JSONResponse({
        "available": True,
        "precision_at_50": m.get("precision_at_50"),
        "recall_at_200": m.get("recall_at_200"),
        "auroc": m.get("auroc"),
    })


@router.get("/api/vessel-types")
def vessel_types() -> JSONResponse:
    df = _load_watchlist()
    if df.is_empty():
        return JSONResponse([])
    return JSONResponse(sorted(df["vessel_type"].unique().to_list()))
