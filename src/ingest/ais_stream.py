"""
aisstream.io WebSocket ingestion.

Maintains a live WebSocket connection to aisstream.io, filters position reports
to the area of interest, and writes them into DuckDB in micro-batches.

Requires AISSTREAM_API_KEY in the environment (or .env file).

Usage:
    uv run python src/ingest/ais_stream.py

    # Custom batch size and flush interval
    uv run python src/ingest/ais_stream.py --batch-size 500 --flush-interval 30
"""

import argparse
import asyncio
import json
import os
import signal
from datetime import UTC, datetime
from typing import Any

import duckdb
import websockets
from dotenv import load_dotenv

from src.ingest.schema import init_schema

load_dotenv()

WEBSOCKET_URL = "wss://stream.aisstream.io/v0/stream"

# Area of interest: Singapore + Malacca Strait (up to 1600 nm)
# Covers: Strait of Malacca, South China Sea, Andaman Sea, Bay of Bengal approaches
BBOX = [[-5.0, 92.0], [22.0, 122.0]]  # [[lat_min, lon_min], [lat_max, lon_max]]

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")
DEFAULT_BATCH_SIZE = 200
DEFAULT_FLUSH_INTERVAL = 60  # seconds


def _parse_position_report(msg: dict[str, Any]) -> dict[str, Any] | None:
    """Extract a flat position record from an aisstream.io WebSocket message.

    Returns None for messages that are not position reports or lack required fields.
    """
    if msg.get("MessageType") != "PositionReport":
        return None

    meta = msg.get("MetaData", {})
    report = msg.get("Message", {}).get("PositionReport", {})

    mmsi = str(meta.get("MMSI") or report.get("UserID") or "")
    lat = report.get("Latitude") or meta.get("latitude")
    lon = report.get("Longitude") or meta.get("longitude")
    time_str = meta.get("time_utc")

    if not mmsi or lat is None or lon is None or not time_str:
        return None

    try:
        # aisstream.io format: "2024-04-02 10:15:30.123 +0000 UTC"
        # Strip optional milliseconds and timezone suffix before parsing.
        ts = time_str.split(" +")[0].split(".")[0]  # → "2024-04-02 10:15:30"
        timestamp = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    except ValueError:
        return None

    return {
        "mmsi": mmsi,
        "timestamp": timestamp,
        "lat": float(lat),
        "lon": float(lon),
        "sog": float(report.get("Sog") or 0),
        "cog": float(report.get("Cog") or 0),
        "nav_status": int(report.get("NavigationalStatus") or 0),
        # PositionReport (NMEA 1/2/3) does not carry ship type; aisstream.io
        # backfills it in MetaData.ShipType from cached Static/Voyage messages.
        "ship_type": int(meta.get("ShipType") or report.get("Type") or 0),
    }


def _flush_batch(batch: list[dict], db_path: str) -> int:
    """INSERT OR IGNORE a batch of position records into DuckDB."""
    if not batch:
        return 0

    import polars as pl

    df = pl.DataFrame(batch).with_columns(  # noqa: F841 — referenced by DuckDB via `FROM df`
        pl.col("nav_status").cast(pl.Int8),
        pl.col("ship_type").cast(pl.Int8),
        pl.col("sog").cast(pl.Float32),
        pl.col("cog").cast(pl.Float32),
    )

    con = duckdb.connect(db_path)
    try:
        before = con.execute("SELECT count(*) FROM ais_positions").fetchone()[0]  # type: ignore[index]
        con.execute("""
            INSERT OR IGNORE INTO ais_positions
                (mmsi, timestamp, lat, lon, sog, cog, nav_status, ship_type)
            SELECT mmsi, timestamp, lat, lon, sog, cog, nav_status, ship_type
            FROM df
        """)
        after = con.execute("SELECT count(*) FROM ais_positions").fetchone()[0]  # type: ignore[index]
    finally:
        con.close()

    return after - before


async def stream(
    api_key: str,
    db_path: str = DEFAULT_DB_PATH,
    bbox: list = BBOX,
    batch_size: int = DEFAULT_BATCH_SIZE,
    flush_interval: float = DEFAULT_FLUSH_INTERVAL,
    duration: float = 0,
) -> None:
    """Connect to aisstream.io and ingest position reports until interrupted.

    Args:
        duration: Stop automatically after this many seconds (0 = run until Ctrl-C).
    """
    init_schema(db_path)

    subscription = {
        "APIKey": api_key,
        "BoundingBoxes": [bbox],
        "FilterMessageTypes": ["PositionReport"],
    }

    batch: list[dict] = []
    total_inserted = 0

    stop_event = asyncio.Event()

    def _handle_signal():
        print("\nShutdown signal received — flushing final batch …")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    deadline: float | None = (loop.time() + duration) if duration > 0 else None

    print(f"Connecting to {WEBSOCKET_URL} …")
    async with websockets.connect(WEBSOCKET_URL) as ws:
        await ws.send(json.dumps(subscription))
        print(
            f"Subscribed — bbox {bbox}, batch_size={batch_size}, flush_interval={flush_interval}s"
        )
        last_flush = loop.time()

        while not stop_event.is_set():
            if deadline is not None and loop.time() >= deadline:
                break

            recv_timeout = min(1.0, deadline - loop.time()) if deadline is not None else 1.0
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=max(recv_timeout, 0.1))
            except TimeoutError:
                continue
            except websockets.ConnectionClosed:
                break

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            record = _parse_position_report(msg)
            if record is None:
                continue

            batch.append(record)
            now = loop.time()

            if len(batch) >= batch_size or (now - last_flush) >= flush_interval:
                n = _flush_batch(batch, db_path)
                total_inserted += n
                print(f"  Flushed {len(batch)} records → {n} inserted (total {total_inserted})")
                batch.clear()
                last_flush = now

    # Flush remainder
    if batch:
        n = _flush_batch(batch, db_path)
        total_inserted += n
        print(f"  Final flush: {len(batch)} records → {n} inserted (total {total_inserted})")

    print(f"Ingestion complete. Total inserted: {total_inserted}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream live AIS from aisstream.io into DuckDB")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="DuckDB path")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--flush-interval",
        type=float,
        default=DEFAULT_FLUSH_INTERVAL,
        help="Max seconds between flushes",
    )
    parser.add_argument(
        "--bbox",
        type=float,
        nargs=4,
        metavar=("LAT_MIN", "LON_MIN", "LAT_MAX", "LON_MAX"),
        help="Bounding box override, e.g. --bbox 25 120 50 150 for seas near Japan",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=0,
        metavar="SECONDS",
        help="Stop streaming after this many seconds (default: 0 = run until Ctrl-C)",
    )
    args = parser.parse_args()

    api_key = os.getenv("AISSTREAM_API_KEY")
    if not api_key:
        raise SystemExit("AISSTREAM_API_KEY not set — add it to .env or the environment")

    bbox = [[args.bbox[0], args.bbox[1]], [args.bbox[2], args.bbox[3]]] if args.bbox else BBOX
    asyncio.run(
        stream(
            api_key,
            db_path=args.db,
            bbox=bbox,
            batch_size=args.batch_size,
            flush_interval=args.flush_interval,
            duration=args.duration,
        )
    )
