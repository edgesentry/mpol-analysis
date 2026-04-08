"""
Sanctions data ingestion.

Downloads the OpenSanctions consolidated dataset (CC0) and loads
relevant entities into the DuckDB sanctions_entities table.

OpenSanctions already merges OFAC SDN, EU Consolidated List, UN Security
Council, and dozens of other lists — making it the single source needed for
the screening pipeline.

Usage:
    uv run python src/ingest/sanctions.py
    uv run python src/ingest/sanctions.py --db path/to/custom.duckdb
"""

import argparse
import json
import os
from pathlib import Path

import duckdb
import httpx
import polars as pl
from dotenv import load_dotenv

from src.ingest.schema import init_schema

load_dotenv()

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/processed/mpol.duckdb")
DEFAULT_RAW_DIR = "data/raw/sanctions"

# OpenSanctions "sanctions" topic — all sanctioned entities from merged lists.
# Switch to the "default" dataset URL for the full entity graph (much larger).
OPENSANCTIONS_URL = "https://data.opensanctions.org/datasets/latest/sanctions/entities.ftm.json"

# FtM schemas we care about for the screening pipeline
RELEVANT_SCHEMAS = {"Vessel", "Company", "Person", "Organization", "LegalEntity"}


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def download_opensanctions(
    out_path: Path,
    url: str = OPENSANCTIONS_URL,
    force: bool = False,
) -> Path:
    """Stream-download the OpenSanctions JSONL file to *out_path*.

    Skips the download if the file already exists unless *force* is True.
    Returns the path to the local file.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and not force:
        print(f"  Already downloaded: {out_path}")
        return out_path

    tmp_path = out_path.with_suffix(".tmp")
    print(f"  Downloading {url} …")
    with httpx.Client(timeout=600, follow_redirects=True) as client:
        with client.stream("GET", url) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            received = 0
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=65_536):
                    f.write(chunk)
                    received += len(chunk)
                    if total:
                        pct = received * 100 // total
                        print(f"\r  {pct}% ({received // 1_048_576} MB)", end="", flush=True)
    print()
    tmp_path.rename(out_path)
    print(f"  Saved: {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def parse_ftm_entity(entity: dict) -> dict | None:
    """Extract a flat sanctions_entities row from an OpenSanctions FtM entity.

    Returns None for schemas not used in the screening pipeline, or if the
    entity is missing required fields.
    """
    schema = entity.get("schema", "")
    if schema not in RELEVANT_SCHEMAS:
        return None

    props = entity.get("properties", {})

    def first(key: str) -> str | None:
        vals = props.get(key)
        return vals[0].strip() if vals else None

    entity_id = entity.get("id", "").strip()
    name = first("name") or entity.get("caption", "").strip()
    if not entity_id or not name:
        return None

    datasets = entity.get("datasets") or []
    list_source = ";".join(sorted(set(datasets)))

    return {
        "entity_id": entity_id,
        "name": name,
        "mmsi": first("mmsi"),
        "imo": first("imoNumber"),
        "flag": first("flag") or first("country"),
        "type": schema,
        "list_source": list_source,
    }


# ---------------------------------------------------------------------------
# DuckDB loading
# ---------------------------------------------------------------------------


def _flush_batch(con: duckdb.DuckDBPyConnection, batch: list[dict]) -> int:
    """INSERT OR IGNORE *batch* rows into sanctions_entities. Returns inserted count."""
    df = pl.DataFrame(  # noqa: F841 — referenced by DuckDB via `FROM df`
        batch,
        schema={
            "entity_id": pl.Utf8,
            "name": pl.Utf8,
            "mmsi": pl.Utf8,
            "imo": pl.Utf8,
            "flag": pl.Utf8,
            "type": pl.Utf8,
            "list_source": pl.Utf8,
        },
    )
    before = con.execute("SELECT count(*) FROM sanctions_entities").fetchone()[0]  # type: ignore[index]
    con.execute("""
        INSERT OR IGNORE INTO sanctions_entities
            (entity_id, name, mmsi, imo, flag, type, list_source)
        SELECT entity_id, name, mmsi, imo, flag, type, list_source
        FROM df
    """)
    return con.execute("SELECT count(*) FROM sanctions_entities").fetchone()[0] - before  # type: ignore[index]


def load_jsonl_to_duckdb(
    jsonl_path: Path,
    db_path: str = DEFAULT_DB_PATH,
    batch_size: int = 5_000,
) -> int:
    """Stream-parse *jsonl_path* and load entities into DuckDB.

    Returns total rows inserted.
    """
    con = duckdb.connect(db_path)
    total = 0
    batch: list[dict] = []

    try:
        with open(jsonl_path) as f:
            for raw_line in f:
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                try:
                    entity = json.loads(raw_line)
                except json.JSONDecodeError:
                    continue

                row = parse_ftm_entity(entity)
                if row is None:
                    continue

                batch.append(row)
                if len(batch) >= batch_size:
                    total += _flush_batch(con, batch)
                    batch.clear()

        if batch:
            total += _flush_batch(con, batch)
    finally:
        con.close()

    return total


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load OpenSanctions data into DuckDB")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="DuckDB path")
    parser.add_argument("--raw-dir", default=DEFAULT_RAW_DIR)
    parser.add_argument(
        "--url",
        default=OPENSANCTIONS_URL,
        help="OpenSanctions JSONL URL (override to use 'default' dataset)",
    )
    parser.add_argument("--force", action="store_true", help="Re-download even if cached")
    args = parser.parse_args()

    init_schema(args.db)

    out_path = Path(args.raw_dir) / "opensanctions_entities.jsonl"
    download_opensanctions(out_path, args.url, force=args.force)

    print(f"Loading {out_path} → {args.db} …")
    n = load_jsonl_to_duckdb(out_path, args.db)
    print(f"Total inserted: {n}")
