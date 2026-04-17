"""Ingest daily GDELT event CSVs and index in LanceDB for geopolitical context retrieval.

Downloads GDELT 1.0 daily export files, filters for conflict/coercion events relevant
to maritime sanctions monitoring, and stores them in a LanceDB table at
data/processed/gdelt.lance with a full-text search index for vessel context retrieval.

Usage:
    uv run python src/ingest/gdelt.py                        # today
    uv run python src/ingest/gdelt.py --date 20260401        # specific date
    uv run python src/ingest/gdelt.py --date 20260401 --days 7  # last 7 days
"""

from __future__ import annotations

import argparse
import io
import os
import zipfile
from datetime import date, timedelta
from pathlib import Path

import httpx
import polars as pl

from pipeline.src.storage.config import is_s3, lance_db_uri, lance_storage_options

DEFAULT_LANCE_PATH = lance_db_uri()
GDELT_BASE_URL = "http://data.gdeltproject.org/events"

# CAMEO root codes to retain — conflict, coercion, sanctions, force posture
_RELEVANT_ROOT_CODES = {"10", "12", "13", "14", "15", "16", "17", "18", "19", "20"}

# Human-readable label for CAMEO root codes used in descriptions
_CAMEO_LABELS: dict[str, str] = {
    "10": "demanded action from",
    "11": "disapproved of",
    "12": "rejected",
    "13": "threatened",
    "14": "protested against",
    "15": "exhibited force posture toward",
    "16": "reduced relations with",
    "17": "coerced",
    "18": "assaulted",
    "19": "fought",
    "20": "engaged in mass violence against",
}

# Column positions in GDELT 1.0 tab-separated export (no header row)
_COL_EVENT_ID = 0
_COL_DATE = 1
_COL_ACTOR1_NAME = 6
_COL_ACTOR1_COUNTRY = 7
_COL_ACTOR2_NAME = 16
_COL_ACTOR2_COUNTRY = 17
_COL_EVENT_CODE = 26
_COL_EVENT_ROOT = 28
_COL_QUAD_CLASS = 29
_COL_GOLDSTEIN = 30
_COL_AVG_TONE = 34
_COL_ACTION_GEO = 52
_COL_ACTION_GEO_COUNTRY = 53
_COL_SOURCE_URL = 57

_ALL_COLS = [
    _COL_EVENT_ID,
    _COL_DATE,
    _COL_ACTOR1_NAME,
    _COL_ACTOR1_COUNTRY,
    _COL_ACTOR2_NAME,
    _COL_ACTOR2_COUNTRY,
    _COL_EVENT_CODE,
    _COL_EVENT_ROOT,
    _COL_QUAD_CLASS,
    _COL_GOLDSTEIN,
    _COL_AVG_TONE,
    _COL_ACTION_GEO,
    _COL_ACTION_GEO_COUNTRY,
    _COL_SOURCE_URL,
]


def _gdelt_url(date_str: str) -> str:
    return f"{GDELT_BASE_URL}/{date_str}.export.CSV.zip"


def download_gdelt_events(date_str: str, dest_dir: str = "data/raw") -> Path:
    """Download and unzip a GDELT daily export CSV; return path to the CSV."""
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    csv_path = dest / f"gdelt_{date_str}.csv"
    if csv_path.exists():
        return csv_path

    url = _gdelt_url(date_str)
    resp = httpx.get(url, timeout=60, follow_redirects=True)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        names = zf.namelist()
        csv_name = next((n for n in names if n.endswith(".CSV")), names[0])
        with zf.open(csv_name) as src, open(csv_path, "wb") as dst:
            dst.write(src.read())

    return csv_path


def _parse_csv(csv_path: Path) -> list[dict]:
    """Parse a GDELT CSV into a list of record dicts, filtering for relevant events."""
    records: list[dict] = []

    try:
        df = pl.read_csv(
            csv_path,
            separator="\t",
            has_header=False,
            infer_schema_length=0,  # all strings initially
            ignore_errors=True,
        )
    except Exception:
        return records

    if df.width <= max(_ALL_COLS):
        return records

    col_names = df.columns  # column_0, column_1, ...

    def col(idx: int) -> str:
        return col_names[idx]

    for row in df.iter_rows(named=True):
        root_code = str(row.get(col(_COL_EVENT_ROOT), "") or "").strip()
        if root_code not in _RELEVANT_ROOT_CODES:
            continue

        actor1 = str(row.get(col(_COL_ACTOR1_NAME), "") or "").strip() or "Unknown"
        actor2 = str(row.get(col(_COL_ACTOR2_NAME), "") or "").strip() or "Unknown"
        actor1_country = str(row.get(col(_COL_ACTOR1_COUNTRY), "") or "").strip().upper()
        actor2_country = str(row.get(col(_COL_ACTOR2_COUNTRY), "") or "").strip().upper()
        action_geo = str(row.get(col(_COL_ACTION_GEO), "") or "").strip()
        action_geo_country = str(row.get(col(_COL_ACTION_GEO_COUNTRY), "") or "").strip().upper()
        event_code = str(row.get(col(_COL_EVENT_CODE), "") or "").strip()
        event_date = str(row.get(col(_COL_DATE), "") or "").strip()
        source_url = str(row.get(col(_COL_SOURCE_URL), "") or "").strip()

        verb = _CAMEO_LABELS.get(root_code, "acted against")
        date_fmt = (
            f"{event_date[:4]}-{event_date[4:6]}-{event_date[6:8]}"
            if len(event_date) == 8
            else event_date
        )

        description = (
            f"{actor1} {verb} {actor2} in {action_geo or 'unknown location'} "
            f"on {date_fmt}. "
            f"Countries: {actor1_country} {actor2_country}. "
            f"Source: {source_url}"
        )

        try:
            avg_tone = float(row.get(col(_COL_AVG_TONE), 0) or 0)
            goldstein = float(row.get(col(_COL_GOLDSTEIN), 0) or 0)
            quad_class = int(row.get(col(_COL_QUAD_CLASS), 0) or 0)
        except (ValueError, TypeError):
            avg_tone, goldstein, quad_class = 0.0, 0.0, 0

        records.append(
            {
                "event_id": str(row.get(col(_COL_EVENT_ID), "") or ""),
                "event_date": event_date,
                "actor1_name": actor1,
                "actor1_country": actor1_country,
                "actor2_name": actor2,
                "actor2_country": actor2_country,
                "event_code": event_code,
                "event_root_code": root_code,
                "quad_class": quad_class,
                "goldstein_scale": goldstein,
                "avg_tone": avg_tone,
                "action_geo": action_geo,
                "action_geo_country": action_geo_country,
                "source_url": source_url,
                "description": description,
            }
        )

    return records


def ingest_gdelt_events(
    date_str: str | None = None,
    lance_path: str = DEFAULT_LANCE_PATH,
    raw_dir: str = "data/raw",
    skip_download: bool = False,
    csv_path: Path | None = None,
) -> int:
    """Ingest one day of GDELT events into LanceDB.

    Args:
        date_str: YYYYMMDD date string. Defaults to yesterday.
        lance_path: Path to LanceDB store.
        raw_dir: Directory to store downloaded CSV files.
        skip_download: If True, expect csv_path to exist already.
        csv_path: Explicit CSV path (overrides download).

    Returns:
        Number of records written.
    """
    import lancedb  # import here to keep module importable without lancedb installed

    if date_str is None:
        date_str = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

    if csv_path is None:
        if skip_download:
            csv_path = Path(raw_dir) / f"gdelt_{date_str}.csv"
        else:
            csv_path = download_gdelt_events(date_str, raw_dir)

    records = _parse_csv(csv_path)
    if not records:
        return 0

    if not is_s3():
        parent = os.path.dirname(lance_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
    storage_opts = lance_storage_options()
    db = (
        lancedb.connect(lance_path, storage_options=storage_opts)
        if storage_opts
        else lancedb.connect(lance_path)
    )

    if "events" in db.table_names():
        table = db.open_table("events")
        table.add(records)
    else:
        table = db.create_table("events", data=records)
        try:
            table.create_fts_index("description", replace=True)
        except Exception:
            pass  # FTS optional; falls back to linear scan

    return len(records)


def query_gdelt_context(
    flag_country: str,
    vessel_name: str = "",
    n: int = 3,
    lance_path: str = DEFAULT_LANCE_PATH,
    days_window: int = 90,
) -> list[dict]:
    """Retrieve relevant GDELT events for a vessel.

    Matches by flag_country in actor countries and optionally vessel_name text.
    Returns up to n records, sorted by recency.
    """
    import lancedb

    storage_opts = lance_storage_options()
    try:
        db = (
            lancedb.connect(lance_path, storage_options=storage_opts)
            if storage_opts
            else lancedb.connect(lance_path)
        )
    except Exception:
        return []
    if "events" not in db.table_names():
        return []

    table = db.open_table("events")

    # Build FTS query from flag country and vessel name tokens
    query_parts = [flag_country] if flag_country else []
    if vessel_name:
        # Add significant tokens from vessel name
        tokens = [t for t in vessel_name.upper().split() if len(t) > 2]
        query_parts.extend(tokens[:2])

    if not query_parts:
        return []

    query_text = " ".join(query_parts)

    try:
        results = table.search(query_text, query_type="fts").limit(n * 3).to_list()
    except Exception:
        # FTS not available — filter by country code in actor fields
        try:
            df = table.to_pandas()
            mask = (
                df["actor1_country"].str.upper().eq(flag_country.upper())
                | df["actor2_country"].str.upper().eq(flag_country.upper())
                | df["action_geo_country"].str.upper().eq(flag_country.upper())
            )
            results = df[mask].head(n * 3).to_dict("records")
        except Exception:
            return []

    # Sort by recency (event_date descending) and return top n
    results.sort(key=lambda r: r.get("event_date", ""), reverse=True)
    return results[:n]


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest GDELT events into LanceDB")
    parser.add_argument("--date", help="YYYYMMDD date (default: yesterday)")
    parser.add_argument("--days", type=int, default=1, help="Number of days to ingest (default: 1)")
    parser.add_argument("--lance-path", default=DEFAULT_LANCE_PATH)
    parser.add_argument("--raw-dir", default="data/raw")
    args = parser.parse_args()

    end_date = (
        date(int(args.date[:4]), int(args.date[4:6]), int(args.date[6:8]))
        if args.date
        else date.today() - timedelta(days=1)
    )

    total = 0
    for i in range(args.days):
        d = end_date - timedelta(days=i)
        ds = d.strftime("%Y%m%d")
        try:
            n = ingest_gdelt_events(ds, args.lance_path, args.raw_dir)
            print(f"{ds}: {n} events ingested")
            total += n
        except Exception as exc:
            print(f"{ds}: failed — {exc}")

    print(f"Total events ingested: {total}")


if __name__ == "__main__":
    main()
