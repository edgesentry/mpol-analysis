from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from pipeline.src.ingest.sanctions import (
    OPENSANCTIONS_URL,
    download_opensanctions,
    load_jsonl_to_duckdb,
)
from pipeline.src.ingest.schema import init_schema


def _count_rows(db_path: Path) -> int:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        row = con.execute("SELECT count(*) FROM sanctions_entities").fetchone()
        return int(row[0]) if row is not None else 0
    finally:
        con.close()


def prepare_public_sanctions_db(
    db_path: Path,
    raw_path: Path,
    *,
    force_download: bool = False,
    force_reload: bool = False,
) -> dict[str, object]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.parent.mkdir(parents=True, exist_ok=True)

    init_schema(str(db_path))

    if force_reload:
        con = duckdb.connect(str(db_path))
        try:
            con.execute("DELETE FROM sanctions_entities")
        finally:
            con.close()

    existing = _count_rows(db_path)
    inserted = 0

    if existing == 0 or force_reload:
        download_opensanctions(raw_path, force=force_download)
        inserted = int(load_jsonl_to_duckdb(raw_path, str(db_path)))

    total = _count_rows(db_path)
    return {
        "db_path": str(db_path.resolve()),
        "raw_path": str(raw_path.resolve()),
        "download_url": OPENSANCTIONS_URL,
        "force_download": force_download,
        "force_reload": force_reload,
        "inserted_rows": inserted,
        "total_rows": total,
        "updated_at_utc": datetime.now(UTC).isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare and persist a reusable DuckDB with OpenSanctions data for tests"
    )
    parser.add_argument(
        "--db",
        default="data/processed/public_eval.duckdb",
        help="Path to persistent DuckDB used by public-data tests",
    )
    parser.add_argument(
        "--raw-path",
        default="data/raw/sanctions/opensanctions_entities.jsonl",
        help="Path to cached OpenSanctions JSONL file",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download JSONL even if a cached file already exists",
    )
    parser.add_argument(
        "--force-reload",
        action="store_true",
        help="Clear sanctions_entities and reload from JSONL",
    )
    parser.add_argument(
        "--metadata-out",
        default="data/processed/public_eval_metadata.json",
        help="Path to write preparation metadata JSON",
    )
    args = parser.parse_args()

    summary = prepare_public_sanctions_db(
        db_path=Path(args.db),
        raw_path=Path(args.raw_path),
        force_download=args.force_download,
        force_reload=args.force_reload,
    )

    metadata_path = Path(args.metadata_out)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(summary, indent=2))

    print(json.dumps(summary, indent=2))
    print(f"Metadata saved: {metadata_path}")


if __name__ == "__main__":
    main()
