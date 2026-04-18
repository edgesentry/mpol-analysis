from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from pipeline.src.ingest.schema import init_schema


def _to_repo_relative(path_value: str, repo_root: Path) -> str:
    path_obj = Path(path_value)
    resolved = path_obj.resolve()
    try:
        return resolved.relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return path_value


def _copy_sample_rows(src_db: Path, dst_db: Path, max_rows: int) -> int:
    src = duckdb.connect(str(src_db), read_only=True)
    dst = duckdb.connect(str(dst_db))
    try:
        rows = src.execute(
            """
            SELECT entity_id, name, mmsi, imo, flag, type, list_source
            FROM sanctions_entities
            ORDER BY entity_id
            LIMIT ?
            """,
            [max_rows],
        ).fetchall()

        if not rows:
            return 0

        dst.executemany(
            """
            INSERT OR IGNORE INTO sanctions_entities
                (entity_id, name, mmsi, imo, flag, type, list_source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        count_row = dst.execute("SELECT count(*) FROM sanctions_entities").fetchone()
        return int(count_row[0]) if count_row is not None else 0
    finally:
        src.close()
        dst.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a small demo DuckDB from prepared public sanctions DB"
    )
    parser.add_argument(
        "--source-db",
        default="data/processed/public_eval.duckdb",
        help="Prepared source DB path",
    )
    parser.add_argument(
        "--demo-db",
        default="data/demo/public_eval_demo.duckdb",
        help="Output demo DB path",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=300,
        help="Maximum sanctions rows to copy into demo DB",
    )
    parser.add_argument(
        "--metadata-out",
        default="data/demo/public_eval_demo_metadata.json",
        help="Metadata output path",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]

    source_db = Path(args.source_db)
    demo_db = Path(args.demo_db)
    if not source_db.exists():
        raise SystemExit(
            f"Source DB not found: {source_db}. Run scripts/prepare_public_sanctions_db.py first."
        )

    demo_db.parent.mkdir(parents=True, exist_ok=True)
    init_schema(str(demo_db))

    inserted = _copy_sample_rows(source_db, demo_db, max(1, args.max_rows))

    summary = {
        "source_db": _to_repo_relative(args.source_db, repo_root),
        "demo_db": _to_repo_relative(args.demo_db, repo_root),
        "max_rows": args.max_rows,
        "inserted_rows": inserted,
        "updated_at_utc": datetime.now(UTC).isoformat(),
    }

    metadata_path = Path(args.metadata_out)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
