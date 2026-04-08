from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Copy bundled demo watchlist parquet into processed output path"
    )
    parser.add_argument(
        "--source",
        default="data/demo/candidate_watchlist_demo.parquet",
        help="Bundled demo watchlist parquet path",
    )
    parser.add_argument(
        "--target",
        default="data/processed/candidate_watchlist.parquet",
        help="Dashboard watchlist parquet path",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Backup target file to <target>.bak before overwrite",
    )
    args = parser.parse_args()

    source = Path(args.source)

    if not source.exists():
        raise SystemExit(f"Source demo watchlist not found: {source}")

    import polars as pl

    from src.storage.config import output_uri, write_parquet

    df = pl.read_parquet(source)
    uri = output_uri("candidate_watchlist.parquet")

    # For local paths, we can still do a backup. S3 backing up is out of scope for this.
    if not uri.startswith("s3://") and args.backup:
        target_path = Path(uri)
        if target_path.exists():
            backup = target_path.with_suffix(target_path.suffix + ".bak")
            import shutil

            shutil.copy2(target_path, backup)
            print(f"Backed up: {backup}")

    write_parquet(df, uri)
    print(f"Copied demo watchlist: {source} -> {uri}")


if __name__ == "__main__":
    main()
