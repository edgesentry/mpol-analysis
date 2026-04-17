"""Write candidate watchlist parquet output."""

from __future__ import annotations

import argparse
import os

import polars as pl
from dotenv import load_dotenv

from pipeline.src.score.composite import DEFAULT_DB_PATH, compute_composite_scores
from pipeline.src.storage.config import output_uri
from pipeline.src.storage.config import write_parquet as write_parquet_uri

load_dotenv()

DEFAULT_OUTPUT_PATH = os.getenv("WATCHLIST_OUTPUT_PATH") or output_uri(
    "candidate_watchlist.parquet"
)


def build_candidate_watchlist(db_path: str = DEFAULT_DB_PATH) -> pl.DataFrame:
    return compute_composite_scores(db_path)


def write_candidate_watchlist(df: pl.DataFrame, output_path: str = DEFAULT_OUTPUT_PATH) -> None:
    write_parquet_uri(df, output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build candidate watchlist parquet")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    watchlist = build_candidate_watchlist(args.db)
    write_candidate_watchlist(watchlist, args.output)
    print(f"Watchlist rows written: {watchlist.height}")


if __name__ == "__main__":
    main()
