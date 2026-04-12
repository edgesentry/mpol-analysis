"""Auto-pull processed data from R2 on startup if the local cache is missing.

Called from ``src.api.main`` during the FastAPI startup event.  Has no effect
when the required files already exist, or when R2 credentials are not
configured (S3_BUCKET / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).

Environment variables
---------------------
DB_PATH                 Path to the region DuckDB (used to detect which region
                        to pull and whether the cache is present).
                        Default: data/processed/singapore.duckdb
S3_BUCKET               R2 bucket name. Default: arktrace-public
S3_ENDPOINT             R2 endpoint URL. Default: arktrace-public R2 endpoint
AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
                        R2 credentials. When absent the public bucket is pulled
                        anonymously (no credentials needed for reads).
AUTO_PULL               Set to "0" or "false" to disable auto-pull entirely.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Maps DuckDB filename stem → user-facing region name (mirrors sync_r2._REGION_PREFIX)
_STEM_TO_REGION: dict[str, str] = {
    "singapore": "singapore",
    "japansea": "japan",
    "middleeast": "middleeast",
    "europe": "europe",
    "gulf": "gulf",
    "mpol": "singapore",  # default DB falls back to singapore region data
}

_DEFAULT_DB_PATH = "data/processed/singapore.duckdb"
_WATCHLIST_PATH = "data/processed/candidate_watchlist.parquet"


def _r2_configured() -> bool:
    return all(os.getenv(v) for v in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"))


def _auto_pull_enabled() -> bool:
    val = os.getenv("AUTO_PULL", "1").lower()
    return val not in ("0", "false", "no", "off")


def _cache_present(db_path: str) -> bool:
    """Return True if both the region DB and the watchlist exist."""
    return Path(db_path).exists() and Path(_WATCHLIST_PATH).exists()


def _region_for_db(db_path: str) -> str:
    stem = Path(db_path).stem
    return _STEM_TO_REGION.get(stem, "singapore")


def maybe_pull() -> None:
    """Pull data from R2 if local cache is missing and credentials are available.

    This function is intentionally synchronous and blocking — the app must not
    start serving requests before the data is ready.
    """
    if not _auto_pull_enabled():
        return

    db_path = os.getenv("DB_PATH", _DEFAULT_DB_PATH)

    if _cache_present(db_path):
        return

    if not _r2_configured():
        logger.warning(
            "Local data cache is missing (%s) and R2 credentials are not set. "
            "The dashboard will show empty data. "
            "Set S3_BUCKET / S3_ENDPOINT / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY "
            "in .env to enable auto-pull, or run: "
            "uv run python scripts/sync_r2.py pull",
            db_path,
        )
        return

    region = _region_for_db(db_path)
    logger.info(
        "Local data cache not found (%s). Pulling region '%s' from R2 …",
        db_path,
        region,
    )

    try:
        _pull(region, db_path)
    except Exception as exc:
        raise RuntimeError(
            f"Auto-pull from R2 failed for region '{region}': {exc}\n"
            "Check your R2 credentials in .env or pull manually:\n"
            "  uv run python scripts/sync_r2.py pull"
        ) from exc


def _pull(region: str, db_path: str) -> None:
    """Invoke the sync_r2 pull logic directly (no subprocess)."""
    from dotenv import load_dotenv

    load_dotenv()

    import sys
    from pathlib import Path as _Path

    scripts_dir = str(_Path(__file__).parents[3] / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)

    from sync_r2 import (
        _DEFAULT_BUCKET,
        _build_r2_fs,
        _pull_zip,
        _read_latest,
    )

    bucket = os.getenv("S3_BUCKET", _DEFAULT_BUCKET)
    data_dir = _Path(os.getenv("DATA_DIR", "data/processed"))

    # Anonymous pull if no write credentials (public bucket)
    anon = not (os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
    fs = _build_r2_fs(anonymous=anon)

    timestamp = _read_latest(fs, bucket)
    if not timestamp:
        raise RuntimeError(
            f"No 'latest' pointer found at {bucket}/latest. "
            "Run: uv run python scripts/sync_r2.py push"
        )

    logger.info("Downloading %s.zip for region '%s' …", timestamp, region)
    downloaded = _pull_zip(fs, bucket, timestamp, data_dir, [region])
    logger.info("Auto-pull complete: %.1f MB downloaded.", downloaded / 1_048_576)
