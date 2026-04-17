"""Auto-pull processed data from R2 on startup if the local cache is missing or stale.

Called from ``src.api.main`` during the FastAPI startup event.

Behaviour
---------
1. If local files are **missing** → pull from R2 (same as before).
2. If local files are **present but older than the R2 latest snapshot** → re-pull.
3. If local files are **current** → no-op (no network round-trip beyond reading
   the tiny ``latest`` pointer file).

Data directory and region resolution order
------------------------------------------
1. ``DB_PATH`` env var (explicit full path — dev / CI; overrides everything)
2. ``ARKTRACE_DATA_DIR`` / ``ARKTRACE_REGION`` env vars
3. ``~/.arktrace/data/<region>.duckdb`` (standard user-level install location)

Environment variables
---------------------
DB_PATH                 Full path to the region DuckDB (overrides all below).
ARKTRACE_REGION         Region to use: singapore (default), japan, middleeast,
                        europe, gulf.
ARKTRACE_DATA_DIR       Override the data directory (default: ~/.arktrace/data/).
S3_BUCKET               R2 bucket name. Default: arktrace-public
S3_ENDPOINT             R2 endpoint URL.
AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
                        R2 credentials. When absent the public bucket is pulled
                        anonymously (no credentials needed for public bucket reads).
AUTO_PULL               Set to "0" or "false" to disable auto-pull entirely.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
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

# Maps user-facing region name → DuckDB filename stem
_REGION_TO_STEM: dict[str, str] = {
    "singapore": "singapore",
    "japan": "japansea",
    "middleeast": "middleeast",
    "europe": "europe",
    "gulf": "gulf",
}

_DEFAULT_REGION = "singapore"
_VALID_REGIONS = set(_REGION_TO_STEM)


def _default_data_dir() -> Path:
    """Return the data directory, honouring env overrides."""
    if explicit := os.getenv("ARKTRACE_DATA_DIR"):
        return Path(explicit).expanduser()
    return Path.home() / ".arktrace" / "data"


def _default_region() -> str:
    """Return the configured region (default: singapore)."""
    region = os.getenv("ARKTRACE_REGION", _DEFAULT_REGION).lower().strip()
    if region not in _VALID_REGIONS:
        logger.warning(
            "ARKTRACE_REGION=%r is not a valid region (%s). Falling back to '%s'.",
            region,
            ", ".join(sorted(_VALID_REGIONS)),
            _DEFAULT_REGION,
        )
        return _DEFAULT_REGION
    return region


def _default_db_path() -> str:
    if explicit := os.getenv("DB_PATH"):
        return explicit
    stem = _REGION_TO_STEM[_default_region()]
    return str(_default_data_dir() / f"{stem}.duckdb")


def _watchlist_path(data_dir: Path) -> Path:
    return data_dir / "candidate_watchlist.parquet"


def _auto_pull_enabled() -> bool:
    val = os.getenv("AUTO_PULL", "1").lower()
    return val not in ("0", "false", "no", "off")


def _r2_configured() -> bool:
    return all(os.getenv(v) for v in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"))


def _cache_present(db_path: Path, watchlist: Path) -> bool:
    """Return True if local data is sufficient to serve the dashboard.

    The watchlist parquet alone is enough for read-only API responses.
    The DuckDB is needed for write operations (reviews etc.) but its absence
    should not trigger an infinite re-pull loop when the R2 snapshot only
    contains watchlist files (e.g. after a ``push-demo`` or ``push-watchlists``
    without a full pipeline DuckDB).
    """
    return watchlist.exists()


def _remote_timestamp(fs, bucket: str) -> datetime | None:
    """Read the R2 ``latest`` pointer and return it as an aware UTC datetime."""
    try:
        from pipeline.scripts.sync_r2 import _read_latest

        ts = _read_latest(fs, bucket)
        if not ts:
            return None
        return datetime.strptime(ts, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except Exception:
        return None


def _local_mtime(db_path: Path, watchlist: Path) -> datetime:
    """Return the oldest mtime among files that actually exist."""
    mtimes = []
    for p in (db_path, watchlist):
        try:
            mtimes.append(p.stat().st_mtime)
        except FileNotFoundError:
            pass
    if not mtimes:
        return datetime.fromtimestamp(0, tz=UTC)  # epoch → always stale
    return datetime.fromtimestamp(min(mtimes), tz=UTC)


def _is_stale(db_path: Path, watchlist: Path, fs, bucket: str) -> bool:
    """Return True if the R2 latest snapshot is newer than the oldest local file."""
    remote_dt = _remote_timestamp(fs, bucket)
    if remote_dt is None:
        return False  # can't determine → assume current
    local_dt = _local_mtime(db_path, watchlist)
    stale = remote_dt > local_dt
    if stale:
        logger.info(
            "Staleness check: remote=%s local=%s → re-download triggered",
            remote_dt.strftime("%Y%m%dT%H%M%SZ"),
            local_dt.strftime("%Y%m%dT%H%M%SZ"),
        )
    else:
        logger.debug(
            "Staleness check: local files are current (remote=%s)",
            remote_dt.strftime("%Y%m%dT%H%M%SZ"),
        )
    return stale


def _region_for_db(db_path: Path) -> str:
    return _STEM_TO_REGION.get(db_path.stem, "singapore")


def _build_fs(bucket: str):
    """Build the R2 filesystem (anonymous for public bucket reads)."""
    import sys

    scripts_dir = str(Path(__file__).parents[3] / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)

    from pipeline.scripts.sync_r2 import _build_r2_fs

    anon = not _r2_configured()
    return _build_r2_fs(anonymous=anon)


def maybe_pull() -> None:
    """Pull data from R2 if the local cache is missing or stale.

    Blocking — the app must not serve requests before data is ready.
    Set ``AUTO_PULL=0`` to skip entirely (useful in offline environments).
    """
    if not _auto_pull_enabled():
        return

    from dotenv import load_dotenv

    load_dotenv()

    import os as _os

    from pipeline.scripts.sync_r2 import _DEFAULT_BUCKET

    db_path = Path(_default_db_path())
    data_dir = db_path.parent
    watchlist = _watchlist_path(data_dir)
    bucket = _os.getenv("S3_BUCKET", _DEFAULT_BUCKET)

    present = _cache_present(db_path, watchlist)

    # Build fs once (used for both staleness check and potential pull)
    try:
        fs = _build_fs(bucket)
    except Exception as exc:
        if not present:
            logger.warning(
                "Cannot connect to R2 and local cache is missing (%s): %s. "
                "The dashboard will show empty data.",
                db_path,
                exc,
            )
        return

    if present:
        if not _is_stale(db_path, watchlist, fs, bucket):
            return  # files exist and are current — nothing to do
        logger.info("Local data at %s is stale. Re-downloading from R2 …", data_dir)
    else:
        logger.info("Local data cache not found at %s. Pulling from R2 …", data_dir)

    region = _region_for_db(db_path)
    try:
        _pull(region, db_path, fs, bucket)
    except Exception as exc:
        # Only hard-fail when credentials are configured (intentional push
        # environment).  In anonymous / CI / offline contexts, degrade
        # gracefully so the app still starts with empty data.
        if _r2_configured():
            raise RuntimeError(
                f"Auto-pull from R2 failed for region '{region}': {exc}\n"
                "Check your R2 credentials in .env or pull manually:\n"
                "  uv run python scripts/sync_r2.py pull"
            ) from exc
        logger.warning(
            "Auto-pull from R2 skipped (%s). "
            "The dashboard will show empty data. "
            "Pull manually: uv run python scripts/sync_r2.py pull",
            exc,
        )


def _pull(region: str, db_path: Path, fs, bucket: str) -> None:
    """Invoke the sync_r2 pull logic directly (no subprocess)."""
    from pipeline.scripts.sync_r2 import _pull_zip, _read_latest

    data_dir = db_path.parent
    data_dir.mkdir(parents=True, exist_ok=True)

    timestamp = _read_latest(fs, bucket)
    if not timestamp:
        raise RuntimeError(
            f"No 'latest' pointer found at {bucket}/latest. "
            "Run: uv run python scripts/sync_r2.py push"
        )

    logger.info("Downloading %s.zip for region '%s' …", timestamp, region)
    downloaded = _pull_zip(fs, bucket, timestamp, data_dir, [region])
    logger.info("Auto-pull complete: %.1f MB downloaded to %s.", downloaded / 1_048_576, data_dir)

    # Restore analyst reviews from R2 so they survive data refreshes (#264).
    # Silently skipped if no reviews.parquet exists yet in R2.
    try:
        import argparse as _argparse

        from pipeline.scripts.sync_r2 import cmd_pull_reviews

        _review_args = _argparse.Namespace(db=str(db_path))
        rc = cmd_pull_reviews(_review_args)
        if rc == 0:
            logger.info("Analyst reviews restored from R2.")
    except Exception as exc:
        logger.debug("pull-reviews skipped: %s", exc)
