"""Centralised storage configuration for local and S3-compatible backends.

App users keep all data under ``~/.arktrace/data/`` (pulled from R2 via
sync_r2.py / bootstrap.py).  Pipeline and CI workflows write intermediate
outputs to ``data/processed/`` on the operator's machine; those files are
never shipped to app users directly.

S3 mode is only enabled when USE_S3=1 is explicitly set, allowing R2
credentials to live in .env for push/pull without accidentally routing all
app reads to the remote bucket.

Environment variables
---------------------
USE_S3                Set to "1" or "true" to route all data reads/writes to S3.
                      Default: off (local disk).
ARKTRACE_DATA_DIR     Override the user data directory (default: ~/.arktrace/data).
S3_BUCKET             Bucket name (required when USE_S3=1)
S3_ENDPOINT           Custom endpoint URL for R2 / MinIO
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION            Default: us-east-1
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _canonical_data_dir() -> str:
    """User-level data directory: ARKTRACE_DATA_DIR env var or ~/.arktrace/data.

    Mirrors bootstrap._default_data_dir() — kept here so config.py has no
    import dependency on bootstrap.py.
    """
    if explicit := os.getenv("ARKTRACE_DATA_DIR"):
        return str(Path(explicit).expanduser())
    return str(Path.home() / ".arktrace" / "data")


def is_s3() -> bool:
    """True only when USE_S3=1 is explicitly set.

    R2 credentials (S3_BUCKET, AWS_ACCESS_KEY_ID, etc.) are used by sync_r2.py
    for push/pull but must not activate S3 mode for the app itself — the app
    always reads from local disk unless USE_S3 is explicitly opted in.
    """
    val = os.getenv("USE_S3", "0").lower()
    return val in ("1", "true", "yes")


_DEFAULT_BUCKET = "arktrace-public"
_DEFAULT_ENDPOINT = "https://b8a0b09feb89390fb6e8cf4ef9294f48.r2.cloudflarestorage.com"
_DEFAULT_REGION = "auto"


def _bucket() -> str:
    return os.getenv("S3_BUCKET", _DEFAULT_BUCKET)


# ---------------------------------------------------------------------------
# Storage options for each library
# ---------------------------------------------------------------------------


def polars_storage_options() -> dict[str, str] | None:
    """Storage options for Polars read_parquet / scan_parquet (object_store format)."""
    if not is_s3():
        return None
    opts: dict[str, str] = {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "aws_region": os.getenv("AWS_REGION", _DEFAULT_REGION),
    }
    endpoint = os.getenv("S3_ENDPOINT", _DEFAULT_ENDPOINT)
    if endpoint:
        opts["aws_endpoint_url"] = endpoint
        opts["aws_allow_http"] = "true"
    return opts


def lance_storage_options() -> dict[str, str] | None:
    """Storage options for lance.write_dataset / lance.dataset (object_store format)."""
    if not is_s3():
        return None
    opts: dict[str, str] = {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "aws_region": os.getenv("AWS_REGION", _DEFAULT_REGION),
    }
    endpoint = os.getenv("S3_ENDPOINT", _DEFAULT_ENDPOINT)
    if endpoint:
        opts["aws_endpoint"] = endpoint
        opts["aws_allow_http"] = "true"
    return opts


# ---------------------------------------------------------------------------
# URI resolution
# ---------------------------------------------------------------------------


def graph_uri(db_path: str) -> str:
    """Lance Graph root URI.

    Returns ``s3://<bucket>/<stem>_graph`` when S3 is enabled, otherwise the
    local sibling directory ``<db_parent>/<stem>_graph``.
    """
    stem = Path(db_path).stem
    if is_s3():
        return f"s3://{_bucket()}/{stem}_graph"
    return str(Path(db_path).parent / f"{stem}_graph")


def lance_db_uri() -> str:
    """LanceDB URI for GDELT event store.

    Returns ``s3://<bucket>/gdelt.lance`` when S3 is enabled, otherwise the
    value of ``GDELT_LANCE_PATH`` (default ``~/.arktrace/data/gdelt.lance``).
    """
    if is_s3():
        return f"s3://{_bucket()}/gdelt.lance"
    return os.getenv(
        "GDELT_LANCE_PATH",
        str(Path(_canonical_data_dir()) / "gdelt.lance"),
    )


def output_uri(filename: str) -> str:
    """Output artifact URI for app data files.

    Returns ``s3://<bucket>/processed/<filename>`` when S3 is enabled,
    otherwise ``<DATA_DIR>/<filename>`` (default ``~/.arktrace/data``).
    App users keep all data under ``~/.arktrace/data/``; pipeline/CI
    operators can override with ``DATA_DIR=data/processed``.
    """
    if is_s3():
        return f"s3://{_bucket()}/processed/{filename}"
    data_dir = os.getenv("DATA_DIR", _canonical_data_dir())
    return os.path.join(data_dir, filename)


# Maps DuckDB filename stem → region watchlist filename.
# Mirrors the RegionPreset definitions in scripts/run_pipeline.py.
_DB_STEM_TO_WATCHLIST: dict[str, str] = {
    "singapore": "singapore_watchlist.parquet",
    "japansea": "japansea_watchlist.parquet",
    "middleeast": "middleeast_watchlist.parquet",
    "europe": "europe_watchlist.parquet",
    "gulf": "gulf_watchlist.parquet",
}


def watchlist_uri() -> str:
    """Resolve the watchlist path for the currently configured region.

    Priority:
      1. ``WATCHLIST_OUTPUT_PATH`` env var (explicit override)
      2. Derived from ``DB_PATH`` stem — e.g. ``singapore.duckdb`` → ``singapore_watchlist.parquet``
      3. Default ``candidate_watchlist.parquet``

    Called at request time (not at import time) so that ``DB_PATH`` changes
    between test runs or region switches are picked up automatically.
    """
    explicit = os.getenv("WATCHLIST_OUTPUT_PATH")
    if explicit:
        return explicit
    db_stem = Path(os.getenv("DB_PATH", str(Path(_canonical_data_dir()) / "singapore.duckdb"))).stem
    filename = _DB_STEM_TO_WATCHLIST.get(db_stem, "candidate_watchlist.parquet")
    return output_uri(filename)


# ---------------------------------------------------------------------------
# Parquet I/O helpers
# ---------------------------------------------------------------------------


def write_parquet(df: pl.DataFrame, uri: str) -> None:
    """Write a Polars DataFrame to Parquet at a local path or S3 URI."""
    if uri.startswith("s3://"):
        _write_parquet_s3(df, uri)
    else:
        os.makedirs(os.path.dirname(uri) or ".", exist_ok=True)
        df.write_parquet(uri)


def _write_parquet_s3(df: pl.DataFrame, uri: str) -> None:
    """Write via PyArrow S3FileSystem (already a transitive dependency)."""
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    without_scheme = uri[len("s3://") :]
    bucket, _, key = without_scheme.partition("/")

    endpoint = os.getenv("S3_ENDPOINT", _DEFAULT_ENDPOINT)
    fs_kwargs: dict[str, str] = {
        "access_key": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "region": os.getenv("AWS_REGION", _DEFAULT_REGION),
    }
    if endpoint:
        fs_kwargs["endpoint_override"] = endpoint.split("://", 1)[-1]
        fs_kwargs["scheme"] = "http" if endpoint.startswith("http://") else "https"

    s3 = pafs.S3FileSystem(**fs_kwargs)
    pq.write_table(df.to_arrow(), f"{bucket}/{key}", filesystem=s3)


def read_parquet(uri: str) -> pl.DataFrame | None:
    """Read Parquet from a local path or S3 URI.

    Returns ``None`` if the file/object does not exist.
    """
    import polars as pl

    if uri.startswith("s3://"):
        try:
            return pl.read_parquet(uri, storage_options=polars_storage_options())
        except Exception:
            return None
    if not os.path.exists(uri):
        return None
    return pl.read_parquet(uri)
