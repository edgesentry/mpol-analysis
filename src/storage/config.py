"""Centralised storage configuration for local and S3-compatible backends.

When S3_BUCKET is set, all storage URIs resolve to s3://<bucket>/...
When it is not set, local paths under data/processed/ are used unchanged,
so the entire pipeline works without any S3 configuration.

Environment variables
---------------------
S3_BUCKET             Bucket name (enables S3 mode when set)
S3_ENDPOINT           Custom endpoint URL, e.g. http://minio:9000 (MinIO)
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


def is_s3() -> bool:
    """True when S3_BUCKET is set in the environment."""
    return bool(os.getenv("S3_BUCKET"))


def _bucket() -> str:
    return os.environ["S3_BUCKET"]


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
        "aws_region": os.getenv("AWS_REGION", "us-east-1"),
    }
    endpoint = os.getenv("S3_ENDPOINT")
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
        "aws_region": os.getenv("AWS_REGION", "us-east-1"),
    }
    endpoint = os.getenv("S3_ENDPOINT")
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
    value of ``GDELT_LANCE_PATH`` (default ``data/processed/gdelt.lance``).
    """
    if is_s3():
        return f"s3://{_bucket()}/gdelt.lance"
    return os.getenv("GDELT_LANCE_PATH", "data/processed/gdelt.lance")


def output_uri(filename: str) -> str:
    """Output artifact URI.

    Returns ``s3://<bucket>/processed/<filename>`` when S3 is enabled,
    otherwise ``<DATA_DIR>/<filename>`` (default ``data/processed``).
    """
    if is_s3():
        return f"s3://{_bucket()}/processed/{filename}"
    data_dir = os.getenv("DATA_DIR", "data/processed")
    return os.path.join(data_dir, filename)


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

    endpoint = os.getenv("S3_ENDPOINT", "")
    fs_kwargs: dict[str, str] = {
        "access_key": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "region": os.getenv("AWS_REGION", "us-east-1"),
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
