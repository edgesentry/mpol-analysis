"""Shared DuckDB connection for the API.

DuckDB does not support multiple concurrent connections to the same file with
different configurations (read-only vs read-write).  All API routes must share
a single connection object so DuckDB sees consistent configuration.

The connection is opened once at first access and reused for the lifetime of
the process.  A threading.Lock serialises concurrent requests so the
synchronous DuckDB driver is not called from multiple threads simultaneously.
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from typing import Generator

import duckdb

_DEFAULT_DB_PATH = "data/processed/mpol.duckdb"

_conn: duckdb.DuckDBPyConnection | None = None
_lock = threading.Lock()


def _db_path() -> str:
    return os.getenv("DB_PATH", _DEFAULT_DB_PATH)


@contextmanager
def get_conn() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Yield the shared DuckDB connection under the global lock.

    Usage::

        from src.api.db import get_conn

        with get_conn() as con:
            rows = con.execute("SELECT ...").fetchall()
    """
    global _conn
    with _lock:
        if _conn is None:
            path = _db_path()
            if os.path.exists(path):
                _conn = duckdb.connect(path)
        if _conn is None:
            yield None  # type: ignore[misc]
            return
        yield _conn
