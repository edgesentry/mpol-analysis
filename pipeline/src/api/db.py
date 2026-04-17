"""Shared DuckDB connection for the API.

DuckDB does not support multiple concurrent connections to the same file with
different configurations (read-only vs read-write).  All API routes must share
a single connection object so DuckDB sees consistent configuration.

The connection is opened once at first access and reused for the lifetime of
the process.  A threading.Lock serialises concurrent requests so the
synchronous DuckDB driver is not called from multiple threads simultaneously.
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from pipeline.src.storage.config import _canonical_data_dir

_DEFAULT_DB_PATH = str(Path(_canonical_data_dir()) / "singapore.duckdb")

logger = logging.getLogger(__name__)

_conn: duckdb.DuckDBPyConnection | None = None
_conn_path: str | None = None
_lock = threading.Lock()


def _db_path() -> str:
    return os.getenv("DB_PATH", _DEFAULT_DB_PATH)


@contextmanager
def get_conn() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Yield the shared DuckDB connection under the global lock.

    The connection is reopened automatically when DB_PATH changes (e.g. between
    test runs that each use a separate tmp_db).

    Usage::

        from pipeline.src.api.db import get_conn

        with get_conn() as con:
            rows = con.execute("SELECT ...").fetchall()
    """
    global _conn, _conn_path
    with _lock:
        current_path = _db_path()
        if _conn is not None and _conn_path != current_path:
            # DB_PATH changed (e.g. test isolation) — close and reopen.
            try:
                _conn.close()
            except Exception:
                # Best-effort close during connection rotation; keep behavior
                # non-fatal, but record details for troubleshooting.
                logger.debug(
                    "Ignoring DuckDB close failure while rotating connection", exc_info=True
                )
            _conn = None
            _conn_path = None
        if _conn is None:
            if os.path.exists(current_path):
                _conn = duckdb.connect(current_path)
                _conn_path = current_path
        if _conn is None:
            yield None  # type: ignore[misc]
            return
        yield _conn
