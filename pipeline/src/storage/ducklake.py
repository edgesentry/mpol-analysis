"""DuckLake catalog management for arktrace pipeline outputs.

DuckLake (v1.0) stores table metadata in a .duckdb catalog file; actual data is
materialised to Parquet files on CHECKPOINT.  This module provides helpers to:

  1. Open / create a local DuckLake catalog.
  2. Write a Polars DataFrame to a named table in the catalog.
  3. Checkpoint the catalog (materialise inlined data → Parquet files).
  4. Return the list of materialised Parquet files so sync_r2.py can upload them.

The catalog lives at ``data/processed/ducklake/catalog.duckdb`` by default.
Parquet files land in ``data/processed/ducklake/data/``.

After checkout_ducklake.py runs, upload with::

    uv run python scripts/sync_r2.py push-ducklake-public
    uv run python scripts/sync_r2.py push-ducklake-private
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

_DEFAULT_CATALOG_DIR = Path("data/processed/ducklake")
DEFAULT_CATALOG_PATH = _DEFAULT_CATALOG_DIR / "catalog.duckdb"
DEFAULT_DATA_PATH = _DEFAULT_CATALOG_DIR / "data"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_ducklake(con: duckdb.DuckDBPyConnection) -> None:
    """Load the ducklake extension into *con* (INSTALL if not already present)."""
    try:
        con.execute("LOAD ducklake")
    except duckdb.CatalogException:
        con.execute("INSTALL ducklake")
        con.execute("LOAD ducklake")


def _open_catalog(
    catalog_path: str | Path,
    data_path: str | Path,
    name: str = "lake",
) -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection with the DuckLake catalog attached.

    The catalog .duckdb file and data directory are created if they do not exist.
    """
    catalog_path = Path(catalog_path)
    data_path = Path(data_path)
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    data_path.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(":memory:")
    _load_ducklake(con)
    con.execute(
        f"ATTACH 'ducklake:{catalog_path.resolve()}' AS {name} (DATA_PATH '{data_path.resolve()}')"
    )
    return con


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def write_table(
    df: pl.DataFrame,
    table_name: str,
    catalog_path: str | Path = DEFAULT_CATALOG_PATH,
    data_path: str | Path = DEFAULT_DATA_PATH,
    *,
    replace: bool = True,
) -> None:
    """Write *df* to *table_name* in the DuckLake catalog.

    Data is initially inlined in the catalog .duckdb file; call
    :func:`checkpoint` afterwards to materialise clean Parquet files.

    Args:
        df:           Polars DataFrame to write.
        table_name:   Target table name (e.g. ``"watchlist"``).
        catalog_path: Path to the DuckLake catalog .duckdb file.
        data_path:    Directory for materialised Parquet files.
        replace:      When True, CREATE OR REPLACE TABLE; otherwise INSERT INTO.
    """
    con = _open_catalog(catalog_path, data_path)
    con.register("_df_arrow", df.to_arrow())

    if replace:
        con.execute(f"CREATE OR REPLACE TABLE lake.{table_name} AS SELECT * FROM _df_arrow")
    else:
        # Ensure table exists first; append rows
        try:
            con.execute(f"INSERT INTO lake.{table_name} SELECT * FROM _df_arrow")
        except duckdb.CatalogException:
            con.execute(f"CREATE TABLE lake.{table_name} AS SELECT * FROM _df_arrow")
    con.close()


def checkpoint(
    catalog_path: str | Path = DEFAULT_CATALOG_PATH,
    data_path: str | Path = DEFAULT_DATA_PATH,
) -> list[Path]:
    """Checkpoint the DuckLake catalog, materialising all inlined data to Parquet.

    Returns the list of Parquet files created or updated under *data_path*.
    Intended to be called once after all :func:`write_table` calls for a pipeline run.
    """
    con = _open_catalog(catalog_path, data_path)
    con.execute("CHECKPOINT lake")
    con.close()

    data_path = Path(data_path)
    return sorted(data_path.rglob("*.parquet"))


def list_tables(
    catalog_path: str | Path = DEFAULT_CATALOG_PATH,
    data_path: str | Path = DEFAULT_DATA_PATH,
) -> list[str]:
    """Return names of all tables currently registered in the DuckLake catalog."""
    if not Path(catalog_path).exists():
        return []
    con = _open_catalog(catalog_path, data_path)
    rows = con.execute("SHOW ALL TABLES").fetchall()
    con.close()
    return [r[2] for r in rows if r[0] == "lake" and r[1] == "main"]


def read_table(
    table_name: str,
    catalog_path: str | Path = DEFAULT_CATALOG_PATH,
    data_path: str | Path = DEFAULT_DATA_PATH,
) -> pl.DataFrame:
    """Read a table from the DuckLake catalog into a Polars DataFrame."""
    con = _open_catalog(catalog_path, data_path)
    result = con.execute(f"SELECT * FROM lake.{table_name}").pl()
    con.close()
    return result
