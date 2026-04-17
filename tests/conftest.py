import pytest

from pipeline.src.ingest.schema import init_schema


@pytest.fixture
def tmp_db(tmp_path):
    """Temporary DuckDB with the full schema initialised."""
    db_path = str(tmp_path / "test.duckdb")
    init_schema(db_path)
    return db_path
