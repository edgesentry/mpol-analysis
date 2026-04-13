"""
Tests for ownership graph feature engineering — focusing on the DuckDB sanctions
fallback introduced in issue #231.
"""

import duckdb
import polars as pl

from src.features.ownership_graph import MAX_HOPS, _apply_direct_sanctions_fallback

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_sanctions_mmsi(db_path: str, mmsi_values: list[str]) -> None:
    con = duckdb.connect(db_path)
    for i, mmsi in enumerate(mmsi_values):
        con.execute(
            "INSERT OR IGNORE INTO sanctions_entities "
            "(entity_id, name, mmsi, imo, flag, type, list_source) "
            "VALUES (?, ?, ?, NULL, NULL, 'Vessel', 'us_ofac_sdn')",
            [f"v-{i}", f"VESSEL {i}", mmsi],
        )
    con.close()


# ---------------------------------------------------------------------------
# _apply_direct_sanctions_fallback
# ---------------------------------------------------------------------------


def test_fallback_sets_distance_zero_for_sanctioned_mmsi(tmp_db):
    """A vessel with graph-derived distance=99 whose MMSI is in sanctions_entities
    must have its distance corrected to 0 by the DuckDB fallback."""
    _seed_sanctions_mmsi(tmp_db, ["613490000"])

    sd_df = pl.DataFrame(
        {"mmsi": ["613490000", "999000000"], "sanctions_distance": [MAX_HOPS, MAX_HOPS]},
        schema={"mmsi": pl.Utf8, "sanctions_distance": pl.Int32},
    )
    result = _apply_direct_sanctions_fallback(sd_df, tmp_db)

    row = result.filter(pl.col("mmsi") == "613490000")
    assert row["sanctions_distance"][0] == 0


def test_fallback_does_not_affect_non_sanctioned_vessel(tmp_db):
    """Vessels not in sanctions_entities must keep their graph-derived distance."""
    _seed_sanctions_mmsi(tmp_db, ["613490000"])

    sd_df = pl.DataFrame(
        {"mmsi": ["613490000", "999000000"], "sanctions_distance": [MAX_HOPS, MAX_HOPS]},
        schema={"mmsi": pl.Utf8, "sanctions_distance": pl.Int32},
    )
    result = _apply_direct_sanctions_fallback(sd_df, tmp_db)

    row = result.filter(pl.col("mmsi") == "999000000")
    assert row["sanctions_distance"][0] == MAX_HOPS


def test_fallback_does_not_downgrade_existing_distance(tmp_db):
    """A vessel already at distance=1 (graph-derived) must stay at 1, not be
    upgraded to 0 just because its MMSI also appears in sanctions_entities."""
    _seed_sanctions_mmsi(tmp_db, ["123456789"])

    sd_df = pl.DataFrame(
        {"mmsi": ["123456789"], "sanctions_distance": [1]},
        schema={"mmsi": pl.Utf8, "sanctions_distance": pl.Int32},
    )
    result = _apply_direct_sanctions_fallback(sd_df, tmp_db)

    assert result["sanctions_distance"][0] == 1


def test_fallback_handles_empty_dataframe(tmp_db):
    """Empty input must pass through without error."""
    sd_df = pl.DataFrame(schema={"mmsi": pl.Utf8, "sanctions_distance": pl.Int32})
    result = _apply_direct_sanctions_fallback(sd_df, tmp_db)
    assert result.is_empty()


def test_fallback_handles_no_sanctions_in_db(tmp_db):
    """If sanctions_entities has no MMSI rows the input must be returned unchanged."""
    sd_df = pl.DataFrame(
        {"mmsi": ["613490000"], "sanctions_distance": [MAX_HOPS]},
        schema={"mmsi": pl.Utf8, "sanctions_distance": pl.Int32},
    )
    result = _apply_direct_sanctions_fallback(sd_df, tmp_db)
    assert result["sanctions_distance"][0] == MAX_HOPS


def test_fallback_corrects_vessel_not_in_lance_graph(tmp_db):
    """The primary use-case: a vessel in ais_positions (hence in the full merged
    feature matrix) but absent from vessel_meta (hence absent from the Lance Graph)
    must have its sanctions_distance corrected when its MMSI is in sanctions_entities.

    This mirrors the build_matrix.py call-site where the fallback receives the
    full 1,240-vessel merged DataFrame, not just the 14-vessel Lance Graph output."""
    _seed_sanctions_mmsi(tmp_db, ["613490000", "620999538"])

    # Simulate the full merged DataFrame: these vessels came via ais_positions
    # but were not in vessel_meta, so the Lance Graph merge gave them distance=99
    full_matrix = pl.DataFrame(
        {
            "mmsi": ["613490000", "620999538", "444000000"],
            "sanctions_distance": [MAX_HOPS, MAX_HOPS, MAX_HOPS],
        },
        schema={"mmsi": pl.Utf8, "sanctions_distance": pl.Int32},
    )
    result = _apply_direct_sanctions_fallback(full_matrix, tmp_db)

    assert result.filter(pl.col("mmsi") == "613490000")["sanctions_distance"][0] == 0
    assert result.filter(pl.col("mmsi") == "620999538")["sanctions_distance"][0] == 0
    assert result.filter(pl.col("mmsi") == "444000000")["sanctions_distance"][0] == MAX_HOPS
