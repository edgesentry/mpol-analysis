"""
Tests for ownership graph feature engineering — focusing on the DuckDB sanctions
fallback introduced in issue #231 and the STS hub degree fixes in issue #233.
"""

import duckdb
import polars as pl
import pyarrow as pa

from pipeline.src.features.ownership_graph import (
    MAX_HOPS,
    _apply_direct_sanctions_fallback,
    _compute_sts_hub_degree,
)
from pipeline.src.graph.store import NODE_SCHEMAS, REL_SCHEMAS

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


# ---------------------------------------------------------------------------
# _compute_sts_hub_degree
# ---------------------------------------------------------------------------


def _make_sts_tables(vessel_mmsis: list[str], pairs: list[tuple[str, str]]) -> dict:
    """Build minimal tables dict for _compute_sts_hub_degree tests."""
    vessel_table = pa.table(
        {"mmsi": vessel_mmsis, "imo": [""] * len(vessel_mmsis), "name": [""] * len(vessel_mmsis)},
        schema=NODE_SCHEMAS["Vessel"],
    )
    sts_table = pa.table(
        {"src_id": [p[0] for p in pairs], "dst_id": [p[1] for p in pairs]},
        schema=REL_SCHEMAS["STS_CONTACT"],
    )
    return {"Vessel": vessel_table, "STS_CONTACT": sts_table}


def test_sts_hub_degree_empty_contacts():
    """Vessels with no STS contacts get degree 0."""
    tables = _make_sts_tables(["111111111", "222222222"], [])
    result = _compute_sts_hub_degree(tables)
    assert result.filter(pl.col("mmsi") == "111111111")["sts_hub_degree"][0] == 0
    assert result.filter(pl.col("mmsi") == "222222222")["sts_hub_degree"][0] == 0


def test_sts_hub_degree_src_side_counted():
    """A vessel that appears as src_id gets degree = number of distinct dst partners."""
    tables = _make_sts_tables(
        ["111111111", "222222222"],
        [("111111111", "222222222")],
    )
    result = _compute_sts_hub_degree(tables)
    assert result.filter(pl.col("mmsi") == "111111111")["sts_hub_degree"][0] == 1


def test_sts_hub_degree_dst_side_counted():
    """A vessel that appears only as dst_id must also get a non-zero degree.

    This is the bidirectionality fix: STS_CONTACT stores pairs with src < dst,
    so without the union both_dirs trick, the dst vessel would always get 0.
    """
    # "222222222" > "111111111", so 111111111 is src and 222222222 is dst
    tables = _make_sts_tables(
        ["111111111", "222222222"],
        [("111111111", "222222222")],
    )
    result = _compute_sts_hub_degree(tables)
    # 222222222 appears only as dst_id — must still get degree 1
    assert result.filter(pl.col("mmsi") == "222222222")["sts_hub_degree"][0] == 1


def test_sts_hub_degree_hub_vessel():
    """A vessel with three distinct STS partners gets degree 3."""
    tables = _make_sts_tables(
        ["111111111", "222222222", "333333333", "444444444"],
        [
            ("111111111", "222222222"),
            ("111111111", "333333333"),
            ("111111111", "444444444"),
        ],
    )
    result = _compute_sts_hub_degree(tables)
    assert result.filter(pl.col("mmsi") == "111111111")["sts_hub_degree"][0] == 3
