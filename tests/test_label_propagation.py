import duckdb
import pyarrow as pa
import pytest

from src.analysis.label_propagation import propagate_labels
from src.graph.store import REL_SCHEMAS, write_tables
from src.ingest.schema import init_schema


@pytest.fixture
def prop_db(tmp_path):
    db_path = str(tmp_path / "prop.duckdb")
    init_schema(db_path)
    return db_path


def _seed_confirmed(db_path: str, mmsi: str, reviewed_at: str = "2026-03-01T00:00:00Z") -> None:
    con = duckdb.connect(db_path)
    try:
        con.execute(
            "INSERT INTO vessel_reviews (mmsi, review_tier, handoff_state, reviewed_by, reviewed_at) "
            "VALUES (?, 'confirmed', 'handoff_completed', 'analyst', ?)",
            [mmsi, reviewed_at],
        )
    finally:
        con.close()


def _write_owned_by(db_path: str, edges: list[tuple[str, str]]) -> None:
    src_ids = [e[0] for e in edges]
    dst_ids = [e[1] for e in edges]
    table = pa.table(
        {
            "src_id": src_ids,
            "dst_id": dst_ids,
            "since": [""] * len(edges),
            "until": [""] * len(edges),
        },
        schema=REL_SCHEMAS["OWNED_BY"],
    )
    write_tables(db_path, {"OWNED_BY": table})


def _write_managed_by(db_path: str, edges: list[tuple[str, str]]) -> None:
    src_ids = [e[0] for e in edges]
    dst_ids = [e[1] for e in edges]
    table = pa.table(
        {
            "src_id": src_ids,
            "dst_id": dst_ids,
            "since": [""] * len(edges),
            "until": [""] * len(edges),
        },
        schema=REL_SCHEMAS["MANAGED_BY"],
    )
    write_tables(db_path, {"MANAGED_BY": table})


def _write_sts_contact(db_path: str, edges: list[tuple[str, str]]) -> None:
    table = pa.table(
        {
            "src_id": [e[0] for e in edges],
            "dst_id": [e[1] for e in edges],
        },
        schema=REL_SCHEMAS["STS_CONTACT"],
    )
    write_tables(db_path, {"STS_CONTACT": table})


# ── Tests ────────────────────────────────────────────────────────────────────

def test_propagation_no_confirmed_labels(prop_db):
    df, result = propagate_labels(prop_db)
    assert result.seed_count == 0
    assert result.propagated_count == 0
    assert df.is_empty()


def test_propagation_seed_only_no_graph_data(prop_db):
    _seed_confirmed(prop_db, "111")
    df, result = propagate_labels(prop_db)
    assert result.seed_count == 1
    assert result.propagated_count == 0
    assert len(df) == 1
    row = df.to_dicts()[0]
    assert row["mmsi"] == "111"
    assert row["hop"] == 0
    assert row["evidence_type"] == "confirmed_direct"
    assert row["propagated_confidence"] == pytest.approx(1.0)


def test_propagation_shared_owner(prop_db):
    _seed_confirmed(prop_db, "111")
    # vessel "222" shares owner company-A with confirmed "111"
    _write_owned_by(prop_db, [("111", "company-A"), ("222", "company-A"), ("333", "company-B")])

    df, result = propagate_labels(prop_db)

    assert result.seed_count == 1
    assert result.propagated_count == 1
    assert result.total_vessels == 2

    mmsi_set = set(df["mmsi"].to_list())
    assert "111" in mmsi_set
    assert "222" in mmsi_set
    assert "333" not in mmsi_set

    peer_row = df.filter(df["mmsi"] == "222").to_dicts()[0]
    assert peer_row["hop"] == 1
    assert peer_row["evidence_type"] == "shared_owner"
    assert peer_row["source_mmsi"] == "111"
    assert peer_row["propagated_confidence"] == pytest.approx(0.65, abs=1e-3)


def test_propagation_shared_manager(prop_db):
    _seed_confirmed(prop_db, "444")
    _write_managed_by(prop_db, [("444", "mgr-X"), ("555", "mgr-X")])

    df, result = propagate_labels(prop_db)

    assert result.propagated_count == 1
    peer_row = df.filter(df["mmsi"] == "555").to_dicts()[0]
    assert peer_row["evidence_type"] == "shared_manager"
    assert peer_row["propagated_confidence"] == pytest.approx(0.60, abs=1e-3)


def test_propagation_sts_contact(prop_db):
    _seed_confirmed(prop_db, "777")
    _write_sts_contact(prop_db, [("777", "888")])

    df, result = propagate_labels(prop_db)

    assert result.propagated_count == 1
    peer_row = df.filter(df["mmsi"] == "888").to_dicts()[0]
    assert peer_row["evidence_type"] == "sts_contact"
    assert peer_row["propagated_confidence"] == pytest.approx(0.50, abs=1e-3)


def test_propagation_sts_contact_bidirectional(prop_db):
    """STS edges should propagate regardless of which vessel is src/dst."""
    _seed_confirmed(prop_db, "999")
    # seed is the dst_id — should still propagate to src_id "100"
    _write_sts_contact(prop_db, [("100", "999")])

    df, result = propagate_labels(prop_db)

    assert result.propagated_count == 1
    assert "100" in set(df["mmsi"].to_list())


def test_propagation_no_duplicate_when_multiple_evidence(prop_db):
    """A peer vessel found via both owner and manager should appear only once."""
    _seed_confirmed(prop_db, "A11")
    _write_owned_by(prop_db, [("A11", "co1"), ("B22", "co1")])
    _write_managed_by(prop_db, [("A11", "mgr1"), ("B22", "mgr1")])

    df, result = propagate_labels(prop_db)
    b22_rows = df.filter(df["mmsi"] == "B22")
    assert len(b22_rows) == 1  # deduplicated


def test_propagation_confirmed_vessel_not_in_propagated(prop_db):
    """A confirmed vessel that is also a peer of another confirmed vessel should not appear twice."""
    _seed_confirmed(prop_db, "C11")
    _seed_confirmed(prop_db, "D22")
    _write_owned_by(prop_db, [("C11", "co2"), ("D22", "co2")])

    df, result = propagate_labels(prop_db)

    # Both are seeds — neither should appear as propagated
    assert result.seed_count == 2
    assert result.propagated_count == 0
    c_rows = df.filter(df["mmsi"] == "C11")
    d_rows = df.filter(df["mmsi"] == "D22")
    assert len(c_rows) == 1 and c_rows.to_dicts()[0]["hop"] == 0
    assert len(d_rows) == 1 and d_rows.to_dicts()[0]["hop"] == 0


def test_propagation_as_of_utc_cutoff(prop_db):
    """Labels after the cutoff should not be included."""
    _seed_confirmed(prop_db, "E11", reviewed_at="2026-05-01T00:00:00Z")
    df, result = propagate_labels(prop_db, as_of_utc="2026-04-01T00:00:00Z")
    assert result.seed_count == 0
    assert df.is_empty()
