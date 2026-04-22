"""Tests for sync_r2.py push-ais-dbs / pull-ais-dbs subcommands."""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import scripts.sync_r2 as sync_r2

# ---------------------------------------------------------------------------
# _ais_db_candidates
# ---------------------------------------------------------------------------


def test_candidates_skips_excluded_stems(tmp_path):
    """mpol, public_eval, backtest_demo, catalog are never returned."""
    for name in ("mpol.duckdb", "public_eval.duckdb", "backtest_demo.duckdb", "catalog.duckdb"):
        (tmp_path / name).write_bytes(b"x" * 2_000_000)

    assert sync_r2._ais_db_candidates(tmp_path, None) == []


def test_candidates_skips_small_dbs(tmp_path):
    """Files smaller than 1 MB are treated as placeholders and excluded."""
    (tmp_path / "japansea.duckdb").write_bytes(b"x" * 500_000)
    assert sync_r2._ais_db_candidates(tmp_path, None) == []


def test_candidates_returns_eligible_dbs(tmp_path):
    """Large non-excluded .duckdb files are returned."""
    for name in ("japansea.duckdb", "blacksea.duckdb"):
        (tmp_path / name).write_bytes(b"x" * 2_000_000)

    result = sync_r2._ais_db_candidates(tmp_path, None)
    assert {p.name for p in result} == {"japansea.duckdb", "blacksea.duckdb"}


def test_candidates_filters_by_region(tmp_path):
    """Only requested regions are returned when --regions is specified."""
    for name in ("japansea.duckdb", "blacksea.duckdb", "europe.duckdb"):
        (tmp_path / name).write_bytes(b"x" * 2_000_000)

    result = sync_r2._ais_db_candidates(tmp_path, ["japan", "europe"])
    assert {p.name for p in result} == {"japansea.duckdb", "europe.duckdb"}


def test_candidates_missing_region_db_skipped(tmp_path):
    """A requested region whose .duckdb does not exist is silently skipped."""
    (tmp_path / "japansea.duckdb").write_bytes(b"x" * 2_000_000)

    result = sync_r2._ais_db_candidates(tmp_path, ["japan", "middleeast"])
    assert [p.name for p in result] == ["japansea.duckdb"]


# ---------------------------------------------------------------------------
# cmd_push_ais_dbs
# ---------------------------------------------------------------------------


def _mock_duckdb_valid(row_count: int = 1000):
    """Return a context manager that mocks duckdb.connect to pass validation."""
    mock_con = MagicMock()
    mock_con.execute.return_value.fetchone.return_value = (row_count,)
    mock_con.__enter__ = MagicMock(return_value=mock_con)
    mock_con.__exit__ = MagicMock(return_value=False)
    return patch("duckdb.connect", return_value=mock_con)


@pytest.fixture
def push_args(tmp_path):
    return argparse.Namespace(data_dir=str(tmp_path), regions=None, force=False)


def test_push_returns_1_when_no_candidates(push_args, tmp_path):
    """Returns error code when no eligible DBs are found."""
    result = sync_r2.cmd_push_ais_dbs(push_args)
    assert result == 1


def test_push_uploads_all_eligible_dbs(push_args, tmp_path):
    """Calls _upload_file once per eligible DB."""
    for name in ("japansea.duckdb", "blacksea.duckdb"):
        (tmp_path / name).write_bytes(b"x" * 2_000_000)

    import pyarrow.fs as pafs

    mock_info = MagicMock()
    mock_info.type = pafs.FileType.NotFound

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [mock_info]

    with _mock_duckdb_valid():
        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs):
            with patch.object(sync_r2, "_upload_file", return_value=2_000_000) as mock_upload:
                result = sync_r2.cmd_push_ais_dbs(push_args)

    assert result == 0
    assert mock_upload.call_count == 2
    uploaded_names = {Path(c.args[1]).name for c in mock_upload.call_args_list}
    assert uploaded_names == {"japansea.duckdb", "blacksea.duckdb"}


def test_push_skips_invalid_db(tmp_path):
    """DB that fails validation (empty table) is skipped and returns 1."""
    (tmp_path / "japansea.duckdb").write_bytes(b"x" * 2_000_000)
    args = argparse.Namespace(data_dir=str(tmp_path), regions=None, force=True)

    with _mock_duckdb_valid(row_count=0):
        with patch.object(sync_r2, "_upload_file") as mock_upload:
            result = sync_r2.cmd_push_ais_dbs(args)

    assert result == 1
    mock_upload.assert_not_called()


def test_push_skips_when_remote_is_newer(push_args, tmp_path):
    """File is skipped when remote LastModified is newer than local mtime."""
    db = tmp_path / "japansea.duckdb"
    db.write_bytes(b"x" * 2_000_000)

    import datetime

    import pyarrow.fs as pafs

    mock_info = MagicMock()
    mock_info.type = pafs.FileType.File
    # Remote mtime far in the future
    mock_info.mtime = datetime.datetime(2099, 1, 1, tzinfo=datetime.UTC)

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [mock_info]

    with _mock_duckdb_valid():
        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs):
            with patch.object(sync_r2, "_upload_file") as mock_upload:
                result = sync_r2.cmd_push_ais_dbs(push_args)

    assert result == 0
    mock_upload.assert_not_called()


def test_push_force_uploads_even_when_remote_newer(tmp_path):
    """--force bypasses the mtime check and always uploads."""
    db = tmp_path / "japansea.duckdb"
    db.write_bytes(b"x" * 2_000_000)

    args = argparse.Namespace(data_dir=str(tmp_path), regions=None, force=True)

    mock_fs = MagicMock()

    with _mock_duckdb_valid():
        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs):
            with patch.object(sync_r2, "_upload_file", return_value=2_000_000) as mock_upload:
                result = sync_r2.cmd_push_ais_dbs(args)

    assert result == 0
    mock_upload.assert_called_once()


def test_push_regions_arg_filters_uploads(tmp_path):
    """--regions limits uploads to the specified subset."""
    for name in ("japansea.duckdb", "blacksea.duckdb", "europe.duckdb"):
        (tmp_path / name).write_bytes(b"x" * 2_000_000)

    args = argparse.Namespace(data_dir=str(tmp_path), regions="japan,blacksea", force=True)

    mock_fs = MagicMock()

    with _mock_duckdb_valid():
        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs):
            with patch.object(sync_r2, "_upload_file", return_value=2_000_000) as mock_upload:
                result = sync_r2.cmd_push_ais_dbs(args)

    assert result == 0
    uploaded_names = {Path(c.args[1]).name for c in mock_upload.call_args_list}
    assert uploaded_names == {"japansea.duckdb", "blacksea.duckdb"}


def test_push_r2_path_uses_private_bucket(tmp_path):
    """Uploaded R2 paths are under arktrace-private-capvista/ais-dbs/."""
    (tmp_path / "japansea.duckdb").write_bytes(b"x" * 2_000_000)
    args = argparse.Namespace(data_dir=str(tmp_path), regions=None, force=True)

    mock_fs = MagicMock()

    with _mock_duckdb_valid():
        with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs):
            with patch.object(sync_r2, "_upload_file", return_value=2_000_000) as mock_upload:
                sync_r2.cmd_push_ais_dbs(args)

    r2_path = mock_upload.call_args.args[2]
    assert r2_path == f"{sync_r2._PRIVATE_BUCKET}/ais-dbs/japansea.duckdb"


# ---------------------------------------------------------------------------
# cmd_pull_ais_dbs
# ---------------------------------------------------------------------------


@pytest.fixture
def pull_args(tmp_path):
    return argparse.Namespace(data_dir=str(tmp_path), regions=None, force=False)


def test_pull_returns_1_without_credentials(pull_args, monkeypatch):
    """Returns error code when AWS credentials are absent."""
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    result = sync_r2.cmd_pull_ais_dbs(pull_args)
    assert result == 1


def test_pull_downloads_all_remote_dbs(pull_args, tmp_path, monkeypatch):
    """Calls _download_file for each .duckdb found in the remote prefix."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")

    import datetime

    import pyarrow.fs as pafs

    past = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)

    def make_info(name):
        info = MagicMock()
        info.type = pafs.FileType.File
        info.path = f"{sync_r2._PRIVATE_BUCKET}/ais-dbs/{name}"
        info.size = 2_000_000
        info.mtime = past
        return info

    infos = [make_info("japansea.duckdb"), make_info("blacksea.duckdb")]

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = infos

    with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs):
        with patch.object(sync_r2, "_download_file", return_value=2_000_000) as mock_dl:
            result = sync_r2.cmd_pull_ais_dbs(pull_args)

    assert result == 0
    assert mock_dl.call_count == 2
    downloaded_names = {Path(c.args[2]).name for c in mock_dl.call_args_list}
    assert downloaded_names == {"japansea.duckdb", "blacksea.duckdb"}


def test_pull_skips_when_local_is_newer(tmp_path, monkeypatch):
    """File is skipped when local mtime is newer than remote LastModified."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")

    import datetime

    import pyarrow.fs as pafs

    # Write a local file with a recent mtime
    local = tmp_path / "japansea.duckdb"
    local.write_bytes(b"x" * 2_000_000)

    past = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)

    info = MagicMock()
    info.type = pafs.FileType.File
    info.path = f"{sync_r2._PRIVATE_BUCKET}/ais-dbs/japansea.duckdb"
    info.size = 2_000_000
    info.mtime = past

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [info]

    args = argparse.Namespace(data_dir=str(tmp_path), regions=None, force=False)
    with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs):
        with patch.object(sync_r2, "_download_file") as mock_dl:
            result = sync_r2.cmd_pull_ais_dbs(args)

    assert result == 0
    mock_dl.assert_not_called()


def test_pull_force_downloads_even_when_local_newer(tmp_path, monkeypatch):
    """--force bypasses mtime check and always downloads."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")

    import datetime

    import pyarrow.fs as pafs

    local = tmp_path / "japansea.duckdb"
    local.write_bytes(b"x" * 2_000_000)

    past = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)

    info = MagicMock()
    info.type = pafs.FileType.File
    info.path = f"{sync_r2._PRIVATE_BUCKET}/ais-dbs/japansea.duckdb"
    info.size = 2_000_000
    info.mtime = past

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [info]

    args = argparse.Namespace(data_dir=str(tmp_path), regions=None, force=True)
    with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs):
        with patch.object(sync_r2, "_download_file", return_value=2_000_000) as mock_dl:
            result = sync_r2.cmd_pull_ais_dbs(args)

    assert result == 0
    mock_dl.assert_called_once()


def test_pull_regions_filter(tmp_path, monkeypatch):
    """--regions limits downloads to the specified subset."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")

    import datetime

    import pyarrow.fs as pafs

    past = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)

    def make_info(name):
        info = MagicMock()
        info.type = pafs.FileType.File
        info.path = f"{sync_r2._PRIVATE_BUCKET}/ais-dbs/{name}"
        info.size = 2_000_000
        info.mtime = past
        return info

    infos = [make_info("japansea.duckdb"), make_info("blacksea.duckdb"), make_info("europe.duckdb")]

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = infos

    args = argparse.Namespace(data_dir=str(tmp_path), regions="japan", force=True)
    with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs):
        with patch.object(sync_r2, "_download_file", return_value=2_000_000) as mock_dl:
            result = sync_r2.cmd_pull_ais_dbs(args)

    assert result == 0
    assert mock_dl.call_count == 1
    assert Path(mock_dl.call_args.args[2]).name == "japansea.duckdb"


def test_pull_returns_0_when_bucket_empty(pull_args, monkeypatch):
    """Returns 0 (not an error) when no .duckdb files are in the bucket."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = []

    with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs):
        result = sync_r2.cmd_pull_ais_dbs(pull_args)

    assert result == 0
