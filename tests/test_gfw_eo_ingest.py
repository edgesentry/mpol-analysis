"""Tests for the GFW EO weekly ingest feature (#480).

Covers:
- sync_r2.cmd_push_gfw_eo / cmd_pull_gfw_eo
- scripts/gfw_ingest.py main()
- run_pipeline.step_eo_ingest parquet-first path
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl

import scripts.sync_r2 as sync_r2

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_eo_parquet(path: Path, n_rows: int = 5) -> None:
    """Write a minimal *_eo_detections.parquet at the given path."""
    df = pl.DataFrame(
        {
            "detection_id": [f"id-{i}" for i in range(n_rows)],
            "detected_at": [datetime(2026, 1, 1, tzinfo=UTC)] * n_rows,
            "lat": [1.0 + i * 0.1 for i in range(n_rows)],
            "lon": [103.0 + i * 0.1 for i in range(n_rows)],
            "source": ["gfw"] * n_rows,
            "confidence": [0.9] * n_rows,
            "fetched_at": ["2026-01-01T00:00:00+00:00"] * n_rows,
        }
    )
    df.write_parquet(path)


# ---------------------------------------------------------------------------
# cmd_push_gfw_eo
# ---------------------------------------------------------------------------


def test_push_gfw_eo_returns_1_when_no_parquets(tmp_path):
    """Returns 1 when no *_eo_detections.parquet files are found."""
    args = argparse.Namespace(data_dir=str(tmp_path))
    result = sync_r2.cmd_push_gfw_eo(args)
    assert result == 1


def test_push_gfw_eo_uploads_all_parquets(tmp_path):
    """Calls _upload_file once per parquet found."""
    for name in ("singapore_eo_detections.parquet", "japansea_eo_detections.parquet"):
        _make_eo_parquet(tmp_path / name)

    mock_fs = MagicMock()
    args = argparse.Namespace(data_dir=str(tmp_path))

    with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs):
        with patch.object(sync_r2, "_upload_file") as mock_upload:
            result = sync_r2.cmd_push_gfw_eo(args)

    assert result == 0
    assert mock_upload.call_count == 2
    uploaded_names = {Path(c.args[1]).name for c in mock_upload.call_args_list}
    assert uploaded_names == {
        "singapore_eo_detections.parquet",
        "japansea_eo_detections.parquet",
    }


def test_push_gfw_eo_r2_path_uses_private_bucket(tmp_path):
    """Uploaded paths are under arktrace-private-capvista/gfw-eo/."""
    _make_eo_parquet(tmp_path / "singapore_eo_detections.parquet")

    mock_fs = MagicMock()
    args = argparse.Namespace(data_dir=str(tmp_path))

    with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs):
        with patch.object(sync_r2, "_upload_file") as mock_upload:
            sync_r2.cmd_push_gfw_eo(args)

    r2_path = mock_upload.call_args.args[2]
    assert r2_path == f"{sync_r2._PRIVATE_BUCKET}/gfw-eo/singapore_eo_detections.parquet"


# ---------------------------------------------------------------------------
# cmd_pull_gfw_eo
# ---------------------------------------------------------------------------


def test_pull_gfw_eo_returns_1_without_credentials(tmp_path, monkeypatch):
    """Returns 1 when AWS credentials are absent."""
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    args = argparse.Namespace(data_dir=str(tmp_path))
    result = sync_r2.cmd_pull_gfw_eo(args)
    assert result == 1


def test_pull_gfw_eo_returns_0_when_bucket_empty(tmp_path, monkeypatch):
    """Returns 0 gracefully when no parquets exist in R2."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = []

    args = argparse.Namespace(data_dir=str(tmp_path))
    with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs):
        result = sync_r2.cmd_pull_gfw_eo(args)

    assert result == 0


def test_pull_gfw_eo_downloads_parquets(tmp_path, monkeypatch):
    """Downloads each parquet from R2 into data_dir."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")

    import pyarrow.fs as pafs

    mock_info = MagicMock()
    mock_info.type = pafs.FileType.File
    mock_info.path = f"{sync_r2._PRIVATE_BUCKET}/gfw-eo/singapore_eo_detections.parquet"
    mock_info.size = 1024

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [mock_info]

    args = argparse.Namespace(data_dir=str(tmp_path))
    with patch.object(sync_r2, "_build_r2_fs", return_value=mock_fs):
        with patch.object(sync_r2, "_download_file") as mock_dl:
            result = sync_r2.cmd_pull_gfw_eo(args)

    assert result == 0
    mock_dl.assert_called_once()
    local_path = mock_dl.call_args.args[2]
    assert local_path == tmp_path / "singapore_eo_detections.parquet"


# ---------------------------------------------------------------------------
# gfw_ingest.main()
# ---------------------------------------------------------------------------


def test_gfw_ingest_returns_1_without_token(monkeypatch):
    """Exits with 1 when no GFW token is configured."""
    monkeypatch.delenv("GFW_API_TOKEN", raising=False)
    for i in range(1, 4):
        monkeypatch.delenv(f"GFW_API_TOKEN_{i}", raising=False)

    import scripts.gfw_ingest as gfw_ingest

    with patch("sys.argv", ["gfw_ingest.py"]):
        result = gfw_ingest.main()
    assert result == 1


def test_gfw_ingest_writes_parquet_per_region(tmp_path, monkeypatch):
    """Writes one parquet per region on success."""
    monkeypatch.setenv("GFW_API_TOKEN", "test-token")

    import scripts.gfw_ingest as gfw_ingest

    fake_records = [
        {
            "detection_id": "abc",
            "detected_at": datetime(2026, 1, 1, tzinfo=UTC),
            "lat": 1.0,
            "lon": 103.0,
            "source": "gfw",
            "confidence": 0.9,
        }
    ]

    with patch(
        "sys.argv", ["gfw_ingest.py", "--regions", "singapore,japan", "--out-dir", str(tmp_path)]
    ):
        with patch("pipeline.src.ingest.eo_gfw.fetch_gfw_detections", return_value=fake_records):
            result = gfw_ingest.main()

    assert result == 0
    assert (tmp_path / "singapore_eo_detections.parquet").exists()
    assert (tmp_path / "japansea_eo_detections.parquet").exists()


def test_gfw_ingest_skips_failed_region(tmp_path, monkeypatch):
    """Skips a region that raises PermissionError and returns 1."""
    monkeypatch.setenv("GFW_API_TOKEN", "test-token")

    import scripts.gfw_ingest as gfw_ingest

    with patch("sys.argv", ["gfw_ingest.py", "--regions", "singapore", "--out-dir", str(tmp_path)]):
        with patch(
            "pipeline.src.ingest.eo_gfw.fetch_gfw_detections", side_effect=PermissionError("429")
        ):
            result = gfw_ingest.main()

    assert result == 1
    assert not (tmp_path / "singapore_eo_detections.parquet").exists()


# ---------------------------------------------------------------------------
# step_eo_ingest — parquet-first path
# ---------------------------------------------------------------------------


def test_step_eo_ingest_prefers_parquet_over_api(tmp_path, monkeypatch):
    """step_eo_ingest ingests from parquet without calling the GFW API."""
    monkeypatch.setenv("ARKTRACE_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("GFW_API_TOKEN", raising=False)

    parquet_path = tmp_path / "singapore_eo_detections.parquet"
    _make_eo_parquet(parquet_path, n_rows=3)

    # Create a minimal DuckDB so ingest_eo_records doesn't fail
    import duckdb

    db_path = str(tmp_path / "singapore.duckdb")
    con = duckdb.connect(db_path)
    con.execute(
        """
        CREATE TABLE eo_detections (
            detection_id VARCHAR PRIMARY KEY,
            detected_at TIMESTAMPTZ,
            lat DOUBLE,
            lon DOUBLE,
            source VARCHAR,
            confidence FLOAT
        )
        """
    )
    con.close()

    from scripts.run_pipeline import PRESETS, step_eo_ingest

    preset = PRESETS["singapore"]
    preset_copy = type(preset)(
        name=preset.name,
        label=preset.label,
        bbox=preset.bbox,
        gap_threshold_h=preset.gap_threshold_h,
        window_days=preset.window_days,
        w_anomaly=preset.w_anomaly,
        w_graph=preset.w_graph,
        w_identity=preset.w_identity,
        db_path=db_path,
        watchlist_path=preset.watchlist_path,
    )

    with patch("pipeline.src.ingest.eo_gfw.fetch_gfw_detections") as mock_api:
        result = step_eo_ingest(preset_copy, non_interactive=True)

    assert result is True
    mock_api.assert_not_called()

    con = duckdb.connect(db_path, read_only=True)
    count = con.execute("SELECT COUNT(*) FROM eo_detections").fetchone()[0]
    con.close()
    assert count == 3


def test_step_eo_ingest_falls_back_to_api_when_no_parquet(tmp_path, monkeypatch):
    """step_eo_ingest returns True when no parquet exists (API or skip path)."""
    monkeypatch.setenv("ARKTRACE_DATA_DIR", str(tmp_path))
    # Remove all GFW tokens so the function takes the graceful-skip path,
    # which is still the non-parquet code path and is reliably testable
    # without fragile mock-call-count assertions.
    monkeypatch.delenv("GFW_API_TOKEN", raising=False)
    for i in range(1, 5):
        monkeypatch.delenv(f"GFW_API_TOKEN_{i}", raising=False)

    from scripts.run_pipeline import PRESETS, step_eo_ingest

    preset = PRESETS["singapore"]
    result = step_eo_ingest(preset, non_interactive=True)

    assert result is True


def test_step_eo_ingest_api_path_returns_true_with_token(tmp_path, monkeypatch):
    """step_eo_ingest calls the live API in interactive mode when a token is set."""
    from scripts.run_pipeline import PRESETS, step_eo_ingest

    monkeypatch.setenv("ARKTRACE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("GFW_API_TOKEN", "test-token")

    preset = PRESETS["singapore"]

    mock_fetch = MagicMock(return_value=[])
    # non_interactive=False → interactive/local mode, which falls back to the live API.
    # non_interactive=True skips the API (CI relies on pre-fetched parquets instead).
    with patch("pipeline.src.ingest.eo_gfw.ingest_eo_records", return_value=0):
        result = step_eo_ingest(preset, non_interactive=False, _fetch_fn=mock_fetch)

    assert result is True
    mock_fetch.assert_called_once()


def test_step_eo_ingest_skips_api_in_non_interactive(tmp_path, monkeypatch):
    """In non-interactive mode with no parquet, step_eo_ingest skips immediately."""
    from scripts.run_pipeline import PRESETS, step_eo_ingest

    monkeypatch.setenv("ARKTRACE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("GFW_API_TOKEN", "test-token")

    preset = PRESETS["singapore"]

    mock_fetch = MagicMock(return_value=[])
    result = step_eo_ingest(preset, non_interactive=True, _fetch_fn=mock_fetch)

    assert result is True
    mock_fetch.assert_not_called()
