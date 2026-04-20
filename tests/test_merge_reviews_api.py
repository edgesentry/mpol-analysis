"""Tests for POST /api/reviews/merge — auth, 202 response, and the
running/pending coalescing guard that prevents concurrent subprocess runs."""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

import pipeline.src.api.routes.reviews as reviews_module
from pipeline.src.api.main import app

client = TestClient(app)

_SECRET = "test-pipeline-secret"
_HEADERS = {"X-Pipeline-Secret": _SECRET}


@pytest.fixture(autouse=True)
def reset_coalesce_state():
    """Reset the module-level running/pending flags between tests and wait for
    any daemon threads spawned by the previous test to finish."""
    with reviews_module._state_lock:
        reviews_module._running = False
        reviews_module._pending = False
    yield
    # Give daemon threads up to 0.5 s to settle so the next test starts clean.
    deadline = time.monotonic() + 0.5
    while time.monotonic() < deadline:
        with reviews_module._state_lock:
            if not reviews_module._running:
                break
        time.sleep(0.01)
    with reviews_module._state_lock:
        reviews_module._running = False
        reviews_module._pending = False


@pytest.fixture
def secret(monkeypatch):
    monkeypatch.setenv("PIPELINE_SECRET", _SECRET)


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def test_no_header_returns_401():
    resp = client.post("/api/reviews/merge")
    assert resp.status_code == 401


def test_wrong_secret_returns_401(secret):
    resp = client.post("/api/reviews/merge", headers={"X-Pipeline-Secret": "wrong"})
    assert resp.status_code == 401


def test_missing_env_var_returns_401():
    # PIPELINE_SECRET not set at all
    resp = client.post("/api/reviews/merge", headers=_HEADERS)
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_valid_request_returns_202_and_runs_merge(secret):
    with patch.object(reviews_module, "_do_merge") as mock_merge:
        resp = client.post("/api/reviews/merge", headers=_HEADERS)
        assert resp.status_code == 202
        assert resp.json() == {"status": "accepted"}
        # Allow the daemon thread to complete.
        time.sleep(0.1)
        mock_merge.assert_called_once()


def test_flags_cleared_after_single_run(secret):
    with patch.object(reviews_module, "_do_merge"):
        client.post("/api/reviews/merge", headers=_HEADERS)
        time.sleep(0.1)

    with reviews_module._state_lock:
        assert not reviews_module._running
        assert not reviews_module._pending


# ---------------------------------------------------------------------------
# Coalescing guard — the critical dedup behaviour
# ---------------------------------------------------------------------------


def test_concurrent_request_sets_pending_and_triggers_one_extra_run(secret):
    """Two requests while a merge is running → exactly 2 total _do_merge calls.

    Timeline:
      T0  request-1 arrives → _running=True, merge starts
      T1  request-2 arrives while merge is in progress → _pending=True, returns 202
      T2  first merge finishes → loop sees _pending, runs once more, clears flags
    Total calls: 2 (not 1 and not 3).
    """
    first_merge_started = threading.Event()
    first_merge_release = threading.Event()
    call_order: list[int] = []

    def controlled_merge():
        n = len(call_order) + 1
        call_order.append(n)
        if n == 1:
            first_merge_started.set()
            first_merge_release.wait(timeout=2)

    with patch.object(reviews_module, "_do_merge", side_effect=controlled_merge):
        # Request 1 — starts the merge thread
        resp1 = client.post("/api/reviews/merge", headers=_HEADERS)
        assert resp1.status_code == 202

        # Wait until the first merge is actually running
        assert first_merge_started.wait(timeout=2), "first merge never started"

        # Request 2 — merge is in-flight, should set _pending
        resp2 = client.post("/api/reviews/merge", headers=_HEADERS)
        assert resp2.status_code == 202

        with reviews_module._state_lock:
            assert reviews_module._pending, "_pending should be True while merge is running"

        # Unblock the first merge
        first_merge_release.set()

        # Wait for both runs to complete
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with reviews_module._state_lock:
                if not reviews_module._running:
                    break
            time.sleep(0.01)

    assert len(call_order) == 2, (
        f"Expected exactly 2 _do_merge calls (1 initial + 1 follow-up), got {len(call_order)}"
    )


def test_three_concurrent_requests_still_only_two_runs(secret):
    """Three requests with the first still running → first run + one follow-up = 2 total."""
    first_started = threading.Event()
    release = threading.Event()
    calls: list[int] = []

    def controlled_merge():
        calls.append(1)
        if len(calls) == 1:
            first_started.set()
            release.wait(timeout=2)

    with patch.object(reviews_module, "_do_merge", side_effect=controlled_merge):
        client.post("/api/reviews/merge", headers=_HEADERS)
        assert first_started.wait(timeout=2)

        # Two more requests while first is running — both should just set _pending
        client.post("/api/reviews/merge", headers=_HEADERS)
        client.post("/api/reviews/merge", headers=_HEADERS)

        release.set()

        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with reviews_module._state_lock:
                if not reviews_module._running:
                    break
            time.sleep(0.01)

    # Regardless of how many requests arrived, only 2 merges run
    assert len(calls) == 2


def test_sequential_requests_each_run_merge(secret):
    """Requests that arrive after a merge completes each trigger their own run."""
    calls: list[int] = []

    with patch.object(reviews_module, "_do_merge", side_effect=lambda: calls.append(1)):
        for _ in range(3):
            client.post("/api/reviews/merge", headers=_HEADERS)
            # Wait for the previous run to fully complete before the next request
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                with reviews_module._state_lock:
                    if not reviews_module._running:
                        break
                time.sleep(0.01)

    assert len(calls) == 3


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_merge_exception_clears_flags(secret):
    """If _do_merge raises, both _running and _pending must be cleared."""
    with patch.object(reviews_module, "_do_merge", side_effect=RuntimeError("boom")):
        client.post("/api/reviews/merge", headers=_HEADERS)
        time.sleep(0.1)

    with reviews_module._state_lock:
        assert not reviews_module._running
        assert not reviews_module._pending
