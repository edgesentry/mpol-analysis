"""Tests for the fcntl file-lock guard in sync_r2.py cmd_merge_reviews.

The lock prevents two concurrent processes (e.g. a queue-triggered run and a
manual run) from executing merge-reviews simultaneously and corrupting R2.
"""

from __future__ import annotations

import argparse
import fcntl
from unittest.mock import patch

import pytest

import scripts.sync_r2 as sync_r2


@pytest.fixture
def args():
    return argparse.Namespace()


def test_skips_when_lock_already_held(args, tmp_path):
    """If the lock file is held by another process, cmd_merge_reviews returns 0
    immediately without calling _cmd_merge_reviews_inner."""
    # Simulate the lock being held by patching fcntl.flock to raise on the
    # non-blocking acquire attempt (LOCK_EX | LOCK_NB).
    original_flock = fcntl.flock

    def flock_side_effect(fd, operation):
        if operation == (fcntl.LOCK_EX | fcntl.LOCK_NB):
            raise BlockingIOError("lock held")
        return original_flock(fd, operation)

    with patch.object(fcntl, "flock", side_effect=flock_side_effect):
        with patch.object(sync_r2, "_cmd_merge_reviews_inner") as mock_inner:
            result = sync_r2.cmd_merge_reviews(args)

    assert result == 0
    mock_inner.assert_not_called()


def test_runs_when_lock_is_free(args):
    """When the lock is not held, _cmd_merge_reviews_inner is called and its
    return value is propagated."""
    with patch.object(sync_r2, "_cmd_merge_reviews_inner", return_value=0) as mock_inner:
        result = sync_r2.cmd_merge_reviews(args)

    assert result == 0
    mock_inner.assert_called_once_with(args)


def test_lock_released_after_success(args):
    """fcntl.LOCK_UN is called after a successful inner run."""
    unlock_calls: list[int] = []
    original_flock = fcntl.flock

    def tracking_flock(fd, operation):
        if operation == fcntl.LOCK_UN:
            unlock_calls.append(1)
        else:
            original_flock(fd, operation)

    with patch.object(fcntl, "flock", side_effect=tracking_flock):
        with patch.object(sync_r2, "_cmd_merge_reviews_inner", return_value=0):
            sync_r2.cmd_merge_reviews(args)

    assert len(unlock_calls) == 1, "LOCK_UN should be called exactly once"


def test_lock_released_after_inner_raises(args):
    """fcntl.LOCK_UN is called in the finally block even if inner raises."""
    unlock_calls: list[int] = []
    original_flock = fcntl.flock

    def tracking_flock(fd, operation):
        if operation == fcntl.LOCK_UN:
            unlock_calls.append(1)
        else:
            original_flock(fd, operation)

    with patch.object(fcntl, "flock", side_effect=tracking_flock):
        with patch.object(sync_r2, "_cmd_merge_reviews_inner", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError, match="boom"):
                sync_r2.cmd_merge_reviews(args)

    assert len(unlock_calls) == 1, "LOCK_UN must be called even when inner raises"


def test_inner_return_value_propagated(args):
    """Non-zero return from inner is passed through to the caller."""
    with patch.object(sync_r2, "_cmd_merge_reviews_inner", return_value=1):
        result = sync_r2.cmd_merge_reviews(args)
    assert result == 1
