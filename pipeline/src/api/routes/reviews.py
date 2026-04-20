"""Review sync endpoint — called by the CF Queue consumer Worker after a user pushes."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import threading
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request

router = APIRouter()
logger = logging.getLogger(__name__)

_SCRIPTS_DIR = Path(__file__).resolve().parents[4] / "scripts"

# ---------------------------------------------------------------------------
# Coalescing run-once guard
#
# Two requests that arrive while a merge is already running must not spawn a
# second concurrent subprocess (risk of interleaved R2 writes and a corrupt
# manifest).  Instead:
#   • The first request starts the merge and sets _running = True.
#   • Any request that arrives while the merge is running sets _pending = True
#     and returns 202 immediately — the pipeline will re-run once the current
#     merge finishes, picking up anything that arrived in the meantime.
#   • After every run the loop checks _pending; if set it runs once more then
#     clears both flags.
# ---------------------------------------------------------------------------
_state_lock = threading.Lock()
_running = False
_pending = False


def _do_merge() -> None:
    """Run sync_r2.py merge-reviews once (subprocess, blocking)."""
    script = _SCRIPTS_DIR / "sync_r2.py"
    result = subprocess.run(
        [sys.executable, "-m", "uv", "run", "python", str(script), "merge-reviews"],
        capture_output=True,
        text=True,
        cwd=str(_SCRIPTS_DIR.parent),
    )
    if result.returncode != 0:
        logger.error("[merge-reviews] failed:\n%s\n%s", result.stdout, result.stderr)
    else:
        logger.info("[merge-reviews] done:\n%s", result.stdout)


def _run_merge_reviews() -> None:
    """Background task: run merge, then re-run once if a new request arrived
    while the first merge was in progress."""
    global _running, _pending

    with _state_lock:
        if _running:
            # Another invocation is already executing; mark that a follow-up
            # run is needed and bail out — the active loop will handle it.
            _pending = True
            return
        _running = True

    try:
        while True:
            _do_merge()
            with _state_lock:
                if _pending:
                    _pending = False
                    # loop: run once more for requests that accumulated above
                else:
                    break
    finally:
        with _state_lock:
            _running = False
            _pending = False


@router.post("/api/reviews/merge", status_code=202)
async def trigger_merge_reviews(
    request: Request,
) -> dict[str, str]:
    """Trigger a server-side merge of all per-user review Parquet files into
    reviews/merged/*.parquet and patch ducklake_manifest.json.

    Called by the CF Queue consumer Worker (workers/review-merge-consumer/).
    Protected by a shared secret in the X-Pipeline-Secret header.

    At most one merge subprocess runs at a time.  Duplicate calls while a merge
    is in-flight set a pending flag so a follow-up run happens automatically
    after the current one finishes — no data is lost and no two merges race.
    """
    secret = request.headers.get("X-Pipeline-Secret", "")
    expected = os.getenv("PIPELINE_SECRET", "")
    if not expected or secret != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Spawn in a daemon thread so FastAPI stays responsive; the coalescing guard
    # above prevents overlapping runs regardless of how many requests arrive.
    t = threading.Thread(target=_run_merge_reviews, daemon=True)
    t.start()

    return {"status": "accepted"}
