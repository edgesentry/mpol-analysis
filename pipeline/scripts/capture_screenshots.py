"""
Capture dashboard screenshots for the Cap Vista Annex A submission.

Prerequisites:
    1. Install playwright browser:
           uv run playwright install chromium
    2. Seed demo data so the dashboard has realistic vessels:
           uv run python scripts/use_demo_watchlist.py --backup
           uv run python scripts/seed_demo_causal_effects.py
           uv run python scripts/seed_demo_sar.py
    3. Start the dashboard in a separate terminal:
           uv run uvicorn src.api.main:app --reload
    4. Run this script:
           uv run python scripts/capture_screenshots.py

Outputs are written to _outputs/screenshots/:
    01_shap_breakdown.png      — SHAP top-5 attribution panel
    02_map_watchlist.png       — map + ranked watchlist side-by-side
    03_causal_badge.png        — ATT estimate + 95% CI causal badge
    04_dispatch_brief.png      — patrol dispatch brief modal
    05_sar_shap.png            — SHAP panel showing unmatched_sar_detections_30d as top signal
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

BASE_URL = "http://localhost:8000"
# Demo vessel seeded by seed_demo_causal_effects.py / use_demo_watchlist.py
DEMO_MMSI = "123456789"
# SAR demo vessel: SARI NOUR — seeded by seed_demo_sar.py with unmatched_sar_detections_30d
SAR_MMSI = "613115678"
OUTPUT_DIR = Path(__file__).parent.parent / "_outputs" / "screenshots"
VIEWPORT = {"width": 1440, "height": 900}


def wait_for_dashboard(page) -> None:
    """Wait until the watchlist table has at least one row."""
    page.wait_for_selector("#watchlist-tbody tr.watchlist-row", timeout=15_000)


def goto(page, url: str) -> None:
    """Navigate and wait for DOM — avoids networkidle timeout from SSE/WebSocket."""
    page.goto(url, wait_until="domcontentloaded")


def capture_map_watchlist(page, out: Path) -> None:
    """Screenshot 2: full-page map + watchlist (default landing view)."""
    goto(page, BASE_URL)
    wait_for_dashboard(page)
    # Let map tiles settle
    time.sleep(2)
    page.screenshot(path=str(out), full_page=False)
    print(f"  saved {out.name}")


def capture_shap_breakdown(page, out: Path) -> None:
    """Screenshot 1: SHAP top-5 breakdown in the review panel."""
    goto(page, BASE_URL)
    wait_for_dashboard(page)
    # Click the top watchlist row to open the review panel
    page.locator("#watchlist-tbody tr.watchlist-row").first.click()
    # Wait for SHAP table to render
    page.wait_for_selector(".shap-table", timeout=10_000)
    time.sleep(0.5)
    panel = page.locator("#review-panel")
    panel.screenshot(path=str(out))
    print(f"  saved {out.name}")


def capture_causal_badge(page, out: Path) -> None:
    """Screenshot 3: ATT causal badge (+ 95% CI) in the review panel."""
    goto(page, BASE_URL)
    wait_for_dashboard(page)
    page.locator("#watchlist-tbody tr.watchlist-row").first.click()
    # Wait for SHAP table first (ensures review panel content has loaded)
    page.wait_for_selector(".shap-table", timeout=10_000)
    # Then wait for causal badge; fall back gracefully if causal_effects.parquet is absent
    try:
        page.wait_for_selector(".att-badge, .causal-badge", timeout=8_000)
    except Exception:
        print(
            "  WARNING: causal badge not found — causal_effects.parquet may be missing; "
            "run seed_demo_causal_effects.py and retry"
        )
    panel = page.locator("#review-panel")
    panel.screenshot(path=str(out))
    print(f"  saved {out.name}")


def capture_sar_shap(page, out: Path) -> None:
    """Screenshot 5: SHAP panel for SAR-flagged vessel (unmatched_sar_detections_30d top signal).

    Requires seed_demo_sar.py to have been run so SARI NOUR (613115678) has
    unmatched_sar_detections_30d injected as its primary SHAP contribution.
    """
    goto(page, BASE_URL)
    wait_for_dashboard(page)
    row = page.locator(f"tr.watchlist-row[data-mmsi='{SAR_MMSI}']")
    if row.count() == 0:
        print(
            f"  WARNING: SAR vessel {SAR_MMSI} not found in watchlist — run seed_demo_sar.py first"
        )
        return
    row.click()
    page.wait_for_selector(".shap-table", timeout=10_000)
    time.sleep(0.5)
    panel = page.locator("#review-panel")
    panel.screenshot(path=str(out))
    print(f"  saved {out.name}")


def capture_dispatch_brief(page, out: Path) -> None:
    """Screenshot 4: patrol dispatch brief modal."""
    goto(page, BASE_URL)
    wait_for_dashboard(page)
    page.locator("#watchlist-tbody tr.watchlist-row").first.click()
    # Enable and click the Generate Brief button
    page.wait_for_selector("#dispatch-brief-btn", timeout=8_000)
    # The button is disabled until a vessel is selected; wait for it to enable
    page.wait_for_function(
        "!document.getElementById('dispatch-brief-btn').disabled",
        timeout=8_000,
    )
    page.locator("#dispatch-brief-btn").click()
    # Wait for the modal and brief body to load
    page.wait_for_selector("#dispatch-modal", state="visible", timeout=10_000)
    # Wait until the body has real content: > 5 chars AND no longer in "Loading…" state.
    # Original threshold (> 50) missed the 30-char error string; > 5 alone catches
    # "Loading…" (10 chars) too early — combining both guards handles all states.
    page.wait_for_function(
        "document.getElementById('dispatch-brief-body').innerText.length > 5 "
        "&& !document.getElementById('dispatch-brief-body').innerText.includes('Loading')",
        timeout=20_000,
    )
    time.sleep(0.5)
    modal = page.locator("#dispatch-modal-inner")
    modal.screenshot(path=str(out))
    print(f"  saved {out.name}")


def main() -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: playwright not installed. Run:")
        print("  uv sync --group dev")
        print("  uv run playwright install chromium")
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    captures = [
        ("01_shap_breakdown.png", capture_shap_breakdown),
        ("02_map_watchlist.png", capture_map_watchlist),
        ("03_causal_badge.png", capture_causal_badge),
        ("04_dispatch_brief.png", capture_dispatch_brief),
        ("05_sar_shap.png", capture_sar_shap),
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport=VIEWPORT,
            color_scheme="dark",
            device_scale_factor=2,
        )
        page = context.new_page()

        # Smoke-check that the dashboard is reachable
        try:
            resp = page.goto(BASE_URL, timeout=5_000)
            if resp is None or not resp.ok:
                raise RuntimeError(f"Dashboard returned {resp and resp.status}")
        except Exception as exc:
            print(f"ERROR: dashboard not reachable at {BASE_URL}: {exc}")
            print("Start it first:  uv run uvicorn src.api.main:app --reload")
            browser.close()
            return 1

        for filename, fn in captures:
            out = OUTPUT_DIR / filename
            print(f"Capturing {filename} …")
            try:
                fn(page, out)
            except Exception as exc:
                print(f"  WARNING: {filename} failed — {exc}")

        browser.close()

    print(f"\nScreenshots written to {OUTPUT_DIR}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
