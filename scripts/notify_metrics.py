"""Send an email summary after a data-publish CI run.

Reads the backtest report JSON produced by run_public_backtest_batch.py and
sends a formatted email with Precision@50, Recall@200, AUROC per region, and
a regression flag if Precision@50 dropped more than 0.02 vs the previous run.

Environment variables (all required unless noted)
-------------------------------------------------
NOTIFY_EMAIL        Recipient address (skip silently if unset)
SMTP_HOST           SMTP server hostname  (default: smtp.gmail.com)
SMTP_PORT           SMTP server port      (default: 587)
SMTP_USER           Sender address / login
SMTP_PASSWORD       SMTP password / app-password
PREVIOUS_P50        Precision@50 from the previous run (optional — used for
                    regression detection; pass via workflow step output)
GITHUB_RUN_ID       Injected automatically by GitHub Actions
GITHUB_REPOSITORY   Injected automatically by GitHub Actions
SNAPSHOT_ID         Timestamp of the R2 snapshot just pushed (optional)
SNAPSHOT_SIZE_MB    Size of the snapshot in MB (optional)
"""

from __future__ import annotations

import json
import os
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

_REPORT_PATH = Path("data/processed/backtest_public_integration_summary.json")
_REGRESSION_THRESHOLD = 0.02


def _load_report() -> dict:
    if not _REPORT_PATH.exists():
        print(f"Report not found: {_REPORT_PATH}", file=sys.stderr)
        sys.exit(1)
    with _REPORT_PATH.open() as f:
        return json.load(f)


def _format_body(
    report: dict, prev_p50: float | None, run_url: str, snapshot_info: str
) -> tuple[str, str]:
    """Return (subject, html_body)."""
    metrics = report.get("metrics_summary", {})
    p50 = metrics.get("precision_at_50", {}).get("mean", 0.0)
    p50_lo = metrics.get("precision_at_50", {}).get("ci95_low")
    p50_hi = metrics.get("precision_at_50", {}).get("ci95_high")
    recall = metrics.get("recall_at_200", {}).get("mean", 0.0)
    regions = report.get("regions", [])
    skipped_regions = report.get("skipped_regions", [])
    skipped_reason = report.get("skipped_reason", "")
    total_positives = report.get("total_known_cases", 0)
    generated_at = report.get("generated_at_utc", "")[:10]

    regression = prev_p50 is not None and (prev_p50 - p50) > _REGRESSION_THRESHOLD
    improvement = prev_p50 is not None and (p50 - prev_p50) > _REGRESSION_THRESHOLD

    if regression:
        subject = (
            f"⚠️ arktrace data publish — Precision@50 regression ({p50:.4f} ↓ from {prev_p50:.4f})"
        )
        status_banner = f'<p style="color:#c0392b;font-weight:bold">⚠️ Regression detected: Precision@50 dropped {prev_p50 - p50:.4f} vs previous run</p>'
    elif improvement:
        subject = (
            f"✅ arktrace data publish — Precision@50 improved ({p50:.4f} ↑ from {prev_p50:.4f})"
        )
        status_banner = f'<p style="color:#27ae60;font-weight:bold">✅ Improvement: Precision@50 up {p50 - prev_p50:.4f} vs previous run</p>'
    else:
        subject = f"arktrace data publish — Precision@50 {p50:.4f} ({generated_at})"
        status_banner = ""

    ci_str = f" (CI 95%: {p50_lo:.4f}–{p50_hi:.4f})" if p50_lo and p50_hi else ""

    skipped_note = ""
    if skipped_regions:
        skipped_note = (
            f'<p style="color:#e67e22"><strong>⚠️ Skipped regions (not evaluated):</strong> '
            f"{', '.join(skipped_regions)}<br>"
            f"<em>{skipped_reason}</em></p>"
        )

    region_rows = ""
    for rs in report.get("region_summary", []):
        region = rs.get("region", "")
        matched = rs.get("matched_total", 0)
        total = rs.get("source_positive_total", 0)
        recall_wl = rs.get("source_recall_in_watchlist", 0)
        region_rows += (
            f"<tr><td>{region}</td><td>{matched}/{total}</td><td>{recall_wl:.0%}</td></tr>"
        )

    html = f"""
<html><body style="font-family:sans-serif;max-width:600px">
<h2>arktrace — Data Publish Summary</h2>
{status_banner}
{skipped_note}
<p><strong>Date:</strong> {generated_at}<br>
<strong>Evaluated regions:</strong> {", ".join(regions) if regions else "none"}<br>
<strong>Snapshot:</strong> {snapshot_info}</p>

<h3>Overall Metrics</h3>
<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse">
  <tr><th>Metric</th><th>Value</th></tr>
  <tr><td>Precision@50</td><td><strong>{p50:.4f}</strong>{ci_str}</td></tr>
  <tr><td>Recall@200</td><td>{recall:.4f}</td></tr>
  <tr><td>Known positives</td><td>{total_positives}</td></tr>
  {"<tr><td>Previous Precision@50</td><td>" + f"{prev_p50:.4f}" + "</td></tr>" if prev_p50 is not None else ""}
</table>

<h3>Per-Region Coverage</h3>
<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse">
  <tr><th>Region</th><th>Positives matched</th><th>Recall in watchlist</th></tr>
  {region_rows}
</table>

<p><a href="{run_url}">View CI run →</a></p>
</body></html>
"""
    return subject, html


def main() -> int:
    recipient = os.getenv("NOTIFY_EMAIL")
    if not recipient:
        print("NOTIFY_EMAIL not set — skipping notification.")
        return 0

    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_password = os.getenv("SMTP_PASSWORD", "")

    if not smtp_user or not smtp_password:
        print("SMTP_USER / SMTP_PASSWORD not set — skipping notification.", file=sys.stderr)
        return 0

    prev_p50_str = os.getenv("PREVIOUS_P50")
    prev_p50 = float(prev_p50_str) if prev_p50_str else None

    run_id = os.getenv("GITHUB_RUN_ID", "")
    repo = os.getenv("GITHUB_REPOSITORY", "edgesentry/arktrace")
    run_url = (
        f"https://github.com/{repo}/actions/runs/{run_id}"
        if run_id
        else f"https://github.com/{repo}/actions"
    )

    snapshot_id = os.getenv("SNAPSHOT_ID", "")
    snapshot_mb = os.getenv("SNAPSHOT_SIZE_MB", "")
    snapshot_info = f"{snapshot_id} ({snapshot_mb} MB)" if snapshot_id else "see CI run"

    report = _load_report()

    # Skip email when all regions were skipped (no real watchlist data in CI).
    # This happens when watchlists.zip has not been pushed to R2 yet or when
    # every region falls below --min-watchlist-size (seeded dummy data only).
    if report.get("total_known_cases", 0) == 0 and not report.get("regions"):
        print(
            "All regions were skipped (total_known_cases=0, evaluated regions=[]).\n"
            "Email suppressed — push real watchlists first:\n"
            "  uv run python scripts/sync_r2.py push-watchlists"
        )
        return 0

    subject, html_body = _format_body(report, prev_p50, run_url, snapshot_info)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = recipient
    msg.attach(MIMEText(html_body, "html"))

    print(f"Sending email to {recipient} via {smtp_host}:{smtp_port} …")
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, recipient, msg.as_string())

    print(f"Email sent: {subject}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
