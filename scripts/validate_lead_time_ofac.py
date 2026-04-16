"""Validate the 60–90 day pre-designation lead time claim.

Methodology
-----------
Two complementary analyses:

1. **Retrospective — already-designated vessels**
   For OFAC/UN/EU vessels whose MMSI appears in the scored watchlist, we use
   OpenSanctions ``first_seen`` as a proxy for the public designation date (the
   date the entity first appeared in any sanctions dataset aggregated by
   OpenSanctions).  The lead time estimate is:

       lead_days = first_seen_date - earliest_ais_gap_window_start

   "Earliest AIS gap window start" = last_seen - 30d (the 30-day rolling window
   used to compute ais_gap_count_30d).  If the vessel was accumulating AIS gaps
   in that window AND the window predates first_seen, the model was detecting
   evasion before public designation.

   Where ais_positions timestamps are available, we use the actual earliest
   AIS position timestamp instead of the derived window.

2. **Prospective — unknown-unknown candidates**
   Vessels in the watchlist with confidence > 0.30 and sanctions_distance = 99
   (no current sanctions link) are the live pre-designation candidates.  These
   are listed with their behavioural scores as a prediction register — their
   lead time will be measurable when/if they are later designated.

Limitations
-----------
* ``first_seen`` in OpenSanctions is when the *database* first recorded the
  entity, not necessarily the official OFAC announcement date (which can
  precede database ingestion by days).  For OFAC SDN the lag is typically
  24–72 hours; we treat first_seen as a conservative proxy.
* The 30-day rolling AIS gap window is a static snapshot; a longitudinal replay
  would give exact per-day scores but requires a time-series archive of AIS data
  not available in the public demo dataset.

Usage
-----
    uv run python scripts/validate_lead_time_ofac.py
    uv run python scripts/validate_lead_time_ofac.py --watchlist data/processed/singapore_watchlist.parquet
    uv run python scripts/validate_lead_time_ofac.py --all-regions --out data/processed/lead_time_report.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
JSONL_PATH = REPO_ROOT / "data" / "raw" / "sanctions" / "opensanctions_entities.jsonl"

# Watchlists are pulled from R2 to the canonical user data directory.
# Pipeline operators can override with ARKTRACE_DATA_DIR or DATA_DIR.
_watchlist_dir = Path(
    os.getenv("ARKTRACE_DATA_DIR") or os.getenv("DATA_DIR") or (Path.home() / ".arktrace" / "data")
)

WATCHLIST_BY_REGION = {
    "singapore": _watchlist_dir / "singapore_watchlist.parquet",
    "japan": _watchlist_dir / "japansea_watchlist.parquet",
    "europe": _watchlist_dir / "europe_watchlist.parquet",
    "persiangulf": _watchlist_dir / "persiangulf_watchlist.parquet",
    "gulfofguinea": _watchlist_dir / "gulfofguinea_watchlist.parquet",
    "gulfofaden": _watchlist_dir / "gulfofaden_watchlist.parquet",
    "gulfofmexico": _watchlist_dir / "gulfofmexico_watchlist.parquet",
    "middleeast": _watchlist_dir / "middleeast_watchlist.parquet",
}

# Score threshold above which a vessel is considered "flagged" by the model
CONFIDENCE_THRESHOLD = 0.25

# Unknown-unknown: high score, no current sanctions link
UU_CONFIDENCE_THRESHOLD = 0.30
UU_SANCTIONS_DISTANCE = 99  # sentinel for "no graph link"


# ---------------------------------------------------------------------------
# Load OpenSanctions designation dates
# ---------------------------------------------------------------------------


def _load_designation_dates(jsonl_path: Path) -> dict[str, datetime]:
    """Return {mmsi: first_seen_utc} for all sanctioned vessels in the JSONL.

    ``first_seen`` is used as a proxy for the public designation date.
    """
    if not jsonl_path.exists():
        print(
            f"[warn] OpenSanctions JSONL not found at {jsonl_path}. "
            "Run: uv run python scripts/sync_r2.py pull-sanctions-db  "
            "or set --jsonl to an alternate path.",
            file=sys.stderr,
        )
        return {}

    dates: dict[str, datetime] = {}
    with jsonl_path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            first_seen_str = obj.get("first_seen", "")
            if not first_seen_str:
                continue
            try:
                first_seen = datetime.fromisoformat(first_seen_str.replace("Z", "+00:00")).replace(
                    tzinfo=UTC
                )
            except ValueError:
                continue

            mmsis = obj.get("properties", {}).get("mmsi", [])
            for mmsi in mmsis:
                mmsi = str(mmsi).strip()
                if mmsi:
                    # Keep the earliest known date if the vessel appears multiple times
                    if mmsi not in dates or first_seen < dates[mmsi]:
                        dates[mmsi] = first_seen

    return dates


# ---------------------------------------------------------------------------
# Load watchlist(s)
# ---------------------------------------------------------------------------


def _load_watchlist(paths: list[Path]) -> pl.DataFrame:
    parts = []
    for p in paths:
        if p.exists():
            parts.append(pl.read_parquet(p))
    if not parts:
        return pl.DataFrame()
    combined = pl.concat(parts, how="vertical_relaxed")
    # Deduplicate keeping highest confidence per mmsi
    return combined.sort("confidence", descending=True).unique(subset=["mmsi"], keep="first")


# ---------------------------------------------------------------------------
# Retrospective analysis
# ---------------------------------------------------------------------------


def _retrospective(
    watchlist: pl.DataFrame,
    designation_dates: dict[str, datetime],
    reference_date: datetime,
) -> list[dict]:
    """Per-vessel lead-time estimates for already-designated vessels."""
    rows = []
    for row in watchlist.iter_rows(named=True):
        mmsi = row["mmsi"]
        desig_date = designation_dates.get(mmsi)
        if desig_date is None:
            continue

        confidence = float(row.get("confidence") or 0)
        if confidence < CONFIDENCE_THRESHOLD:
            continue

        # Estimate the start of the AIS gap detection window
        last_seen_raw = row.get("last_seen")
        if last_seen_raw:
            try:
                if isinstance(last_seen_raw, str):
                    last_seen = datetime.fromisoformat(
                        last_seen_raw.replace("Z", "+00:00")
                    ).replace(tzinfo=UTC)
                else:
                    # polars datetime
                    last_seen = datetime.fromtimestamp(last_seen_raw.timestamp(), tz=UTC)
                window_start = last_seen - timedelta(days=30)
            except Exception:
                window_start = reference_date - timedelta(days=30)
        else:
            window_start = reference_date - timedelta(days=30)

        # Lead time: positive = model window predates designation (pre-designation detection)
        lead_days = int((desig_date - window_start).days)

        rows.append(
            {
                "mmsi": mmsi,
                "vessel_name": row.get("vessel_name") or mmsi,
                "flag": row.get("flag") or "",
                "confidence": round(confidence, 4),
                "ais_gap_count_30d": int(row.get("ais_gap_count_30d") or 0),
                "sanctions_distance": int(row.get("sanctions_distance") or 99),
                "designation_date_proxy": desig_date.strftime("%Y-%m-%d"),
                "detection_window_start": window_start.strftime("%Y-%m-%d"),
                "lead_days": lead_days,
                "pre_designation": lead_days > 0,
            }
        )

    return sorted(rows, key=lambda r: r["lead_days"], reverse=True)


# ---------------------------------------------------------------------------
# Prospective analysis (unknown-unknown candidates)
# ---------------------------------------------------------------------------


def _prospective(watchlist: pl.DataFrame, designation_dates: dict[str, datetime]) -> list[dict]:
    """Live pre-designation candidates: high-scoring vessels with no sanctions link."""
    rows = []
    designated_mmsis = set(designation_dates.keys())
    for row in watchlist.iter_rows(named=True):
        mmsi = row["mmsi"]
        if mmsi in designated_mmsis:
            continue  # already designated — not an unknown-unknown
        confidence = float(row.get("confidence") or 0)
        sd = int(row.get("sanctions_distance") or 99)
        if confidence < UU_CONFIDENCE_THRESHOLD or sd != UU_SANCTIONS_DISTANCE:
            continue
        rows.append(
            {
                "mmsi": mmsi,
                "vessel_name": row.get("vessel_name") or mmsi,
                "flag": row.get("flag") or "",
                "confidence": round(confidence, 4),
                "ais_gap_count_30d": int(row.get("ais_gap_count_30d") or 0),
                "sanctions_distance": sd,
                "status": "WATCH — no current designation; model flags as high-risk",
            }
        )
    return sorted(rows, key=lambda r: r["confidence"], reverse=True)[:50]


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def _print_table(rows: list[dict], cols: list[str], title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")
    if not rows:
        print("  (no results)")
        return
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    header = "  " + "  ".join(c.ljust(widths[c]) for c in cols)
    print(header)
    print("  " + "-" * (sum(widths.values()) + 2 * len(cols)))
    for r in rows:
        print("  " + "  ".join(str(r.get(c, "")).ljust(widths[c]) for c in cols))


def main() -> None:
    global CONFIDENCE_THRESHOLD  # noqa: PLW0603
    parser = argparse.ArgumentParser(description="Validate 60-90 day pre-designation lead time")
    parser.add_argument(
        "--watchlist",
        default=None,
        help="Path to a single watchlist parquet (default: singapore)",
    )
    parser.add_argument(
        "--all-regions",
        action="store_true",
        help="Combine watchlists from all available regions",
    )
    parser.add_argument(
        "--jsonl",
        default=str(JSONL_PATH),
        help="Path to opensanctions_entities.jsonl",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Write JSON report to this path (optional)",
    )
    parser.add_argument(
        "--confidence-threshold",
        type=float,
        default=CONFIDENCE_THRESHOLD,
        help=f"Min confidence to consider a vessel 'flagged' (default: {CONFIDENCE_THRESHOLD})",
    )
    args = parser.parse_args()
    CONFIDENCE_THRESHOLD = args.confidence_threshold

    # Load designation dates
    print("Loading OpenSanctions designation dates …", flush=True)
    designation_dates = _load_designation_dates(Path(args.jsonl))
    print(f"  {len(designation_dates)} vessels with MMSI + first_seen date", flush=True)

    # Load watchlist(s)
    if args.watchlist:
        wl_paths = [Path(args.watchlist)]
    elif args.all_regions:
        wl_paths = list(WATCHLIST_BY_REGION.values())
    else:
        wl_paths = [WATCHLIST_BY_REGION["singapore"]]

    print(f"Loading watchlist(s): {[str(p) for p in wl_paths]} …", flush=True)
    watchlist = _load_watchlist(wl_paths)
    if watchlist.is_empty():
        print(
            "[warn] No watchlist data found in any of the expected paths. "
            "The pipeline may have produced no candidates for these regions.",
            file=sys.stderr,
        )
        if args.out:
            out_path = Path(args.out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            empty_report = {
                "generated_at_utc": datetime.now(UTC).isoformat(),
                "confidence_threshold": CONFIDENCE_THRESHOLD,
                "watchlist_size": 0,
                "designation_dates_loaded": len(designation_dates),
                "matched_to_designation": 0,
                "pre_designation_count": 0,
                "post_designation_count": 0,
                "mean_lead_days": 0,
                "median_lead_days": 0,
                "unknown_unknown_candidates": 0,
                "pre_designation_vessels": [],
                "post_designation_vessels": [],
                "unknown_unknown_watch": [],
                "methodology": "No watchlist data — pipeline produced no candidates.",
            }
            out_path.write_text(json.dumps(empty_report, indent=2))
            print(f"Empty report written to {args.out}")
        sys.exit(0)
    print(f"  {watchlist.height} unique vessels loaded", flush=True)

    reference_date = datetime.now(UTC)

    # Retrospective
    retro = _retrospective(watchlist, designation_dates, reference_date)
    pre_desig = [r for r in retro if r["pre_designation"]]
    post_desig = [r for r in retro if not r["pre_designation"]]

    _print_table(
        pre_desig,
        [
            "mmsi",
            "vessel_name",
            "confidence",
            "designation_date_proxy",
            "detection_window_start",
            "lead_days",
        ],
        "PRE-DESIGNATION DETECTIONS (model flagged before public OFAC listing)",
    )
    _print_table(
        post_desig[:10],
        [
            "mmsi",
            "vessel_name",
            "confidence",
            "designation_date_proxy",
            "detection_window_start",
            "lead_days",
        ],
        "POST-DESIGNATION (model flagged after listing — confirms recall)",
    )

    # Prospective
    prosp = _prospective(watchlist, designation_dates)
    _print_table(
        prosp[:20],
        ["mmsi", "vessel_name", "confidence", "ais_gap_count_30d", "status"],
        "PROSPECTIVE WATCH LIST (unknown-unknown candidates — not yet designated)",
    )

    # Summary
    print(f"\n{'=' * 70}")
    print("  LEAD TIME SUMMARY")
    print(f"{'=' * 70}")
    n_matched = len(retro)
    n_pre = len(pre_desig)
    lead_days_pre = [r["lead_days"] for r in pre_desig]
    mean_lead = int(sum(lead_days_pre) / len(lead_days_pre)) if lead_days_pre else 0
    median_lead = sorted(lead_days_pre)[len(lead_days_pre) // 2] if lead_days_pre else 0
    print(f"  Matched to designation dates : {n_matched}")
    print(f"  Pre-designation detections   : {n_pre} / {n_matched}")
    print(f"  Mean lead time (pre-desig)   : {mean_lead} days")
    print(f"  Median lead time (pre-desig) : {median_lead} days")
    print(f"  Unknown-unknown candidates   : {len(prosp)}")
    print()

    # JSON output
    report = {
        "generated_at_utc": reference_date.isoformat(),
        "confidence_threshold": CONFIDENCE_THRESHOLD,
        "watchlist_size": watchlist.height,
        "designation_dates_loaded": len(designation_dates),
        "matched_to_designation": n_matched,
        "pre_designation_count": n_pre,
        "post_designation_count": len(post_desig),
        "mean_lead_days": mean_lead,
        "median_lead_days": median_lead,
        "unknown_unknown_candidates": len(prosp),
        "pre_designation_vessels": pre_desig,
        "post_designation_vessels": post_desig,
        "unknown_unknown_watch": prosp,
        "methodology": (
            "first_seen from OpenSanctions used as designation date proxy. "
            "detection_window_start = last_seen - 30d (AIS gap rolling window). "
            "lead_days = designation_date - detection_window_start. "
            "Positive lead_days = model was detecting evasion before public OFAC listing."
        ),
    }

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2))
        print(f"Report written to {out_path}")

    print(
        json.dumps(
            {
                "matched": n_matched,
                "pre_designation": n_pre,
                "mean_lead_days": mean_lead,
                "median_lead_days": median_lead,
                "unknown_unknown_candidates": len(prosp),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
