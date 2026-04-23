"""Fetch GFW EO vessel-presence detections for all active regions and save as parquet.

Run this script (typically weekly via gfw-ingest.yml) to pre-fetch GFW data
so the daily data-publish pipeline can ingest it without calling the API directly.

Output: data/processed/{region_prefix}_eo_detections.parquet per region.

Usage:
    uv run python scripts/gfw_ingest.py
    uv run python scripts/gfw_ingest.py --regions singapore,japan,europe,blacksea
    uv run python scripts/gfw_ingest.py --days 30 --out-dir data/processed
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Active regions for GFW EO ingest — subset of run_pipeline.py PRESETS that
# have real AIS streaming data and are included in the public backtest.
_ACTIVE_REGIONS = ["singapore", "japan", "europe", "blacksea", "middleeast"]

# Maps pipeline region name → (file_prefix, bbox as lon_min,lat_min,lon_max,lat_max)
# bbox source: run_pipeline.py PRESETS (stored as lat_min,lon_min,lat_max,lon_max)
_REGION_META: dict[str, tuple[str, tuple[float, float, float, float]]] = {
    "singapore": ("singapore", (92.0, -5.0, 122.0, 22.0)),
    "japan": ("japansea", (115.0, 25.0, 145.0, 48.0)),
    "europe": ("europe", (-22.0, 30.0, 42.0, 72.0)),
    "blacksea": ("blacksea", (27.0, 40.0, 42.0, 48.0)),
    "middleeast": ("middleeast", (32.0, -10.0, 80.0, 30.0)),
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Pre-fetch GFW EO detections for all regions")
    parser.add_argument(
        "--regions",
        default=",".join(_ACTIVE_REGIONS),
        help=f"Comma-separated regions to fetch (default: {','.join(_ACTIVE_REGIONS)})",
    )
    parser.add_argument(
        "--days", type=int, default=30, help="Lookback window in days (default: 30)"
    )
    parser.add_argument(
        "--out-dir",
        default=os.getenv("ARKTRACE_DATA_DIR", str(Path.home() / ".arktrace" / "data")),
        metavar="DIR",
        help="Output directory for parquet files",
    )
    args = parser.parse_args()

    import polars as pl

    from pipeline.src.ingest.eo_gfw import fetch_gfw_detections

    tokens = [t for t in [os.getenv("GFW_API_TOKEN", "")] if t]
    tokens += [v for k, v in sorted(os.environ.items()) if k.startswith("GFW_API_TOKEN_") and v]
    if not tokens:
        print("Error: GFW_API_TOKEN not set — cannot fetch GFW EO detections.", file=sys.stderr)
        return 1

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    regions = [r.strip() for r in args.regions.split(",") if r.strip()]

    fetched_at = datetime.now(UTC).isoformat()
    success, skipped = 0, 0

    for region in regions:
        if region not in _REGION_META:
            print(f"[warn] Unknown region '{region}' — skipping", flush=True)
            skipped += 1
            continue

        prefix, bbox = _REGION_META[region]
        out_path = out_dir / f"{prefix}_eo_detections.parquet"
        print(
            f"[{region}] fetching GFW EO detections (bbox={bbox}, days={args.days}) ...", flush=True
        )

        try:
            records = fetch_gfw_detections(bbox=bbox, days=args.days, api_tokens=tokens)
        except PermissionError as exc:
            print(f"  [skip] {exc}", flush=True)
            skipped += 1
            continue
        except Exception as exc:
            print(f"  [skip] GFW API error: {exc}", flush=True)
            skipped += 1
            continue

        if not records:
            print("  0 detections — writing empty parquet so pull-gfw-eo can distribute it")

        df = pl.DataFrame(
            records or [],
            schema={
                "detection_id": pl.Utf8,
                "detected_at": pl.Datetime("us", "UTC"),
                "lat": pl.Float64,
                "lon": pl.Float64,
                "source": pl.Utf8,
                "confidence": pl.Float32,
            },
        ).with_columns(pl.lit(fetched_at).alias("fetched_at"))

        df.write_parquet(out_path)
        print(f"  {len(df)} detections → {out_path}", flush=True)
        success += 1

    print(f"\nDone. {success} region(s) fetched, {skipped} skipped.")
    return 0 if skipped == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
