"""Build per-region GEBCO 200m depth masks as H3-resolution-8 cell sets.

Downloads GEBCO 2024 bathymetry data via the public WCS endpoint for each
region bounding box, converts deep-water pixels (depth ≤ -200m) to H3 cells
at resolution 8, and writes ``{region_prefix}_deep_cells.parquet`` to the
output directory.

Run once locally, then push to R2 with:
    uv run python scripts/sync_r2.py push-gebco-masks

Usage:
    uv run python scripts/build_gebco_mask.py
    uv run python scripts/build_gebco_mask.py --regions singapore,japan
    uv run python scripts/build_gebco_mask.py --out-dir data/processed
"""

from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# GEBCO 2024 WCS endpoint — publicly accessible, no auth required.
# Returns a GeoTIFF with signed int16 depth values (negative = below sea level).
_GEBCO_WCS = (
    "https://www.gebco.net/data_and_products/gebco_web_services/web_map_service/mapserv"
    "?SERVICE=WCS&REQUEST=GetCoverage&VERSION=1.0.0&COVERAGE=gebco_latest_2"
    "&CRS=EPSG:4326&FORMAT=GeoTIFF"
    "&BBOX={west},{south},{east},{north}&WIDTH={width}&HEIGHT={height}"
)

# WCS raster resolution: 1 arc-minute ≈ 1.85 km.  Matches GEBCO native resolution
# and produces files of ~5 MB per region — small enough to commit-free push to R2.
_ARC_MINUTES_PER_DEG = 60

# H3 resolution 8 cells are ~0.7 km edge, ~0.74 km² area.
_H3_RES = 8

# Filter threshold: cells where depth ≤ this are considered deep water.
DEPTH_THRESHOLD_M = -200

# Active backtest regions: name → (lat_min, lon_min, lat_max, lon_max)
# Bboxes match run_pipeline.py PRESETS.
_REGIONS: dict[str, tuple[float, float, float, float]] = {
    "singapore": (-5.0, 92.0, 22.0, 122.0),
    "japan": (25.0, 115.0, 48.0, 145.0),
    "blacksea": (40.0, 27.0, 48.0, 42.0),
    "europe": (30.0, -22.0, 72.0, 42.0),
    "middleeast": (-10.0, 32.0, 30.0, 80.0),
}

# Maps region name → file prefix used for the DB and parquet files
_REGION_PREFIX: dict[str, str] = {
    "singapore": "singapore",
    "japan": "japansea",
    "blacksea": "blacksea",
    "europe": "europe",
    "middleeast": "middleeast",
}


def _download_gebco(bbox: tuple[float, float, float, float], out_path: Path) -> None:
    """Download GEBCO GeoTIFF for the given bbox to out_path."""
    import httpx

    lat_min, lon_min, lat_max, lon_max = bbox
    lat_span = lat_max - lat_min
    lon_span = lon_max - lon_min
    height = int(lat_span * _ARC_MINUTES_PER_DEG)
    width = int(lon_span * _ARC_MINUTES_PER_DEG)

    url = _GEBCO_WCS.format(
        south=lat_min,
        north=lat_max,
        west=lon_min,
        east=lon_max,
        width=width,
        height=height,
    )
    print(f"  Downloading GEBCO GeoTIFF ({width}×{height} px) ...", flush=True)
    with httpx.Client(timeout=300, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
    out_path.write_bytes(resp.content)
    print(f"  Downloaded {out_path.stat().st_size / 1_048_576:.1f} MB", flush=True)


def build_deep_cell_mask(
    bbox: tuple[float, float, float, float],
    depth_threshold: int = DEPTH_THRESHOLD_M,
) -> set[str]:
    """Return the set of H3 res-8 cells in bbox where GEBCO depth ≤ depth_threshold.

    Downloads GEBCO data for the bbox, converts depth raster pixels to H3 cells,
    and returns the subset where depth is at or below the threshold (deep water).
    """
    import h3
    import numpy as np
    import rasterio

    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        tif_path = Path(tmp.name)

    try:
        _download_gebco(bbox, tif_path)

        with rasterio.open(tif_path) as src:
            data = src.read(1).astype(np.int32)  # depth in metres, negative = below sea
            transform = src.transform
            height, width = data.shape

        print(f"  Raster: {width}×{height} pixels, depth range [{data.min()}, {data.max()}] m")

        # Sample pixel centres and convert to H3 cells
        rows, cols = np.where(data <= depth_threshold)
        if len(rows) == 0:
            print("  Warning: no pixels ≤ threshold — check bbox or GEBCO download")
            return set()

        # rasterio xy gives (x=lon, y=lat) for each pixel centre
        xs, ys = rasterio.transform.xy(transform, rows, cols)
        lats = np.asarray(ys, dtype=np.float64)
        lons = np.asarray(xs, dtype=np.float64)

        deep_cells: set[str] = set()
        try:
            # h3-py >= 4
            for lat, lon in zip(lats, lons):
                deep_cells.add(h3.latlng_to_cell(lat, lon, _H3_RES))
        except AttributeError:
            # h3-py < 4
            for lat, lon in zip(lats, lons):
                deep_cells.add(h3.geo_to_h3(lat, lon, _H3_RES))

        print(f"  {len(deep_cells):,} deep-water H3 cells (depth ≤ {depth_threshold} m)")
        return deep_cells

    finally:
        tif_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build GEBCO 200m depth masks per region")
    parser.add_argument(
        "--regions",
        default=",".join(_REGIONS),
        help=f"Comma-separated regions (default: {','.join(_REGIONS)})",
    )
    parser.add_argument(
        "--out-dir",
        default=os.getenv("ARKTRACE_DATA_DIR", str(Path.home() / ".arktrace" / "data")),
        metavar="DIR",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=DEPTH_THRESHOLD_M,
        metavar="M",
        help=f"Depth threshold in metres (default: {DEPTH_THRESHOLD_M})",
    )
    args = parser.parse_args()

    import polars as pl

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    regions = [r.strip() for r in args.regions.split(",") if r.strip()]
    success, skipped = 0, 0

    for region in regions:
        if region not in _REGIONS:
            print(f"[warn] Unknown region '{region}' — skipping")
            skipped += 1
            continue

        prefix = _REGION_PREFIX[region]
        out_path = out_dir / f"{prefix}_deep_cells.parquet"
        print(f"\n[{region}] Building depth mask (threshold={args.depth} m) ...")

        try:
            deep_cells = build_deep_cell_mask(_REGIONS[region], depth_threshold=args.depth)
        except Exception as exc:
            print(f"  [error] {exc}")
            skipped += 1
            continue

        df = pl.DataFrame({"h3_cell": sorted(deep_cells)})
        df.write_parquet(out_path)
        print(f"  Saved {len(df):,} cells → {out_path}")
        success += 1

    print(f"\nDone. {success} region(s) built, {skipped} skipped.")
    print("Push to R2 with: uv run python scripts/sync_r2.py push-gebco-masks")
    return 0 if skipped == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
