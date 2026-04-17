"""Generate sample Parquet files + manifest for local dev/testing.

Writes to app/public/fixtures/ so Vite serves them at /fixtures/.
The app reads VITE_MANIFEST_URL from .env.development to use these
instead of the R2 bucket.

Usage:
    uv run python scripts/generate_dev_fixtures.py
"""

from __future__ import annotations

import json
import random
import sys
from pathlib import Path

import polars as pl

_REPO_ROOT = Path(__file__).resolve().parents[1]
_OUT_DIR = _REPO_ROOT / "app" / "public" / "fixtures"

REGIONS = ["singapore", "japansea", "middleeast", "europe", "gulfofaden"]
FLAGS = ["CN", "PA", "MH", "LR", "BS", "KM", "SG", "HK", "CY", "MT"]
VESSEL_TYPES = ["Tanker", "Bulk Carrier", "Container", "General Cargo", "Ro-Ro"]
NAMES = [
    "PACIFIC GLORY", "OCEAN SPIRIT", "GOLDEN TRADE", "SEA FALCON",
    "NORTHERN STAR", "BLUE HORIZON", "IRON MAIDEN", "SWIFT ARROW",
    "DARK SHADOW", "SILVER MOON", "DAWN TRADER", "GHOST WIND",
    "CORAL SEA", "NEPTUNE", "POSEIDON", "ARGO", "HERMES", "ATLAS",
    "TITAN", "OLYMPUS", "PERSEUS", "ORION", "PHOENIX", "HYDRA",
    "MEDUSA", "LEVIATHAN", "KRAKEN", "TRITON", "CALYPSO", "TETHYS",
]

# Rough lat/lon centres per region
_REGION_CENTRES: dict[str, tuple[float, float]] = {
    "singapore":  (1.3, 104.0),
    "japansea":   (37.0, 135.0),
    "middleeast": (24.0, 58.0),
    "europe":     (54.0, 10.0),
    "gulfofaden": (12.0, 46.0),
}


def _rand_mmsi(rng: random.Random) -> str:
    return str(rng.randint(200_000_000, 799_999_999))


def _jitter(centre: float, spread: float, rng: random.Random) -> float:
    return round(centre + rng.uniform(-spread, spread), 4)


def make_watchlist(n: int = 120, seed: int = 42) -> pl.DataFrame:
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        region = rng.choice(REGIONS)
        lat_c, lon_c = _REGION_CENTRES[region]
        rows.append(
            {
                "mmsi": _rand_mmsi(rng),
                "vessel_name": rng.choice(NAMES) + f" {i + 1}",
                "flag": rng.choice(FLAGS),
                "vessel_type": rng.choice(VESSEL_TYPES),
                "confidence": round(rng.uniform(0.35, 0.99), 4),
                "last_lat": _jitter(lat_c, 6.0, rng),
                "last_lon": _jitter(lon_c, 8.0, rng),
                "last_seen": "2026-04-17T12:00:00Z",
                "region": region,
            }
        )
    return pl.DataFrame(rows)


def make_validation_metrics() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "backtest_p_at_50": 0.24,
                "backtest_auroc": 0.91,
                "backtest_recall_at_200": 0.68,
                "model_version": "dev-fixture",
            }
        ]
    )


def main() -> int:
    _OUT_DIR.mkdir(parents=True, exist_ok=True)

    watchlist = make_watchlist()
    wl_path = _OUT_DIR / "watchlist.parquet"
    watchlist.write_parquet(wl_path)
    print(f"  watchlist.parquet        {len(watchlist)} rows  ({wl_path.stat().st_size} bytes)")

    metrics = make_validation_metrics()
    vm_path = _OUT_DIR / "validation_metrics.parquet"
    metrics.write_parquet(vm_path)
    print(f"  validation_metrics.parquet {len(metrics)} rows  ({vm_path.stat().st_size} bytes)")

    # Build manifest pointing to Vite-served /fixtures/ paths
    manifest = {
        "generated_at": "2026-04-17T00:00:00Z",
        "base_url": "/fixtures",
        "files": [
            {
                "key": "fixtures/watchlist.parquet",
                "url": "/fixtures/watchlist.parquet",
                "size_bytes": wl_path.stat().st_size,
                "table": "watchlist",
                "register_as": "watchlist.parquet",
            },
            {
                "key": "fixtures/validation_metrics.parquet",
                "url": "/fixtures/validation_metrics.parquet",
                "size_bytes": vm_path.stat().st_size,
                "table": "validation_metrics",
                "register_as": "validation_metrics.parquet",
            },
        ],
    }
    manifest_path = _OUT_DIR / "ducklake_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"  ducklake_manifest.json   ({manifest_path.stat().st_size} bytes)")
    print()
    print(f"Fixtures written to {_OUT_DIR}")
    print("Set VITE_MANIFEST_URL=/fixtures/ducklake_manifest.json in app/.env.development")
    print("to use these fixtures instead of R2.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
