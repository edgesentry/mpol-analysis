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
from datetime import date, timedelta
from pathlib import Path

import polars as pl

_REPO_ROOT = Path(__file__).resolve().parents[1]
_OUT_DIR = _REPO_ROOT / "app" / "public" / "fixtures"

REGIONS = ["singapore", "japansea", "middleeast", "europe", "gulfofaden"]
FLAGS = ["CN", "PA", "MH", "LR", "BS", "KM", "SG", "HK", "CY", "MT"]
VESSEL_TYPES = ["Tanker", "Bulk Carrier", "Container", "General Cargo", "Ro-Ro"]
NAMES = [
    "PACIFIC GLORY",
    "OCEAN SPIRIT",
    "GOLDEN TRADE",
    "SEA FALCON",
    "NORTHERN STAR",
    "BLUE HORIZON",
    "IRON MAIDEN",
    "SWIFT ARROW",
    "DARK SHADOW",
    "SILVER MOON",
    "DAWN TRADER",
    "GHOST WIND",
    "CORAL SEA",
    "NEPTUNE",
    "POSEIDON",
    "ARGO",
    "HERMES",
    "ATLAS",
    "TITAN",
    "OLYMPUS",
    "PERSEUS",
    "ORION",
    "PHOENIX",
    "HYDRA",
    "MEDUSA",
    "LEVIATHAN",
    "KRAKEN",
    "TRITON",
    "CALYPSO",
    "TETHYS",
]

_REGIMES = ["OFAC Iran", "OFAC Russia", "UN DPRK"]

# Rough lat/lon centres per region
_REGION_CENTRES: dict[str, tuple[float, float]] = {
    "singapore": (1.3, 104.0),
    "japansea": (37.0, 135.0),
    "middleeast": (24.0, 58.0),
    "europe": (54.0, 10.0),
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


def make_causal_effects(watchlist: pl.DataFrame, seed: int = 42) -> pl.DataFrame:
    """Per-vessel ATT estimates — ~90% of watchlist vessels get a regime assignment."""
    rng = random.Random(seed)
    rows = []
    for mmsi in watchlist["mmsi"].to_list():
        if rng.random() > 0.1:  # 90% coverage
            att = round(rng.uniform(-0.1, 0.65), 3)
            half_width = round(rng.uniform(0.05, 0.15), 3)
            p = round(rng.uniform(0.001, 0.5), 4)
            rows.append(
                {
                    "mmsi": mmsi,
                    "regime": rng.choice(_REGIMES),
                    "att_estimate": att,
                    "att_ci_lower": round(att - half_width, 3),
                    "att_ci_upper": round(att + half_width, 3),
                    "p_value": p,
                    "is_significant": p < 0.05,
                }
            )
    return pl.DataFrame(rows)


def make_score_history(watchlist: pl.DataFrame, days: int = 30, seed: int = 42) -> pl.DataFrame:
    """30-day daily confidence history for every watchlist vessel."""
    rng = random.Random(seed)
    anchor_date = date(2026, 4, 17)
    rows = []
    for mmsi, base_conf in zip(watchlist["mmsi"].to_list(), watchlist["confidence"].to_list()):
        conf = float(base_conf)
        for d in range(days):
            score_date = (anchor_date - timedelta(days=days - 1 - d)).isoformat()
            conf = max(0.1, min(0.99, conf + rng.uniform(-0.04, 0.04)))
            rows.append({"mmsi": mmsi, "score_date": score_date, "confidence": round(conf, 4)})
    return pl.DataFrame(rows).with_columns(pl.col("confidence").cast(pl.Float32))


def main() -> int:
    _OUT_DIR.mkdir(parents=True, exist_ok=True)

    watchlist = make_watchlist()
    wl_path = _OUT_DIR / "watchlist.parquet"
    watchlist.write_parquet(wl_path)
    print(f"  watchlist.parquet          {len(watchlist)} rows  ({wl_path.stat().st_size} bytes)")

    metrics = make_validation_metrics()
    vm_path = _OUT_DIR / "validation_metrics.parquet"
    metrics.write_parquet(vm_path)
    print(f"  validation_metrics.parquet {len(metrics)} rows  ({vm_path.stat().st_size} bytes)")

    causal = make_causal_effects(watchlist)
    ce_path = _OUT_DIR / "causal_effects.parquet"
    causal.write_parquet(ce_path)
    print(f"  causal_effects.parquet     {len(causal)} rows  ({ce_path.stat().st_size} bytes)")

    history = make_score_history(watchlist)
    sh_path = _OUT_DIR / "score_history.parquet"
    history.write_parquet(sh_path)
    print(f"  score_history.parquet      {len(history)} rows  ({sh_path.stat().st_size} bytes)")

    from datetime import UTC, datetime

    manifest = {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
            {
                "key": "fixtures/causal_effects.parquet",
                "url": "/fixtures/causal_effects.parquet",
                "size_bytes": ce_path.stat().st_size,
                "table": "causal_effects",
                "register_as": "causal_effects.parquet",
            },
            {
                "key": "fixtures/score_history.parquet",
                "url": "/fixtures/score_history.parquet",
                "size_bytes": sh_path.stat().st_size,
                "table": "score_history",
                "register_as": "score_history.parquet",
            },
        ],
    }
    manifest_path = _OUT_DIR / "ducklake_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"  ducklake_manifest.json     ({manifest_path.stat().st_size} bytes)")
    print()
    print(f"Fixtures written to {_OUT_DIR}")
    print("Set VITE_MANIFEST_URL=/fixtures/ducklake_manifest.json in app/.env.development")
    print("to use these fixtures instead of R2.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
