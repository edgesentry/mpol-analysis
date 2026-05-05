---
name: arktrace-run-pipeline
description: Run the arktrace shadow fleet screening pipeline for a region. Use when scoring vessels, refreshing the watchlist, or running the full pipeline end-to-end.
license: Apache-2.0
compatibility: Requires uv, DuckDB, .env with API keys
metadata:
  repo: arktrace
---

## Interactive (recommended)

```bash
uv run python scripts/run_pipeline.py
```

Prompts for region selection and handles all flags automatically.

## Non-interactive

```bash
uv run python scripts/run_pipeline.py --region singapore --non-interactive
uv run python scripts/run_pipeline.py --region japan --non-interactive
```

Available regions: `singapore`, `japan`, `middleeast`, `europe`, `persiangulf`, `gulfofguinea`, `gulfofaden`, `gulfofmexico`, `blacksea`

## Step-by-step (manual)

```bash
uv run python -m pipeline.src.ingest.schema
uv run python -m pipeline.src.ingest.marine_cadastre
uv run python -m pipeline.src.ingest.sanctions
uv run python -m pipeline.src.ingest.vessel_registry
uv run python -m pipeline.src.features.ais_behavior
uv run python -m pipeline.src.features.identity
uv run python -m pipeline.src.features.ownership_graph
uv run python -m pipeline.src.features.trade_mismatch
uv run python -m pipeline.src.score.mpol_baseline
uv run python -m pipeline.src.score.anomaly
uv run python -m pipeline.src.score.causal_sanction
uv run python -m pipeline.src.score.composite
uv run python -m pipeline.src.score.watchlist
```

## EO detections (requires GFW_API_TOKEN)

```bash
uv run python -m pipeline.src.ingest.eo_gfw --bbox 95,1,110,6 --days 30
# Without token:
uv run python -m pipeline.src.ingest.eo_gfw --csv data/raw/eo_detections_sample.csv
```
