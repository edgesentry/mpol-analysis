---
name: arktrace-demo-data
description: Download the arktrace demo dataset from R2 for local development. Use when you need sample data without running the full pipeline.
license: Apache-2.0
compatibility: Requires AWS CLI or wrangler; no credentials needed for public bucket
metadata:
  repo: arktrace
---

```bash
uv run python scripts/sync_r2.py --demo
```

Downloads a lightweight Parquet bundle to `data/processed/`. No API keys required — the demo bundle is public.

After download, start the dashboard:

```bash
cd app && npm run dev   # http://localhost:5173
```

See [references/demo-data.md](references/demo-data.md) for bundle contents, size, and update cadence.
