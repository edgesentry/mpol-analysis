---
name: arktrace-run-dashboard
description: Run the arktrace analyst dashboard locally. Use when developing the React frontend or verifying UI changes.
license: Apache-2.0
compatibility: Requires Node.js, npm
metadata:
  repo: arktrace
---

```bash
cd app && npm install   # first time only
cd app && npm run dev   # http://localhost:5173
```

The dev server fetches Parquet files from Cloudflare R2 (same as production). No local backend process needed — the browser queries data directly via DuckDB-WASM.
