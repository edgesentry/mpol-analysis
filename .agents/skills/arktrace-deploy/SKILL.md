---
name: arktrace-deploy
description: Deploy arktrace to Cloudflare Pages and configure R2, CI, and environment variables. Use when setting up a new deployment or updating production configuration.
license: Apache-2.0
compatibility: Requires wrangler CLI, Cloudflare account, GitHub Actions access
metadata:
  repo: arktrace
---

## Dashboard (Cloudflare Pages)

```bash
cd app && npm run build
npx wrangler pages deploy dist --project-name arktrace
```

## R2 bucket setup

```bash
npx wrangler r2 bucket create arktrace-public
npx wrangler r2 bucket create arktrace-private
```

## Environment variables (Cloudflare Pages)

Set via Cloudflare Dashboard → Pages → Settings → Environment variables:
- `AISSTREAM_API_KEY`
- `EQUASIS_USERNAME` / `EQUASIS_PASSWORD`
- `GFW_API_TOKEN`

## CI pipeline publish

```bash
uv run python scripts/sync_r2.py --region singapore
```

See [references/deployment.md](references/deployment.md) for full architecture, Docker pipeline setup, and GitHub Actions configuration.
