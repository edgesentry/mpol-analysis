# arktrace

Shadow fleet candidate screening — AIS ingestion → causal inference → ranked watchlist → analyst dashboard.

**Live app:** [arktrace.edgesentry.io](https://arktrace.edgesentry.io)

## Quick start

```bash
# Dashboard (no server required)
cd app && npm install && npm run dev   # http://localhost:5173

# Pipeline
uv sync --all-extras
uv run python scripts/run_pipeline.py --region singapore --non-interactive
```

## What it does

Applies Difference-in-Differences (DiD) causal modelling to identify vessels whose behaviour changed *because of* a sanction event — not merely anomalous vessels. Two-phase architecture:

1. **Deterministic scoring pipeline** — AIS features, Isolation Forest, HDBSCAN, ownership graph risk, DiD causal model, SHAP attribution. No LLM.
2. **Bounded text synthesis** — browser generates plain-language patrol briefs via a local LLM with strict anti-hallucination constraints. LLM cannot modify scores or access external data. See [docs/ref-llm-grounding.md](docs/ref-llm-grounding.md).

**Validated metrics (blind run, singapore, 2026-04-14):**

| Metric | Value |
|---|---|
| AUROC | 1.0 |
| Precision@50 (multi-region, ≥50 labels) | 0.68 |
| Precision@50 contractual gate | ≥ 0.60 |

## Scope

**This repo:** AIS ingestion → feature engineering → scoring → watchlist → browser dashboard.

**Out of scope:** Physical vessel inspection, edge sensors, VDES — those belong in [edgesentry-rs](https://github.com/edgesentry/edgesentry-rs).

## Agent Skills

```bash
npx skills add edgesentry/arktrace
```

## License

Apache-2.0 OR MIT
