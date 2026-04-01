# capvista-mpol-analysis

Open-source Maritime Pattern of Life (MPOL) analysis pipeline for identifying shadow fleet vessel candidates using public data.

Built for **Cap Vista Accelerator Solicitation 5.0, Challenge 1** (deadline: 29 April 2026).

## What It Does

Ingests public AIS, sanctions, vessel registry, and trade flow data to produce a ranked watchlist of candidate shadow fleet vessels — ships operating in the regulatory grey zone through AIS manipulation, flag/name laundering, and illicit ship-to-ship transfers.

**Output:** `data/processed/candidate_watchlist.parquet` — ranked vessels with composite confidence scores and SHAP-explained top signals, ready to hand off to a patrol officer.

## Documentation

Full documentation is in [`docs/`](docs/):

| Document | Contents |
|---|---|
| [Introduction](docs/introduction.md) | What it does, how it fits the full system, Cap Vista alignment |
| [Background](docs/background.md) | Shadow fleet problem, geography, evasion techniques, prior art |
| [Architecture](docs/architecture.md) | Pipeline diagram, data storage design, feature and scoring design |
| [Technical Solution](docs/technical-solution.md) | Tech stack, data sources, algorithms, output schema |
| [Scenarios](docs/scenarios.md) | End-to-end workflows: morning brief, investigation, streaming, patrol handoff |
| [Roadmap](docs/roadmap.md) | Phase A (screening) + Phase B (field investigation in edgesentry OSS) |
| [Field Investigation](docs/field-investigation.md) | Physical vessel measurement, evidence capture, VDES reporting (edgesentry-rs/app) |

## Tech Stack

| Layer | Tool |
|---|---|
| Analytical store | DuckDB |
| Feature engineering | Polars |
| Ownership graph | Neo4j Community (Docker) + GDS plugin |
| ML / scoring | scikit-learn (HDBSCAN, Isolation Forest) |
| Explainability | SHAP |
| Dashboard | Streamlit |
| Language | Python 3.12 |
| Packaging | uv |

## Scope

**This repo:** Public data ingestion → feature engineering → shadow fleet scoring → ranked candidate watchlist.

**Out of scope:** Physical vessel inspection, edge sensor measurement, VDES reporting — implemented in [edgesentry-rs](https://github.com/edgesentry/edgesentry-rs) and edgesentry-app. See [docs/field-investigation.md](docs/field-investigation.md) for the design.

## License

Apache-2.0 OR MIT
