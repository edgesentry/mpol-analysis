# arktrace

Open-source Maritime Pattern of Life (MPOL) analysis pipeline for identifying shadow fleet vessel candidates using public data.

Built for **Cap Vista Accelerator Solicitation 5.0, Challenge 1** (deadline: 29 April 2026).

## What It Does

Ingests public AIS, sanctions, vessel registry, and trade flow data to produce a ranked watchlist of candidate shadow fleet vessels — ships operating in the regulatory grey zone through AIS manipulation, flag/name laundering, and illicit ship-to-ship transfers.

**Output:** `data/processed/candidate_watchlist.parquet` — ranked vessels with composite confidence scores and SHAP-explained top signals, ready to hand off to a patrol officer.

## Why this project is called Arktrace?

`Arktrace` is a portmanteau of "Ark" (denoting protection, sanctuary, and the traditional maritime vessel) and "Trace" (representing the digital footprint, vessel tracks, and the pursuit of evidence).

It signifies our mission to safeguard maritime integrity by uncovering hidden truths within complex global data—serving as the analytical "Ark" that protects global trade lanes through advanced forensic "Tracing."

## Documentation

Full documentation is in [`docs/`](docs/):

| Document | Contents |
|---|---|
| [Introduction](docs/index.md) | What it does, how it fits the full system, Cap Vista alignment |
| [Background](docs/background.md) | Shadow fleet problem, geography, evasion techniques, prior art |
| [Architecture](docs/architecture.md) | Pipeline diagram, data storage design, feature and scoring design |
| [Technical Solution](docs/technical-solution.md) | Tech stack, data sources, algorithms, output schema |
| [Scenarios](docs/scenarios.md) | End-to-end workflows: morning brief, investigation, streaming, patrol handoff |
| [Roadmap](docs/roadmap.md) | Phase A (screening) + Phase B (field investigation in edgesentry OSS) |
| [Field Investigation](docs/field-investigation.md) | Physical vessel measurement, evidence capture, VDES reporting (edgesentry-rs/app) |
| [Backtesting Validation](docs/backtesting-validation.md) | Historical offline evaluation workflow, labels policy, and threshold tuning |

## Scope

**This repo:** Public data ingestion → feature engineering → shadow fleet scoring → ranked candidate watchlist.

**Out of scope:** Physical vessel inspection, edge sensor measurement, VDES reporting — implemented in [edgesentry-rs](https://github.com/edgesentry/edgesentry-rs) and edgesentry-app. See [docs/field-investigation.md](docs/field-investigation.md) for the design.

## License

Apache-2.0 OR MIT
