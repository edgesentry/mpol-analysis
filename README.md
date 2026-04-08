# arktrace

**Causal Inference Engine for Shadow Fleet Prediction** — identifies vessels that *causally respond* to sanction announcements with evasion behaviour, surfacing unknown-unknown threats **60–90 days before** they appear on public sanctions lists.

Built for **Cap Vista Accelerator Solicitation 5.0, Challenge 1** (deadline: 29 April 2026).

## Quick Start

```bash
# Pull pre-built image (linux/amd64 and linux/arm64)
docker pull ghcr.io/edgesentry/arktrace:latest
docker compose up
```

Or build from source:

```bash
git clone https://github.com/edgesentry/arktrace && cd arktrace
docker compose up
```

## What It Does

arktrace applies Difference-in-Differences (DiD) causal modelling to identify vessels whose behaviour changed *specifically because of* a sanction event — not merely vessels that look anomalous. AIS position history, ownership graph proximity, and trade flow data serve as the evidentiary substrate; the novel methodology is causal inference and network-based backtracking propagation.

**Output:** `data/processed/candidate_watchlist.parquet` — ranked vessels with SHAP-explained causal and network signals, pre-designation lead time backtested at 60–90 days before OFAC listing, ready to hand off to a patrol officer.

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
| [Triage Governance](docs/triage-governance.md) | Tier taxonomy, evidence policy, escalation lifecycle, and KPI spec |

## Scope

**This repo:** Public data ingestion → feature engineering → shadow fleet scoring → ranked candidate watchlist.

**Out of scope:** Physical vessel inspection, edge sensor measurement, VDES reporting — implemented in [edgesentry-rs](https://github.com/edgesentry/edgesentry-rs) and edgesentry-app. See [docs/field-investigation.md](docs/field-investigation.md) for the design.

## License

Apache-2.0 OR MIT
