# Pipeline Catalog (Operations View)

This document is an operations-first map of the pipelines in arktrace.
It focuses on four questions:

1. What pipeline type exists?
2. Why does it exist?
3. When should it run?
4. What result should operators expect?

## Scope Clarification: Backend Jobs vs Web UI Operations

The pipeline types in this document refer to backend execution jobs (batch, scheduled, or event-triggered runs), not manual button-by-button work in the web dashboard.

- Backend jobs:
	- Run by CI/CD, schedulers (cron/orchestrator), long-running services, or operator-triggered CLI/API calls.
	- Produce and refresh data artifacts such as DuckDB tables, watchlists, scores, and evaluation reports.
- Web UI operations:
	- Human review and investigation workflow in the dashboard (filter, inspect vessel detail, submit tier/handoff decisions).
	- Consume pipeline outputs and write review feedback, but do not replace core ingestion/scoring/evaluation pipeline execution.

For implementation-level commands and flags, see [Pipeline Operations](pipeline-operations.md).

## Pipeline Types At A Glance

| Pipeline Type | Primary Purpose | Typical Run Timing | Expected Result |
|---|---|---|---|
| Full Screening (11-step) | Generate ranked shadow-fleet candidates from raw/public data | Initial setup, regional refresh, periodic baseline run | Updated watchlist and supporting artifacts for analyst triage |
| Continuous Monitoring (Streaming) | Keep vessel risk ranking fresh with live AIS and alerting | Persistent operations mode (minutes-level cadence) | Near-real-time watchlist updates and threshold-based alerts |
| Historical Backtesting Validation | Validate ranking quality on historical evidence-backed windows | Before model/weight changes, pre-release, governance reviews | Reproducible metrics + threshold recommendations |
| Review-Feedback Evaluation | Learn from human review outcomes and detect quality drift | Weekly/monthly quality cycle, after enough reviews accumulate | Tier-aware and ops-aware quality report with regression checks |
| Public Data Integration Batch | End-to-end known-case coverage check across regions | Post-merge main branch validation and major-change dry runs | Public-overlap integration report and known-case floor check |
| Demo / Smoke Pipeline | Fast environment and UI verification without full ingestion | Demos, incident triage, local sanity checks | Deterministic watchlist for quick dashboard and flow validation |
| Data Publishing (CI) | Build and publish fresh artifacts to R2 for app users | Weekly (Monday 02:00 UTC) and after each successful integration run | Generation zip + demo bundle pushed to `arktrace-public` |

## 1) Full Screening Pipeline (9-step)

### Purpose

Run the complete MPOL screening flow from ingestion to dashboard-ready watchlist.

### When To Run

- New environment bring-up
- Region switch (Singapore/Japan/Middle East/Europe/Gulf)
- Scheduled refresh run (for non-streaming operations)
- Before analyst shift when starting from stale data

### Main Inputs

- Region preset
- Optional live AIS streaming duration
- Optional historical backfill (Marine Cadastre where applicable)
- Optional geopolitical corridor filter

### Expected Outputs

- Region DuckDB (`data/processed/<region>.duckdb`)
- Ranked watchlist (`data/processed/candidate_watchlist.parquet`)
- Scoring artifacts (`composite_scores.parquet`, `<region>_causal_effects.parquet`)
- Validation metrics (`validation_metrics.json`)

### Operational Success Criteria

- Non-empty watchlist with confidence ordering
- Dashboard loads candidates and filters correctly
- No failed step in the 9-step run log

## 2) Continuous Monitoring Pipeline (Streaming)

### Purpose

Maintain continuously updated risk ranking and operational alerts from live AIS.

### When To Run

- During active monitoring windows
- For ports/chokepoints requiring minutes-level visibility
- In always-on operations with periodic re-scoring

### Main Inputs

- Active AIS stream
- Region/bbox configuration
- Alert threshold policy (for confidence crossing)

### Expected Outputs

- AIS micro-batches appended to DuckDB
- Re-scored candidates at operational cadence
- Alert events (SSE/webhook depending on deployment)

### Operational Success Criteria

- Data freshness within target polling interval
- Alerts fire on threshold crossings without excessive lag
- Re-scoring job remains stable over long runtime

## 3) Historical Backtesting Validation Pipeline

### Purpose

Measure ranking quality using historical windows and evidence-backed labels.

### When To Run

- Before promoting scoring/model changes
- Before publishing performance claims
- At recurring quality review checkpoints

### Main Inputs

- Evaluation manifest (versioned windows)
- Window watchlist snapshots
- Labels CSV with source traceability and confidence

### Expected Outputs

- Backtest report (`data/processed/backtest_report.json`)
- Window metrics and cross-window summary
- Capacity-aware threshold hints (`ops_thresholds`)

### Operational Success Criteria

- Metrics generated reproducibly for the same manifest
- Threshold recommendations are backed by labeled support
- No regression beyond agreed governance tolerance

## 4) Review-Feedback Evaluation Pipeline

### Purpose

Close the human-in-the-loop learning loop using latest `vessel_reviews` outcomes.

### When To Run

- Weekly threshold refresh
- Monthly governance review
- After major review volume increase or process change

### Main Inputs

- Latest review snapshot (optionally frozen by `as_of_utc`)
- Region watchlists
- Optional prior report baseline for drift comparison

### Expected Outputs

- Review feedback report (`data/processed/review_feedback_evaluation.json`)
- Tier-aware mix and operations-aware hit-rate summaries
- Region/capacity threshold recommendations
- Drift/regression pass/fail checks

### Operational Success Criteria

- Snapshot reproducibility (same `as_of_utc` yields same result)
- Regression checks clearly indicate pass/fail by region
- Report is actionable for threshold update decisions

## 5) Public Data Integration Batch Pipeline

### Purpose

Run a medium-scale end-to-end verification using practical public positive-label sources.

### When To Run

- Automatically after merge to `main`
- Manually before high-risk release or major data-path change

### Main Inputs

- Public sanctions snapshot DB
- Multi-region pipeline outputs
- Known-case thresholds (`min_known_cases`, `max_known_cases`)

### Expected Outputs

- Integration manifest/report/summary artifacts
- Region-specific evaluation label files
- Known-case floor pass/fail result

### Operational Success Criteria

- Public data refresh and ingestion complete
- Known-case floor is met (when strict mode enabled)
- Found-vs-missed coverage remains within acceptable bounds

## 6) Delayed-Label Intelligence (Backtracking) Pipeline

### Purpose

Convert newly confirmed vessel labels into forward-looking detection power without requiring a full model retrain:

1. **Causal rewind** — retroactively scans trailing 12 months of AIS data per confirmed vessel and surfaces precursor signals (AIS gap uplift, STS proxy, low-SOG fraction) that appeared before confirmation.
2. **Label propagation** — traverses the ownership/STS graph from confirmed MMSIs to identify and risk-uplift related entities (shared owner, shared manager, STS contact).

### When To Run

- After any new `confirmed` label is ingested via the review panel
- Weekly sweep to catch batch-confirmed outcomes
- Incremental mode (`--since`) after each shift's review session

### Main Inputs

- `vessel_reviews` table (confirmed-tier entries)
- Lance Graph ownership/STS datasets
- `ais_positions` table (trailing 12-month window per vessel)

### Expected Outputs

- `data/processed/backtracking_report.json` — full structured report
- `data/processed/backtracking_report.md` — human-readable summary with precursor signal table and propagated entity list
- `regression_checks.pass` field (True = all confirmed vessels successfully rewound)

### Operational Success Criteria

- `regression_checks.pass` is `true` in every run
- At least one precursor signal detected for vessels with sufficient AIS history
- Propagated entities are traceable to a specific confirmed seed via `source_mmsi` + `evidence_type`

See [Backtracking Runbook](backtracking-runbook.md) for full CLI reference and demo scenario.

---

## 7) Demo / Smoke Pipeline

### Purpose

Provide fast deterministic validation of dashboard and operator flow without full data processing.

### When To Run

- Demo preparation
- Rapid post-deploy sanity checks
- Environment troubleshooting

### Main Inputs

- Bundled demo watchlist fixture

### Expected Outputs

- Processed watchlist replaced with deterministic demo data
- Dashboard map/table interaction becomes immediately testable

### Operational Success Criteria

- UI loads non-empty candidate list quickly
- Core interaction paths work (filter, detail, review actions)

## 8) Data Publishing Pipeline (CI)

### Purpose

Build fresh pipeline artifacts for all five regions and publish them to
`arktrace-public` so app users can pull pre-built data without running the
pipeline locally.

### When To Run

- Automatically every Monday 02:00 UTC (`data-publish.yml` schedule).
- Automatically after every successful `Public Backtest Integration` run on `main`.
- Manually via `workflow_dispatch` after a data update.

### Main Inputs

| Source | What | Notes |
|---|---|---|
| `arktrace-private-capvista` | Custom feed CSVs (AIS, SAR, cargo, sanctions) | Pulled at start of run; skipped gracefully if credentials absent (forks) |
| Public data (GDELT, OpenSanctions) | Fetched during pipeline run | No API key required |
| `--seed-dummy` flag | Injects 10 known OFAC vessels into scoring | Ensures backtest has sufficient labeled positives |

### Pipeline Steps

1. Pull custom feeds from `arktrace-private-capvista` → `_inputs/custom_feeds/` (`continue-on-error`).
2. Run `run_public_backtest_batch.py` for all 5 regions in seed mode — custom feeds are ingested at step 5 of the 11-step pipeline.
3. `sync_r2.py push --keep 1` — zip all region artifacts, upload to `arktrace-public`, delete previous generation.
4. `sync_r2.py push-demo` — overwrite `demo.zip` (lightweight bundle for quick developer setup).
5. `sync_r2.py push-sanctions-db --force` — upload `public_eval.duckdb`.
6. Lead time validation (`validate_lead_time_ofac.py`).
7. Metrics email notification (`notify_metrics.py`).

### Expected Outputs

| Artifact | Destination | Consumer |
|---|---|---|
| `<timestamp>.zip` (all 5 regions) | `arktrace-public` | App users via `sync_r2.py pull` |
| `latest` (plain-text pointer) | `arktrace-public` | `sync_r2.py pull` staleness check |
| `demo.zip` | `arktrace-public` | Developers via `sync_r2.py pull-demo` |
| `public_eval.duckdb` | `arktrace-public` | Integration tests via `sync_r2.py pull-sanctions-db` |

Each generation zip contains, per region:
`{region}.duckdb`, `{region}_watchlist.parquet`, `{region}_causal_effects.parquet`,
`{region}_graph/`, `candidate_watchlist.parquet`, `validation_metrics.json`.

### Operational Success Criteria

- All 5 regions produce non-empty watchlists.
- `push` step reports a non-empty snapshot ID in its output.
- Metrics email is sent (or gracefully skipped if SMTP secrets absent).
- No region DuckDB or watchlist parquet is missing from the zip.

See [r2-data-layout.md](r2-data-layout.md) for the full bucket layout, actor responsibilities, and credential model.

---

## Suggested Operations Cadence

| Cadence | Pipeline | Goal | Trigger Type | Triggered By (if Event) |
|---|---|---|---|---|
| Continuous | Continuous Monitoring | Live situational awareness | Scheduled (always-on service loop) | N/A |
| Daily or per watch | Full Screening (if non-streaming mode) | Fresh candidate ranking | Scheduled or Event-triggered | Duty operations officer / shift lead |
| Weekly | Review-Feedback Evaluation | Threshold tuning and drift control | Scheduled | N/A |
| Weekly (Monday 02:00 UTC) | Data Publishing (CI) | Publish fresh artifacts to R2 for app users | Scheduled + Event-triggered | CI schedule or after Public Backtest Integration succeeds |
| After each confirmed label | Delayed-Label Intelligence (Backtracking) | Precursor discovery + graph uplift | Event-triggered | Analyst submitting confirmed review / weekly sweep |
| Pre-release | Historical Backtesting + Public Integration Batch | Quality gate before change promotion | Event-triggered | Release owner / CI pipeline on release candidate |
| On-demand | Demo/Smoke | Fast verification and incident checks | Event-triggered | Analyst / operator / incident commander |

## Decision Guide

- Need fresh candidate generation from raw data: run Full Screening.
- Need live alerting and near-real-time updates: run Continuous Monitoring.
- Need evidence-backed quality measurement on historical windows: run Historical Backtesting.
- Need to tune thresholds from analyst outcomes and check drift: run Review-Feedback Evaluation.
- Need to convert a new confirmed label into precursor insights and graph uplift: run Backtracking.
- Need broad post-merge safety check on practical known positives: run Public Data Integration Batch.
- Need a quick UI/environment confidence check: run Demo/Smoke.
- Need to publish fresh pre-built artifacts to R2 for app users: run Data Publishing (CI) via `workflow_dispatch` or wait for the weekly schedule.
