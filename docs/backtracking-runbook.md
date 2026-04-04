# Backtracking Runbook

Operational guide for the delayed-label intelligence loop (Issue #67).

## Overview

The backtracking loop converts delayed confirmed labels (sanctions, field investigations) into:
1. **Causal rewind** — retroactive scan of the trailing 12 months to surface precursor signals
2. **Label propagation** — uplift risk scores for related entities via ownership and STS graphs

The loop is designed for incremental execution: re-run after each new confirmed label is ingested using `--since` to process only newly arrived labels.

## When to run

| Trigger | Command |
|---|---|
| New confirmed label ingested | `bash scripts/run_operations_shell.sh` → option 2 + `scripts/run_backtracking.py` |
| Scheduled weekly sweep | `uv run python scripts/run_backtracking.py` (full pass) |
| Incremental since checkpoint | `uv run python scripts/run_backtracking.py --since <ISO-timestamp>` |

## Running the loop

### Full pass (all confirmed labels)

```bash
uv run python scripts/run_backtracking.py \
  --db data/processed/mpol.duckdb \
  --output data/processed/backtracking_report.json \
  --md-output data/processed/backtracking_report.md
```

### Incremental pass (only labels confirmed since a checkpoint)

```bash
uv run python scripts/run_backtracking.py \
  --db data/processed/mpol.duckdb \
  --since 2026-04-01T00:00:00Z \
  --output data/processed/backtracking_report.json \
  --md-output data/processed/backtracking_report.md
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--db` | `data/processed/mpol.duckdb` | DuckDB database path |
| `--output` | `data/processed/backtracking_report.json` | JSON report artifact |
| `--md-output` | `data/processed/backtracking_report.md` | Markdown summary |
| `--since` | _(none — all)_ | ISO timestamp cutoff for incremental mode |
| `--as-of-utc` | _(now)_ | Upper bound timestamp for label selection |
| `--rewind-days` | `365` | How far back to scan per confirmed vessel |

## Output artifacts

### JSON report (`backtracking_report.json`)

```json
{
  "generated_at": "2026-04-04T00:00:00+00:00",
  "since_utc": null,
  "new_confirmed_mmsis": ["123456789"],
  "rewind": {
    "vessel_count": 1,
    "vessels": [
      {
        "mmsi": "123456789",
        "confirmed_at": "2026-04-01T00:00:00+00:00",
        "ais_records_scanned": 3847,
        "rewind_days": 365,
        "precursor_signals": [
          {
            "feature": "ais_gap_count",
            "recent_value": 8.3,
            "baseline_value": 0.4,
            "uplift_ratio": 20.75
          }
        ],
        "monthly_snapshots": [...]
      }
    ]
  },
  "propagation": {
    "seed_count": 1,
    "propagated_count": 3,
    "vessels": [
      {"mmsi": "123456789", "hop": 0, "evidence_type": "confirmed_direct", ...},
      {"mmsi": "987654321", "hop": 1, "evidence_type": "shared_owner", ...}
    ]
  },
  "regression_checks": {
    "confirmed_vessel_count": 1,
    "rewind_vessel_count": 1,
    "propagated_entity_count": 3,
    "pass": true
  }
}
```

### Markdown summary (`backtracking_report.md`)

Human-readable summary with precursor signals per vessel and a propagation table.

## Modules

| Module | Purpose |
|---|---|
| `src/analysis/causal_rewind.py` | Per-vessel retroactive feature analysis |
| `src/analysis/label_propagation.py` | Graph-based uplift to related entities |
| `src/analysis/backtracking_runner.py` | Orchestrator + Markdown/JSON output |
| `scripts/run_backtracking.py` | CLI entry point |

## Precursor signals

A feature is flagged as a precursor signal when its average in the **0–90 day pre-confirmation window** exceeds the **90–365 day baseline** by more than 50% (uplift ratio > 1.5).

| Feature | Interpretation |
|---|---|
| `ais_gap_count` | Elevated AIS blackout frequency before confirmation |
| `sts_candidate_proxy` | Increased anchoring activity (STS proxy) |
| `low_sog_fraction` | More time at near-zero speed (drifting, loitering) |

## Label propagation evidence types

| Evidence type | Confidence | Relationship |
|---|---|---|
| `confirmed_direct` | 1.0 | Vessel itself is confirmed |
| `shared_owner` | 0.65 | Co-owned vessel (same direct owner company) |
| `shared_manager` | 0.60 | Co-managed vessel (same technical manager) |
| `sts_contact` | 0.50 | Vessel with recorded STS co-location contact |

## Demo scenario (Cap Vista)

1. Replay Jan–Mar AIS where vessels are still gray
2. Ingest one delayed confirmed-black label on Apr 1:
   ```bash
   # POST via API or direct DB insert
   curl -X POST http://localhost:8000/api/reviews \
     -H "Content-Type: application/json" \
     -d '{"mmsi":"123456789","review_tier":"confirmed","handoff_state":"handoff_completed","rationale":"Field inspection confirmed","reviewed_by":"officer-001"}'
   ```
3. Run backtracking with rewind to Jan:
   ```bash
   uv run python scripts/run_backtracking.py \
     --since 2026-04-01T00:00:00Z \
     --rewind-days 90
   ```
4. Review `backtracking_report.md` for:
   - Precursor signals identified in Jan–Mar AIS data
   - Related vessels uplifted via ownership graph

## Regression checks

The `regression_checks.pass` field is `true` when the number of rewound vessels equals the number of confirmed vessels in the processing window. A `false` value indicates a processing failure and requires investigation.

Run tests to validate the loop:
```bash
uv run --group dev python -m pytest tests/test_label_propagation.py tests/test_causal_rewind.py tests/test_backtracking_runner.py -q
```
