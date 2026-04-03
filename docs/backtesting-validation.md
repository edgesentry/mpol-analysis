# Historical Backtesting Validation

This document defines the reproducible offline evaluation workflow used to validate shadow-fleet candidate detection quality against historical public evidence.

## Why Backtesting

We cannot know all true shadow-fleet vessels in real time. Backtesting provides a practical validation loop by replaying historical windows with known outcomes and measuring ranking quality.

Primary objective: maximize operational triage utility (high hit-rate in top-N candidates), not claim perfect 100% classification.

## Inputs

1. A versioned manifest file listing evaluation windows
2. A watchlist parquet per window
3. A labels CSV per window with evidence-backed positive/negative labels

Templates:

- `config/evaluation_manifest.sample.json`
- `config/eval_labels.template.csv`

## Label Policy

- `label`: `positive` or `negative`
- `label_confidence`: `high`, `medium`, `weak` (or `unknown`)
- `evidence_source`/`evidence_url`: public source traceability

Recommended:

- Use only evidence available up to each window end date
- Keep label confidence explicit to avoid over-claiming
- Prefer MMSI and IMO where possible

## Run Backtest

```bash
uv run python -m src.score.backtest \
  --manifest config/evaluation_manifest.sample.json \
  --output data/processed/backtest_report.json \
  --review-capacities 25,50,100
```

## Output

`data/processed/backtest_report.json` includes:

- Window-level metrics
- Cross-window summary with mean and 95% CI (when multiple windows exist)
- Stratified metrics by vessel type
- False-positive/false-negative example rows
- Operational threshold suggestions by review capacity

Core metrics reported:

- `precision_at_50`
- `precision_at_100`
- `recall_at_100`
- `recall_at_200`
- `auroc`
- `pr_auc`
- `calibration_error` (ECE)

## Threshold Recommendation Policy

The report includes:

1. `recommended_threshold`: score threshold maximizing F1 on labeled set
2. `ops_thresholds`: min score and hit-rate for specific review capacities

Use `ops_thresholds` for deployment defaults when analyst capacity is fixed.

## CI Integration

Unit tests validate backtest metric/report generation (`tests/test_backtest.py`).

For full offline evaluations in CI, add a scheduled job with curated historical artifacts and publish `backtest_report.json` as an artifact.
