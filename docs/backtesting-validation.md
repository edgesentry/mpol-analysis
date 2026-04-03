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

## Automation Boundary

This section clarifies what can be automated end-to-end and what still requires human judgment.

### Can be automated

1. Data extraction and file generation
- Generate draft labels CSV rows from sanctions tables (MMSI/IMO/name/list source).
- Generate manifest windows with watchlist and label file paths.
- Validate required columns and file shape before running backtest.

2. Backtest execution and metric reporting
- Run historical window evaluation from manifest.
- Compute ranking and classification metrics (Precision@K, Recall@K, AUROC, PR-AUC, calibration error).
- Generate threshold suggestions for fixed review capacities.
- Export JSON reports for CI artifacts and dashboards.

3. Regression monitoring
- Run repeatable checks in CI on curated historical windows.
- Alert when key metrics drift below agreed thresholds.

### Cannot be fully automated

1. Ground-truth certainty
- Public data is incomplete and delayed; many true shadow-fleet outcomes are never formally published.
- Therefore, a fully complete positive/negative truth set cannot be auto-derived.

2. High-confidence negative labeling
- "No public evidence" is not the same as "truly negative."
- Negative labels with high confidence require analyst review and policy criteria.

3. Evidence quality and temporal validity
- Source credibility, evidence freshness, and timeline consistency require human validation.
- Leakage checks (ensuring evidence was known within the historical window) require governance decisions.

4. Operational decisioning
- The model provides ranked candidates and scores; officers decide investigation priority.
- Final status assignment (confirmed/cleared/inconclusive) is a human-in-the-loop decision.

### Recommended split of responsibilities

- Automation handles: candidate generation, metric computation, report generation, regression checks.
- Human review handles: evidence adjudication, label confidence assignment, final investigative decisions.
- Feedback loop combines both: human outcomes are fed back into periodic model/threshold updates.

## Label Policy

- `label`: `positive` or `negative`
- `label_confidence`: `high`, `medium`, `weak` (or `unknown`)
- `evidence_source`/`evidence_url`: public source traceability

Recommended:

- Use only evidence available up to each window end date
- Keep label confidence explicit to avoid over-claiming
- Prefer MMSI and IMO where possible

## Public Data for Identified Vessels

Yes, we can build a useful labeled set from public data.

### Practical positive-label sources

1. Sanctions lists (machine-readable, strongest baseline)
- OFAC SDN (US)
- UN sanctions lists
- EU sanctions lists

2. Government and intergovernmental disclosures
- Enforcement advisories and designation notices
- Public case summaries naming vessels, IMO, or MMSI

3. Reputable investigative datasets/reports
- Open investigations that provide vessel identifiers and dated evidence

### How to use these sources in evaluation

- Treat sanctions/designations as high-confidence positives when vessel identifiers are present.
- Include source URL and publication date in labels.
- Map identifiers by MMSI and IMO (prefer both when available).
- Freeze each evaluation window by date to prevent future information leakage.

### Limits to keep in mind

- Public data will not cover all true shadow-fleet vessels.
- Some records are delayed, incomplete, or ambiguous.
- Therefore, backtesting measures practical ranking utility, not perfect population recall.

### Critical evaluation caveat

- Cases that are neither detected by our algorithm nor publicly confirmed (not identified/caught in open sources) are treated as unknown and excluded from strict success/failure judgment.
- The primary objective is to detect cases that are publicly confirmed with credible evidence.
- Beyond that boundary, public data alone cannot provide rigorous ground truth for complete-recall evaluation.

### Recommended confidence mapping

- `high`: explicit sanctioned/officially designated vessel with identifier match
- `medium`: multiple credible public sources with strong identifier evidence
- `weak`: plausible but incomplete evidence (keep for analysis, not primary KPI)

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

## Public Data Integration Test (Opt-in)

We provide an opt-in integration test that actually downloads public sanctions data,
loads DuckDB, and evaluates found-vs-missed outcomes against practical positive-label sources.

### Prepare once and reuse DB (recommended)

Because OpenSanctions ingestion can take time, prepare a persistent DB once and reuse it in later tests.

```bash
uv run python scripts/prepare_public_sanctions_db.py \
  --db data/processed/public_eval.duckdb
```

This writes:

1. Persistent DB: `data/processed/public_eval.duckdb`
2. Cached raw file: `data/raw/sanctions/opensanctions_entities.jsonl`
3. Metadata snapshot: `data/processed/public_eval_metadata.json`

To refresh data:

```bash
uv run python scripts/prepare_public_sanctions_db.py \
  --db data/processed/public_eval.duckdb \
  --force-download \
  --force-reload
```

Run manually:

```bash
RUN_PUBLIC_DATA_TESTS=1 \
  PUBLIC_SANCTIONS_DB=data/processed/public_eval.duckdb \
  uv run --group dev python -m pytest tests/test_public_data_backtest_integration.py -v
```

Optional fallback (not recommended for daily runs): if `PUBLIC_SANCTIONS_DB` does not exist,
you can allow the test to prepare data on demand by setting `PREPARE_PUBLIC_DATA_IF_MISSING=1`.

## Demo-size Sample Dataset

For demos, you can build a small sample DB from the prepared public DB.

```bash
uv run python scripts/build_public_sanctions_demo_sample.py \
  --source-db data/processed/public_eval.duckdb \
  --demo-db data/demo/public_eval_demo.duckdb \
  --max-rows 300
```

This is useful for fast demos and local smoke checks without full-size ingestion.
The `data/demo/` folder is intended to be committed to Git as portable demo fixtures.

Bundled dashboard fixture:

- `data/demo/candidate_watchlist_demo.parquet`

To load it into the dashboard input path quickly:

```bash
uv run python scripts/use_demo_watchlist.py --backup
```

## Main-merge Integration Batch (Known-case Check)

Run a medium-scale batch that:

1. Reuses (or refreshes) the public sanctions DB.
2. Runs multi-region pipeline output generation.
3. Builds public-overlap labels per region.
4. Executes backtesting and verifies a minimum known-case floor.

Local equivalent run:

```bash
uv run python scripts/run_public_backtest_batch.py \
  --regions singapore,japan,middleeast,europe,gulf \
  --gdelt-days 14 \
  --seed-dummy \
  --max-known-cases 200 \
  --min-known-cases 30 \
  --strict-known-cases
```

Outputs:

- `data/processed/evaluation_manifest_public_integration.json`
- `data/processed/backtest_report_public_integration.json`
- `data/processed/backtest_public_integration_summary.json`
- `data/processed/eval_labels_public_*_integration.csv`

GitHub Actions workflow:

- `.github/workflows/public-backtest-integration.yml`

Execution policy:

- This integration batch runs automatically on `push` to `main` (post-merge).
- It is not scheduled as a nightly cron job.

If your target is "tens to hundreds" of known cases, tune:

- `--max-known-cases` (upper cap)
- `--min-known-cases` (required floor)
- `--regions` and `--gdelt-days` (candidate pool breadth)

What this test checks:

1. Public sanctions data is downloadable and loadable into DuckDB.
2. Labels can be derived from practical positive-label sources (OFAC/UN/EU-like tags).
3. Backtest report includes:
   - `source_positive_coverage.matched_total` (found by algorithm output overlap)
   - `source_positive_coverage.missed_total` (publicly identified positives not found)

Boundary reminder:

- Cases not publicly identified/caught are outside strict pass/fail ground truth.
- This test evaluates detection of publicly evidenced cases, which is the reliable scope for open-data validation.
