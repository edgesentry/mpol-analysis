# Evaluation Metrics and Validated Baselines

This document records the evaluation framework, metric definitions, accepted baselines, and how to reproduce results. It is intended for both internal development tracking and external proposal review.

---

## Metric Definitions

| Metric | Definition | Where computed |
|--------|-----------|----------------|
| **Precision@K** | Fraction of confirmed positives in the top-K ranked candidates | `src/score/validate.py`, `src/score/prelabel_evaluation.py`, `src/score/backtest.py` |
| **Recall@K** | Fraction of all known positives recovered in the top-K ranked candidates | Same |
| **AUROC** | Area under the ROC curve across the full ranked list; requires both positives and negatives in the labeled set | Same |
| **PR-AUC** | Area under the Precision-Recall curve; more informative than AUROC on imbalanced sets | Same |
| **ECE** (calibration error) | Expected Calibration Error — measures whether the confidence score aligns with empirical hit rate | `src/score/backtest.py` |

---

## Acceptance Criterion

| Metric | Target | Meaning |
|--------|--------|---------|
| **Precision@50** | **≥ 0.60** | At least 30 of the top-50 ranked candidates are confirmed OFAC-listed vessels |

This is the primary KPI for Phase A screening. Recall@200 and AUROC are tracked but have no hard threshold — they inform model development decisions rather than pass/fail gates.

---

## Measured Results

### End-to-End Pipeline Run (Singapore Region)

Measured on a full local pipeline run against the Singapore region AIS dataset. See `docs/local-e2e-test.md` for reproduction steps.

| Metric | Value | Notes |
|--------|-------|-------|
| Precision@50 | **0.62** | Exceeds ≥ 0.60 acceptance criterion |
| Scoring wall-clock time | ~45 min | Full pipeline on a laptop (M-series, 16 GB RAM) with no GPU |

### Analyst Pre-Label Holdout Set

The holdout set is the primary leading-indicator evaluation slice — it measures early-detection capability against vessels not yet on any public sanctions list.

**Holdout composition (v1.0):**

| Attribute | Value |
|-----------|-------|
| Total vessels | 60 |
| Regions | singapore (20), middleeast (20), europe (20) |
| `suspected-positive` | ~30 |
| `uncertain` | ~10 |
| `analyst-negative` | ~15 |
| Evidence window | 2025-09 through 2025-11 |
| Source file | `data/demo/analyst_prelabels_demo.csv` |

Evaluation excludes `uncertain` labels from binary metrics (`y_true = None`). Use `--min-confidence-tier medium` in evaluation runs to exclude weak labels from the primary KPI slice.

**Metrics reported per run:** Precision@50, Precision@100, Recall@50, Recall@100, AUROC, PR-AUC, plus per-confidence-tier breakdown and disagreement cases (model-high / analyst-negative and model-low / analyst-positive). See `src/score/prelabel_evaluation.py` for schema.

> **Note:** The pre-label holdout set uses analyst-curated evidence, not public sanctions lists. It intentionally tests early detection before public confirmation. Treat these results as a leading-indicator slice, not as claims of confirmed-case recall.

### Distinction: Targets vs. Measured

| Value | Status |
|-------|--------|
| Precision@50 = 0.62 | **Measured** — from full local pipeline run, Singapore region |
| Precision@50 ≥ 0.60 | **Acceptance target** — design criterion |
| Recall@200, AUROC, PR-AUC | **Tracked** — no hard threshold; values depend on label set used |
| ECE | **Tracked in backtest** — no hard threshold |

AUROC and PR-AUC values require a labeled set with both positives and negatives. On the demo holdout (60 vessels, ~30 positives), these metrics are computed at runtime and depend on the current watchlist ranking. They are not fixed constants — run `src/score/prelabel_evaluation.py` to produce current values.

---

## Baselines

### Random Baseline

A random ranker produces Precision@50 equal to the base rate of confirmed positives in the candidate pool. For a pool of 500 candidates with 50 known OFAC vessels, random Precision@50 ≈ 0.10. arktrace's 0.62 represents a **6× lift over random**.

### HDBSCAN-Only Baseline

HDBSCAN alone (without Isolation Forest or graph risk signal) classifies vessels as MPOL-normal (`baseline_noise_score = 0.0`) or MPOL-noise (`1.0`). This is a binary signal — it does not produce a ranked list on its own. The HDBSCAN noise score contributes 25% of the final anomaly score:

```
anomaly_score = 0.75 × norm(isolation_forest_raw) + 0.25 × baseline_noise_score
```

### Composite Model

Default scoring weights:

| Component | Default weight | Auto-calibration range |
|-----------|---------------|----------------------|
| Anomaly score | 0.40 | N/A |
| Graph risk score | 0.40 | [0.20, 0.65] via C3 causal model |
| Identity score | 0.20 | N/A |

`w_graph` is automatically adjusted by `src/score/causal_sanction.py` based on whether the causal sanction-response effect is statistically significant for the current AIS window. The calibrated value is logged to `data/processed/<region>_causal_effects.parquet`.

---

## Operational Thresholds

The evaluation pipeline produces recommended score thresholds for fixed analyst review capacities. These are output per-run and not fixed constants — they depend on the score distribution of the current watchlist.

| Review capacity | What it means |
|-----------------|---------------|
| 25 vessels / day | Minimum viable triage queue |
| 50 vessels / day | Standard operational queue (matched to Precision@50 metric) |
| 100 vessels / day | High-throughput surge capacity |

Fields in evaluation output: `ops_thresholds[].min_score` (score cutoff), `ops_thresholds[].hit_rate` (fraction of review capacity occupied by true positives at that threshold).

---

## How to Reproduce

### Precision@50 on a full pipeline run

```bash
# Run the full pipeline for Singapore
bash scripts/run_pipeline.sh --region singapore

# Read the metric
cat data/processed/validation_metrics.json | python3 -c \
  "import json,sys; m=json.load(sys.stdin); print('Precision@50:', m['precision_at_50'])"
```

### Pre-label holdout evaluation

```bash
uv run python -m src.score.prelabel_evaluation \
  --watchlist data/processed/candidate_watchlist.parquet \
  --prelabels-csv data/demo/analyst_prelabels_demo.csv \
  --output data/processed/prelabel_evaluation.json \
  --end-date 2025-11-15 \
  --min-confidence-tier medium \
  --review-capacities 25,50,100
```

Output written to `data/processed/prelabel_evaluation.json`. See `docs/prelabel-governance.md` for label policy, leakage control, and confidence-tier guidance.

### Backtest across historical windows

```bash
uv run python -m src.score.backtest \
  --manifest config/evaluation_manifest.sample.json \
  --output data/processed/backtest_report.json
```

See `docs/backtesting-validation.md` for manifest format and label policy.

---

## Relationship to Evaluation Slices

arktrace maintains two separate evaluation slices. Do not merge them.

| Slice | Label source | Indicator type | Primary metric |
|-------|-------------|----------------|----------------|
| Public-label backtest | OFAC / UN / EU sanctions | Lagging — measures confirmed-case recall | Precision@50, Recall@200 |
| Analyst pre-label holdout | Analyst curation | Leading — measures early detection before public confirmation | Precision@50 on holdout |

If the public-label backtest is strong but pre-label precision is low, the model detects known entities but misses novel evasion patterns. Both slices must improve together for the system to be operationally useful.

---

## Output Files

| File | Contents |
|------|---------|
| `data/processed/validation_metrics.json` | Precision@50, Recall@200, AUROC for the most recent pipeline run |
| `data/processed/prelabel_evaluation.json` | Full pre-label holdout report including per-confidence-tier breakdown, ops thresholds, and disagreement cases |
| `data/processed/backtest_report.json` | Per-window backtest metrics across the evaluation manifest |
| `data/processed/<region>_causal_effects.parquet` | C3 causal weight calibration output — includes calibrated `w_graph` value |
