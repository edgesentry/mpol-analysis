# Evaluation Metrics

This document describes the evaluation framework, metric definitions, acceptance thresholds, and how to reproduce results. Current metric values are produced by the CI pipeline and stored in `data/processed/validation_metrics.json` — do not read specific numbers from this doc.

---

## Metric Definitions

| Metric | Definition | Where computed |
|--------|-----------|----------------|
| **Precision@K** | Fraction of confirmed positives in the top-K ranked candidates | `src/score/validate.py`, `src/score/prelabel_evaluation.py`, `src/score/backtest.py` |
| **Recall@K** | Fraction of all known positives recovered in the top-K ranked candidates | Same |
| **AUROC** | Area under the ROC curve across the full ranked list; requires both positives and negatives in the labeled set | Same |
| **PR-AUC** | Area under the Precision-Recall curve; more informative than AUROC on imbalanced sets | Same |
| **ECE** (calibration error) | Expected Calibration Error — measures whether the confidence score aligns with empirical hit rate | `src/score/backtest.py` |

All metrics are computed over the **labeled subset** only — vessels with a known positive or weak-negative label — not over all watchlist candidates. When fewer than 50 labeled rows exist, Precision@50 equals the fraction of all labeled rows that are positive.

---

## Acceptance Thresholds

Two distinct P@50 thresholds exist and must not be confused:

| Metric | Threshold | Role | Scope | Enforcement |
|--------|-----------|------|-------|-------------|
| **Precision@50** | ≥ 0.60 | **Contractual Acceptance Gate** — minimum commitment to Cap Vista in the Scope of Work | Cap Vista trial | Annex A § 1; Week 7 trial report |
| **Precision@50** | ≥ 0.68 | **Demonstrated Technical Ceiling** — achieved on multi-region public-data backtesting; internal CI regression gate | Multi-region (≥ 50 labeled positives) | Manual review via `scripts/run_public_backtest_batch.py` |
| **Precision@50** | ≥ 0.25 | **Integration test floor** — catches a genuinely broken scorer | Single-region | CI (`tests/test_public_data_backtest_integration.py`, #235) |

The 0.68 figure is a position of strength — it reflects what the model has already demonstrated on public data, not an additional promise to Cap Vista. The contractual obligation is 0.60. The 0.68 CI gate prevents regression below the demonstrated ceiling.

> **Structural ceiling note**: For a single-region labeled set with N positives and M negatives where N + M < 50, the maximum achievable P@50 = N / (N + M). Perfect ranking still falls short of 0.68 in low-label-density regions. AUROC is the more informative metric in this case.

AUROC and Recall@200 have no hard threshold — they inform development decisions rather than pass/fail gates.

---

## Evaluation Slices

arktrace maintains two independent evaluation slices. Do not merge them.

| Slice | Label source | Indicator type | Primary metric |
|-------|-------------|----------------|----------------|
| **Public-label backtest** | OFAC / UN / EU sanctions | Lagging — measures confirmed-case recall | Precision@50, Recall@200, AUROC |
| **Analyst pre-label holdout** | Analyst curation | Leading — measures early detection before public confirmation | Precision@50 on holdout |

If the public-label backtest is strong but pre-label precision is low, the model detects known entities but misses novel evasion patterns. Both slices must improve together for the system to be operationally useful.

---

## Seeded vs. Blind Metrics — Disclosure

arktrace distinguishes two evaluation modes. These must not be conflated when reporting results.

| Mode | Flag | Purpose | Valid for |
|---|---|---|---|
| **Blind** | *(no `--seed-dummy`)* | Canonical performance measurement on live public-label data | Published baselines, Cap Vista reporting, CI regression gate |
| **Seeded (demo)** | `--seed-dummy` | CI floor check — verifies scorer is not broken by injecting 10 known OFAC vessels with synthetic positions | Local smoke test, CI known-case gate only |

**Why seeded metrics are higher:** The 10 injected vessels (CELINE, ELINE, REX 1, ANHONA, etc.) are confirmed OFAC positives with synthetic AIS positions crafted to produce high anomaly scores. A seeded run guarantees these vessels surface in the top-50, mechanically inflating AUROC, P@50, and Recall.

### Documented Blind Run Results

Most recent blind evaluation (singapore, 2026-04-14, `validation_metrics.json`):

| Metric | Value | Notes |
|---|---|---|
| AUROC | 1.0 | 3 confirmed OFAC positives — all ranked above all negatives |
| Precision@50 | 0.06 | 3 positives / 50 labeled rows (structural ceiling = 3/50 = 0.06 with perfect ranking) |
| Recall@200 | 1.0 | All confirmed positives appear in top 200 |
| Positive count | 3 | Confirmed OFAC vessels in the Singapore AIS dataset at time of run |

> The structural ceiling on P@50 in the Singapore single-region blind run is 0.06 (3 positives / 50 labeled rows). AUROC = 1.0 confirms the model ranks all confirmed positives above all negatives — perfect discrimination — but the label density is too low for P@50 to be a meaningful standalone metric. This is expected and documented: see [Acceptance Thresholds](#acceptance-thresholds) structural ceiling note.

---

## Baselines

### Random Baseline

A random ranker produces Precision@50 equal to the base rate of confirmed positives in the candidate pool. For a pool of 500 candidates with 50 known OFAC vessels, random Precision@50 ≈ 0.10.

### HDBSCAN-Only Baseline

HDBSCAN alone classifies vessels as MPOL-normal (`baseline_noise_score = 0.0`) or MPOL-noise (`1.0`) — a binary signal, not a ranked list. The HDBSCAN noise score contributes 25% of the final anomaly score:

```
anomaly_score = 0.75 × norm(isolation_forest_raw) + 0.25 × baseline_noise_score
```

### Composite Model Weights

Default weights per region (region-specific values defined in `scripts/run_pipeline.py` `PRESETS`):

| Component | Default weight | Auto-calibration |
|-----------|---------------|-----------------|
| Anomaly score | 0.40 | Fixed |
| Graph risk score | 0.35–0.40 | [0.20, 0.65] via C3 causal model |
| Identity score | 0.20 | Fixed |

`w_graph` is automatically adjusted by `src/score/causal_sanction.py` when the causal sanction-response effect is statistically significant. The calibrated value is logged to `data/processed/<region>_causal_effects.parquet`.

---

## How to Get Current Metrics

### From CI

The `data-publish` workflow runs weekly and after every main-branch push. Metrics are:
- Emailed to the configured `NOTIFY_EMAIL`
- Written to `data/processed/validation_metrics.json` (also pushed to R2)
- Available as GitHub Actions artifacts under the `pipeline-artifacts-*` upload

### Locally — Public-Label Backtest

> **Seeded vs. blind runs — read before using `--seed-dummy`.**
> `--seed-dummy` patches 10 known OFAC-sanctioned vessels (CELINE, ELINE, and others) directly into the database with synthetic AIS positions. These vessels are confirmed positives injected with known-bad features and will rank near the top, inflating AUROC, Precision@50, and Recall metrics compared to a blind run on live data. **Never cite seeded-run metrics as baseline performance figures.** Use seeded runs only to verify that the scorer is not broken (CI known-case floor check). Use the blind command below for any metric you intend to publish or compare.

**Blind evaluation (canonical baseline — no seeding):**

```bash
uv run python scripts/run_public_backtest_batch.py \
  --regions singapore,japan,europe,blacksea \
  --gdelt-days 14 \
  --stream-duration 0 \
  --min-known-cases 5 \
  --strict-known-cases

cat data/processed/backtest_public_integration_summary.json
```

**Demo / CI floor check (seeded — do not use as baseline):**

```bash
uv run python scripts/run_public_backtest_batch.py \
  --regions singapore,japan,europe,blacksea \
  --gdelt-days 14 \
  --stream-duration 0 \
  --seed-dummy \
  --min-known-cases 30 \
  --strict-known-cases
```

AUROC requires both positives and negatives in the labeled set. It is computed automatically when `run_public_backtest_batch.py` runs the full pipeline (not `--skip-pipeline`) and enough negatives are sampled from the low-confidence tail of the watchlist.

### Locally — Pre-Label Holdout

```bash
uv run python -m src.score.prelabel_evaluation \
  --watchlist data/processed/candidate_watchlist.parquet \
  --prelabels-csv data/demo/analyst_prelabels_demo.csv \
  --output data/processed/prelabel_evaluation.json \
  --end-date 2025-11-15 \
  --min-confidence-tier medium \
  --review-capacities 25,50,100
```

Output written to `data/processed/prelabel_evaluation.json`. See `docs/prelabel-governance.md` for label policy and confidence-tier guidance.

### Locally — Historical Backtest

```bash
uv run python -m src.score.backtest \
  --manifest config/evaluation_manifest.sample.json \
  --output data/processed/backtest_report.json
```

See `docs/backtesting-validation.md` for manifest format and label policy.

---

## Operational Thresholds

The evaluation pipeline outputs recommended score thresholds for fixed analyst review capacities. These are per-run outputs, not fixed constants — they depend on the score distribution of the current watchlist.

| Review capacity | Meaning |
|-----------------|---------|
| 25 vessels / day | Minimum viable triage queue |
| 50 vessels / day | Standard operational queue (matched to Precision@50 metric) |
| 100 vessels / day | High-throughput surge capacity |

Fields in evaluation output: `ops_thresholds[].min_score` (score cutoff), `ops_thresholds[].hit_rate` (fraction of capacity occupied by true positives at that threshold).

---

## Output Files

| File | Contents |
|------|---------|
| `data/processed/validation_metrics.json` | Precision@50, Recall@200, AUROC for the most recent pipeline run |
| `data/processed/backtest_public_integration_summary.json` | Multi-region public-label backtest summary |
| `data/processed/backtest_report_public_integration.json` | Per-window backtest metrics |
| `data/processed/prelabel_evaluation.json` | Pre-label holdout report including per-tier breakdown and ops thresholds |
| `data/processed/<region>_causal_effects.parquet` | C3 causal weight calibration — includes calibrated `w_graph` |
