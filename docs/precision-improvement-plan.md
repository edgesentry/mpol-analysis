# Precision@50 Improvement Plan — 0.62 → 0.68

This document explains what is needed to reach the ≥ 0.68 submission target from the current 0.62 baseline, and the steps being taken to get there. No code knowledge required.

---

## Background

Precision@50 is the primary evaluation metric for arktrace: of the top 50 vessels ranked by the model, what fraction are confirmed sanctioned vessels? The Phase A acceptance criterion is ≥ 0.60. For the submission demo, the target is ≥ 0.68 — enough margin above the threshold to demonstrate reliable performance rather than a marginal pass.

The 0.62 baseline was measured on a full Singapore pipeline run with a properly populated AIS dataset. See [evaluation-metrics.md](evaluation-metrics.md) for the full reproduction steps.

---

## What Drives the Score

Three signal families combine into the final confidence score:

| Signal | Weight | What it needs |
|---|---|---|
| **Graph risk** — how close is the vessel to a sanctioned entity in the ownership network? | 55% | A current sanctions database (refreshed via job 9) and a populated ownership graph |
| **Anomaly** — does the vessel's movement and identity behaviour look like a known shadow fleet operator? | 35% | Enough AIS position history to compute gap, loitering, and co-location signals |
| **Identity** — how often has the vessel changed flag, name, or owner? | 10% | Vessel registry data (already loaded) |

Graph risk dominates. If the sanctions database is stale — which causes every vessel to get a "no sanctions connection" score — the model loses its most powerful signal regardless of how good the AIS data is.

---

## What Is Being Done

### Step 1 — Continuous AIS collection (Issue #189) — in progress

An aisstream.io WebSocket collector running as a macOS launchd agent (`scripts/install_aisstream_agent.sh`) continuously writes vessel positions to `ais_positions` for the Singapore / Malacca bbox. The agent survives reboots and restarts automatically after crashes.

**Why this matters:** More vessels in the database means:
- A larger fleet for the model to rank, making Precision@50 more meaningful and stable
- Richer AIS behavioral signals (gap patterns, co-location events, loitering) feeding the anomaly component
- More ship-to-ship transfer candidate events feeding the graph risk component

**What "enough data" looks like:**

| Indicator | Check |
|---|---|
| ≥ 500 distinct vessels | `SELECT COUNT(DISTINCT mmsi) FROM ais_positions` |
| Active ingestion | `SELECT COUNT(*) FROM ais_positions WHERE timestamp > NOW() - INTERVAL '5 minutes'` (should be > 0) |
| Last position recent | `SELECT MAX(timestamp) FROM ais_positions` (should be within minutes) |

The 48–72 hour collection window is the minimum needed for co-location events (two vessels in the same location in the same 30-minute window) to accumulate meaningfully. Check the position count after 12 hours and re-run job 1 + job 16 to see if the score is moving. If it has not moved after 12 hours, weight tuning is a faster path.

### Step 2 — Sanctions DB refresh — done (2026-04-11)

Job 9 was run on 2026-04-11, downloading 69,482 current OpenSanctions entities into `data/processed/public_eval.duckdb`. This must be re-run before the submission demo to ensure the latest designations are included.

### Step 3 — Rebuild vessel_features (job 1) — run after each data change

Every time new AIS data accumulates or the sanctions DB is refreshed, job 1 (Full Screening) must be re-run to rebuild `vessel_features`. The model scores a static snapshot — it does not pick up new data automatically.

### Step 4 — Validate (job 16) — run after job 1

Job 16 option 1 (Quick validate) re-scores and measures Precision@50 against OFAC labels in the database. This is the fast feedback loop: job 1 (~10 min) → job 16 (~1 min) → read the number.

---

## Fallback: Weight Tuning

If AIS collection alone does not reach 0.68, the following parameter changes can be made without waiting for more data. Each takes under 5 minutes and the effect is measurable immediately via job 16.

| Change | Expected effect |
|---|---|
| Raise `contamination` from 0.03 to 0.05 in `src/score/anomaly.py` | Tighter top of the anomaly score distribution |
| Raise `w_graph` from 0.55 to 0.60 in `src/score/composite.py` | Increases weight on the sanctions graph signal (only effective if sanctions data is current) |

Do not change both at once — measure after each change so you know which had the effect.

---

## Submission Demo Checklist (Issue #196)

Run these steps in order before the demo. Allow ~20 minutes.

1. **Job 9** — Prepare public sanctions DB (force-download = yes). Downloads the latest OpenSanctions data.
2. **Job 3** — Historical Backtesting (rebuilds the Lance ownership graph from fresh sanctions + vessel registry).
3. **Confirm the launchd agent has been running ≥ 48h** and `ais_positions` has 500+ distinct vessels.
4. **Job 1** — Full Screening (singapore region, stream duration = 0). Rebuilds `vessel_features` from all accumulated data.
5. **Job 16 → option 3** — Public OpenSanctions integration test. Confirm Precision@50 ≥ 0.68.
6. Capture dashboard screenshot for submission evidence.

---

## Known Gaps to Close Before Demo

| Gap | Tracked in |
|---|---|
| Integration test has no `precision_at_50 >= 0.68` assertion | #186 |
