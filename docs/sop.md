# arktrace Analyst Standard Operating Procedure

**Version:** 1.0  
**Applies to:** Singapore / Malacca Strait PoC deployment (Cap Vista Solicitation 5.0)  
**Audience:** Maritime security analysts and patrol officers using the arktrace dashboard

---

## 1. Alert Receipt

**Watchlist update cadence:** The pipeline re-scores the full fleet every **15 minutes**. The dashboard auto-refreshes on a matching interval via Server-Sent Events (SSE). No manual refresh is required.

**Notification channels:**

| Channel | Trigger | Action |
|---|---|---|
| Dashboard SSE (in-browser) | Watchlist rank changes ≥ 5 positions | Banner notification; re-sort watchlist |
| Email digest (optional) | Daily 06:00 local — top-10 highest-confidence candidates | Review before morning brief |
| Patrol brief PDF | On demand — analyst clicks "Dispatch" for a specific vessel | Auto-generated; sign off before transmission |

**Confidence thresholds for alert escalation:**

| Band | Score | Meaning | Default action |
|---|---|---|---|
| Confirmed | ≥ 0.70 | Strong multi-signal convergence | Immediate senior analyst review; consider dispatch |
| Probable | 0.50 – 0.69 | Credible evasion signature | Analyst review within 4 hours |
| Suspect | 0.30 – 0.49 | Weak or single-signal flag | Monitor; re-evaluate at next re-score |
| Cleared | — | Manually reviewed; no further action | Suppressed from active watchlist |
| Inconclusive | — | Evidence reviewed; outcome uncertain | Hold; flag for next patrol debrief |

---

## 2. Review Tiers

Work through candidates top-down by composite score. For each vessel:

1. **Skim the top-3 SHAP signals** — visible in the watchlist table. If all three are weak (< 0.10 contribution each), mark Inconclusive and move on.
2. **Open the vessel detail panel** — expand SHAP waterfall, causal ATT badge, ownership graph, and last-position map.
3. **Apply the evidence checklist** (§ 3 below).
4. **Assign a tier** (Confirmed / Probable / Suspect / Cleared / Inconclusive) and record the rationale in the notes field.
5. **Escalate or close** per the dispatch decision procedure (§ 4).

Target review time: **≤ 5 minutes per vessel** for Suspect; **≤ 15 minutes** for Probable/Confirmed.

---

## 3. Evidence Review Checklist

Work through the following for every Probable or Confirmed candidate:

### 3a. SHAP Signal Review

- [ ] Identify the top-3 SHAP contributors (feature name + contribution fraction)
- [ ] Confirm at least 2 of the 3 are behavioural signals (`ais_gap_count_30d`, `ais_gap_max_hours`, `sts_candidate_count`, `position_jump_count`) rather than structural ones (`ownership_depth`, `sanctions_distance`) alone
- [ ] Note whether the dominant signal is consistent with the vessel's declared voyage (e.g., a tanker in a known STS zone with high `sts_candidate_count` is more credible than a cargo vessel with low port traffic)

### 3b. Causal ATT Badge

- [ ] Read the DiD ATT value and 95% CI displayed on the vessel card
- [ ] Confirm the p-value is below threshold (p < 0.05 default; configurable)
- [ ] If ATT is positive and significant: the vessel's evasion behaviour intensified *specifically after* the most recent relevant sanctions announcement — document which announcement
- [ ] If ATT is not significant: downgrade to Suspect regardless of composite score

### 3c. Ownership Graph

- [ ] Check `sanctions_distance` (hops to nearest OFAC/EU/UN listed entity)
- [ ] Review the graph path depth: distance ≤ 2 hops → strong indicator; 3–4 hops → moderate; ≥ 5 hops → structural noise
- [ ] Note any shared manager / shared registered address links (`shared_manager_risk`, `shared_address_centrality`)

### 3d. Last 10 AIS Positions

- [ ] Verify the vessel's declared destination matches its actual track
- [ ] Check for position jumps (gap > 50 knots implied speed = GPS spoofing candidate)
- [ ] Identify any AIS-off periods near known loading terminals (Kharg Island, Bandar Abbas, Primorsk, Ust-Luga)
- [ ] Note last reported port call and whether it matches declared cargo type

### 3e. GDELT Geopolitical Context

- [ ] Review the GDELT event summary in the analyst chat panel
- [ ] Check whether the vessel's flag state or declared owner is mentioned in recent GDELT sanctions or incident news
- [ ] If GDELT shows an active enforcement event in the vessel's AOI, flag for expedited review

---

## 4. Dispatch Decision

### Sign-off authority

| Score band | Required sign-off |
|---|---|
| Confirmed (≥ 0.70) | Senior analyst or duty officer |
| Probable (0.50 – 0.69) | Analyst (self-authorised) |
| Suspect (< 0.50) | Not dispatched; monitor only |

### Procedure

1. Open the vessel detail panel and click **"Dispatch Brief"**
2. Review the auto-generated PDF: vessel identity, composite score, top SHAP signals, ATT result, ownership graph summary, last-known position, and recommended patrol action
3. Add any free-text analyst notes in the "Commander's Note" field
4. Sign off in the dashboard (digital acknowledgement logged with analyst ID and timestamp)
5. Transmit the brief through the authorised channel (email / secure message / ops board)

### Decision record

Every dispatch action is logged automatically with:
- Vessel MMSI and name
- Score at time of dispatch
- Analyst ID
- Timestamp (UTC)
- Tier assigned (Confirmed / Probable)

Cleared and Inconclusive decisions are also logged but do not generate a brief.

---

## 5. Feedback Loop

### Patrol outcome recording

After a patrol action, the duty officer records the outcome in the dashboard feedback panel:

| Outcome | Label | Effect |
|---|---|---|
| Vessel boarded; evasion confirmed | `confirmed_positive` | Adds to positive training set |
| Vessel boarded; no violation found | `confirmed_negative` | Adds to negative training set |
| Vessel not intercepted (weather / resource) | `no_intercept` | No label added |
| Intelligence passed to partner agency | `referred` | No label added |

### Manual label input

Analysts can also manually label vessels from historical intercept records or partner intelligence using the dashboard's label import tool (CSV upload: `mmsi, label, source, date`). Labels are stored in the `prelabel` table and incorporated at the next pipeline run.

### Model recalibration trigger

The pipeline automatically recalibrates the causal model weight (`w_graph`) and re-trains the Isolation Forest baseline when:
- ≥ 10 new confirmed labels have been added since the last calibration, **or**
- The weekly Precision@50 spot-check drops more than 0.05 below the 4-week rolling average

Recalibration runs as part of the next scheduled 15-minute pipeline cycle. No manual intervention required. The new model version is logged with a calibration timestamp visible in the dashboard footer.

---

## 6. Week 7 Trial Report Reference

This SOP is referenced in the Week 7 trial deliverables. The trial report will include:
- Aggregate dispatch decision log (count by tier; no vessel-specific PII unless Cap Vista consents)
- Feedback loop summary: labels added, recalibration events triggered
- Recommendation for any SOP amendments based on 7-week operational experience
