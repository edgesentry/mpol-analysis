# LLM Grounding and Anti-Hallucination Policy

arktrace uses a local LLM exclusively for **text synthesis** — converting pre-computed, deterministic scores into plain-language patrol briefs. The LLM has no access to external data, cannot modify scores, and cannot query the pipeline.

---

## Two-Phase Architecture

```
Phase 1 — Deterministic scoring  (no LLM)
  AIS positions → feature engineering → DiD causal model → SHAP attribution
  Output: confidence score, ATT ± CI, top_signals JSON, ownership graph path
          ↓  immutable — passed as read-only context to Phase 2

Phase 2 — Bounded text synthesis  (LLM)
  Context block (vessel metadata + Phase 1 outputs) → system prompt + user prompt
  Output: 2-3 sentence plain-language brief
```

The LLM only sees what Phase 1 produces. It cannot alter scores, fetch external data, or access any information outside the context block.

---

## System Prompt

Injected as `role: "system"` on every brief request:

```
You are a maritime intelligence analyst writing patrol dispatch briefs.
You will be given a structured vessel context block containing pre-computed scores,
SHAP signal attributions, and verified registry data. Your only job is to synthesise
that context into a concise plain-language brief.

STRICT CONSTRAINTS — violation invalidates the brief:
- Do NOT invent, infer, or guess any MMSI, IMO number, vessel name, flag, owner,
  or position not present in the context block.
- Do NOT add sanctions designations, ownership links, or cargo claims not stated
  in the context block.
- Do NOT speculate about intent beyond what the provided signals support.
- Every factual claim must be traceable to a field in the context block.
- Output plain text only — no markdown, no bullet points, no headers.
- Maximum 3 sentences.
```

---

## User Prompt (context block)

Injected as `role: "user"`. Contains only fields present in the scored vessel row:

```
Write a 2-3 sentence risk assessment for the vessel below.
Focus on probable cause of the anomaly, regional context, and recommended follow-up action.
Only reference data present in the context block below.

Vessel: <vessel_name or mmsi>
MMSI: <mmsi>
Flag: <flag>               ← omitted if null
Type: <vessel_type>        ← omitted if null
Region: <region>           ← omitted if null
Last seen: <last_seen>     ← omitted if null
Position: <lat>°, <lon>°   ← omitted if null
Anomaly confidence: <confidence>
```

No SHAP signal values, ownership paths, or ATT coefficients are included in the user prompt at this stage — the brief is intentionally scoped to vessel-level summary. The full signal breakdown is displayed separately in the SHAP panel.

---

## Output Constraints

| Parameter | Value | Rationale |
|---|---|---|
| `max_tokens` | 200 | Hard ceiling — prevents rambling or invented detail |
| `temperature` | 0.3 | Low variance — reproducible, factual tone |
| Output format | Plain text, ≤ 3 sentences | No markdown structures that could embed unverified claims |

---

## What the LLM Cannot Do

| Prohibited action | Enforced by |
|---|---|
| Invent MMSIs, IMO numbers, or vessel names | System prompt constraint + context block contains only verified registry fields |
| Add sanctions designations not in the context | System prompt constraint; sanctions data is displayed from the deterministic pipeline, not the brief |
| Modify the confidence score or ATT estimate | LLM output is stored in `analyst_briefs` table; scores are stored separately and never overwritten by brief generation |
| Access external APIs or the internet | Local inference endpoint only (`localhost`); no outbound network access from the LLM process |
| Persist state between requests | Stateless API call; no conversation history is maintained |

---

## Verifiability

Every claim in an analyst brief can be verified against the displayed SHAP panel and vessel detail row. The brief is clearly labelled "Analyst brief" and visually separated from the deterministic SHAP attribution and ATT outputs. Analysts are instructed in the [SOP](https://edgesentry.github.io/indago/sop/) to treat the brief as a synthesis aid, not a primary evidence source.
