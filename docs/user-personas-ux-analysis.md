# User Personas & UX Analysis

This document defines the three primary arktrace user personas, maps their workflows against the current dashboard, and identifies UX gaps with prioritised improvement recommendations.

---

## Personas

### Persona 1 — Maya · Maritime Intelligence Analyst

**Organisation:** Coast guard intelligence unit or port authority intel cell  
**Background:** 5–8 years in maritime intelligence; OSINT/SIGINT background; comfortable with spreadsheets and network graphs; not a software engineer  
**Working context:** Dedicated analyst workstation; shift-based (not always the same person every day); works cases over days or weeks

**Goals**
- Build a complete picture of a suspicious vessel's ownership network, history, and geopolitical context
- Produce an intelligence report that satisfies an evidentiary standard (tier taxonomy: Confirmed / Probable / Suspect)
- Identify unknown-unknowns: vessels that should be on the watchlist but aren't flagged yet

**Key workflows**
1. Receives a tip (MMSI or vessel name) → opens vessel panel → reads SHAP signals and LLM brief
2. Follows ownership chain → checks if associated companies appear in other rows
3. Reviews GDELT geopolitical events in the chat panel → asks follow-up questions
4. Assigns review tier with evidence references and detailed rationale
5. Marks vessel as `handoff_recommended` once evidence threshold is met

**Pain points on the current UI**
- Has to open the review panel to read the LLM brief — no standalone "brief" surface before committing to a review action
- No way to search by vessel name or IMO — has to find a vessel by scrolling the table or knowing its MMSI
- Causal "Shadow Signal" badge is displayed but not explained in context; has to consult docs to understand it
- Chat history is not persisted across page reloads — loses multi-turn context when refreshing

---

### Persona 2 — Kenji · Watch Duty Officer

**Organisation:** Port maritime security operations centre  
**Background:** 10+ years maritime operations; limited analytical depth but strong situational awareness; works under time pressure during shift handover  
**Working context:** Multi-monitor setup; 4–6 hour watches; must brief the next duty officer at handover

**Goals**
- In the first 10 minutes of a watch: know which vessels require attention today
- Make fast go/no-go patrol assignment decisions with minimal clicks
- Hand off a clear prioritised list to the outgoing crew

**Key workflows**
1. Opens dashboard → glances at KPI bar (candidates, high-confidence count)
2. Sorts watchlist by confidence → checks if any new high alerts appeared since last watch
3. Clicks a high-confidence vessel → reads top SHAP signals
4. Marks it `queued_review` or `in_review` to claim it for this watch
5. At end of watch: exports or verbally briefs the priority list

**Pain points on the current UI**
- No "since last watch" filter — can't quickly isolate vessels that are *newly* high-confidence versus ones already being worked
- No quick-assign button — marking a vessel `in_review` requires opening the full review panel form
- Alert toasts disappear after a few seconds — no alert log or "missed alerts" indicator
- Table rows don't visually distinguish vessels that already have a review tier from unreviewed ones — everything looks the same at a glance
- No shift-handover export: must manually note priorities; no "export current queue" affordance

---

### Persona 3 — Priya · Field Investigation Coordinator

**Organisation:** Port authority operations cell; liaison between screening platform and patrol assets  
**Background:** Former patrol officer; now desk-based; orchestrates the handoff from intelligence to physical investigation; accountable for wasted investigations  
**Working context:** Coordinates multiple active cases simultaneously; needs to track state across days

**Goals**
- Know which vessels are cleared for handoff and ensure patrol assets receive correct targeting data
- Track investigation outcomes back into the platform (closed / confirmed / cleared)
- Minimise wasted patrol sorties by validating evidence before approving handoff

**Key workflows**
1. Filters watchlist to `handoff_recommended` tier → reviews analyst rationale and evidence references
2. Reads LLM-generated dispatch brief to check it meets the minimum evidence bar
3. Changes state to `handoff_accepted` → generates patrol JSON from dispatch brief
4. Receives investigation outcome from patrol → logs result, closes case
5. Reviews `handoff_completed` vessels with `cleared` outcome to monitor wasted-investigation rate

**Pain points on the current UI**
- The "Generate Brief" button in the review panel is disabled until the form fields are filled — confusing when reviewing an already-submitted review
- No case queue view: no filtered view of just the vessels in `handoff_recommended` or `handoff_accepted` state
- No outcome logging UI: closing a case and recording the patrol result requires raw API calls; no form in the dashboard
- Dispatch brief modal has no print or export-to-PDF affordance
- No audit trail view: can't see who changed a handoff state or when without querying the DB directly

---

## User Journey Analysis

### Maya's Journey — Investigating MMSI 273456782

| Step | Current experience | Friction |
|---|---|---|
| 1. Find vessel | Scrolls table to MMSI or filters by confidence | **High** — no name/IMO search |
| 2. Read brief | Must open review panel, wait for LLM to stream | **Medium** — brief is gated behind review action |
| 3. Check ownership | Chat panel → types query | **Low** — works well |
| 4. Understand causal badge | Hovers — no tooltip; consults docs | **High** — no in-context explanation |
| 5. Assign tier | Opens review panel, fills form | **Low** — form is functional |
| 6. Return next session | Reloads page, review state reloaded from API | **Low** — state persists in DB |

### Kenji's Journey — Morning Watch Triage

| Step | Current experience | Friction |
|---|---|---|
| 1. See today's new alerts | Checks alert toasts; if missed, no record | **High** — no alert history |
| 2. Sort by confidence | Table sorts by default; works | **Low** |
| 3. Spot already-reviewed vs new | All rows look the same visually | **High** — no review-state badge on rows |
| 4. Quick-claim a vessel | Must open review panel and save form | **Medium** — 5 clicks for a simple state change |
| 5. Brief next officer | Manual verbal / notes | **High** — no watch-handover export |

### Priya's Journey — Handoff Coordination

| Step | Current experience | Friction |
|---|---|---|
| 1. Find handoff-ready vessels | No filter for handoff state | **High** — must scan all 288 rows |
| 2. Validate evidence | Reads review summary in panel | **Low** |
| 3. Accept handoff | Changes dropdown, saves | **Low** |
| 4. Confirm outcome | No UI — raw API call required | **High** — completely missing |
| 5. Review wasted rate | No dashboard metric for this KPI | **High** — completely missing |

---

## UX Gap Assessment

### P0 — Blocking core workflows

| ID | Gap | Affects |
|---|---|---|
| UX-01 | No vessel search by name or IMO | Maya |
| UX-02 | No review-state indicator on watchlist rows (tier badge, colour coding) | Kenji |
| UX-03 | Alert history / missed-alert log | Kenji |
| UX-04 | No handoff-state filter on the watchlist table | Priya |

### P1 — Significant friction

| ID | Gap | Affects |
|---|---|---|
| UX-05 | LLM brief gated behind review form — no standalone brief surface | Maya, Priya |
| UX-06 | Causal "Shadow Signal" badge has no tooltip or explanation | Maya |
| UX-07 | No quick-action (single-click claim to `in_review`) | Kenji |
| UX-08 | Outcome logging (closing a case with patrol result) has no UI | Priya |
| UX-09 | Chat history not persisted across page reloads | Maya |
| UX-10 | Dispatch brief modal has no print / copy-to-clipboard affordance | Priya |

### P2 — Quality-of-life

| ID | Gap | Affects |
|---|---|---|
| UX-11 | Table pagination / virtual scroll (288 rows loaded at once) | All |
| UX-12 | Watch-handover export: "export current queue as PDF/JSON" | Kenji |
| UX-13 | Score history sparkline per vessel (trend over time) | Maya |
| UX-14 | Audit trail panel (who changed state, when) | Priya |
| UX-15 | Map and table scroll sync (clicking map vessel scrolls to table row) | All |

---

## Recommended Improvements (by priority)

### P0 — Implement first

**UX-01: Global vessel search**
Add a search input at the top of the sidebar. Search across `vessel_name`, `mmsi`, and `imo` fields client-side (data already in the DOM). Highlight the matching row and pan the map to it.

**UX-02: Review-state row badges**
Add a coloured tier pill and handoff-state pill directly on each watchlist table row (already fetched via `/api/reviews/{mmsi}` on row load). Use colour to communicate state: grey = unreviewed, blue = in_review, amber = probable, red = confirmed, green = cleared.

**UX-03: Alert history drawer**
Keep a client-side ring buffer (last 50 alerts). Add a bell icon to the KPI bar with unread count. Clicking opens a drawer showing timestamp, MMSI, vessel name, and confidence for each alert.

**UX-04: Handoff-state filter**
Add a `handoff_state` multi-select to the filter panel (alongside vessel type and confidence). Default: all states shown. Allows Priya to filter to `handoff_recommended` in two clicks.

### P1 — Next sprint

**UX-05: Vessel brief panel (standalone)**
Expose the LLM brief at the top of the vessel detail area (the right-hand panel that opens on row click), above the "Review" button. Load it automatically on vessel click, not behind a review gate. Keep the current cache — it's just a matter of surfacing it earlier in the flow.

**UX-06: Causal badge tooltip**
On hover of the "Shadow Signal" badge, show a tooltip explaining what the causal score means and listing the contributing signals with plain-language descriptions.

**UX-07: One-click claim**
Add a "Claim" button on each table row (next to the existing "Review" button) that fires `POST /api/reviews` with `handoff_state=in_review` and the current user's analyst-id. No panel needed for this action.

**UX-08: Outcome logging**
Add a fourth section to the review panel for `handoff_completed` state: a small form with `outcome` (confirmed / cleared / inconclusive) and `outcome_notes`. Fires a `PATCH /api/reviews/{mmsi}/outcome` endpoint.

---

## Implementation Notes

- **UX-01, UX-02, UX-04** are client-side only — no new API endpoints required.
- **UX-03** requires a small client-side ring buffer; SSE alerts already fire — just need to be stored.
- **UX-05** is a layout change in `index.html` — the `/api/briefs/{mmsi}/cached` and `/api/briefs/{mmsi}` endpoints already exist.
- **UX-07** needs one new lightweight API call — already handled by the existing `POST /api/reviews` endpoint.
- **UX-08** requires a new `outcome` column in `vessel_reviews` and a PATCH endpoint.

All P0 items can be completed with changes only to `src/viz/templates/index.html` and minor additions to `src/api/routes/reviews.py`.
