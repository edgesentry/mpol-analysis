# OSINT Investigation — Test Cases

Vessel profiles for manual testing of the in-app OSINT investigation workflow.
Each entry covers a confirmed sanctioned vessel or stateless-MMSI candidate from
the live top-50 watchlist (snapshot: 2026-05-03).

---

## How the investigation workflow fits together

```
indago (data pipeline)
    │
    ├─ Ingests AIS stream (positions, gaps, STS events)
    ├─ Computes behavioural features per vessel
    │    ais_gap_count_30d, sts_candidate_count,
    │    chokepoint_exit_gap_count, ais_pre_gap_regularity,
    │    imo_type_mismatch, imo_scrapped_flag, …
    ├─ Scores with anomaly detection + composite model → confidence [0,1]
    ├─ Cross-references OpenSanctions DB → sanctions_distance
    ├─ Detects ITU-unallocated MMSIs → stateless_mmsi flag
    └─ Publishes candidate_watchlist.parquet → maridb-public R2
                │
                ▼ (push.py copies to arktrace-public)
arktrace (analyst app)
    │
    ├─ Pulls watchlist from arktrace-public R2 → registers in DuckDB-WASM
    ├─ Renders vessels on map + ranked table (confidence-sorted)
    ├─ Analyst selects vessel → detail panel opens
    │    · Confidence badge, SHAP signal bars, analyst brief (LLM)
    │    · "Investigate" button opens the 5-step OSINT panel:
    │
    │   Step 1 — Triage
    │     Local LLM reads vessel data from DuckDB, identifies
    │     top 3 evasion indicators (bounded prompt, ≤10 words each)
    │
    │   Step 2 — OSINT links
    │     Pre-built MarineTraffic / VesselFinder / OFAC URLs
    │     generated from MMSI and IMO
    │
    │   Step 3 — Analyst notes
    │     Analyst opens links, pastes findings into text area
    │
    │   Step 4 — Synthesis
    │     Local LLM combines vessel data + analyst notes →
    │     2-sentence threat assessment
    │
    │   Step 5 — Briefing
    │     Local LLM drafts 3-sentence DSTA/MPA briefing
    │     Analyst edits → Approve → saved to DuckDB (OPFS)
    │     "Copy as Markdown" → paste into GitHub Issue (audit trail)
    │
    └─ Session persists across reloads (DuckDB OPFS-backed)
```

### What indago provides

| indago output | How arktrace uses it |
|---|---|
| `confidence` score | Ranks vessels in the watchlist; drives triage priority |
| `sanctions_distance` | Flags vessels with ownership proximity to sanctioned entities |
| `top_signals` (SHAP) | Displayed as signal bars; passed to LLM as investigation context |
| `ais_gap_count_30d` | Key evasion indicator surfaced in triage step |
| `sts_candidate_count` | STS event count surfaced in triage step |
| Stateless MMSI flag | Surfaced as high-confidence evasion signal in triage |
| `candidate_watchlist.parquet` | The entire ranked vessel list the analyst works from |

### What the analyst does

1. **Open arktrace app** — watchlist loads from R2 into DuckDB-WASM automatically
2. **Review top candidates** — sorted by confidence, filtered by region
3. **Select a vessel** — click any row to open the detail panel
4. **Click Investigate** — opens the 5-step OSINT panel
5. **Step 1** — click "Run triage", read the LLM's 3 evasion indicators
6. **Step 2** — open the OSINT links (MarineTraffic, VesselFinder, OFAC) in new tabs
7. **Step 3** — paste relevant findings from those pages into the notes field
8. **Step 4** — click "Synthesise →", review the 2-sentence threat assessment
9. **Step 5** — click "Draft briefing →", edit if needed, click "Approve"
10. **Copy as Markdown** — paste into a GitHub Issue for team audit trail

The local LLM (Qwen2.5-7B via llama-server) runs on the same machine as the
browser — no data leaves the device. Start it with `bash scripts/run_llama.sh`
before opening the app.

---

## Test cases

Vessel profiles for manual testing. Use the expected findings to validate LLM
output at each step.

---

## Confirmed sanctioned vessels (sanctions_distance = 0)

### 1. DOBRYNYA — MMSI 273449240

| Field | Value |
|---|---|
| MMSI | 273449240 |
| IMO | — |
| Flag | Russia |
| Type | Unknown |
| Confidence | 0.674 (Rank 1) |
| Sanctions basis | OFAC Jan 2025 — EO 14024 (Russia harmful activities) |
| Sanctioned entity | Rosnefteflot / Rosneft infrastructure fleet |

**What arktrace detected:**
Russia-flagged vessel with the highest confidence score in the watchlist. High
`sts_candidate_count` and `ais_gap_max_hours` signals. No IMO in registry data —
consistent with deliberate identity suppression. Sanctioned as part of the January
2025 OFAC action targeting Rosneft's shadow tanker fleet supporting Sakhalin and
Arctic crude exports.

**OSINT links:**

| Source | Link |
|---|---|
| MarineTraffic | [DOBRYNYA](https://www.marinetraffic.com/en/ais/details/ships/shipid:350731) |
| VesselFinder | [273449240](https://www.vesselfinder.com/?mmsi=273449240) |
| OFAC SDN search | [Rosnefteflot](https://sanctionssearch.ofac.treas.gov/?searchText=Rosnefteflot) |

**Expected triage output (Step 1):**

1. Highest confidence score in watchlist (0.674)
2. Russia flag — OFAC Jan 2025 direct designation
3. No IMO — identity suppression indicator

**Expected synthesis (Step 4):**
Russia-flagged tanker fleet vessel directly sanctioned under OFAC EO 14024 for
supporting Rosneft crude export operations. Confidence driven by AIS evasion
behaviour and sanctions network proximity.

---

### 2. ANHONA — MMSI 312171000

| Field | Value |
|---|---|
| MMSI | 312171000 |
| IMO | 9354521 |
| Flag | Belize |
| Type | Oil Products Tanker (~46,000 DWT, built 2008) |
| Confidence | 0.521 (Rank 2) |
| Sanctions basis | OFAC Oct 2024 — EO 13846 (Iran oil sector) |
| Sanctioned entity | Harry Victor Ship Management and Operation L.L.C. (UAE) |

**What arktrace detected:**
Belize-flagged tanker transporting Iranian petrochemicals for Triliance Petrochemical
Co. (US-sanctioned) via a UAE-based shell company. Classic identity layering:
Iranian product → UAE management → Belize flag. The operator's network position
would have scored suspicious before the October 2024 OFAC designation.

**OSINT links:**

| Source | Link |
|---|---|
| MarineTraffic (MMSI) | [312171000](https://www.marinetraffic.com/en/ais/details/ships/mmsi:312171000) |
| MarineTraffic (IMO) | [9354521](https://www.marinetraffic.com/en/ais/details/ships/imo:9354521) |
| VesselFinder | [312171000](https://www.vesselfinder.com/?mmsi=312171000) |
| OFAC SDN search | [Harry Victor Ship Management](https://sanctionssearch.ofac.treas.gov/?searchText=Harry+Victor+Ship+Management) |
| OFAC SDN search | [ANHONA](https://sanctionssearch.ofac.treas.gov/?searchText=ANHONA) |

**Expected triage output (Step 1):**

1. Belize flag — open registry used for Iranian crude identity layering
2. IMO 9354521 — operator designated OFAC Oct 2024
3. Tanker type consistent with petrochemical transport

**Expected synthesis (Step 4):**
ANHONA is a Belize-flagged tanker operated by a UAE shell company sanctioned for
facilitating Iranian petrochemical exports. Evasion pattern: Iranian product obscured
through UAE management layer before delivery to buyers.

---

### 3. SCF ENTERPRISE — MMSI 273312060

| Field | Value |
|---|---|
| MMSI | 273312060 |
| IMO | — |
| Flag | Russia |
| Type | Unknown |
| Confidence | 0.516 (Rank 3) |
| Sanctions basis | OFAC Jan 2025 — EO 14024 (Russia harmful activities) |
| Sanctioned entity | Sovcomflot / Sakhalin-2 LNG project |

**What arktrace detected:**
Russia-flagged vessel designated in the same January 2025 OFAC action as DOBRYNYA,
targeting Sovcomflot — Russia's state shipping company managing the Sakhalin-2 LNG
export terminal. No IMO registered. AIS gap behaviour consistent with dark voyages
on the Russia–Asia LNG route.

**OSINT links:**

| Source | Link |
|---|---|
| MarineTraffic (MMSI) | [273312060](https://www.marinetraffic.com/en/ais/details/ships/mmsi:273312060) |
| VesselFinder | [273312060](https://www.vesselfinder.com/?mmsi=273312060) |
| OFAC SDN search | [SCF Enterprise](https://sanctionssearch.ofac.treas.gov/?searchText=SCF+Enterprise) |
| OFAC SDN search | [Sovcomflot](https://sanctionssearch.ofac.treas.gov/?searchText=Sovcomflot) |

**Expected triage output (Step 1):**

1. Russia flag — Sovcomflot / Sakhalin-2 OFAC Jan 2025
2. No IMO — identity suppression, same pattern as DOBRYNYA
3. AIS gap behaviour consistent with dark transit

**Expected synthesis (Step 4):**
SCF ENTERPRISE is a Sovcomflot vessel sanctioned for supporting Sakhalin-2 LNG
exports in violation of EO 14024. Pattern of AIS gaps suggests deliberate dark
voyages on Russia-to-Asia routes.

---

### 4. PIONEER 92 — MMSI 457133000

| Field | Value |
|---|---|
| MMSI | 457133000 |
| IMO | 9340934 |
| Flag | Mongolia |
| Type | Tug |
| Confidence | 0.495 (Rank 4) |
| Sanctions basis | OFAC — EO 13902 (Iran petroleum sector) |
| Sanctioned entity | Logos Marine Pte. Ltd., Singapore |

**What arktrace detected:**
Mongolia-flagged tug whose operator (Logos Marine, Singapore) is sanctioned for
supporting at least 7 Iran-affiliated tankers at Singapore's Eastern Outer Port
Limit (EOPL) via ship-to-ship (STS) transfers. The STS operations obscured Iranian
crude provenance before delivery to Chinese buyers. Directly matches the pattern
described in the Al Jazeera April 30 2026 investigation into Singapore-area STS
networks.

**OSINT links:**

| Source | Link |
|---|---|
| MarineTraffic (MMSI) | [457133000](https://www.marinetraffic.com/en/ais/details/ships/mmsi:457133000) |
| MarineTraffic (IMO) | [9340934](https://www.marinetraffic.com/en/ais/details/ships/imo:9340934) |
| VesselFinder | [457133000](https://www.vesselfinder.com/?mmsi=457133000) |
| OFAC SDN search | [Logos Marine](https://sanctionssearch.ofac.treas.gov/?searchText=Logos+Marine) |
| OFAC SDN search | [PIONEER 92](https://sanctionssearch.ofac.treas.gov/?searchText=PIONEER+92) |

**Expected triage output (Step 1):**

1. Mongolia flag — open registry used for Iranian STS support vessels
2. Operator Logos Marine designated OFAC EO 13902
3. Tug type — consistent with STS coordination role at EOPL

**Expected synthesis (Step 4):**
PIONEER 92 is a Mongolia-flagged tug operated by a Singapore company sanctioned
for coordinating Iranian crude STS transfers at Singapore EOPL. Direct link to
active Iran crude evasion network identified in recent OSINT reporting.

---

## Stateless MMSI candidates (unallocated MID)

These vessels broadcast MMSI numbers with ITU-unallocated MID prefixes. No
legitimate vessel can hold these identifiers — they are a documented shadow fleet
evasion technique. No OSINT lookup will return a vessel record; the signal itself
is the finding.

| MMSI | MID | Confidence | MarineTraffic | VesselFinder |
|---|---|---|---|---|
| 400789012 | 400 | 0.606 | [link](https://www.marinetraffic.com/en/ais/details/ships/mmsi:400789012) | [link](https://www.vesselfinder.com/?mmsi=400789012) |
| 400123456 | 400 | 0.599 | [link](https://www.marinetraffic.com/en/ais/details/ships/mmsi:400123456) | [link](https://www.vesselfinder.com/?mmsi=400123456) |
| 400345678 | 400 | 0.591 | [link](https://www.marinetraffic.com/en/ais/details/ships/mmsi:400345678) | [link](https://www.vesselfinder.com/?mmsi=400345678) |

**Expected result:** MarineTraffic and VesselFinder will return no vessel record —
this confirms the MMSI is unallocated. The absence of a record is itself evidence
of evasion.

**Expected synthesis for any stateless MMSI:**
This vessel broadcasts an ITU-unallocated MMSI (MID 400), a tactic used by shadow
fleet operators to appear invisible to standard AIS tracking platforms. No registry
record exists by design. Recommend cross-referencing against SAR imagery for the
last known position.

---

## Additional confirmed vessel

### MARIE DE LOURDES I — MMSI 248000368

| Field | Value |
|---|---|
| MMSI | 248000368 |
| IMO | — |
| Flag | Malta |
| Confidence | ~0.38 (Rank 12) |
| Sanctions basis | OFAC — EO 13726 (Libya illicit crude) |

**Note:** Below the default confidence filter (0.4). Set minimum confidence to 0.3
in the app to see this vessel in the watchlist.

**OSINT links:**

| Source | Link |
|---|---|
| MarineTraffic (MMSI) | [248000368](https://www.marinetraffic.com/en/ais/details/ships/mmsi:248000368) |
| VesselFinder | [248000368](https://www.vesselfinder.com/?mmsi=248000368) |
| OFAC SDN search | [MARIE DE LOURDES](https://sanctionssearch.ofac.treas.gov/?searchText=MARIE+DE+LOURDES) |

---

## Source investigation

### Al Jazeera — 2026-04-30

**[Tracking the shadow fleet: How Iran evaded the US naval blockade in Hormuz](https://www.aljazeera.com/economy/2026/4/30/tracking-the-shadow-fleet-how-iran-evaded-the-us-naval-blockade-in-hormuz)**

Investigative report published 30 April 2026. Key findings that triggered this watchlist cross-reference:

- Named vessels **Flora, Genoa, Skywave, and Pola** as Iran-linked tankers active in the Strait of Hormuz, deliberately disabling or jamming AIS signals to hide identities and destinations
- Documented widespread AIS manipulation and use of fake flags (landlocked nation registries including Botswana, San Marino, Comoros) to obscure ownership
- Operating firms primarily based in Iran (15.7%), China (13%), Greece (11%), UAE (9.7%)
- At least 26 ships from Iran's shadow fleet circumvented the US naval blockade since it was imposed

**Cross-reference result:** No direct name match found for Flora/Genoa/Skywave/Pola in the arktrace watchlist — shadow fleet vessels change names frequently. MMSI/IMO cross-reference is required. The vessels in this test case document (PIONEER 92, ANHONA, DOBRYNYA, SCF ENTERPRISE) were identified via sanctions_distance=0 scoring, not name search.

---

## References

- [Local LLM setup](local-llm-setup.md) — start llama-server before running investigations
- [LLM grounding policy](llm-grounding.md) — what the LLM may and may not claim
- [indago OSINT workflow](https://edgesentry.github.io/indago/osint-workflow-design/) — automated workflow (Case B)
