# OSINT Investigation — Test Cases

Vessel profiles for manual testing of the in-app OSINT investigation workflow.
Each entry covers a confirmed sanctioned vessel or stateless-MMSI candidate from
the live top-50 watchlist (snapshot: 2026-05-03).

To test: open the arktrace app, select the vessel by MMSI, click **Investigate**,
and follow the 5-step panel. Use the expected findings below to validate LLM output
at each step.

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
| MarineTraffic (MMSI) | [273449240](https://www.marinetraffic.com/en/ais/details/ships/mmsi:273449240) |
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

## References

- [Local LLM setup](local-llm-setup.md) — start llama-server before running investigations
- [LLM grounding policy](llm-grounding.md) — what the LLM may and may not claim
- [indago OSINT workflow](https://edgesentry.github.io/indago/osint-workflow-design/) — automated workflow (Case B)
