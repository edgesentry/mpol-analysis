# Background

## The Shadow Fleet Problem

A shadow fleet vessel is one that deliberately obscures its identity, ownership, or activities to evade sanctions, avoid port state control, or facilitate illicit cargo transfers. The problem has grown substantially since 2022 as sanctions on Russian, Iranian, and Venezuelan oil exports drove demand for opaque maritime logistics.

Estimates from industry sources (Lloyd's List Intelligence, CSIS, Windward) suggest 600–1,400 vessels operate in the shadow fleet at any given time, representing 10–15% of global tanker capacity.

### Geography

The challenge area covers **up to 1,600nm from Singapore to water depth of 200m**. This encompasses:

- The **Strait of Malacca** — one of the world's busiest chokepoints; ~80,000 vessel transits/year
- The **South China Sea** — major STS transfer zone, especially around the Batam/Riau Islands area and deeper waters off Malaysia
- The **Andaman Sea** and approaches to the **Bay of Bengal**
- Parts of the **Indonesian Archipelago** (Lombok Strait, Sunda Strait)

### Key Evasion Techniques

**AIS manipulation:**
- Gaps > 6 hours in active shipping lanes are anomalous; intentional gaps are a primary STS indicator
- Position jumping (consecutive positions implying >50 knots) indicates falsified GPS input
- MMSI spoofing: broadcasting another vessel's identity

**Identity laundering:**
- The average shadow fleet tanker changes name 2–4× and flag state 1–3× over a 3-year period (Windward, 2023)
- Flag states with weak oversight (Palau, Cameroon, Gabon) are disproportionately represented
- Ownership is typically structured through 3–5 shell company layers across multiple jurisdictions

**Illicit STS transfers:**
- Two vessels drift in proximity (within 0.5nm) with AIS showing "moored" or "at anchor" while not near any port
- Typically occur in international waters just outside EEZ boundaries
- Duration: 4–24 hours

## Why Existing Tools Are Insufficient

The Cap Vista challenge statement explicitly excludes:
- Real-time vessel monitoring based on AIS alone
- Standard AIS + satellite fusion anomaly detection
- Off-the-shelf vessel behavior profiling or risk scoring platforms

The gap is **contextual fusion**: linking AIS behavior to ownership graph proximity to sanctioned entities, trade flow data that contradicts declared routes, and the velocity of identity change — combined with explainability that allows a human analyst to understand and act on a flag.

## Regulatory Context

| Regime | Relevance |
|---|---|
| OFAC SDN (US) | Primary source of sanctioned vessel/entity ground truth |
| EU Regulation 2022/428 | EU sanctions list; additional vessel/entity coverage |
| UN Security Council 1718/1737 | DPRK and Iran programmes |
| MAS (Singapore) | MAS TF-02 guidance on sanctions screening for financial institutions |
| IMO Resolution A.1155(32) | Carriage of AIS; basis for gap analysis as anomaly indicator |

## Prior Art and Our Differentiation

| Approach | Used by | Limitation |
|---|---|---|
| AIS gap detection | Windward, MarineTraffic | Single signal; easy to game |
| Sanctions list screening | Refinitiv, Dow Jones | Static lists; no behavioral signal |
| Vessel behavior profiling | Pole Star, FleetMon | Proprietary; no ownership context |
| Satellite SAR correlation | Orbital Insight, Hawkeye360 | High cost; coverage gaps |

**Our differentiation:** We combine AIS behavioral signals with **ownership graph risk distance** (Neo4j BFS to sanctioned entities), **identity volatility** (flag/name/owner change rates), and **trade flow mismatch** (UN Comtrade declared cargo vs detected route) — all with SHAP-based per-vessel explanation. The entire stack runs on open-source software with no cloud dependency.
