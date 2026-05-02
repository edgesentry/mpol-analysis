# Feature Engineering

The arktrace pipeline computes 27 features across five families for every vessel MMSI. All features are written to the `vessel_features` DuckDB table by `src/features/build_matrix.py`.

## Feature families

| Family | Module | Features | Backend |
|---|---|---|---|
| AIS Behavioral | `ais_behavior.py` | 8 | DuckDB / Polars |
| Identity Volatility | `identity.py` | 7 | Lance Graph + DuckDB |
| Ownership Graph | `ownership_graph.py` | 5 | Lance Graph (Polars joins) |
| Trade Flow Mismatch | `trade_mismatch.py` | 2 | DuckDB + Comtrade API |
| EO Fusion | `eo_fusion.py` | 4 | DuckDB (GFW API via `eo_gfw.py` / CSV) |
| SAR Detections | `sar_detections.py` | 1 | DuckDB |
| **Total** | | **27** | |

---

## AIS Behavioral features

Source: `ais_positions` table, computed over a rolling window (default 30 days, configurable with `--window`).

### `ais_gap_count_30d`

Count of AIS transmission gaps longer than the configured threshold (default 10 hours) in the last 30 days.

**Shadow fleet signal:** Deliberate AIS switch-off is the primary evasion technique for sanctioned tankers. The threshold is set to 10h because Singapore/Malacca anchorage wait times of 8–12h are normal commercial behaviour; genuine shadow-fleet dark periods for STS transfers are 12–48h and are still captured reliably.

**Implementation:** The Polars lazy pipeline computes the time delta between consecutive position rows for each MMSI. Gaps are counted and summed per vessel over the rolling window.

### `ais_gap_max_hours`

Duration in hours of the longest single AIS gap in the window.

**Shadow fleet signal:** Compliant vessels rarely go dark for more than 2–4 hours. Gaps above 12 hours indicate a port call without AIS, an at-sea dark period, or equipment failure. Gaps above 24 hours in open water are a strong evasion indicator.

### `position_jump_count`

Count of consecutive position pairs where the implied speed exceeds 50 knots (calculated via Haversine distance / elapsed time).

**Shadow fleet signal:** GPS spoofing is endemic in the Taiwan Strait, Black Sea, and Persian Gulf approaches. A vessel that "jumps" 200 km in 30 minutes without leaving any intermediate positions is almost certainly receiving a spoofed GPS signal, often to mask its true location during a dark STS transfer.

**Implementation:** Uses a 1-hour sliding window for robustness against occasional timestamp errors.

### `sts_candidate_count`

Count of distinct vessels that have occupied the same H3 hexagon (resolution 8, ~0.7 km cell edge) within 2 hours of the subject vessel.

**Shadow fleet signal:** Ship-to-Ship transfers occur at anchorages and in open water. Two tankers sharing the same ~0.7 km cell for a sustained period without a declared port call are STS candidates. H3 resolution 8 is chosen to match the beam of a VLCC at anchor (width ≈ 60 m) within the cell precision.

**Implementation:** H3 hexagon IDs are pre-computed for all positions; a self-join on hexagon + time window identifies co-located vessels. When a GEBCO 200 m depth mask is present (`{region}_deep_cells.parquet` alongside the DB, built by `scripts/build_gebco_mask.py`), co-locations in water shallower than 200 m are excluded — removing false positives from port anchorages and shallow straits such as the Malacca Strait.

### `port_call_ratio`

Fraction of time in the window spent within 5 nm of a known port, as a proxy for legitimate port activity.

**Shadow fleet signal:** Shadow fleet tankers minimise declared port calls to avoid physical inspection and AIS-based monitoring by port state control authorities. A low `port_call_ratio` combined with high loitering hours suggests the vessel is active at sea but avoiding port records.

### `loitering_hours_30d`

Total hours spent moving slower than 2 knots outside declared moorage areas, accumulated over the window.

**Shadow fleet signal:** Loitering at sea at very low SOG (below steerage way) is a behavioural precursor to dark STS. Genuine commercial tankers loiter only when waiting for a berth, which shows up near ports. Open-water low-speed drifting suggests rendezvous behaviour.

---

## Identity Volatility features

Source: Lance Graph datasets (ownership changes, name aliases) + `vessel_meta` DuckDB table. Computed over a 2-year lookback.

### `flag_changes_2y`

Number of flag state changes recorded in the vessel registry over the past 2 years.

**Shadow fleet signal:** Legitimate shipping companies rarely reflag vessels. Repeated reflagging — especially to open-registry states (Panama, Marshall Islands, Comoros) — is a known evasion technique to escape the watch-list of any single port state authority, reset OFAC exposure tracking, and complicate due-diligence checks.

### `name_changes_2y`

Number of vessel name changes in 2 years.

**Shadow fleet signal:** Name changes are used to break continuity between a vessel's current identity and its history of sanctioned voyages. A vessel renamed from "ATLANTIC SUN" to "PACIFIC STAR" can avoid automated blocklist checks that match on vessel name.

### `owner_changes_2y`

Number of registered owner changes in 2 years.

**Shadow fleet signal:** Ownership obfuscation through rapid beneficial-owner changes is a key sanctions evasion technique. This feature counts distinct ownership transitions recorded in the Lance Graph OWNED_BY dataset over 2 years.

### `high_risk_flag_ratio`

Fraction of companies in the vessel's full ownership chain that are registered in high-risk flag states.

**High-risk flags:** KP, IR, VE, SY, CU, RU, KM, GA, CM, PW, KI, TG, SL, ST

**Shadow fleet signal:** Even if the vessel itself flies a neutral flag, shell companies up the ownership chain may be registered in North Korea, Iran, or Venezuela. This ratio surfaces ownership-level exposure that vessel-flag screening misses.

### `ownership_depth`

BFS path length from the vessel to the ultimate beneficial owner, capped at 5.

**Shadow fleet signal:** The average legitimate tanker has an ownership chain of 2–3 hops (vessel → shipowner → holding company). Chains of 4–6 hops suggest deliberate opacity: SPVs nested inside other SPVs to frustrate beneficial ownership disclosure requirements.

---

## Ownership Graph features

Source: Lance Graph datasets, computed via Polars joins. All graph features use `sanctions_distance` from the merged OpenSanctions dataset.

### `sanctions_distance`

Minimum BFS hop count from the vessel to any node in the ownership graph that carries a `SANCTIONED_BY` relationship.

| Value | Meaning |
|---|---|
| 0 | Vessel itself is directly designated |
| 1 | Registered owner or manager is designated |
| 2 | Parent company or beneficial owner is designated |
| 99 | No graph connection to any sanctioned entity |

**Computation:** Primary path is BFS over the Lance Graph (`src/features/ownership_graph.py`). The Lance Graph Vessel table is seeded from `vessel_meta`, which only covers vessels that have explicit AIS registry metadata. A DuckDB fallback in `src/features/build_matrix.py` (`_apply_direct_sanctions_fallback`) corrects any remaining `distance=99` rows: after the full feature matrix merge, any vessel whose MMSI appears directly in `sanctions_entities` receives `distance=0`. This ensures MMSI-only OFAC/UN/EU entries (vessels designated without an IMO number, or stored under a non-`Vessel` FtM schema type) are not penalised by a Lance Graph data-coverage gap.

**Shadow fleet signal:** This is the strongest individual predictor in the model. A vessel 1–2 hops from an OFAC/EU/UN entity has a >60% empirical probability of appearing in open-source shadow fleet incident reports.

### `cluster_sanctions_ratio`

Fraction of vessels sharing the same registered owner (via the OWNED_BY dataset) that are individually sanctioned (i.e. have `sanctions_distance = 0`).

**Shadow fleet signal:** Sanctioned fleets tend to operate in clusters. If 50% of the vessels sharing a manager are on the OFAC list, the remaining 50% are likely operating on behalf of the same beneficial owner but have not yet been individually designated.

### `shared_manager_risk`

Minimum `sanctions_distance` across all vessels co-managed with this vessel.

**Shadow fleet signal:** A vessel managed by a company that also manages an OFAC-listed tanker inherits operational risk even if its own ownership chain looks clean.

### `shared_address_centrality`

Count of distinct vessels sharing the same registered company address.

**Shadow fleet signal:** Shell companies used as nominee owners for sanctioned fleets frequently register multiple vessels at the same address. High centrality (> 5 vessels at one address) is a red flag for a nominee ownership structure.

### `sts_hub_degree`

Count of distinct vessels with which this vessel has had AIS-confirmed STS proximity events (from `sts_candidate_count` data).

**Shadow fleet signal:** A vessel that repeatedly co-locates with many different partner vessels is functioning as an STS hub — a central intermediary in a dark transfer network. Hub degree > 3 is rare in legitimate bunkering operations.

---

## Trade Flow Mismatch features

Source: DuckDB `trade_flow` table (populated from the UN Comtrade+ REST API, free tier 500 requests/day). Restricted to crude oil (HS 2709) and petroleum products (HS 2710).

### `route_cargo_mismatch`

Binary flag indicating whether the vessel is a tanker operating on routes from sanctioned exporters with no corresponding bilateral trade record in Comtrade.

| Value | Condition |
|---|---|
| 1.0 | Tanker (AIS type 80–89) from a sanctioned flag state (KP, IR, VE, SY, CU, RU) with zero Comtrade crude imports from that flag in the period |
| 0.5 | Some trade volume but below expected for vessel size |
| 0.0 | Not a tanker, or not from a sanctioned flag |

**Shadow fleet signal:** Iranian crude exports have been ~0 in official UN Comtrade records since 2019, yet ~1.5 mbpd of Iranian crude moves via dark tanker networks each year. A tanker arriving from Iranian waters with no matching Comtrade import record is operating off the books.

### `declared_vs_estimated_cargo_value`

Difference (USD) between the declared cargo value from AIS voyage data and the UN Comtrade statistical estimate for the same route.

**Shadow fleet signal:** Deliberate under-declaration of cargo value is used to reduce tax and duty exposure in destination countries. A large positive discrepancy (declared < estimated) is consistent with dark oil sales.

---

## EO Fusion features

Source: `eo_detections` DuckDB table, populated from the [Global Fishing Watch Vessel Presence API](https://globalfishingwatch.org/our-apis/) or a local CSV fallback. Computed over a 30-day rolling window by `src/features/eo_fusion.py`.

**Requires:** `GFW_API_TOKEN` in `.env` for live ingestion, or a local CSV via `--csv`. Pass `--skip-eo` to `build_matrix.py` to skip this family entirely (features default to 0).

### `eo_dark_count_30d`

Count of EO (Electro-Optical satellite imagery) vessel detections in the last 30 days that were **not** matched to an AIS broadcast within 0.1° / 120 min and were attributed to this vessel via AIS gap + 0.5° proximity.

**Shadow fleet signal:** A vessel detected by satellite imagery that is simultaneously dark on AIS is operating without a transponder — the clearest observable indicator of intentional AIS manipulation. Each such unmatched detection during an AIS gap is a direct observation of dark-vessel behaviour.

**Implementation:** GFW detections are matched to AIS broadcasts by position (≤ 0.1°) and time (≤ 120 min). Unmatched detections within 0.5° of a vessel's last known position during an AIS gap are attributed to that vessel. The 30-day count is written to `vessel_features`.

### `eo_ais_mismatch_ratio`

Fraction of all EO detections attributed to this vessel (matched + unmatched) that were unmatched (dark): `eo_dark_count_30d / total_attributed_detections`.

**Shadow fleet signal:** A vessel that appears in satellite imagery only when it is also broadcasting on AIS has a ratio near 0 — consistent with compliant behaviour. A vessel with a ratio above 0.5 is dark during more than half its satellite observations, indicating a systematic pattern of AIS suppression rather than occasional equipment failure.

---

## Chokepoint Exit / AIS Compliance Weaponization features

Source: `ais_positions` DuckDB table. Computed by `compute_chokepoint_gap_features()` in `src/features/ais_behavior.py`. Added in arktrace#543 (2026-05-02).

**Background:** The Al Jazeera shadow fleet investigation (2026-04-30) confirmed a new evasion pattern: vessels transmit perfect, uninterrupted AIS *during* strait passage to appear compliant, then go dark immediately after clearing the chokepoint. The existing `ais_gap_count_30d` does not distinguish where gaps begin.

### `chokepoint_exit_gap_count`

Number of AIS gap onsets (last known position before going dark) that fall within 50 nautical miles of a major chokepoint exit: Singapore Strait (east/west), Malacca north, Strait of Hormuz east, Bab-el-Mandeb, Suez Canal south, Aegean/Dardanelles, Cape of Good Hope.

**Shadow fleet signal:** Legitimate commercial traffic does not go dark at chokepoint exits. Port congestion and anchorage delays produce gaps *near ports*, not in open water immediately past a major strait. Near-zero false-positive rate for compliant vessels; high specificity for deliberate AIS suppression after chokepoint transit.

**Default when missing:** 0

### `ais_pre_gap_regularity`

Mean coefficient of variation (CV = std/mean) of AIS transmission intervals in the 6 hours before each gap onset, averaged across all qualifying gaps (> `GAP_THRESHOLD_H`) per vessel.

A CV near 0 means transmissions were machine-like regular — identical intervals, as if a timer was set to broadcast exactly every N seconds before cutting. Legitimate vessels have noisy, irregular transmission intervals driven by satellite coverage, congestion, and transponder behaviour. CV is calculated only for gaps with ≥ 3 pre-gap pings; vessels with no qualifying gaps receive the neutral default of 1.0.

**Shadow fleet signal:** Suspiciously regular transmission (low CV) immediately preceding a long dark period is the hallmark of deliberate AIS compliance: a vessel broadcasting on a strict timer to appear compliant during a monitored strait transit, then switching off once clear.

**Default when missing:** 1.0 (noisy/normal baseline)

---

## IMO Identity Spoofing features

Source: `vessel_meta` DuckDB table (AIS-reported type) joined against `equasis_vessel_ref` (Equasis registered type and scrapped status). Computed by `compute_imo_mismatch_features()` in `src/features/identity.py`. Added in arktrace#544 (2026-05-02).

**Requires:** Equasis reference data loaded via `upsert_equasis_vessel_ref(db_path, equasis_csv)`. When `equasis_vessel_ref` is empty, both features default to `False` for all vessels (graceful degradation).

**Background:** Shadow fleet vessels increasingly adopt IMO numbers from scrapped or fictitious ships to pass ownership graph lookups as clean entities. A vessel broadcasting as a 2018-built chemical tanker (AIS ship_type=68) but registered under an IMO number belonging to a 2005-built VLCC (Equasis vessel_type=80) is a clear identity inconsistency.

### `imo_type_mismatch`

Boolean. `True` when the broad ship-type category reported in AIS messages differs from the category registered for the same IMO number in Equasis.

Broad categories (ITU-R M.1371): fishing (30), service/tug (31–57), passenger (60–69), cargo (70–79), tanker (80–89). Mismatch is only raised when both the AIS and Equasis categories are known (non-zero); vessels with unknown/other types on either side are not flagged.

**Shadow fleet signal:** A vessel cannot physically change type. A tanker hull claiming a cargo IMO, or a fishing vessel broadcasting a tanker IMO, is either operating with a spoofed identity document or has adopted the IMO of a scrapped vessel whose former type is on record.

**Default when missing:** `False`

### `imo_scrapped_flag`

Boolean. `True` when the vessel's reported IMO number appears in the Equasis deleted/scrapped vessel registry (`scrapped=True` in `equasis_vessel_ref`).

**Shadow fleet signal:** Scrapped vessels are removed from active Lloyd's/Equasis registries. An active AIS broadcast using a scrapped IMO is definitive evidence of identity fraud — the vessel either never existed, or the physical vessel was decommissioned and its IMO number reused to create a clean identity.

**Default when missing:** `False`

---

## Build matrix

`src/features/build_matrix.py` merges all four feature families on MMSI using DuckDB JOINs and writes the result to the `vessel_features` table. Missing values are filled with sensible defaults:

| Column | Default when missing |
|---|---|
| `sanctions_distance` | 99 (no graph connection) |
| `cluster_sanctions_ratio` | 0.0 |
| `shared_manager_risk` | 99 |
| `high_risk_flag_ratio` | 0.0 |
| `ownership_depth` | 1 |
| All count features | 0 |

Pass `--skip-graph` to run without loading Lance Graph datasets (graph features default to safe values).
