# Regional Analysis Playbooks

Configuration guidance for running the MPOL screening pipeline across five maritime areas of interest. Each section is written for a specific analyst persona and covers bounding box settings, signal priorities, dashboard filters, and workarounds for scenarios the current scripts do not yet support natively.

**Personas (implemented):** Singapore/Malacca · Japan Sea/DPRK · US Gulf · Europe/Baltic · Middle East/Indian Ocean

## Regional importance ranking

Understanding which regions carry the most weight in global maritime security helps prioritise where to deploy the pipeline first. Ranks 1–5 have full playbooks below. Ranks 6–8 are emerging priorities without dedicated playbooks yet — use the bbox values and signal notes as a starting point with any existing persona as a template.

| Rank | Region | Persona | Key threat | Approx. bbox |
|---|---|---|---|---|
| 1 | **Middle East / Indian Ocean** | Persona 5 | Iranian crude (~1.5–2 mbpd illicit); Houthi Red Sea attacks | `−10 32 30 80` |
| 2 | **Singapore / Malacca Strait** | Persona 1 | Iranian + Russian crude STS blending; 40% of global seaborne trade | `−5 92 22 122` (default) |
| 3 | **Europe (Baltic / Black Sea)** | Persona 4 | Russian shadow fleet (~400–600 vessels); G7 price cap evasion | `30 −22 72 42` |
| 4 | **Japan Sea / East China Sea** | Persona 2 | DPRK coal/fuel UN sanctions violations | `25 115 48 145` |
| 5 | **US Coastal / Gulf of Mexico** | Persona 3 | Venezuelan crude; Caribbean smuggling; OFAC enforcement | `8 −98 32 −60` |
| 6 | **West Africa / Gulf of Guinea** | — | Nigerian crude diversion, illicit bunkering off Bonny/Escravos; significant piracy threat (IMB top-ranked region); Angolan and Gabonese crude mislabelling | `−10 −5 15 15` |
| 7 | **Cape of Good Hope / South Atlantic** | — | Massively increased traffic since 2024 Red Sea rerouting; new STS hub emerging off South Africa (Algoa Bay); limited surveillance coverage creates blind spots | `−40 10 −25 40` |
| 8 | **Arctic / Northern Sea Route** | — | Russia routing LNG and crude via the Northern Sea Route to bypass EU/G7 controls; AIS coverage is sparse above 70°N; growing volume from Yamal and Arctic LNG 2 | `65 20 85 180` |

---

## Persona 1 — Singapore / Malacca Strait Analyst

**Who:** Maritime security analyst at a port authority or regional coast guard (e.g., MPA Singapore, ReCAAP). Monitoring the world's busiest chokepoint for shadow tankers evading Iran, Russia, and Venezuela oil sanctions.

**Primary signals:** AIS gaps during transit, STS transfers at known anchorages (West of Batam, Karimata Strait), high-risk flag states, vessels with direct ownership links to OFAC/EU-listed entities.

### Step-by-step configuration

**A1 — AIS ingestion**

This is the default configuration. No changes needed.

```bash
uv run python src/ingest/ais_stream.py
# default bbox: [[-5.0, 92.0], [22.0, 122.0]] (Malacca + South China Sea approaches)
```

**A3 — Feature engineering**

Use the default 30-day rolling window. The Malacca Strait is a high-frequency transit corridor so 30 days captures enough passages.

```bash
uv run python src/features/ais_behavior.py --window 30
```

**A4 — Composite scoring**

Default weights are tuned for this region (`0.4 × anomaly + 0.4 × graph + 0.2 × identity`). No changes needed.

*Note: when running via `scripts/run_pipeline.py`, the C3 causal model automatically calibrates `w_graph` before Step 8. The value above is the fallback if insufficient AIS data is available.*

**A4 — Bunker barge exclusion (Singapore-specific)**

Singapore waters contain a large population of legitimate service craft: bunker barges (AIS type 51–54), pilot tenders (type 51), and harbour tugs (types 31–32). These vessels loiter at low SOG near anchorages and refuelling points — the same behavioural signature as shadow-fleet STS transfers. Without exclusion, including them in the HDBSCAN training baseline compresses anomaly scores for genuine dark-vessel events.

The exclusion is **on by default**. No configuration needed for Singapore. To verify:

```bash
# Service vessel types excluded from HDBSCAN training (still scored by Isolation Forest):
# 31, 32 (tug/supply), 51-59 (pilot, SAR, fire-fighting, law enforcement, medical)
uv run python src/score/mpol_baseline.py --db data/processed/singapore.duckdb
```

To revert to legacy behaviour (not recommended for Singapore):

```bash
uv run python src/score/mpol_baseline.py --no-exclude-service-vessels
```

**A5 / Dashboard — Filters to apply**

| Filter | Value | Reason |
|---|---|---|
| Vessel type | Tanker | Iranian/Russian crude moves in tankers |
| Minimum confidence | 0.55 | Strait traffic is dense; lower threshold catches more candidates |
| Top N | 100 | High vessel density warrants a wider review list |

Key columns to sort by: `sts_candidate_count`, `sanctions_distance`, `ais_gap_count_30d`.

### Workarounds

**Historical AIS replay (pre-ingestion):** `ais_stream.py` is live-only. To analyse a past incident (e.g., a reported STS event from last month), run a DuckDB query against accumulated `ais_positions` data filtered by timestamp and bbox directly. No script change needed if you have the historical data in the DB.

**Expanding the bbox to cover the Indian Ocean approaches:** Pass `--bbox` to override the default:

```bash
uv run python src/ingest/ais_stream.py --bbox -10 60 25 122
# covers Arabian Sea + Bay of Bengal approaches to Malacca
```

Note: a larger bbox increases WebSocket message volume significantly. Reduce `--flush-interval` to 30s to avoid memory pressure.

---

## Persona 2 — Japan Sea / East China Sea Analyst

**Who:** Analyst at Japan Coast Guard, a UN Panel of Experts on DPRK, or a sanctions intelligence firm monitoring North Korean coal exports and fuel imports in violation of UN Security Council resolutions (UNSCR 2371, 2375, 2397).

**Primary signals:** Vessels going dark near DPRK waters, position jumps (GPS spoofing is endemic near the Korean Peninsula), STS transfers in the East China Sea, ownership links to DPRK-adjacent shell companies, high `cluster_sanctions_ratio` in the Lance Graph (DPRK-connected networks are tightly clustered).

### Step-by-step configuration

**A1 — AIS ingestion**

Override the bbox to cover the Japan Sea, Yellow Sea, and East China Sea:

```bash
uv run python src/ingest/ais_stream.py \
  --bbox 25 115 48 145
# lat 25–48°N, lon 115–145°E
# covers Yellow Sea, Bohai, Japan Sea, East China Sea
```

Use a separate DuckDB file to keep Japan Sea data isolated from the default Singapore DB:

```bash
DB_PATH=data/processed/japansea.duckdb uv run python src/ingest/schema.py
DB_PATH=data/processed/japansea.duckdb uv run python src/ingest/ais_stream.py \
  --bbox 25 115 48 145 --db data/processed/japansea.duckdb
```

**A2 — Sanctions loading**

No changes needed. OpenSanctions already merges OFAC, EU, and UN lists. The UN consolidated list (which targets DPRK entities) is included automatically.

**A3 — Feature engineering**

Increase the AIS behavioral window to 60 days. DPRK-linked vessels make infrequent, long voyages — a 30-day window misses the full gap pattern.

```bash
DB_PATH=data/processed/japansea.duckdb \
  uv run python src/features/ais_behavior.py \
  --db data/processed/japansea.duckdb \
  --window 60
```

Run all other feature scripts with `--db data/processed/japansea.duckdb`.

**A4 — Composite scoring**

The default weight puts `graph_risk_score` at 40%. For DPRK analysis this is correct — ownership graph proximity to UN-listed entities is the strongest signal. No weight change needed.

*Note: when running via `scripts/run_pipeline.py`, the C3 causal model automatically calibrates `w_graph` before Step 8. The value above is the fallback if insufficient AIS data is available.*

**Dashboard — Filters to apply**

| Filter | Value | Reason |
|---|---|---|
| Vessel type | Tanker, Cargo | Coal and petroleum product carriers |
| Minimum confidence | 0.60 | Tighter — DPRK evasion is highly deliberate |
| Top N | 50 | Focus on highest-certainty candidates |

Key columns: `position_jump_count` (GPS spoofing near DPRK), `ais_gap_count_30d` (dark periods in transit), `sanctions_distance` (UN list proximity).

Launch the dashboard pointed at the Japan Sea DB:

```bash
WATCHLIST_OUTPUT_PATH=data/processed/japansea_watchlist.parquet \
  uv run uvicorn src.api.main:app --reload
  # open http://localhost:8000
```

### Workarounds

**No historical AIS for Japan Sea:** aisstream.io is live-only and Marine Cadastre does not cover this region. To backfill:
- Run `ais_stream.py` continuously for several days before scoring.
- Alternatively, AISHub (`www.aishub.net`) offers a free data-sharing programme where members can download historical NMEA data for non-commercial use. Export to CSV and load via `load_csv_to_duckdb()` directly (the function accepts any CSV with the Marine Cadastre column schema).

**Narrowing to DPRK-adjacent waters only:** The full bbox (`25–48°N, 115–145°E`) is large. To focus specifically on the waters around DPRK (Yellow Sea west coast and Japan Sea east coast), run a second ingestion pass:

```bash
uv run python src/ingest/ais_stream.py \
  --bbox 36 124 42 132 --db data/processed/japansea.duckdb
# tight DPRK coastal corridor
```

**AIS gap threshold:** The default gap threshold is 6 hours. In Japan Sea analysis, vessels may go dark for 12–48 hours near DPRK. Pass the flag directly:

```bash
DB_PATH=data/processed/japansea.duckdb \
  uv run python src/features/ais_behavior.py \
  --db data/processed/japansea.duckdb \
  --window 60 \
  --gap-threshold-hours 12
```

---

## Persona 3 — US Coastal / Gulf of Mexico Analyst

**Who:** USCG maritime intelligence officer or OFAC compliance analyst monitoring Venezuelan crude smuggling through the Caribbean, Gulf of Mexico STS operations, and Cuban embargo violations.

**Primary signals:** STS transfers in the Gulf of Mexico (particularly the Yucatan Channel and offshore platforms), vessels transiting between Venezuela and US-adjacent waters, flag-of-convenience vessels with weak port state control history.

### Step-by-step configuration

**A1 — AIS ingestion**

Override the bbox to the Gulf of Mexico and Caribbean:

```bash
uv run python src/ingest/ais_stream.py \
  --bbox 8 -98 32 -60 --db data/processed/gulf.duckdb
# Gulf of Mexico + Caribbean + Venezuelan approaches
```

For US West Coast (e.g., Pacific sanctions enforcement):

```bash
uv run python src/ingest/ais_stream.py \
  --bbox 28 -135 50 -115 --db data/processed/uswest.duckdb
```

**Marine Cadastre for historical backfill (US region only)**

This is the one region where Marine Cadastre is directly useful. Pass `--marine-cadastre-year` to the pipeline, and it runs automatically using the Gulf bounding box:

```bash
PIPELINE_REGION=gulf docker compose run --rm pipeline \
  uv run python scripts/run_pipeline.py \
  --region gulf --non-interactive \
  --marine-cadastre-year 2023

# Multiple years
PIPELINE_REGION=gulf docker compose run --rm pipeline \
  uv run python scripts/run_pipeline.py \
  --region gulf --non-interactive \
  --marine-cadastre-year 2022 --marine-cadastre-year 2023
```

**A3 — Feature engineering**

Use a shorter window for US coastal traffic — vessels transit faster and more frequently:

```bash
DB_PATH=data/processed/gulf.duckdb \
  uv run python src/features/ais_behavior.py \
  --db data/processed/gulf.duckdb \
  --window 14
```

**A4 — Composite scoring**

For US/OFAC analysis, ownership graph proximity is less discriminating (more vessels have OFAC exposure in this region) and behavioral anomaly is more important. Pass weights via CLI flags:

```bash
uv run python src/score/composite.py \
  --db data/processed/gulf.duckdb \
  --w-anomaly 0.50 --w-graph 0.30 --w-identity 0.20
```

*Note: when running via `scripts/run_pipeline.py`, the C3 causal model automatically calibrates `w_graph` before Step 8. The value above is the fallback if insufficient AIS data is available.*

**Dashboard — Filters to apply**

| Filter | Value | Reason |
|---|---|---|
| Vessel type | Tanker, Cargo | Venezuelan crude and refined products |
| Minimum confidence | 0.50 | Broader net for initial screening |
| Top N | 75 | Gulf traffic is dense |

Launch pointed at the Gulf DB:

```bash
WATCHLIST_OUTPUT_PATH=data/processed/gulf_watchlist.parquet \
  uv run uvicorn src.api.main:app --reload
  # open http://localhost:8000
```

### Workarounds

**Marine Cadastre bbox:** The CLI defaults to Singapore. Pass `--bbox lat_min lon_min lat_max lon_max` to override, e.g. `--bbox 8 -98 32 -60` for the Gulf.

**Composite weights:** Pass flags directly to `composite.py`:

```bash
uv run python src/score/composite.py \
  --db data/processed/gulf.duckdb \
  --w-anomaly 0.50 --w-graph 0.30 --w-identity 0.20
```

**Multiple regions simultaneously:** The pipeline uses a single `DB_PATH`. To run all three regions in parallel, use separate `.env` files:

```bash
# Terminal 1 — Singapore
DB_PATH=data/processed/sg.duckdb uv run python src/ingest/ais_stream.py

# Terminal 2 — Japan Sea
DB_PATH=data/processed/japansea.duckdb uv run python src/ingest/ais_stream.py \
  --bbox 25 115 48 145 --db data/processed/japansea.duckdb

# Terminal 3 — Gulf
DB_PATH=data/processed/gulf.duckdb uv run python src/ingest/ais_stream.py \
  --bbox 8 -98 32 -60 --db data/processed/gulf.duckdb
```

Each region gets its own DuckDB file. Run the full feature + scoring pipeline separately for each by passing `--db` to every script.

---

## Persona 4 — European Waters Analyst

**Who:** Analyst at EMSA (European Maritime Safety Agency), a national coast guard (e.g., UK HMCG, Danish Maritime Authority), or an EU sanctions compliance team monitoring Russian crude exports following the February 2022 invasion of Ukraine and the G7 price cap regime.

**Primary signals:** AIS dark periods near Russian Baltic export terminals (Primorsk, Ust-Luga), STS transfers in international waters off the Greek coast or the Strait of Gibraltar, vessels transiting the Bosphorus with suspiciously low declared cargo values, rapid flag changes away from EU/G7 registries, ownership chains routed through UAE or Turkey to obscure Russian beneficial ownership.

**Key sub-regions:**

| Sub-region | Coverage | Primary concern |
|---|---|---|
| Baltic Sea | 54–66°N, 10–30°E | Russian crude exports from Primorsk and Ust-Luga |
| North Sea | 51–62°N, −5–10°E | Re-export via Rotterdam/ARA hub |
| Mediterranean | 30–46°N, −6–36°E | STS off Greece/Malta, Libyan crude |
| Black Sea / Bosphorus | 40–47°N, 26–42°E | Russian Novorossiysk crude transiting Turkish Straits |

### Step-by-step configuration

**A1 — AIS ingestion**

Use a broad European bbox that covers all four sub-regions:

```bash
DB_PATH=data/processed/europe.duckdb uv run python src/ingest/schema.py

uv run python src/ingest/ais_stream.py \
  --bbox 30 -22 72 42 --db data/processed/europe.duckdb
# lat 30–72°N, lon 22°W–42°E
# covers Atlantic approaches, North Sea, Baltic, Mediterranean, Black Sea
```

To focus on the Baltic alone (lower volume, faster iteration):

```bash
uv run python src/ingest/ais_stream.py \
  --bbox 54 10 66 30 --db data/processed/baltic.duckdb
```

**A2 — Sanctions loading**

No changes needed. The EU consolidated sanctions list is already included in OpenSanctions CC0 data loaded by `sanctions.py`. Russian-linked entities sanctioned under EU Regulation 833/2014 will be present.

**A3 — Feature engineering**

Russian Baltic tankers make slow, deliberate voyages with predictable patterns — deviations are highly meaningful. Use a 45-day window to capture the full loading-transit-delivery cycle:

```bash
DB_PATH=data/processed/europe.duckdb \
  uv run python src/features/ais_behavior.py \
  --db data/processed/europe.duckdb \
  --window 45
```

Run all other feature scripts with `--db data/processed/europe.duckdb`.

**A4 — Composite scoring**

For European/Russian sanctions analysis, identity volatility is a very strong signal — the Russian shadow fleet aggressively re-flags and renames vessels. Shift weight toward identity via CLI flags:

```bash
uv run python src/score/composite.py \
  --db data/processed/europe.duckdb \
  --w-anomaly 0.35 --w-graph 0.35 --w-identity 0.30
```

*Note: when running via `scripts/run_pipeline.py`, the C3 causal model automatically calibrates `w_graph` before Step 8. The value above is the fallback if insufficient AIS data is available.*

**Dashboard — Filters to apply**

| Filter | Value | Reason |
|---|---|---|
| Vessel type | Tanker | Russian crude and oil products dominate the shadow fleet here |
| Minimum confidence | 0.55 | European AIS coverage is dense; false positives are lower |
| Top N | 75 | Baltic shadow fleet is estimated at 400–600 vessels globally |

Key columns to review: `flag_changes_2y` (vessels cycling through Palau, Gabon, Cameroon flags), `owner_changes_2y` (rapid ownership restructuring to obscure Russian links), `sts_candidate_count` (Greek anchorage STS operations).

Launch the dashboard pointed at the Europe DB:

```bash
WATCHLIST_OUTPUT_PATH=data/processed/europe_watchlist.parquet \
  uv run uvicorn src.api.main:app --reload
  # open http://localhost:8000
```

### Workarounds

**No historical AIS for European waters:** Marine Cadastre is US-only. For historical backfill:
- **MarineTraffic / VesselFinder:** Both offer historical AIS data exports (paid). Export to CSV with columns matching the Marine Cadastre schema (`MMSI`, `BaseDateTime`, `LAT`, `LON`, `SOG`, `COG`, `VesselType`) and load via `load_csv_to_duckdb()` with a custom bbox:

```python
from src.ingest.marine_cadastre import load_csv_to_duckdb
from pathlib import Path

BALTIC_BBOX = {"lat_min": 54.0, "lat_max": 66.0, "lon_min": 10.0, "lon_max": 30.0}
load_csv_to_duckdb(Path("data/raw/baltic_historical.csv"),
                   db_path="data/processed/europe.duckdb",
                   bbox=BALTIC_BBOX)
```

- **AISHub:** Free data-sharing programme. Members can download historical NMEA logs, convert to CSV, and load the same way.

**Bosphorus / Turkish Straits chokepoint monitoring:** The Bosphorus (41°N, 29°E) is a critical transit point for Black Sea crude. To monitor it specifically, run a focused ingestion stream in parallel:

```bash
uv run python src/ingest/ais_stream.py \
  --bbox 40 26 42 30 --db data/processed/bosphorus.duckdb
# tight bbox around the Turkish Straits
```

Vessels appearing in both the Black Sea and Mediterranean DBs within a plausible transit window (12–24 h) are confirmed Bosphorus transits.

**EU sanctions list filtering:** `sanctions.py` loads all OpenSanctions datasets merged. To understand which entities are EU-specific, query the DB after loading:

```bash
uv run python - <<'EOF'
import duckdb
con = duckdb.connect("data/processed/europe.duckdb", read_only=True)
print(con.execute("""
    SELECT source, COUNT(*) AS n
    FROM sanctions_entities
    GROUP BY source ORDER BY n DESC
""").df())
con.close()
EOF
```

**`high_risk_flag_ratio` for European context:** The identity feature uses a global list of weak port-state-control flags. For Russian shadow fleet analysis, add Gabon (GA), Palau (PW), and Cameroon (CM) to the high-risk flag list if they are not already included — these are the primary re-flagging destinations observed since 2022. Check `src/features/identity.py` for the flag list definition.

---

---

## Persona 5 — Middle East / Indian Ocean Analyst

**Who:** Analyst at the US Fifth Fleet (Bahrain), the Combined Maritime Forces (CMF), the IMO, or a commercial maritime risk firm (e.g., Ambrey, Dryad Global) monitoring Iranian crude exports, Houthi-threatened Red Sea corridors, and Gulf STS operations.

**Why this region ranks #1:** Iran exports an estimated 1.5–2.0 million barrels per day in violation of OFAC sanctions, almost entirely via shadow tankers. The Strait of Hormuz (21°N, 57°E) is the single most critical maritime chokepoint — 20% of global oil flows through it. Since late 2023 the Red Sea has become a kinetic threat zone, forcing rerouting around the Cape of Good Hope and creating new shadow fleet opportunities in the Indian Ocean.

**Primary signals:** Repeated AIS gaps in the Arabian Gulf (loading at Kharg Island or Bandar Abbas without declaring), STS transfers off Fujairah (UAE) and in the Gulf of Oman, position jumps near Hormuz (Iranian GPS jamming is documented), vessels rebranding between Iranian and Malaysian/Chinese flags, ownership chains to IRGC-linked holding companies.

**Key sub-regions:**

| Sub-region | Coverage | Primary concern |
|---|---|---|
| Arabian Gulf | 22–30°N, 48–57°E | Iranian crude loading at Kharg Island, Bandar Abbas |
| Strait of Hormuz | 24–27°N, 55–60°E | Chokepoint transit; spoofing and dark periods |
| Gulf of Oman / Fujairah | 22–27°N, 56–62°E | STS hub for Iranian crude ship-to-ship transfers |
| Red Sea | 12–30°N, 32–44°E | Houthi threat zone; vessels rerouting or declaring false destinations |
| Indian Ocean approaches | −10–22°N, 55–80°E | Iranian crude en route to India, China, and blending hubs |

### Step-by-step configuration

**A1 — AIS ingestion**

Use a broad bbox covering the Arabian Gulf through the Indian Ocean:

```bash
DB_PATH=data/processed/middleeast.duckdb uv run python src/ingest/schema.py

uv run python src/ingest/ais_stream.py \
  --bbox -10 32 30 80 --db data/processed/middleeast.duckdb
# lat 10°S–30°N, lon 32°E–80°E
# covers Red Sea, Arabian Gulf, Gulf of Oman, western Indian Ocean
```

To focus on the Strait of Hormuz and Fujairah STS zone only:

```bash
uv run python src/ingest/ais_stream.py \
  --bbox 22 55 27 62 --db data/processed/hormuz.duckdb
```

For the Red Sea corridor (Houthi threat zone):

```bash
uv run python src/ingest/ais_stream.py \
  --bbox 11 32 30 44 --db data/processed/redsea.duckdb
```

**A2 — Sanctions loading**

No changes needed. OFAC SDN already contains several hundred Iranian entities including IRGC-linked shipping companies, vessels, and owners. OpenSanctions merges these with EU and UN Iran-specific designations.

**A3 — Feature engineering**

Use a 60-day window — Iranian shadow tankers make long round trips (Arabian Gulf → China) of 30–50 days. A 30-day window will cut off the gap evidence mid-voyage.

```bash
DB_PATH=data/processed/middleeast.duckdb \
  uv run python src/features/ais_behavior.py \
  --db data/processed/middleeast.duckdb \
  --window 60
```

Increase the AIS gap threshold to detect Iranian-style dark periods (typically 12–72 h near Kharg Island):

```bash
DB_PATH=data/processed/middleeast.duckdb \
  uv run python src/features/ais_behavior.py \
  --db data/processed/middleeast.duckdb \
  --window 60 \
  --gap-threshold-hours 12
```

Run all other feature scripts with `--db data/processed/middleeast.duckdb`.

**A4 — Composite scoring**

For Iranian crude, all three signal categories are strong. The default weights are appropriate. However, `sanctions_distance` carries outsized predictive power here because Iran operates a tightly-connected network where most vessels are within 2–3 hops of an OFAC-listed entity. No weight change required.

*Note: when running via `scripts/run_pipeline.py`, the C3 causal model automatically calibrates `w_graph` before Step 8. The value above is the fallback if insufficient AIS data is available.*

**Dashboard — Filters to apply**

| Filter | Value | Reason |
|---|---|---|
| Vessel type | Tanker | Iranian crude is the dominant cargo |
| Minimum confidence | 0.60 | High-quality signal environment; set threshold higher to reduce noise |
| Top N | 100 | Iranian shadow fleet is large (~300–400 active vessels) |

Key columns to review: `ais_gap_count_30d` and `ais_gap_max_hours` (dark loading periods), `position_jump_count` (GPS spoofing near Hormuz), `sts_candidate_count` (Fujairah STS), `sanctions_distance` (IRGC network proximity).

Launch the dashboard:

```bash
WATCHLIST_OUTPUT_PATH=data/processed/middleeast_watchlist.parquet \
  uv run uvicorn src.api.main:app --reload
  # open http://localhost:8000
```

### Workarounds

**Iranian GPS jamming creates false position jumps:** The `position_jump_count` feature flags consecutive positions implying speed > 50 knots. Near Hormuz this is often GPS jamming rather than actual spoofing by the vessel. To separate the two, filter by geographic proximity to known jamming zones before acting on this signal — it remains a valid flag but should be weighted lower for Hormuz transits specifically. No automated workaround exists yet; manual review of flagged vessels' last known positions is recommended.

**Red Sea rerouting creates anomalous patterns for legitimate vessels:** Since 2024, many non-shadow-fleet vessels have stopped transiting the Red Sea, creating unusual behavioral patterns (e.g., long Cape of Good Hope detours) that may elevate `anomaly_score` for innocent vessels. To reduce false positives, filter out vessels whose last position is in the Cape of Good Hope corridor (lat −35°–−25°N, lon 15°–35°E) from the final watchlist:

```python
import polars as pl
df = pl.read_parquet("data/processed/middleeast_watchlist.parquet")
# Exclude vessels last seen near Cape of Good Hope
cape_bbox = (df["last_lat"].is_between(-35, -25)) & (df["last_lon"].is_between(15, 35))
df = df.filter(~cape_bbox)
df.write_parquet("data/processed/middleeast_watchlist_filtered.parquet")
```

**No historical AIS for the region:** Marine Cadastre is US-only. For backfill:
- **AISHub** free sharing programme covers Indian Ocean reasonably well.
- **UN Panel of Experts reports** (published annually) name specific vessels — cross-reference their MMSIs against the watchlist as a validation set instead of OFAC alone.

**Running Hormuz and Red Sea as separate focused streams:** Both sub-regions can run as separate ingestion processes with their own DBs while the broad Middle East stream runs in the background:

```bash
# Terminal 1 — broad Middle East
uv run python src/ingest/ais_stream.py --bbox -10 32 30 80 --db data/processed/middleeast.duckdb

# Terminal 2 — Hormuz chokepoint (high resolution)
uv run python src/ingest/ais_stream.py --bbox 22 55 27 62 --db data/processed/hormuz.duckdb

# Terminal 3 — Red Sea threat zone
uv run python src/ingest/ais_stream.py --bbox 11 32 30 44 --db data/processed/redsea.duckdb
```

Score each DB independently and merge the top candidates manually for the daily brief.

---

## Running the pipeline

The recommended entry point is `scripts/run_pipeline.py`, which handles region selection, passes all flags automatically, and walks through each step interactively:

```bash
uv run python scripts/run_pipeline.py                          # interactive
uv run python scripts/run_pipeline.py --region japan --non-interactive
```

## Feature gaps and planned improvements

| Gap | Affected personas | Workaround |
|---|---|---|
| ~~No CLI flag for `GAP_THRESHOLD_H`~~ | ~~Japan Sea, Middle East~~ | **Resolved** — use `--gap-threshold-hours` on `ais_behavior.py` |
| ~~No CLI flags for composite weights~~ | ~~US Gulf, Europe~~ | **Resolved** — use `--w-anomaly`, `--w-graph`, `--w-identity` on `composite.py` |
| Marine Cadastre bbox hardcoded for Singapore | US Gulf, Europe | Call `load_csv_to_duckdb()` with custom `bbox` dict |
| No historical AIS for non-US regions | Japan Sea, Europe, Middle East | Run `ais_stream.py` to accumulate data; or import AISHub/MarineTraffic CSV exports |
| Single `DB_PATH` per pipeline run | All multi-region | Use separate DuckDB files and pass `--db` to every script |
| No sub-region filtering within a bbox | Europe (Bosphorus), Middle East (Hormuz/Red Sea) | Run a second `ais_stream.py` instance with a tighter bbox and separate DB |
| Iranian GPS jamming inflates `position_jump_count` | Middle East | Manual review of Hormuz-area vessels; no automated filter yet |
| Red Sea rerouting creates false anomaly elevation | Middle East | Post-score filter excluding Cape of Good Hope positions (see Persona 5 workaround) |
