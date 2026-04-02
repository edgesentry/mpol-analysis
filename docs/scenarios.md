# Scenarios

## Scenario 1 — Duty Officer Morning Brief

### Context

At the start of each watch, a duty officer at the port maritime security centre opens the screening dashboard. They need to know which vessels currently transiting the area of interest warrant closer attention today.

### Workflow

1. **Open dashboard** (`streamlit run src/viz/dashboard.py`)
   - Map shows all vessels in the area of interest, colour-coded by confidence score (green < 0.4, yellow 0.4–0.7, red > 0.7)
   - Ranked table on the right shows top candidates with confidence score and top signals

2. **Filter by area or vessel type**
   - Officer filters to vessels currently within 50nm of the relevant chokepoint (Singapore Strait for Persona 1, Strait of Hormuz for Persona 5, Baltic exits for Persona 4 — see [regional-playbooks.md](regional-playbooks.md))
   - Narrows to tankers and bulk carriers (highest STS transfer risk)

3. **Inspect a candidate**
   - Officer clicks vessel MMSI `123456789` (confidence: 0.83)
   - Top signals: `ais_gap_count_30d = 12 (contrib 0.34)`, `sanctions_distance = 1 (contrib 0.29)`, `flag_changes_2y = 3 (contrib 0.18)`
   - Officer sees: vessel went dark 12 times in the last month, is directly owned by a company one hop from an OFAC-listed entity, and has changed flag 3 times in 2 years
   - Last known position: 1.15°N, 103.6°E (west of Batam Island)

4. **Dispatch decision**
   - Officer notes the vessel is within patrol range and assigns it as Priority 1 for the next patrol sortie
   - Watchlist row is exported as JSON and loaded into edgesentry-app on the patrol vessel

---

## Scenario 2 — Historical Investigation of a Known Incident

### Context

An analyst is investigating a specific STS transfer event that was reported by a satellite imagery provider. They want to identify all vessels involved, trace their ownership, and assess whether any connected vessels are still operating in the area.

### Workflow

1. **Load event data**
   - Analyst defines bounding box (lat/lon) and time window around the reported STS event
   - `src/ingest/ais_stream.py` replays AIS positions for that window from DuckDB

2. **Identify co-located pairs**
   - `src/features/ais_behavior.py` outputs `sts_candidate_count` events for the window
   - Two vessels identified: MMSI `987654321` and `111222333`

3. **Trace ownership graph**
   - Neo4j Cypher query: find all companies connected to both vessels within 3 hops
   - Result: both vessels share a common management company registered in the Marshall Islands, which is in turn a subsidiary of a Dubai holding company that manages 4 other vessels in the watchlist

4. **Score the cluster**
   - All 6 vessels in the ownership cluster are rescored with updated `cluster_sanctions_ratio`
   - 3 of the 6 are promoted into the top-20 candidates

5. **Export for report**
   - Analyst exports the ownership subgraph as PNG (Neo4j Browser) and the candidate rows as CSV
   - This feeds into a formal intelligence report

---

## Scenario 3 — Continuous Monitoring (Streaming)

### Context

The pipeline runs as a persistent process, ingesting live AIS from aisstream.io and re-scoring vessels every 15 minutes. Alerts are generated when a vessel's confidence score crosses 0.75.

### Workflow

1. `src/ingest/ais_stream.py` maintains a WebSocket connection to aisstream.io with a bounding box filter for the 1,600nm area of interest

2. Incoming AIS positions are appended to the DuckDB `ais_positions` table in micro-batches (every 60 seconds)

3. `src/features/ais_behavior.py` re-computes behavioral features for vessels that had AIS activity in the last 15 minutes

4. `src/score/composite.py` re-scores updated vessels and writes to `candidate_watchlist.parquet`

5. If any vessel's confidence score crosses 0.75 (configurable), a Streamlit toast notification is shown and an optional webhook fires (e.g. to a Slack channel or MQTT topic)

6. At the start of each watch (every 6 hours), a full re-score of all vessels in the area runs to catch vessels that were dark and have reappeared

---

## Scenario 4 — Handoff to Physical Investigation

### Context

The screening pipeline has identified a Priority 1 candidate. A patrol vessel is dispatched. The duty officer needs to hand the watchlist entry to the patrol crew.

### Workflow

1. **Export from dashboard**
   - Officer selects the candidate row and clicks "Export for patrol"
   - Output: `patrol_task_MMSI_<timestamp>.json`

   ```json
   {
     "mmsi": "123456789",
     "imo": "9876543",
     "vessel_name": "OCEAN GLORY",
     "confidence": 0.83,
     "top_signals": [...],
     "last_lat": 1.15,
     "last_lon": 103.6,
     "last_seen": "2026-04-15T06:12:00Z",
     "flag": "KH",
     "flag_changes_2y": 3,
     "sanctions_distance": 1
   }
   ```

2. **Load into edgesentry-app** (patrol vessel field PC)
   - The JSON is transferred via VDES downlink or USB to the patrol vessel
   - edgesentry-app displays the target on chart with live AIS overlay

3. **Physical investigation proceeds**
   - See [plan-field-investigation.md](field-investigation.md) for the full workflow implemented in edgesentry-rs / edgesentry-app

4. **Evidence report flows back**
   - Signed `AuditRecord` from edgesentry-audit is received at the shore VDES station
   - The port operations centre dashboard is updated with the investigation result
   - The candidate row in the watchlist is annotated with outcome (`confirmed`, `cleared`, `inconclusive`)
