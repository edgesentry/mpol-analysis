# Custom Feed Integration Guide

The Proprietary Fusion Gateway (`pipeline/src/ingest/custom_feeds.py`) ingests any
CSV dropped into `_inputs/custom_feeds/` — no code changes required.  This page
shows ready-to-use templates for the three recommended commercial data providers.

---

## How it works

1. **Drop** a CSV into `_inputs/custom_feeds/`.
2. **Optionally** place a `<stem>.columnmap.json` sidecar alongside it to map
   provider-specific column names to the Arktrace schema.
3. **Run** the pipeline (or `uv run python pipeline/src/ingest/custom_feeds.py`).

The feed type is detected automatically from column signatures and filename prefix.
Files ending with `_sample` are always skipped (smoke-test fixtures only).

---

## Spire Maritime — satellite AIS feed

**Target table:** `ais_positions`  
**Detection:** filename starts with `ais_`; columns `mmsi`, `latitude`, `longitude` auto-detected.

Spire exports use `latitude`/`longitude` instead of `lat`/`lon`.  A columnmap
sidecar handles the rename transparently.

### Sample file: `ais_spire_sample.csv`

```csv
mmsi,timestamp,latitude,longitude,speed_over_ground,course_over_ground,heading,navigational_status,imo,vessel_name,call_sign,ship_type,length,width,draught
123456789,2026-04-01T00:00:00Z,1.2847,103.8610,5.2,127.3,125,0,9876543,GOLDEN STAR,9HSK4,70,185,28,8.2
567891234,2026-04-01T01:30:00Z,1.4201,103.6823,0.1,0.0,511,1,7654321,SEA WOLF,A8KL2,80,220,32,12.5
```

### Sidecar: `ais_spire_sample.columnmap.json`

```json
{
  "lat": "latitude",
  "lon": "longitude",
  "timestamp": "timestamp",
  "mmsi": "mmsi",
  "sog": "speed_over_ground",
  "cog": "course_over_ground",
  "heading": "heading",
  "nav_status": "navigational_status",
  "vessel_name": "vessel_name",
  "imo": "imo",
  "call_sign": "call_sign",
  "vessel_type": "ship_type",
  "length": "length",
  "width": "width",
  "draft": "draught"
}
```

### Live integration steps

1. Configure Spire to deliver files to `_inputs/custom_feeds/` (SFTP push or S3 sync).
2. Name files `ais_spire_YYYYMMDD.csv` (prefix `ais_` ensures detection).
3. Copy `docs/examples/ais_spire_sample.columnmap.json` → `_inputs/custom_feeds/ais_spire_YYYYMMDD.columnmap.json`
   (or use a single shared sidecar with a stable filename and symlink it).
4. Run the pipeline — rows are inserted with `INSERT OR IGNORE` so re-delivery is safe.

---

## ICEYE — persistent SAR detection feed

**Target table:** `sar_detections`  
**Detection:** filename starts with `sar_`.

ICEYE exports use `object_id`, `acquisition_datetime`, `latitude_dd`/`longitude_dd`,
`vessel_length_m`, and `detection_confidence`.  A columnmap sidecar maps these to the
Arktrace SAR schema.

### Sample file: `sar_iceye_sample.csv`

```csv
object_id,acquisition_datetime,latitude_dd,longitude_dd,vessel_length_m,scene_id,detection_confidence
ICEYE-20260401-0001,2026-04-01T02:14:33Z,1.2847,103.8610,87.3,ICEYE-X14_SLC_20260401T021433,0.92
ICEYE-20260401-0002,2026-04-01T02:14:33Z,1.3105,103.7892,112.0,ICEYE-X14_SLC_20260401T021433,0.85
```

### Sidecar: `sar_iceye_sample.columnmap.json`

```json
{
  "detection_id": "object_id",
  "detected_at": "acquisition_datetime",
  "lat": "latitude_dd",
  "lon": "longitude_dd",
  "length_m": "vessel_length_m",
  "source_scene": "scene_id",
  "confidence": "detection_confidence"
}
```

### Live integration steps

1. Configure ICEYE Tasking API webhook or S3 delivery to `_inputs/custom_feeds/`.
2. Name files `sar_iceye_YYYYMMDD.csv`.
3. Copy `docs/examples/sar_iceye_sample.columnmap.json` → `_inputs/custom_feeds/sar_iceye.columnmap.json`
   (a single shared sidecar applies to all date-suffixed ICEYE files).
4. Dark vessels (those without an AIS match) are automatically cross-referenced
   by the `compute_eo_features` pipeline step.

---

## Lloyd's List Intelligence — vessel watchlist feed

**Target table:** `sanctions_entities`  
**Detection:** filename starts with `sanctions_`; columns `name`, `list_source` required.

Lloyd's List Intelligence provides flagged vessel and entity records. Export the
watchlist as a CSV with at minimum `name` and `list_source` columns.  IMO and MMSI
enable automatic cross-referencing with AIS tracks.

### Sample file: `sanctions_lloyds_sample.csv`

```csv
name,list_source,mmsi,imo,flag,type
GOLDEN STAR SHIPPING CO,lloyds-watchlist,123456789,9876543,KHM,vessel
PACIFIC BRIDGE TRADING LTD,lloyds-watchlist,,,PAN,entity
HORIZON MARITIME HOLDINGS,lloyds-watchlist,567891234,1234567,TZA,vessel
```

**Column reference:**

| Column | Required | Description |
|--------|----------|-------------|
| `name` | Yes | Vessel or entity name |
| `list_source` | Yes | Use `lloyds-watchlist` (appears in alert attribution) |
| `mmsi` | Recommended | 9-digit MMSI for AIS cross-reference |
| `imo` | Recommended | IMO number for ownership graph linkage |
| `flag` | Optional | ISO 3166-1 alpha-3 flag state |
| `type` | Optional | `vessel` or `entity` |

### Live integration steps

1. Export the Lloyd's List watchlist from the Intelligence Centre as CSV.
2. Drop the file into `_inputs/custom_feeds/sanctions_lloyds_YYYYMMDD.csv`.
3. No sidecar needed — column names match the schema directly.
4. Run the pipeline; entities are inserted with `INSERT OR IGNORE` (deduplication
   by `entity_id`; auto-generated from name+list_source on first insert).

---

## Schema reference

| Feed type | Required columns | Optional columns | Target table |
|-----------|-----------------|------------------|--------------|
| AIS | `mmsi`, `lat`/`latitude`, `lon`/`longitude`, `timestamp` | `sog`, `cog`, `heading`, `vessel_name`, `imo`, `vessel_type` | `ais_positions` |
| SAR | `lat`, `lon`, `detected_at` | `detection_id`, `length_m`, `source_scene`, `confidence` | `sar_detections` |
| Sanctions | `name`, `list_source` | `mmsi`, `imo`, `flag`, `type` | `sanctions_entities` |
| Cargo | `reporter`, `partner`, `hs_code`, `period` | `trade_value_usd`, `route_key` | `trade_flow` |

## Dry-run verification

```bash
uv run python pipeline/src/ingest/custom_feeds.py --dry-run
```

Prints detected feed type for every file without writing to the database.
Use this to verify column detection before a live pipeline run.
