# Field Investigation — Physical Vessel Measurement, Evidence Capture & Secure Reporting

> **Implementation scope:** This workflow is implemented in [edgesentry-rs](https://github.com/edgesentry/edgesentry-rs) and edgesentry-app — not in this repository. This document is a requirements and design proposal to guide that implementation, and to complete the end-to-end picture for the Cap Vista proposal.

---

## System Overview

```
Port Operations Centre
  ├── candidate_watchlist.parquet  ← from this repo (capvista-mpol-analysis)
  ├── Live AIS feed
  └── VDES shore station
       │  task assignment + watchlist JSON (VDE-TDM ch 26/86)
       ▼
Patrol Vessel / UAV
  ├── Phase 1: Remote Surveillance
  │     ├── AIS live track of target (cross-check vs watchlist)
  │     ├── EO camera (long-range) → vessel identification at distance
  │     └── Decision: proceed to close approach Y/N
  │
  └── Phase 2: Close-Range Investigation
        ├── Sensor stack (tiered — see below)
        ├── Edge PC (edgesentry-app)
        │     ├── AI inference: hull marking OCR / 3D deviation
        │     ├── Evidence bundle (photos, point cloud, detections)
        │     └── edgesentry-audit: Ed25519 sign + BLAKE3 hash chain
        └── VDES report transmission → Port Operations Centre (ASM ch 24/84)
```

---

## Phase 1: Remote Surveillance

### Inputs
- `candidate_watchlist.parquet` — MMSI, last known position, confidence score, top signals
- Live AIS stream (aisstream.io WebSocket on patrol vessel)

### Capabilities Required (edgesentry-app)

| Capability | Detail |
|---|---|
| Watchlist loader | Ingest `candidate_watchlist.parquet`, display ranked candidates on chart |
| AIS cross-check | Overlay live AIS track of target MMSI; flag if broadcast position diverges from predicted track |
| Long-range EO viewer | Display feed from optical camera, capture screenshot with GPS + timestamp |
| Go/no-go decision log | Duty officer records decision with reason; signed as an `edgesentry-audit` event |

### Hardware (remote surveillance tier)

| Item | Approx. cost | Purpose |
|---|---|---|
| Long-range PTZ camera (e.g. Hikvision DS-2DE4425IWG) | ~$400 | Optical ID at 2–5km |
| Ruggedised tablet (Samsung Galaxy Tab Active) | ~$600 | edgesentry-app display |
| GPS receiver (u-blox M9N) | ~$50 | Position tagging |

---

## Phase 2: Close-Range Investigation

### Measurement Tiers

All tiers feed into the same edgesentry-app pipeline and produce the same signed evidence bundle format.

#### Tier 1 — Camera + OCR (~$500 total hardware)

| | |
|---|---|
| **Hardware** | Hi-res camera (Sony RX100 or GoPro Hero) + GPS logger |
| **Software** | OpenCV + Tesseract OCR |
| **Detects** | Vessel identity: IMO number, vessel name, call sign, flag markings |
| **Accuracy** | Sufficient for identity cross-check; no structural measurement |
| **Use case** | First-pass ID; all deployments; minimum viable |

```
Camera frame → OpenCV preprocessing → Tesseract OCR
  → extracted: {imo_number, vessel_name, call_sign}
  → cross-check vs DuckDB vessel registry (Equasis + ITU MMSI)
  → match/mismatch flag → evidence bundle
```

#### Tier 2 — LiDAR + Point Cloud (~$3k–$8k hardware)

| | |
|---|---|
| **Hardware** | Livox Mid-360 (~$1k) or Ouster OS0-32 (~$3k) + NVIDIA Jetson Orin field PC |
| **Software** | `trilink-core` (point cloud projection/deviation) + `edgesentry-inspect` |
| **Detects** | Hull shape deviation vs registry silhouette; structural damage; waterline position (load estimation) |
| **Accuracy** | 5–10mm deviation at 10m range |
| **Use case** | Structural verification; draught/load cross-check; high-priority targets |

```
LiDAR scan → PointCloud
  → trilink-core::project_to_depth_map / height_map
  → AI inference (edgesentry-inspect): surface_void, deformation, hull_marking
  → trilink-core::unproject → world-space detections
  → deviation vs registry hull reference (PLY)
  → heatmap PNG + report JSON → evidence bundle
```

#### Tier 3 — Multi-spectral / Thermal (~$10k–$30k hardware)

| | |
|---|---|
| **Hardware** | FLIR Boson+ thermal camera (~$3k) + hyperspectral imager |
| **Software** | Custom ONNX model; edgesentry-app HTTP inference endpoint |
| **Detects** | Engine heat signature (running vs declared idle); cargo type proxy; night operation |
| **Accuracy** | Qualitative; complements Tier 1/2 |
| **Use case** | High-value interdiction; night ops; UAV payload |

### Cost Summary

| Tier | Hardware cost | Capability | Recommended for |
|---|---|---|---|
| 1 — Camera + OCR | ~$500 | Identity cross-check | All deployments |
| 2 — LiDAR | ~$3k–$8k | Identity + structural | High-priority; patrol boats |
| 3 — Thermal/multispectral | ~$10k–$30k | Identity + cargo + night | High-value; UAV payload |

**Recommended starting point:** Tier 1 + Tier 2 on a standard patrol vessel (~$8k total), Tier 3 as optional UAV escalation.

---

## Evidence Bundle Format

All tiers produce a signed JSON bundle consumed by `edgesentry-audit`:

```json
{
  "mmsi": "123456789",
  "imo": "9876543",
  "vessel_name_observed": "STAR PHOENIX",
  "vessel_name_ais": "OCEAN GLORY",
  "flag_observed": "PA",
  "flag_ais": "KH",
  "position": {"lat": 1.2631, "lon": 103.8200},
  "timestamp_utc": "2026-04-15T08:34:00Z",
  "tier": 2,
  "detections": [
    {"class": "imo_number", "value": "9876543", "confidence": 0.94},
    {"class": "name_mismatch", "value": true}
  ],
  "heatmap_png": "<S3 key>",
  "photos": ["<S3 key 1>", "<S3 key 2>"],
  "inspector_id": "officer-007",
  "watchlist_confidence": 0.87,
  "watchlist_top_signals": [
    {"feature": "ais_gap_count_30d", "value": 12, "contribution": 0.31}
  ]
}
```

This JSON is wrapped in an `edgesentry-audit` `AuditRecord`:
- **Ed25519 signature** — device key in hardware secure element; proves record origin
- **BLAKE3 hash chain** (`prev_record_hash`) — deletion or reordering is cryptographically detectable
- **Sequence number** — strictly monotonic; replay detectable

---

## Secure Reporting: VDES

### Why VDES

VHF Data Exchange System (ITU-R M.2092) operates in the maritime VHF band (156–174 MHz):
- Data rate: ~307 kbps on ASM channels
- Range: ~40nm ship-to-shore; ~200nm via VDES-SAT
- No internet dependency — critical for grey-zone operations
- Integrates with existing AIS infrastructure

### Channel Plan

| Channel | Use |
|---|---|
| ASM 1/2 (ch 24/84) | Evidence bundle transmission (signed JSON + heatmap) — patrol → shore |
| VDE-TDM (ch 26/86) | Watchlist task download — shore → patrol vessel |
| AIS (ch 87/88) | Vessel tracking continuity (standard AIS) |

### Message Flow

```
Patrol Vessel (edgesentry-app)
  1. Complete evidence bundle → edgesentry-audit AuditRecord (Ed25519 signed)
  2. Compress: zstd (JSON) + JPEG (heatmap)
  3. Fragment into VDES ASM frames (max 1023 bytes/frame)
  4. Transmit on ASM ch 24/84 with ARQ (automatic repeat request)
       │
       │  VHF (~40nm) or VDES-SAT (global)
       ▼
Shore Station (VDES transceiver + REST gateway)
  5. Reassemble frames → AuditRecord
  6. edgesentry-audit IngestService::ingest()
       ├── verify Ed25519 signature
       ├── verify BLAKE3 hash chain
       └── reject if out-of-sequence or tampered
  7. Store to S3 Object Lock (WORM) — immutable evidence
  8. Push alert to Port Operations Centre dashboard
```

### Fallback Channels

| Channel | Bandwidth | Range | Use case |
|---|---|---|---|
| VDES ASM | ~307 kbps | ~40nm / global (SAT) | Primary — full evidence bundle |
| Iridium SBD | 340 bytes/msg | Global | Alert only — MMSI + confidence score |
| LoRaWAN | ~250 bps | ~5–15km | Harbour area — minimal alert |
| 4G/5G LTE | Broadband | Port area | Fallback when within cellular range |

---

## edgesentry-rs / edgesentry-app Implementation Requirements

### edgesentry-rs additions needed
- `AuditRecord` serialisation to VDES ASM frame format (fragmentation + reassembly)
- `IngestService` VDES transport adapter (alongside existing MQTT/HTTP)
- Shore-side VDES gateway process (receives ASM frames → REST → ingest)

### edgesentry-app additions needed
- **Watchlist tab**: load `candidate_watchlist.parquet`, display map + ranked list
- **AIS live overlay**: WebSocket AIS client; overlay target track on chart
- **Evidence capture wizard**: guided flow — photo → OCR → LiDAR (if available) → sign → transmit
- **VDES transmit queue**: queue signed bundles, transmit when channel available, retry on failure
- **Tier selection**: config flag for Tier 1 / 2 / 3 sensor stack

### trilink-core additions needed (for Tier 2)
- Hull silhouette reference format (PLY from vessel registry CAD)
- `scan_delta` (CP3, already in trilink-core roadmap) for T0/T1 change detection between inspections
