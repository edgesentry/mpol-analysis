# Cost and Scaling

This document addresses the Cap Vista Solicitation 5.0 Challenge 1 "Option Cost" requirement:

> *Cost of scaling the solution*

---

## 1. Software Cost

All components are open-source with no licensing fees.

| Component | Licence | Cost |
|---|---|---|
| DuckDB | MIT | Free |
| Polars | MIT | Free |
| Lance / LanceDB | Apache 2.0 | Free |
| scikit-learn / SHAP | BSD / MIT | Free |
| FastAPI / HTMX | MIT | Free |
| OpenSanctions data | CC0 | Free |
| aisstream.io | Free tier (API key) | Free |
| UN Comtrade+ API | Free (500 req/day) | Free |
| GDELT | Open / free | Free |
| MinIO | AGPL / commercial | Free (self-hosted) |

There are no per-vessel or per-user licensing fees at any scale.

---

## 2. Infrastructure Cost — Screening Layer (Phase A)

The pipeline runs fully in-process with no external database server. DuckDB, LanceDB, and Lance Graph are embedded, serverless libraries. Infrastructure cost scales with AIS data volume and number of monitored regions, not by per-vessel count.

| Deployment scope | Active vessels/month | Storage | Compute | Estimated monthly cost |
|---|---|---|---|---|
| Single strait (e.g. Singapore / Malacca) | ~5k | ~20 GB | 1 CPU core continuous | ~$30–$80 (cloud VM) or $0 (on-prem) |
| Regional (e.g. SE Asia, 3–5 chokepoints) | ~20k | ~80 GB | 2–4 CPU cores | ~$150–$400 (cloud) |
| Global (all 5 major regions) | ~100k | ~400 GB | 4–8 CPU cores or parallel VMs | ~$400–$1,000 (cloud) |

**On-premises deployment** (port operations centre server): hardware cost is a one-off CapEx; ongoing cost is electricity and maintenance only. The Docker Compose stack (`docker compose up`) brings up the full system in two containers. See [docs/deployment.md](deployment.md).

Object storage (MinIO self-hosted or S3-compatible): ~$0.023/GB/month on AWS S3; ~$0 on-prem. At global scale (~400 GB/month active data): < $10/month.

---

## 3. Infrastructure Cost — Edge Deployment (Phase B)

Phase B runs on the patrol vessel or UAV ground station. No internet connectivity is required once the watchlist and databases are synced from S3.

### Hardware (one-off cost per patrol vessel)

| Tier | Hardware | One-off cost |
|---|---|---|
| Tier 1 — Camera + OCR | Hi-res camera (Sony RX100 or GoPro) + GPS logger + ruggedised tablet | ~$1,500 |
| Tier 2 — LiDAR hull scan | Livox Mid-360 + NVIDIA Jetson Orin 8 GB | ~$4,000–$9,000 |
| Tier 3 — Thermal / multispectral | FLIR Boson+ + hyperspectral imager | ~$13,000–$33,000 |

**Recommended starting configuration:** Tier 1 + Tier 2 per patrol vessel (~$8,000–$10,000 total hardware). Tier 3 as optional UAV escalation. See [docs/field-investigation.md](field-investigation.md) for full sensor stack detail.

### Ongoing cost per vessel

- Software: $0 (open-source)
- Connectivity: Iridium SBD (~$0.10/message) for alert-only fallback; primary reporting via VDES (no per-message cost)
- Maintenance: standard IT/hardware maintenance

---

## 4. Cost vs. Commercial Alternatives

Commercial maritime intelligence platforms are priced per deployment and typically do not offer ownership graph analysis, trade flow fusion, or SHAP explainability.

| Solution | Annual licence | Ownership graph | Trade flow | Explainability | Edge-deployable |
|---|---|---|---|---|---|
| **arktrace** | **$0 software** | ✅ | ✅ | ✅ SHAP | ✅ |
| Windward | ~$100k+ | Partial | ✗ | ✗ | ✗ |
| Pole Star | ~$50k+ | ✗ | ✗ | ✗ | ✗ |
| MarineTraffic Enterprise | ~$80k+ | Partial | ✗ | ✗ | ✗ |
| Palantir / commercial C2 | ~$500k+ | ✅ | Partial | Partial | Partial |

arktrace delivers comparable or superior signal at a fraction of the cost because it is built from composable open-source components rather than a monolithic licensed platform.

---

## 5. Scaling Economics

Scaling cost is driven by three factors:

1. **AIS data volume** — linear with number of regions and vessels monitored. DuckDB handles 100M+ row datasets on a single node without cluster infrastructure.
2. **Re-score cycle frequency** — default 15-minute cadence. Reducing to 5 minutes doubles compute; increasing to 1 hour reduces it by 3×. Configurable via `--cadence` flag.
3. **Graph depth** — deeper ownership traversal (more `CONTROLLED_BY` hops) increases LanceGraph query time. Practically bounded at 4–5 hops for the vessels of interest.

Storage growth rate: ~20 GB/month per regional AIS stream. At 5 regions: ~100 GB/month, or ~$2.30/month on S3-standard.

**Multi-region scaling strategy:** one DuckDB file per region (e.g. `data/processed/europe.duckdb`); shared Lance Graph for ownership data (global). Each region's pipeline run is independent and can execute in parallel on separate VMs or cores. See [docs/regional-playbooks.md](regional-playbooks.md) for per-region configuration.

---

## Related Documents

- [docs/deployment.md](deployment.md) — infrastructure setup and Docker Compose configuration
- [docs/field-investigation.md](field-investigation.md) — Phase B hardware tiers and sensor stack
- [docs/trial-specification.md](trial-specification.md) — trial demonstration strategy and platform requirements
- [docs/regional-playbooks.md](regional-playbooks.md) — per-region AIS bbox, DuckDB paths, and weight tuning
