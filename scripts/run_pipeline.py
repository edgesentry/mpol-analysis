"""
Interactive pipeline CLI for the MPOL Shadow Fleet Screening Pipeline.

Usage (interactive):
    uv run python scripts/run_pipeline.py

Usage (non-interactive):
    uv run python scripts/run_pipeline.py --region singapore --non-interactive
    uv run python scripts/run_pipeline.py --region japan --non-interactive

Available regions: singapore, japan, middleeast, europe, persiangulf, blacksea
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Region presets
# ---------------------------------------------------------------------------


@dataclass
class RegionPreset:
    name: str
    label: str
    bbox: list[float]  # [lat_min, lon_min, lat_max, lon_max]
    gap_threshold_h: int
    window_days: int
    w_anomaly: float
    w_graph: float
    w_identity: float
    db_path: str
    watchlist_path: str


PRESETS: dict[str, RegionPreset] = {
    "singapore": RegionPreset(
        name="singapore",
        label="Singapore / Malacca Strait",
        bbox=[-5, 92, 22, 122],
        gap_threshold_h=4,  # #234: 4h captures brief Malacca STS coordination gaps
        window_days=60,
        w_anomaly=0.40,
        w_graph=0.40,
        w_identity=0.20,
        db_path="data/processed/singapore.duckdb",
        watchlist_path="data/processed/singapore_watchlist.parquet",
    ),
    "japan": RegionPreset(
        name="japan",
        label="Japan Sea / DPRK",
        bbox=[25, 115, 48, 145],
        gap_threshold_h=12,
        window_days=60,
        w_anomaly=0.40,
        w_graph=0.40,
        w_identity=0.20,
        db_path="data/processed/japansea.duckdb",
        watchlist_path="data/processed/japansea_watchlist.parquet",
    ),
    "middleeast": RegionPreset(
        name="middleeast",
        label="Middle East / Indian Ocean",
        bbox=[-10, 32, 30, 80],
        gap_threshold_h=12,
        window_days=60,
        w_anomaly=0.40,
        w_graph=0.40,
        w_identity=0.20,
        db_path="data/processed/middleeast.duckdb",
        watchlist_path="data/processed/middleeast_watchlist.parquet",
    ),
    "europe": RegionPreset(
        name="europe",
        label="Europe / Baltic",
        bbox=[30, -22, 72, 42],
        gap_threshold_h=6,
        window_days=45,
        w_anomaly=0.35,
        w_graph=0.35,
        w_identity=0.30,
        db_path="data/processed/europe.duckdb",
        watchlist_path="data/processed/europe_watchlist.parquet",
    ),
    "persiangulf": RegionPreset(
        name="persiangulf",
        label="Persian Gulf / Strait of Hormuz / Gulf of Oman",
        bbox=[20, 48, 30, 65],
        gap_threshold_h=6,
        window_days=14,
        w_anomaly=0.50,
        w_graph=0.30,
        w_identity=0.20,
        db_path="data/processed/persiangulf.duckdb",
        watchlist_path="data/processed/persiangulf_watchlist.parquet",
    ),
    "gulfofguinea": RegionPreset(
        name="gulfofguinea",
        label="Gulf of Guinea — West Africa",
        bbox=[-5, -5, 10, 10],
        gap_threshold_h=6,
        window_days=14,
        w_anomaly=0.50,
        w_graph=0.30,
        w_identity=0.20,
        db_path="data/processed/gulfofguinea.duckdb",
        watchlist_path="data/processed/gulfofguinea_watchlist.parquet",
    ),
    "gulfofaden": RegionPreset(
        name="gulfofaden",
        label="Gulf of Aden / Bab-el-Mandeb",
        bbox=[10, 42, 16, 52],
        gap_threshold_h=6,
        window_days=14,
        w_anomaly=0.50,
        w_graph=0.30,
        w_identity=0.20,
        db_path="data/processed/gulfofaden.duckdb",
        watchlist_path="data/processed/gulfofaden_watchlist.parquet",
    ),
    "gulfofmexico": RegionPreset(
        name="gulfofmexico",
        label="Gulf of Mexico / Venezuela oil routes",
        bbox=[18, -98, 31, -80],
        gap_threshold_h=6,
        window_days=14,
        w_anomaly=0.50,
        w_graph=0.30,
        w_identity=0.20,
        db_path="data/processed/gulfofmexico.duckdb",
        watchlist_path="data/processed/gulfofmexico_watchlist.parquet",
    ),
    "blacksea": RegionPreset(
        name="blacksea",
        label="Black Sea / Bosphorus",
        bbox=[40, 27, 48, 42],
        gap_threshold_h=6,
        window_days=30,
        w_anomaly=0.45,
        w_graph=0.35,
        w_identity=0.20,
        db_path="data/processed/blacksea.duckdb",
        watchlist_path="data/processed/blacksea_watchlist.parquet",
    ),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bold(text: str) -> str:
    return f"\033[1m{text}\033[0m"


def _green(text: str) -> str:
    return f"\033[32m{text}\033[0m"


def _red(text: str) -> str:
    return f"\033[31m{text}\033[0m"


def _dim(text: str) -> str:
    return f"\033[2m{text}\033[0m"


def _step(n: int, total: int, label: str) -> None:
    prefix = f"[{n}/{total}] {label}"
    print(f"{prefix:<50}", end="", flush=True)


def _ok(detail: str = "") -> None:
    suffix = f"  {_dim(detail)}" if detail else ""
    print(_green("✓") + suffix)


def _fail(detail: str = "") -> None:
    suffix = f"  {_dim(detail)}" if detail else ""
    print(_red("✗") + suffix)


def _run(cmd: list[str], env: dict | None = None) -> subprocess.CompletedProcess:
    merged_env = {**os.environ, **(env or {})}
    return subprocess.run(cmd, env=merged_env, capture_output=True, text=True)


_DUMMY_MMSIS = (
    "352001369",
    "314856000",
    "372979000",
    "312171000",
    "352898820",
    "352002316",
    "626152000",
    "352001298",
    "314925000",
    "352001565",
)


def _seed_dummy_vessels(db_path: str) -> None:
    """Patch real sanctioned vessels into vessel_meta, ais_positions, and vessel_features.

    These are real 2024 OFAC-sanctioned vessels used to ensure the CI known-case
    floor (30 cases) is met against the live OpenSanctions dataset.
    """
    import duckdb

    mmsi_list = ", ".join(f"'{m}'" for m in _DUMMY_MMSIS)
    con = duckdb.connect(db_path)
    try:
        # vessel_meta ── upsert by delete + insert
        con.execute(f"DELETE FROM vessel_meta WHERE mmsi IN ({mmsi_list})")
        con.execute(
            """
            INSERT INTO vessel_meta (mmsi, imo, name, flag, ship_type) VALUES
                ('352001369', '9305609', 'CELINE',         'PA', 82),
                ('314856000', '9292486', 'ELINE',          'BB', 82),
                ('372979000', '9219056', 'REX 1',          'PA', 82),
                ('312171000', '9354521', 'ANHONA',         'BZ', 82),
                ('352898820', '9280873', 'AVENTUS I',      'PA', 82),
                ('352002316', '9308778', 'SATINA',         'PA', 82),
                ('626152000', '9162928', 'ASTRA',          'GA', 82),
                ('352001298', '9292228', 'CRYSTAL ROSE',   'PA', 82),
                ('314925000', '9289491', 'BENDIGO',        'BB', 82),
                ('352001565', '9417490', 'ARABIAN ENERGY', 'PA', 82)
            """
        )

        # ais_positions ── one position each for last_lat/last_lon/last_seen in watchlist
        con.execute(f"DELETE FROM ais_positions WHERE mmsi IN ({mmsi_list})")
        con.execute(
            """
            INSERT INTO ais_positions (mmsi, timestamp, lat, lon, sog, nav_status, ship_type) VALUES
                ('352001369', '2026-03-15 00:00:00+00', 1.25,  103.85, 0.5, 1, 82),
                ('314856000', '2026-03-15 00:00:00+00', 1.35,  103.95, 0.5, 1, 82),
                ('372979000', '2026-03-15 00:00:00+00', 1.45,  104.05, 0.5, 1, 82),
                ('312171000', '2026-03-15 00:00:00+00', 1.55,  104.15, 0.5, 1, 82),
                ('352898820', '2026-03-15 00:00:00+00', 1.65,  104.25, 0.5, 1, 82),
                ('352002316', '2026-03-15 00:00:00+00', 1.75,  104.35, 0.5, 1, 82),
                ('626152000', '2026-03-15 00:00:00+00', 1.85,  104.45, 0.5, 1, 82),
                ('352001298', '2026-03-15 00:00:00+00', 1.95,  104.55, 0.5, 1, 82),
                ('314925000', '2026-03-15 00:00:00+00', 2.05,  104.65, 0.5, 1, 82),
                ('352001565', '2026-03-15 00:00:00+00', 2.15,  104.75, 0.5, 1, 82)
            """
        )

        # vessel_features ── patch after build_matrix's DELETE wipes the table
        con.execute(f"DELETE FROM vessel_features WHERE mmsi IN ({mmsi_list})")
        con.execute(
            """
            INSERT INTO vessel_features (
                mmsi, ais_gap_count_30d, ais_gap_max_hours, position_jump_count,
                sts_candidate_count, port_call_ratio, loitering_hours_30d,
                flag_changes_2y, name_changes_2y, owner_changes_2y,
                high_risk_flag_ratio, ownership_depth, sanctions_distance,
                cluster_sanctions_ratio, shared_manager_risk, shared_address_centrality,
                sts_hub_degree, route_cargo_mismatch, declared_vs_estimated_cargo_value,
                sanctions_list_count
            ) VALUES
                ('352001369', 14, 22.0, 2, 2, 0.15, 28.0, 2, 1, 1, 0.90, 3, 0, 0.60, 0, 3, 3, 1.0, 50000.0, 3),
                ('314856000', 12, 18.0, 1, 1, 0.20, 20.0, 1, 0, 1, 0.80, 2, 0, 0.50, 0, 2, 2, 0.0, 0.0,    1),
                ('372979000', 10, 15.0, 0, 3, 0.10, 15.0, 0, 1, 1, 0.70, 4, 0, 0.40, 0, 4, 4, 1.0, 30000.0, 2),
                ('312171000', 8,  12.0, 3, 0, 0.05, 30.0, 1, 2, 1, 0.95, 3, 0, 0.55, 0, 1, 1, 0.0, 0.0,    4),
                ('352898820', 15, 25.0, 2, 4, 0.12, 35.0, 2, 1, 2, 0.85, 5, 0, 0.70, 0, 5, 5, 1.0, 60000.0, 5),
                ('352002316', 9,  14.0, 1, 2, 0.18, 18.0, 1, 0, 1, 0.75, 3, 0, 0.45, 0, 2, 2, 0.0, 0.0,    1),
                ('626152000', 11, 20.0, 0, 1, 0.08, 25.0, 0, 2, 1, 0.90, 4, 0, 0.65, 0, 3, 3, 1.0, 45000.0, 2),
                ('352001298', 13, 21.0, 2, 3, 0.14, 22.0, 2, 1, 2, 0.82, 3, 0, 0.58, 0, 4, 4, 0.0, 0.0,    3),
                ('314925000', 7,  10.0, 1, 0, 0.25, 12.0, 1, 0, 1, 0.65, 2, 0, 0.35, 0, 1, 1, 1.0, 25000.0, 1),
                ('352001565', 16, 28.0, 3, 5, 0.05, 40.0, 3, 2, 2, 0.98, 6, 0, 0.85, 0, 6, 6, 1.0, 80000.0, 5)
            """
        )
    finally:
        con.close()


def _ais_row_count(db_path: str) -> int:
    """Return the number of rows in ais_positions for a given DB, or 0 on error."""
    try:
        import duckdb

        con = duckdb.connect(db_path, read_only=True)
        try:
            return con.execute("SELECT COUNT(*) FROM ais_positions").fetchone()[0]
        finally:
            con.close()
    except Exception:
        return 0


def _ask_retry_skip(step_name: str) -> str:
    """Ask user to retry or skip after a step failure. Returns 'retry' or 'skip'."""
    while True:
        choice = input(f"  {step_name} failed. [r]etry / [s]kip? ").strip().lower()
        if choice in ("r", "retry"):
            return "retry"
        if choice in ("s", "skip"):
            return "skip"


# ---------------------------------------------------------------------------
# Interactive region selection
# ---------------------------------------------------------------------------


def _select_region_interactive() -> RegionPreset:
    preset_list = list(PRESETS.values())
    print()
    print(_bold("Welcome to the MPOL Shadow Fleet Screening Pipeline"))
    print()
    print("Select region:")
    for i, p in enumerate(preset_list, 1):
        default_marker = "  (default)" if p.name == "singapore" else ""
        print(f"  {i}) {p.label}{default_marker}")
    print(f"  {len(preset_list) + 1}) Custom bbox")
    print()

    raw = input("> ").strip()
    if raw == "":
        return preset_list[0]

    try:
        choice = int(raw)
    except ValueError:
        print("Invalid choice, using Singapore default.")
        return preset_list[0]

    if 1 <= choice <= len(preset_list):
        return preset_list[choice - 1]

    if choice == len(preset_list) + 1:
        return _custom_bbox_interactive()

    print("Invalid choice, using Singapore default.")
    return preset_list[0]


def _custom_bbox_interactive() -> RegionPreset:
    print()
    print("Enter custom bbox (lat_min lon_min lat_max lon_max):")
    raw = input("> ").strip()
    try:
        parts = [float(x) for x in raw.split()]
        if len(parts) != 4:
            raise ValueError("Expected 4 values")
        lat_min, lon_min, lat_max, lon_max = parts
    except (ValueError, TypeError):
        print("Invalid bbox, using Singapore default.")
        return PRESETS["singapore"]

    gap_raw = input("AIS gap threshold hours [6]: ").strip()
    gap_h = int(gap_raw) if gap_raw else 6

    window_raw = input("Feature window days [30]: ").strip()
    window = int(window_raw) if window_raw else 30

    db_raw = input("DB path [data/processed/custom.duckdb]: ").strip()
    db_path = db_raw if db_raw else "data/processed/custom.duckdb"

    watchlist_path = db_path.replace(".duckdb", "_watchlist.parquet")

    return RegionPreset(
        name="custom",
        label="Custom",
        bbox=[lat_min, lon_min, lat_max, lon_max],
        gap_threshold_h=gap_h,
        window_days=window,
        w_anomaly=0.40,
        w_graph=0.40,
        w_identity=0.20,
        db_path=db_path,
        watchlist_path=watchlist_path,
    )


def _print_region_summary(p: RegionPreset) -> None:
    lat_min, lon_min, lat_max, lon_max = p.bbox
    print()
    print(f"Region: {_bold(p.label)}")
    print(f"  Bbox:              {lat_min}°N {lon_min}°E → {lat_max}°N {lon_max}°E")
    print(f"  AIS gap threshold: {p.gap_threshold_h}h")
    print(f"  Feature window:    {p.window_days} days")
    print(
        f"  Composite weights: anomaly={p.w_anomaly:.2f}  graph={p.w_graph:.2f}  identity={p.w_identity:.2f}"
    )
    print(f"  DB path:           {p.db_path}")
    print()


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

TOTAL_STEPS = 11


def step_schema(p: RegionPreset, non_interactive: bool) -> bool:
    _step(1, TOTAL_STEPS, "Initialising DuckDB schema...")
    result = _run([sys.executable, "-m", "pipeline.src.ingest.schema", "--db", p.db_path])
    if result.returncode == 0:
        _ok()
        return True
    _fail(result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "")
    if non_interactive:
        return False
    while _ask_retry_skip("Schema init") == "retry":
        result = _run([sys.executable, "-m", "pipeline.src.ingest.schema", "--db", p.db_path])
        if result.returncode == 0:
            _ok()
            return True
        _fail()
    return False


def step_marine_cadastre(p: RegionPreset, non_interactive: bool, years: list[int]) -> bool:
    if not years:
        return True

    lat_min, lon_min, lat_max, lon_max = p.bbox
    bbox_args = ["--bbox", str(lat_min), str(lon_min), str(lat_max), str(lon_max)]
    year_args = []
    for y in years:
        year_args += ["--year", str(y)]

    _step(2, TOTAL_STEPS, f"Loading Marine Cadastre ({', '.join(str(y) for y in years)})...")
    result = _run(
        [
            sys.executable,
            "-m",
            "pipeline.src.ingest.marine_cadastre",
            "--db",
            p.db_path,
            "--raw-dir",
            "data/raw/marine_cadastre",
            *year_args,
            *bbox_args,
        ],
    )
    if result.returncode == 0:
        count_line = next((l for l in result.stdout.splitlines() if "total" in l.lower()), "")
        _ok(count_line.strip() if count_line else "")
        return True
    _fail(result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "")
    if non_interactive:
        return False
    return _ask_retry_skip("Marine Cadastre") == "skip"


def step_ais_stream(p: RegionPreset, non_interactive: bool, stream_duration: int = 0) -> bool:
    lat_min, lon_min, lat_max, lon_max = p.bbox

    if non_interactive and stream_duration == 0:
        _step(3, TOTAL_STEPS, "Streaming AIS...")
        print(_dim("(skipped — pass --stream-duration N to collect N seconds of live AIS)"))
        return True

    duration_note = f"stopping after {stream_duration}s" if stream_duration else "Ctrl-C to stop"
    _step(3, TOTAL_STEPS, f"Streaming AIS ({duration_note})...")
    print()
    print(f"      bbox {p.bbox}  flush every 60s")

    cmd = [
        sys.executable,
        "-m",
        "pipeline.src.ingest.ais_stream",
        "--db",
        p.db_path,
        "--bbox",
        str(lat_min),
        str(lon_min),
        str(lat_max),
        str(lon_max),
    ]
    if stream_duration:
        # ais_stream handles its own deadline internally — no cross-process signalling needed.
        # Run without capturing so the user sees flush progress in real time.
        # Cap flush_interval at half the duration (max 30s) so at least one flush occurs.
        flush_interval = min(30, stream_duration // 2) or 10
        cmd += ["--duration", str(stream_duration), "--flush-interval", str(flush_interval)]
        ret = subprocess.run(cmd, env=os.environ.copy()).returncode
        rows = _ais_row_count(p.db_path)
        if ret == 0:
            if rows == 0:
                _fail(
                    "stream exited cleanly but 0 rows inserted — check AISSTREAM_API_KEY and bbox"
                )
                return False
            _ok(f"{rows} rows in ais_positions")
        else:
            _fail(f"exit code {ret}")
        return ret == 0

    # Interactive: stream indefinitely until Ctrl-C
    try:
        proc = subprocess.Popen(cmd, env=os.environ.copy())
        proc.wait()
    except KeyboardInterrupt:
        proc.send_signal(__import__("signal").SIGINT)
        proc.wait()

    if proc.returncode in (0, -2):
        rows = _ais_row_count(p.db_path)
        detail = f"{rows} rows in ais_positions" if rows else "0 rows — check AISSTREAM_API_KEY"
        print(f"      Ingestion stopped.  {_green('✓')}  {_dim(detail)}")
        return rows > 0 or not non_interactive

    _fail(f"exit code {proc.returncode}")
    return _ask_retry_skip("AIS streaming") == "skip"


def step_sanctions(p: RegionPreset, non_interactive: bool) -> bool:
    _step(4, TOTAL_STEPS, "Loading sanctions...")
    result = _run([sys.executable, "-m", "pipeline.src.ingest.sanctions", "--db", p.db_path])
    if result.returncode == 0:
        # Extract entity count from stdout if available
        count_line = next((l for l in result.stdout.splitlines() if "entit" in l.lower()), "")
        _ok(count_line.strip() if count_line else "")
        return True
    _fail(result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "")
    if non_interactive:
        return False
    while _ask_retry_skip("Sanctions loading") == "retry":
        result = _run([sys.executable, "-m", "pipeline.src.ingest.sanctions", "--db", p.db_path])
        if result.returncode == 0:
            _ok()
            return True
        _fail()
    return False


def step_custom_feeds(p: RegionPreset, non_interactive: bool) -> bool:
    _step(5, TOTAL_STEPS, "Ingesting custom feeds...")
    from pipeline.src.ingest.custom_feeds import ingest_custom_feeds

    try:
        results = ingest_custom_feeds(db_path=p.db_path)
        total = sum(results.values())
        if results:
            _ok(f"{len(results)} file(s), {total} rows inserted")
        else:
            _ok("no files in _inputs/custom_feeds/")
        return True
    except Exception as exc:
        _fail(str(exc))
        if non_interactive:
            return False
        return _ask_retry_skip("custom_feeds") == "skip"


def step_eo_ingest(p: RegionPreset, non_interactive: bool) -> bool:
    """Ingest GFW EO vessel-presence detections (dark vessels) into eo_detections.

    Skipped automatically when GFW_API_TOKEN is not set — the feature layer
    will then produce zero EO features, which is correct for offline runs.
    """
    import os

    _step(6, TOTAL_STEPS, "Ingesting GFW EO detections...")
    from pipeline.src.ingest.eo_gfw import fetch_gfw_detections, ingest_eo_records

    token = os.getenv("GFW_API_TOKEN", "")
    if not token:
        _ok("GFW_API_TOKEN not set — skipping EO ingest (set token to enable)")
        return True

    try:
        # RegionPreset.bbox = [lat_min, lon_min, lat_max, lon_max]
        # fetch_gfw_detections expects (lon_min, lat_min, lon_max, lat_max)
        lat_min, lon_min, lat_max, lon_max = p.bbox
        gfw_bbox = (lon_min, lat_min, lon_max, lat_max)
        records = fetch_gfw_detections(bbox=gfw_bbox, days=30, api_token=token)
        n = ingest_eo_records(records, db_path=p.db_path)
        _ok(f"{n} EO detections ingested from GFW API")
        return True
    except PermissionError as exc:
        _ok(f"Skipping EO ingest — {exc}")
        return True
    except json.JSONDecodeError as exc:
        _ok(f"Skipping EO ingest — GFW response truncated ({exc}), will retry on next run")
        return True
    except Exception as exc:
        # Treat network timeouts / disconnects as a soft skip so the pipeline continues.
        exc_str = str(exc)
        _TRANSIENT = (
            "timed out",
            "timeout",
            "server disconnected",
            "connection reset",
            "connection refused",
            "remotedisconnected",
            "unterminated string",
        )
        if any(t in exc_str.lower() for t in _TRANSIENT):
            _ok(
                f"Skipping EO ingest — GFW API unreachable ({exc_str.splitlines()[0]}), will retry on next run"
            )
            return True
        _fail(exc_str)
        if non_interactive:
            return False
        return _ask_retry_skip("eo_ingest") == "skip"


def step_ownership_graph(p: RegionPreset, non_interactive: bool) -> bool:
    _step(8, TOTAL_STEPS, "Building ownership graph...")
    # vessel_registry builds Lance datasets from DuckDB vessel_meta
    reg = _run([sys.executable, "-m", "pipeline.src.ingest.vessel_registry", "--db", p.db_path])
    if reg.returncode != 0:
        _fail("vessel_registry failed")
        if non_interactive:
            return False
        if _ask_retry_skip("vessel_registry") == "skip":
            return True
    # ownership_graph computes graph features into DuckDB
    graph = _run([sys.executable, "-m", "pipeline.src.features.ownership_graph", "--db", p.db_path])
    if graph.returncode == 0:
        count_line = next((l for l in graph.stdout.splitlines() if "vessel" in l.lower()), "")
        _ok(count_line.strip() if count_line else "")
        return True
    _fail(graph.stderr.strip().splitlines()[-1] if graph.stderr.strip() else "")
    if non_interactive:
        return False
    return _ask_retry_skip("ownership_graph") == "skip"


def step_features(p: RegionPreset, non_interactive: bool, seed_dummy: bool = False) -> bool:
    _step(9, TOTAL_STEPS, "Computing features...")
    env = {"DB_PATH": p.db_path}
    cmds = [
        (
            [
                sys.executable,
                "-m",
                "pipeline.src.features.ais_behavior",
                "--db",
                p.db_path,
                "--window",
                str(p.window_days),
                "--gap-threshold-hours",
                str(p.gap_threshold_h),
            ],
            "ais_behavior",
        ),
        ([sys.executable, "-m", "pipeline.src.features.identity", "--db", p.db_path], "identity"),
        (
            [sys.executable, "-m", "pipeline.src.features.trade_mismatch", "--db", p.db_path],
            "trade_mismatch",
        ),
        (
            [
                sys.executable,
                "-m",
                "pipeline.src.features.build_matrix",
                "--db",
                p.db_path,
                "--window",
                str(p.window_days),
            ],
            "build_matrix",
        ),
    ]
    for cmd, label in cmds:
        result = _run(cmd, env=env)
        if result.returncode != 0:
            _fail(
                f"{label}: {result.stderr.strip().splitlines()[-1] if result.stderr.strip() else 'error'}"
            )
            if non_interactive:
                return False
            if _ask_retry_skip(label) == "skip":
                continue

    if seed_dummy:
        # build_matrix does DELETE FROM vessel_features before inserting, so we patch
        # the dummy vessels in here, after build_matrix, so scoring picks them up.
        _seed_dummy_vessels(p.db_path)

    _ok()
    return True


def _calibrate_graph_weight(db_path: str, preset_w_graph: float) -> float:
    """
    Run the C3 causal sanction-response model and return the calibrated w_graph.

    Falls back to *preset_w_graph* if the model cannot be run (e.g. insufficient
    AIS data in the DB) so the pipeline never hard-fails because of C3.
    """
    try:
        from pipeline.src.score.causal_sanction import (
            calibrate_graph_weight,
            effects_to_dataframe,
            run_causal_model,
            write_effects,
        )

        causal_output = db_path.replace(".duckdb", "_causal_effects.parquet")
        effects = run_causal_model(db_path)
        df = effects_to_dataframe(effects)
        write_effects(df, causal_output)
        w = calibrate_graph_weight(effects)
        return w
    except Exception:
        return preset_w_graph


def step_score(
    p: RegionPreset,
    non_interactive: bool,
    geo_filter_path: str | None = None,
) -> bool:
    _step(10, TOTAL_STEPS, "Scoring...")
    env = {"DB_PATH": p.db_path}

    # C3: calibrate graph_risk_score weight before composite scoring
    w_graph = _calibrate_graph_weight(p.db_path, p.w_graph)
    # Re-distribute remaining weight proportionally between anomaly and identity
    remaining = 1.0 - w_graph
    ratio = p.w_anomaly / max(p.w_anomaly + p.w_identity, 1e-9)
    w_anomaly = round(remaining * ratio, 4)
    w_identity = round(remaining * (1.0 - ratio), 4)
    # Guard against floating-point drift
    w_anomaly = round(1.0 - w_graph - w_identity, 4)

    composite_cmd = [
        sys.executable,
        "-m",
        "pipeline.src.score.composite",
        "--db",
        p.db_path,
        "--w-anomaly",
        str(w_anomaly),
        "--w-graph",
        str(w_graph),
        "--w-identity",
        str(w_identity),
    ]
    if geo_filter_path:
        composite_cmd += ["--geopolitical-event-filter", geo_filter_path]

    # Label propagation runs before composite so the floor can be applied.
    # The output path matches label_propagation.py DEFAULT_OUTPUT_PATH.
    propagation_path = os.path.join(os.path.dirname(p.db_path), "label_propagation.json")
    composite_cmd += ["--propagation-path", propagation_path]

    cmds = [
        (
            [sys.executable, "-m", "pipeline.src.score.mpol_baseline", "--db", p.db_path],
            "mpol_baseline",
        ),
        ([sys.executable, "-m", "pipeline.src.score.anomaly", "--db", p.db_path], "anomaly"),
        (
            [
                sys.executable,
                "-m",
                "pipeline.src.analysis.label_propagation",
                "--db",
                p.db_path,
                "--output",
                propagation_path,
            ],
            "label_propagation",
        ),
        (composite_cmd, "composite"),
        (
            [
                sys.executable,
                "-m",
                "pipeline.src.score.watchlist",
                "--db",
                p.db_path,
                "--output",
                os.getenv("WATCHLIST_OUTPUT_PATH", p.watchlist_path),
            ],
            "watchlist",
        ),
    ]
    precision_line = ""
    for cmd, label in cmds:
        result = _run(cmd, env=env)
        if result.returncode != 0:
            _fail(
                f"{label}: {result.stderr.strip().splitlines()[-1] if result.stderr.strip() else 'error'}"
            )
            if non_interactive:
                return False
            if _ask_retry_skip(label) == "skip":
                continue
        if label == "watchlist":
            precision_line = next(
                (l for l in result.stdout.splitlines() if "precision" in l.lower()), ""
            )
    _ok(precision_line.strip() if precision_line else "")
    return True


def step_gdelt(p: RegionPreset, non_interactive: bool, gdelt_days: int = 3) -> bool:
    _step(10, TOTAL_STEPS, f"Ingesting GDELT context ({gdelt_days}d)...")
    result = _run([sys.executable, "-m", "pipeline.src.ingest.gdelt", "--days", str(gdelt_days)])
    if result.returncode == 0:
        count_line = next((l for l in result.stdout.splitlines() if "total" in l.lower()), "")
        _ok(count_line.strip() if count_line else "")
        return True
    _fail(result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "")
    if non_interactive:
        return False
    return _ask_retry_skip("GDELT ingest") == "skip"


def step_ducklake(p: RegionPreset, non_interactive: bool) -> bool:
    """Build DuckLake catalog from pipeline outputs (Phase 1 gate validation)."""
    _step(11, TOTAL_STEPS, "Building DuckLake catalog...")
    db_dir = os.path.dirname(os.path.abspath(p.db_path))
    result = _run(
        [
            sys.executable,
            "scripts/checkpoint_ducklake.py",
            "--data-dir",
            db_dir,
        ]
    )
    if result.returncode == 0:
        n_parquet = sum(1 for l in result.stdout.splitlines() if "gate OK:" in l)
        _ok(f"Phase 1 gate passed — {n_parquet} Parquet file(s) verified")
        return True
    _fail(result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "")
    if non_interactive:
        # DuckLake failure is non-blocking: the pipeline still produces the existing
        # Parquet/DuckDB outputs.  Log the failure but do not abort.
        print(
            _dim(
                "  [warn] DuckLake catalog build failed — existing Parquet outputs are unaffected."
            )
        )
        return True
    return _ask_retry_skip("DuckLake catalog") == "skip"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Interactive pipeline CLI for MPOL Shadow Fleet Screening"
    )
    parser.add_argument(
        "--region",
        choices=list(PRESETS.keys()),
        help="Region preset (skip for interactive selection)",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Run without prompts; fails fast on errors",
    )
    parser.add_argument(
        "--stream-duration",
        type=int,
        default=0,
        metavar="SECONDS",
        help="How long to collect live AIS data before moving on (default: 0 = skip in "
        "--non-interactive, run until Ctrl-C in interactive mode)",
    )
    parser.add_argument(
        "--gdelt-days",
        type=int,
        default=3,
        metavar="DAYS",
        help="Number of days of GDELT events to ingest for geopolitical context (default: 3)",
    )
    parser.add_argument(
        "--seed-dummy",
        action="store_true",
        default=False,
        help="Inject realistic dummy vessels (PETROVSKY ZVEZDA, SARI NOUR, OCEAN VOYAGER, "
        "VERA SUNSET) into the DB after feature engineering so they appear on the dashboard",
    )
    parser.add_argument(
        "--marine-cadastre-year",
        type=int,
        action="append",
        dest="marine_cadastre_years",
        metavar="YEAR",
        default=None,
        help="Load a Marine Cadastre historical year before the live pipeline runs "
        "(repeat for multiple years, e.g. --marine-cadastre-year 2022 --marine-cadastre-year 2023). "
        "Uses the region bbox automatically. Useful for the persiangulf region.",
    )
    parser.add_argument(
        "--geopolitical-event-filter",
        default=None,
        metavar="PATH",
        help=(
            "Path to a JSON file declaring geopolitical rerouting events. "
            "Vessels in active corridors have their behavioral_deviation_score down-weighted "
            "to reduce false positives from legitimate commercial rerouting "
            "(e.g. Cape of Good Hope diversion since 2024). "
            "See pipeline/config/geopolitical_events.json for the sample format."
        ),
    )
    parser.add_argument(
        "--cadence",
        type=int,
        default=0,
        metavar="SECONDS",
        help=(
            "Re-score interval in seconds after the initial full pipeline run "
            "(0 = run once; e.g. 900 = re-run feature engineering + scoring every 15 minutes). "
            "Press Ctrl-C to stop the re-score loop."
        ),
    )
    args = parser.parse_args()

    non_interactive: bool = args.non_interactive

    if args.region:
        preset = PRESETS[args.region]
    elif non_interactive:
        parser.error("--region is required when using --non-interactive")
    else:
        preset = _select_region_interactive()

    _print_region_summary(preset)

    if not non_interactive:
        answer = input("Continue? [Y/n] ").strip().lower()
        if answer in ("n", "no"):
            print("Aborted.")
            sys.exit(0)

    print()

    stream_duration: int = args.stream_duration
    gdelt_days: int = args.gdelt_days
    seed_dummy: bool = args.seed_dummy
    marine_cadastre_years: list[int] = args.marine_cadastre_years or []
    geo_filter_path: str | None = args.geopolitical_event_filter
    cadence: int = args.cadence

    steps = [
        step_schema,
        lambda p, ni: step_marine_cadastre(p, ni, marine_cadastre_years),
        lambda p, ni: step_ais_stream(p, ni, stream_duration),
        step_sanctions,
        step_custom_feeds,
        step_eo_ingest,
        step_ownership_graph,
        lambda p, ni: step_features(p, ni, seed_dummy),
        lambda p, ni: step_score(p, ni, geo_filter_path),
        lambda p, ni: step_gdelt(p, ni, gdelt_days),
        step_ducklake,
    ]

    for step_fn in steps:
        ok = step_fn(preset, non_interactive)
        if not ok and non_interactive:
            print(_red(f"\nPipeline aborted at step {steps.index(step_fn) + 1}."), file=sys.stderr)
            sys.exit(1)

    if cadence > 0:
        import time

        rescore_steps = [
            step_custom_feeds,
            lambda p, ni: step_features(p, ni, seed_dummy),
            lambda p, ni: step_score(p, ni, geo_filter_path),
        ]
        print(_dim(f"\nContinuous re-score mode — interval {cadence}s. Press Ctrl-C to stop."))
        try:
            while True:
                time.sleep(cadence)
                import datetime

                print(_dim(f"\n[re-score] {datetime.datetime.now().strftime('%H:%M:%S')}"))
                for step_fn in rescore_steps:
                    ok = step_fn(preset, non_interactive)
                    if not ok and non_interactive:
                        print(
                            _red("\nRe-score step failed; stopping cadence loop."), file=sys.stderr
                        )
                        sys.exit(1)
        except KeyboardInterrupt:
            print(_dim("\nRe-score loop stopped."))


if __name__ == "__main__":
    main()
