"""
Interactive pipeline CLI for the MPOL Shadow Fleet Screening Pipeline.

Usage (interactive):
    uv run python scripts/run_pipeline.py

Usage (non-interactive):
    uv run python scripts/run_pipeline.py --region singapore --non-interactive
    uv run python scripts/run_pipeline.py --region japan --non-interactive

Available regions: singapore, japan, middleeast, europe, gulf
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from typing import Optional

# ---------------------------------------------------------------------------
# Region presets
# ---------------------------------------------------------------------------

@dataclass
class RegionPreset:
    name: str
    label: str
    bbox: list[float]          # [lat_min, lon_min, lat_max, lon_max]
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
        gap_threshold_h=6,
        window_days=30,
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
    "gulf": RegionPreset(
        name="gulf",
        label="US Gulf / Caribbean",
        bbox=[8, -98, 32, -60],
        gap_threshold_h=6,
        window_days=14,
        w_anomaly=0.50,
        w_graph=0.30,
        w_identity=0.20,
        db_path="data/processed/gulf.duckdb",
        watchlist_path="data/processed/gulf_watchlist.parquet",
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


def _run(cmd: list[str], env: Optional[dict] = None) -> subprocess.CompletedProcess:
    merged_env = {**os.environ, **(env or {})}
    return subprocess.run(cmd, env=merged_env, capture_output=True, text=True)


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
    print(f"  Composite weights: anomaly={p.w_anomaly:.2f}  graph={p.w_graph:.2f}  identity={p.w_identity:.2f}")
    print(f"  DB path:           {p.db_path}")
    print()


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

TOTAL_STEPS = 8


def step_schema(p: RegionPreset, non_interactive: bool) -> bool:
    _step(1, TOTAL_STEPS, "Initialising DuckDB schema...")
    result = _run(
        [sys.executable, "-m", "src.ingest.schema", "--db", p.db_path]
    )
    if result.returncode == 0:
        _ok()
        return True
    _fail(result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "")
    if non_interactive:
        return False
    while _ask_retry_skip("Schema init") == "retry":
        result = _run([sys.executable, "-m", "src.ingest.schema", "--db", p.db_path])
        if result.returncode == 0:
            _ok()
            return True
        _fail()
    return False


def step_neo4j(p: RegionPreset, non_interactive: bool) -> bool:
    _step(2, TOTAL_STEPS, "Starting Neo4j...")
    check = _run(["docker", "inspect", "-f", "{{.State.Running}}", "neo4j-mpol"])
    if check.returncode == 0 and check.stdout.strip() == "true":
        _ok("already running")
        return True

    start = _run(["bash", "scripts/start_neo4j.sh"])
    if start.returncode == 0:
        _ok()
        return True

    _fail("could not start Neo4j — is Docker running?")
    if non_interactive:
        return False
    return _ask_retry_skip("Neo4j") == "skip"


def step_ais_stream(p: RegionPreset, non_interactive: bool, stream_duration: int = 0) -> bool:
    import signal as _signal

    lat_min, lon_min, lat_max, lon_max = p.bbox

    if non_interactive and stream_duration == 0:
        _step(3, TOTAL_STEPS, "Streaming AIS...")
        print(_dim("(skipped — pass --stream-duration N to collect N seconds of live AIS)"))
        return True

    duration_note = f"  stopping after {stream_duration}s" if stream_duration else "  Ctrl-C to stop"
    _step(3, TOTAL_STEPS, f"Streaming AIS ({duration_note.strip()})...")
    print()
    print(f"      bbox {p.bbox}  flush every 60s")

    cmd = [
        sys.executable, "-m", "src.ingest.ais_stream",
        "--db", p.db_path,
        "--bbox", str(lat_min), str(lon_min), str(lat_max), str(lon_max),
    ]
    proc = subprocess.Popen(cmd, env=os.environ.copy())
    try:
        proc.wait(timeout=stream_duration if stream_duration else None)
    except subprocess.TimeoutExpired:
        proc.send_signal(_signal.SIGINT)
        proc.wait()
        print(f"      Streaming stopped after {stream_duration}s.  {_green('✓')}")
        return True
    except KeyboardInterrupt:
        proc.send_signal(_signal.SIGINT)
        proc.wait()
        print(f"      ^C  Ingestion stopped.  {_green('✓')}")
        return True

    if proc.returncode == 0 or proc.returncode == -_signal.SIGINT:
        _ok()
        return True

    _fail(f"exit code {proc.returncode}")
    if non_interactive:
        return False
    return _ask_retry_skip("AIS streaming") == "skip"


def step_sanctions(p: RegionPreset, non_interactive: bool) -> bool:
    _step(4, TOTAL_STEPS, "Loading sanctions...")
    result = _run([sys.executable, "-m", "src.ingest.sanctions", "--db", p.db_path])
    if result.returncode == 0:
        # Extract entity count from stdout if available
        count_line = next(
            (l for l in result.stdout.splitlines() if "entit" in l.lower()), ""
        )
        _ok(count_line.strip() if count_line else "")
        return True
    _fail(result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "")
    if non_interactive:
        return False
    while _ask_retry_skip("Sanctions loading") == "retry":
        result = _run([sys.executable, "-m", "src.ingest.sanctions", "--db", p.db_path])
        if result.returncode == 0:
            _ok()
            return True
        _fail()
    return False


def step_ownership_graph(p: RegionPreset, non_interactive: bool) -> bool:
    _step(5, TOTAL_STEPS, "Building ownership graph...")
    # vessel_registry populates Neo4j from DuckDB vessel_meta
    reg = _run([sys.executable, "-m", "src.ingest.vessel_registry", "--db", p.db_path])
    if reg.returncode != 0:
        _fail("vessel_registry failed")
        if non_interactive:
            return False
        if _ask_retry_skip("vessel_registry") == "skip":
            return True
    # ownership_graph computes graph features into DuckDB
    graph = _run([sys.executable, "-m", "src.features.ownership_graph"])
    if graph.returncode == 0:
        count_line = next(
            (l for l in graph.stdout.splitlines() if "vessel" in l.lower()), ""
        )
        _ok(count_line.strip() if count_line else "")
        return True
    _fail(graph.stderr.strip().splitlines()[-1] if graph.stderr.strip() else "")
    if non_interactive:
        return False
    return _ask_retry_skip("ownership_graph") == "skip"


def step_features(p: RegionPreset, non_interactive: bool) -> bool:
    _step(6, TOTAL_STEPS, "Computing features...")
    env = {"DB_PATH": p.db_path}
    cmds = [
        ([sys.executable, "-m", "src.features.ais_behavior",
          "--db", p.db_path,
          "--window", str(p.window_days),
          "--gap-threshold-hours", str(p.gap_threshold_h)], "ais_behavior"),
        ([sys.executable, "-m", "src.features.identity", "--db", p.db_path], "identity"),
        ([sys.executable, "-m", "src.features.trade_mismatch", "--db", p.db_path], "trade_mismatch"),
        ([sys.executable, "-m", "src.features.build_matrix", "--db", p.db_path], "build_matrix"),
    ]
    for cmd, label in cmds:
        result = _run(cmd, env=env)
        if result.returncode != 0:
            _fail(f"{label}: {result.stderr.strip().splitlines()[-1] if result.stderr.strip() else 'error'}")
            if non_interactive:
                return False
            if _ask_retry_skip(label) == "skip":
                continue
    _ok()
    return True


def step_score(p: RegionPreset, non_interactive: bool) -> bool:
    _step(7, TOTAL_STEPS, "Scoring...")
    env = {"DB_PATH": p.db_path}
    cmds = [
        ([sys.executable, "-m", "src.score.mpol_baseline", "--db", p.db_path], "mpol_baseline"),
        ([sys.executable, "-m", "src.score.anomaly", "--db", p.db_path], "anomaly"),
        ([sys.executable, "-m", "src.score.composite",
          "--db", p.db_path,
          "--w-anomaly", str(p.w_anomaly),
          "--w-graph", str(p.w_graph),
          "--w-identity", str(p.w_identity)], "composite"),
        ([sys.executable, "-m", "src.score.watchlist",
          "--db", p.db_path,
          "--output", p.watchlist_path], "watchlist"),
    ]
    precision_line = ""
    for cmd, label in cmds:
        result = _run(cmd, env=env)
        if result.returncode != 0:
            _fail(f"{label}: {result.stderr.strip().splitlines()[-1] if result.stderr.strip() else 'error'}")
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


def step_dashboard(p: RegionPreset, non_interactive: bool) -> bool:
    _step(8, TOTAL_STEPS, "Launching dashboard...")
    if non_interactive:
        print(_dim("(skipped in non-interactive mode)"))
        return True

    env = {"WATCHLIST_OUTPUT_PATH": p.watchlist_path}
    print()
    print("      http://localhost:8501")
    merged_env = {**os.environ, **env}
    try:
        subprocess.run(
            [sys.executable, "-m", "streamlit", "run", "src/viz/dashboard.py"],
            env=merged_env,
        )
    except KeyboardInterrupt:
        pass
    return True


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

    steps = [
        step_schema,
        step_neo4j,
        lambda p, ni: step_ais_stream(p, ni, stream_duration),
        step_sanctions,
        step_ownership_graph,
        step_features,
        step_score,
        step_dashboard,
    ]

    for step_fn in steps:
        ok = step_fn(preset, non_interactive)
        if not ok and non_interactive:
            print(_red(f"\nPipeline aborted at step {steps.index(step_fn) + 1}."), file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
