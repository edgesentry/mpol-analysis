#!/usr/bin/env bash

set -u

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

prompt() {
  local label="$1"
  local default_value="${2-}"
  local value
  if [[ -n "$default_value" ]]; then
    read -r -p "$label [$default_value]: " value
    if [[ -z "$value" ]]; then
      value="$default_value"
    fi
  else
    read -r -p "$label: " value
  fi
  printf '%s' "$value"
}

prompt_yes_no() {
  local label="$1"
  local default_value="${2-false}"
  local marker="y/N"
  local value

  if [[ "$default_value" == "true" ]]; then
    marker="Y/n"
  fi

  read -r -p "$label ($marker): " value
  value="$(tr '[:upper:]' '[:lower:]' <<< "$value")"

  if [[ -z "$value" ]]; then
    [[ "$default_value" == "true" ]] && return 0
    return 1
  fi

  [[ "$value" == "y" || "$value" == "yes" ]]
}

run_cmd() {
  local cmd=("$@")
  echo
  echo "Running command:"
  echo "  ${cmd[*]}"
  echo
  (
    cd "$PROJECT_ROOT" && "${cmd[@]}"
  )
  return $?
}

print_watchlist_summary() {
  local watchlist_path="$1"
  WATCHLIST_PATH="$watchlist_path" uv run python - <<'PY'
import os
from pathlib import Path
import polars as pl

path = Path(os.environ["WATCHLIST_PATH"]).resolve()
if not path.exists():
    print(f"Result: watchlist not found at {path}")
    raise SystemExit(0)

df = pl.read_parquet(path)
print(f"Result: watchlist rows = {df.height}")
if df.height > 0 and {"mmsi", "confidence"}.issubset(set(df.columns)):
    row = df.sort("confidence", descending=True).head(1).to_dicts()[0]
    print(f"Top candidate: mmsi={row.get('mmsi')} confidence={row.get('confidence')}")
print(f"Artifact: {path}")
PY
}

run_full_screening() {
  echo
  echo "[1] Full Screening"

  local region
  region="$(prompt "Region (singapore/japan/middleeast/europe/gulf)" "singapore")"
  region="$(tr '[:upper:]' '[:lower:]' <<< "$region")"

  case "$region" in
    singapore|japan|middleeast|europe|gulf) ;;
    *)
      echo "Unsupported region: $region"
      return
      ;;
  esac

  local stream_duration
  stream_duration="$(prompt "Stream duration seconds (0 to skip)" "0")"

  local seed_dummy="false"
  if prompt_yes_no "Seed dummy vessels" "false"; then
    seed_dummy="true"
  fi

  local cmd=(uv run python scripts/run_pipeline.py --region "$region" --non-interactive)
  if [[ "$stream_duration" =~ ^[0-9]+$ ]] && (( stream_duration > 0 )); then
    cmd+=(--stream-duration "$stream_duration")
  fi
  if [[ "$seed_dummy" == "true" ]]; then
    cmd+=(--seed-dummy)
  fi

  if ! run_cmd "${cmd[@]}"; then
    echo "Result: FAILED"
    return
  fi

  echo "Result: SUCCESS"
  local watchlist
  case "$region" in
    singapore) watchlist="$PROJECT_ROOT/data/processed/singapore_watchlist.parquet" ;;
    japan) watchlist="$PROJECT_ROOT/data/processed/japansea_watchlist.parquet" ;;
    middleeast) watchlist="$PROJECT_ROOT/data/processed/middleeast_watchlist.parquet" ;;
    europe) watchlist="$PROJECT_ROOT/data/processed/europe_watchlist.parquet" ;;
    gulf) watchlist="$PROJECT_ROOT/data/processed/gulf_watchlist.parquet" ;;
  esac
  print_watchlist_summary "$watchlist"
}

run_review_feedback() {
  echo
  echo "[2] Review-Feedback Evaluation"

  local db_path
  db_path="$(prompt "DuckDB path" "data/processed/mpol.duckdb")"
  local output_path
  output_path="$(prompt "Output report path" "data/processed/review_feedback_evaluation.json")"

  if ! run_cmd uv run python scripts/run_review_feedback_evaluation.py --db "$db_path" --output "$output_path"; then
    echo "Result: FAILED"
    return
  fi

  local abs_output="$PROJECT_ROOT/$output_path"
  REPORT_PATH="$abs_output" uv run python - <<'PY'
import json
import os
from pathlib import Path

path = Path(os.environ["REPORT_PATH"]).resolve()
if not path.exists():
    print("Result: SUCCESS, but output report was not found")
    raise SystemExit(0)

report = json.loads(path.read_text())
summary = report.get("summary", {}) if isinstance(report, dict) else {}
print("Result: SUCCESS")
print(
    "Summary: "
    f"reviewed_vessel_count={summary.get('reviewed_vessel_count', 0)}, "
    f"regions_evaluated={summary.get('regions_evaluated', 0)}, "
    f"overall_drift_pass={summary.get('overall_drift_pass', True)}"
)
print(f"Artifact: {path}")
PY
}

run_backtesting_public_batch() {
  echo
  echo "[3] Historical Backtesting + Public Integration Batch"

  local regions
  regions="$(prompt "Regions (comma-separated)" "singapore,japan,middleeast,europe,gulf")"

  local strict_flag=()
  if prompt_yes_no "Enable strict known-case floor" "false"; then
    strict_flag=(--strict-known-cases)
  fi

  if ! run_cmd uv run python scripts/run_public_backtest_batch.py --regions "$regions" "${strict_flag[@]}"; then
    echo "Result: FAILED"
    return
  fi

  local summary_path="$PROJECT_ROOT/data/processed/backtest_public_integration_summary.json"
  local report_path="$PROJECT_ROOT/data/processed/backtest_report_public_integration.json"
  SUMMARY_PATH="$summary_path" REPORT_PATH="$report_path" uv run python - <<'PY'
import json
import os
from pathlib import Path

summary_path = Path(os.environ["SUMMARY_PATH"]).resolve()
report_path = Path(os.environ["REPORT_PATH"]).resolve()
if not summary_path.exists():
    print("Result: SUCCESS, but summary report was not found")
    raise SystemExit(0)

summary = json.loads(summary_path.read_text())
print("Result: SUCCESS")
print(
    "Summary: "
    f"regions={summary.get('regions', [])}, "
    f"total_known_cases={summary.get('total_known_cases', 0)}"
)
print(f"Artifacts: {summary_path}, {report_path}")
PY
}

run_demo_smoke() {
  echo
  echo "[4] Demo/Smoke"

  local cmd=(uv run python scripts/use_demo_watchlist.py)
  if prompt_yes_no "Backup existing candidate_watchlist.parquet" "true"; then
    cmd+=(--backup)
  fi

  if ! run_cmd "${cmd[@]}"; then
    echo "Result: FAILED"
    return
  fi

  echo "Result: SUCCESS"
  print_watchlist_summary "$PROJECT_ROOT/data/processed/candidate_watchlist.parquet"
}

run_backtracking() {
  echo
  echo "[5] Delayed-Label Intelligence (Backtracking)"

  local db_path
  db_path="$(prompt "DuckDB path" "data/processed/mpol.duckdb")"
  local since
  since="$(prompt "Process labels confirmed since (ISO timestamp, leave blank for all)" "")"
  local output_path
  output_path="$(prompt "JSON report output path" "data/processed/backtracking_report.json")"
  local md_output_path
  md_output_path="$(prompt "Markdown summary output path" "data/processed/backtracking_report.md")"

  local cmd=(uv run python scripts/run_backtracking.py
    --db "$db_path"
    --output "$output_path"
    --md-output "$md_output_path"
  )
  if [[ -n "$since" ]]; then
    cmd+=(--since "$since")
  fi

  if ! run_cmd "${cmd[@]}"; then
    echo "Result: FAILED"
    return
  fi

  local abs_output="$PROJECT_ROOT/$output_path"
  REPORT_PATH="$abs_output" uv run python - <<'PY'
import json
import os
from pathlib import Path

path = Path(os.environ["REPORT_PATH"]).resolve()
if not path.exists():
    print("Result: SUCCESS, but report was not found")
    raise SystemExit(0)

report = json.loads(path.read_text())
rc = report.get("regression_checks", {})
status = "PASS" if rc.get("pass") else "FAIL"
print("Result: SUCCESS")
print(
    f"Summary: confirmed={rc.get('confirmed_vessel_count', 0)}, "
    f"rewound={rc.get('rewind_vessel_count', 0)}, "
    f"propagated={rc.get('propagated_entity_count', 0)}, "
    f"regression={status}"
)
print(f"Artifact: {path}")
PY
}

run_prepare_sanctions_db() {
  echo
  echo "[7] Prepare Public Sanctions DB"

  local db_path
  db_path="$(prompt "DuckDB output path" "data/processed/public_eval.duckdb")"

  local force_download_flag=()
  if prompt_yes_no "Force re-download raw data" "false"; then
    force_download_flag=(--force-download)
  fi

  local force_reload_flag=()
  if prompt_yes_no "Force re-reload into DB (even if already loaded)" "false"; then
    force_reload_flag=(--force-reload)
  fi

  if ! run_cmd uv run python scripts/prepare_public_sanctions_db.py \
      --db "$db_path" "${force_download_flag[@]}" "${force_reload_flag[@]}"; then
    echo "Result: FAILED"
    return
  fi

  echo "Result: SUCCESS"
  echo "Artifact: $PROJECT_ROOT/$db_path"
}

run_build_sanctions_demo() {
  echo
  echo "[8] Build Sanctions Demo Sample"

  local source_db
  source_db="$(prompt "Source DuckDB path" "data/processed/public_eval.duckdb")"
  local demo_db
  demo_db="$(prompt "Demo DuckDB output path" "data/demo/public_eval_demo.duckdb")"
  local max_rows
  max_rows="$(prompt "Max rows per entity type" "300")"

  if ! run_cmd uv run python scripts/build_public_sanctions_demo_sample.py \
      --source-db "$source_db" \
      --demo-db "$demo_db" \
      --max-rows "$max_rows"; then
    echo "Result: FAILED"
    return
  fi

  echo "Result: SUCCESS"
  echo "Artifact: $PROJECT_ROOT/$demo_db"
}

run_causal_analysis() {
  echo
  echo "[9] Causal Analysis & Drift Check"

  local db_path
  db_path="$(prompt "DuckDB path" "data/processed/singapore.duckdb")"
  local top_n
  top_n="$(prompt "Top-N unknown-unknown candidates to show" "5")"

  echo
  echo "── Drift Monitor ──────────────────────────────────────────────────────────────"
  if ! run_cmd uv run python src/analysis/monitor.py --db "$db_path" --json \
      2>/dev/null | uv run python - <<PY
import json, sys
data = json.load(sys.stdin)
s = data["summary"]
print(f"Result: ok={s['ok']}  warning={s['warning']}  critical={s['critical']}")
for a in data["alerts"]:
    icon = {"ok": "✓", "warning": "⚠", "critical": "✗"}.get(a["severity"], "?")
    print(f"  {icon} [{a['severity'].upper()}] {a['check_name']}: {a['message']}")
PY
  then
    echo "Result: FAILED (drift monitor)"
    return
  fi

  echo
  echo "── Unknown-Unknown Causal Reasoner ────────────────────────────────────────────"
  DB_PATH="$db_path" TOP_N="$top_n" uv run python - <<'PY'
import os
from src.analysis.causal import score_unknown_unknowns
from src.score.causal_sanction import run_causal_model

db = os.environ["DB_PATH"]
top_n = int(os.environ.get("TOP_N", "5"))

try:
    effects = run_causal_model(db)
    sig = sum(1 for e in effects if e.is_significant)
    print(f"C3 causal effects: {len(effects)} regimes, {sig} significant")
except Exception as exc:
    print(f"C3 model unavailable ({exc}), running without causal evidence")
    effects = []

candidates = score_unknown_unknowns(db_path=db, causal_effects=effects or None)
print(f"Unknown-unknown candidates: {len(candidates)}")
if not candidates:
    print("  (no vessels meet the minimum signal threshold)")
else:
    for c in candidates[:top_n]:
        signals = ", ".join(s.feature for s in c.matching_signals)
        print(f"  mmsi={c.mmsi}  score={c.causal_score:.3f}  signals=[{signals}]")
    if candidates:
        print()
        print("Sample prompt context for top candidate:")
        print(candidates[0].prompt_context())
PY
}

run_seed_dev_data() {
  echo
  echo "[6] Seed Dev Data"

  local db_path
  db_path="$(prompt "DuckDB path to also seed (leave blank to update watchlist parquet only)" "data/processed/mpol.duckdb")"

  local cmd=(uv run python scripts/seed_dev_watchlist.py)
  if [[ -n "$db_path" ]]; then
    cmd+=(--db "$db_path")
  fi

  if ! run_cmd "${cmd[@]}"; then
    echo "Result: FAILED"
    return
  fi

  echo "Result: SUCCESS"
  print_watchlist_summary "$PROJECT_ROOT/data/processed/candidate_watchlist.parquet"
}

main_menu() {
  while true; do
    echo
    echo "=== arktrace Operations Shell ==="
    echo "  Run this shell on a schedule or ad-hoc to operate the full intelligence pipeline."
    echo
    echo "── PRODUCTION ──────────────────────────────────────────────────────────────────"
    echo "1) Full Screening"
    echo "     What: ingest live AIS stream, score all vessels, output ranked candidate watchlist"
    echo "     When: scheduled daily (or on-demand after an AIS feed update)"
    echo "      Who: ops / data engineer"
    echo
    echo "2) Review-Feedback Evaluation"
    echo "     What: compute Precision@K and analyst agreement metrics from confirmed reviews"
    echo "     When: after analysts have reviewed a new batch of vessels (weekly or post-sprint)"
    echo "      Who: data scientist, ML engineer"
    echo
    echo "3) Historical Backtesting"
    echo "     What: replay scoring against known-positive vessels across all regions"
    echo "     When: after a model change or scoring parameter update, before promoting to prod"
    echo "      Who: ML engineer, QA"
    echo
    echo "5) Delayed-Label Intelligence"
    echo "     What: retroactively detect precursor signals for newly confirmed vessels;"
    echo "           propagate risk labels to co-owned/managed/STS-contacted vessels"
    echo "     When: after analysts confirm new vessels (run weekly or post-batch)"
    echo "      Who: data scientist, intelligence analyst"
    echo
    echo "── DEVELOPMENT / LOCAL TESTING ─────────────────────────────────────────────────"
    echo "4) Demo/Smoke"
    echo "     What: load a fixed demo watchlist so the dashboard has realistic data without"
    echo "           running a full pipeline"
    echo "     When: preparing a demo, smoke-testing UI changes, or onboarding a new dev"
    echo "      Who: developer, product"
    echo
    echo "6) Seed Dev Data"
    echo "     What: append dummy vessels to candidate_watchlist.parquet; optionally seed"
    echo "           DuckDB with confirmed reviews + AIS history + ownership graph so the"
    echo "           backtracking loop can be evaluated locally"
    echo "     When: setting up a local dev environment or testing the backtracking loop"
    echo "      Who: developer, data engineer"
    echo
    echo "9) Causal Analysis & Drift Check"
    echo "     What: run drift monitor (data + concept drift) and score unknown-unknown"
    echo "           evasion candidates against a given DB"
    echo "     When: after a pipeline run, or to verify issue #63 acceptance criteria"
    echo "      Who: data scientist, intelligence analyst"
    echo
    echo "── DATA SETUP (run once / when sanctions data is stale) ─────────────────────────"
    echo "7) Prepare Sanctions DB"
    echo "     What: download OpenSanctions dataset and load it into public_eval.duckdb"
    echo "     When: initial setup, or when the sanctions dataset needs refreshing (monthly)"
    echo "      Who: data engineer"
    echo
    echo "8) Build Sanctions Demo Sample"
    echo "     What: slice a small demo DuckDB from the full sanctions DB for fast UI testing"
    echo "     When: after job 7, or when the demo dataset is out of date"
    echo "      Who: developer, product"
    echo
    echo "────────────────────────────────────────────────────────────────────────────────"
    echo "q) Quit"

    local choice
    read -r -p "Select job: " choice
    choice="$(tr '[:upper:]' '[:lower:]' <<< "$choice")"

    case "$choice" in
      1) run_full_screening ;;
      2) run_review_feedback ;;
      3) run_backtesting_public_batch ;;
      4) run_demo_smoke ;;
      5) run_backtracking ;;
      6) run_seed_dev_data ;;
      9) run_causal_analysis ;;
      7) run_prepare_sanctions_db ;;
      8) run_build_sanctions_demo ;;
      q|quit|exit)
        echo "Bye"
        return
        ;;
      *)
        echo "Invalid selection"
        ;;
    esac
  done
}

main_menu
