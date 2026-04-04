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
    echo "1) Full Screening              — ingest AIS + score vessels → ranked candidate watchlist"
    echo "2) Review-Feedback Evaluation  — compute Precision@K and threshold recommendations from analyst decisions"
    echo "3) Historical Backtesting      — validate scoring against known-positive vessels across all regions"
    echo "4) Demo/Smoke                  — load demo watchlist for fast UI and dashboard testing"
    echo "5) Delayed-Label Intelligence  — causal rewind + label propagation from confirmed labels"
    echo "6) Seed Dev Data               — seed watchlist parquet + DuckDB for local testing and backtracking evaluation"
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
