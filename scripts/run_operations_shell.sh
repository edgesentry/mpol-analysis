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
  (cd "$PROJECT_ROOT" && uv run python scripts/print_watchlist_summary.py "$watchlist_path")
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
  (cd "$PROJECT_ROOT" && uv run python scripts/print_review_feedback_report.py --report "$abs_output")
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
  (cd "$PROJECT_ROOT" && uv run python scripts/print_backtest_report.py \
      --summary "$summary_path" --report "$report_path")
}

seed_demo_causal_effects() {
  # Auto-detect MinIO running at localhost:9000 and configure S3 vars so
  # write_parquet_uri sends the file to MinIO (where the dashboard reads from)
  # rather than the local filesystem.
  if curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; then
    export S3_BUCKET="${S3_BUCKET:-arktrace}"
    export S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
    export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
    export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
    export AWS_REGION="${AWS_REGION:-us-east-1}"
    echo "  MinIO detected at localhost:9000 — writing to s3://$S3_BUCKET/processed/"
  else
    echo "  MinIO not detected — writing to local data/processed/"
  fi

  (cd "$PROJECT_ROOT" && uv run python scripts/seed_demo_causal_effects.py)
}

run_demo_smoke() {
  echo
  echo "[5] Demo/Smoke"

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

  echo
  echo "── Seeding demo causal_effects.parquet (ATT badge in review panel) ────────────"
  if ! (cd "$PROJECT_ROOT" && seed_demo_causal_effects); then
    echo "Warning: causal effects seeding failed — ATT badge will not appear in dashboard"
  fi

  echo
  echo "── Seeding SAR demo signals (unmatched_sar_detections_30d in SHAP panel) ──────"
  if ! run_cmd uv run python scripts/seed_demo_sar.py; then
    echo "Warning: SAR seeding failed — SAR SHAP screenshot (05_sar_shap.png) will not render"
  fi
}

run_backtracking() {
  echo
  echo "[4] Delayed-Label Intelligence (Backtracking)"

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
  (cd "$PROJECT_ROOT" && uv run python scripts/print_backtracking_report.py --report "$abs_output")
}

run_prepare_sanctions_db() {
  echo
  echo "[9] Prepare Public Sanctions DB"

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
  echo "[10] Build Sanctions Demo Sample"

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

run_prelabel_evaluation() {
  echo
  echo "[8] Pre-Label Holdout Evaluation"

  local watchlist_path
  watchlist_path="$(prompt "Watchlist parquet path" "data/processed/candidate_watchlist.parquet")"

  local source_choice
  echo "  Pre-label source:"
  echo "    1) Demo CSV  (data/demo/analyst_prelabels_demo.csv)"
  echo "    2) DuckDB    (analyst_prelabels table)"
  read -r -p "  Choose [1]: " source_choice
  source_choice="${source_choice:-1}"

  local end_date
  end_date="$(prompt "Leakage cutoff date (ISO-8601, leave blank to include all)" "")"

  local min_tier
  min_tier="$(prompt "Min confidence tier (high/medium/weak)" "medium")"

  local output_path
  output_path="$(prompt "Output report path" "data/processed/prelabel_evaluation.json")"

  local cmd=(uv run python -m src.score.prelabel_evaluation
    --watchlist "$watchlist_path"
    --output "$output_path"
    --min-confidence-tier "$min_tier"
    --review-capacities 25,50,100
  )

  if [[ "$source_choice" == "2" ]]; then
    local db_path
    db_path="$(prompt "DuckDB path" "data/processed/mpol.duckdb")"
    cmd+=(--db "$db_path")
  else
    cmd+=(--prelabels-csv "data/demo/analyst_prelabels_demo.csv")
  fi

  if [[ -n "$end_date" ]]; then
    cmd+=(--end-date "$end_date")
  fi

  if ! run_cmd "${cmd[@]}"; then
    echo "Result: FAILED"
    return
  fi

  local abs_output="$PROJECT_ROOT/$output_path"
  (cd "$PROJECT_ROOT" && uv run python scripts/print_prelabel_report.py --report "$abs_output")
}

run_causal_analysis() {
  echo
  echo "[7] Causal Analysis & Drift Check"

  local db_path
  db_path="$(prompt "DuckDB path" "data/processed/singapore.duckdb")"
  local top_n
  top_n="$(prompt "Top-N unknown-unknown candidates to show" "5")"

  echo
  echo "── Drift Monitor ──────────────────────────────────────────────────────────────"
  if ! (cd "$PROJECT_ROOT" && uv run python src/analysis/monitor.py --db "$db_path" --json \
      2>/dev/null | uv run python scripts/print_monitor_summary.py); then
    echo "Result: FAILED (drift monitor)"
    return
  fi

  echo
  echo "── Unknown-Unknown Causal Reasoner ────────────────────────────────────────────"
  (cd "$PROJECT_ROOT" && run_cmd uv run python scripts/run_causal_reasoner.py \
      --db "$db_path" --top-n "$top_n")
}

run_sar_feature_smoke() {
  echo
  echo "[11] SAR Feature Smoke Test"

  local db_path
  db_path="$(prompt "DuckDB path (will be created fresh, OVERWRITES existing file)" "data/processed/mpol.duckdb")"

  local gap_hours
  gap_hours="$(prompt "AIS gap duration hours" "12")"

  local vessel_lat
  vessel_lat="$(prompt "Vessel last-known lat" "1.0")"

  local vessel_lon
  vessel_lon="$(prompt "Vessel last-known lon" "103.0")"

  (cd "$PROJECT_ROOT" && run_cmd uv run python scripts/smoke_sar_feature.py \
      --db "$db_path" --gap-hours "$gap_hours" --lat "$vessel_lat" --lon "$vessel_lon")
  local rc=$?
  if [[ $rc -ne 0 ]]; then
    echo "Result: FAILED"
    return
  fi
  echo "Artifact: $db_path"

  if ! prompt_yes_no "Run full pipeline + launch dashboard to verify in app" "false"; then
    return
  fi

  echo
  echo "── Building feature matrix ────────────────────────────────────────────────────"
  if ! run_cmd uv run python src/features/build_matrix.py --db "$db_path" --skip-graph; then
    echo "Result: FAILED (build_matrix)"
    return
  fi

  echo
  echo "── Scoring (composite + watchlist) ───────────────────────────────────────────"
  local watchlist_path="$PROJECT_ROOT/data/processed/candidate_watchlist.parquet"
  if ! run_cmd uv run python src/score/composite.py \
      --db "$db_path" \
      --output "$watchlist_path"; then
    echo "Result: FAILED (composite scoring)"
    return
  fi

  print_watchlist_summary "$watchlist_path"
  echo
  echo "── To verify in the dashboard ────────────────────────────────────────────────"
  echo "  1. Start the app:  docker compose up dashboard"
  echo "  2. Open: http://localhost:8000"
  echo "  3. Click vessel 123456789 on the map → detail panel → Signals tab"
  echo "     Look for: 'Unmatched Sar Detections 30D  3 detections'"
  echo "  4. Open: http://localhost:8000/api/vessels/123456789/dispatch-brief"
  echo "     Check 'signals' array for unmatched_sar_detections_30d"
}

run_ingest_eo_csv() {
  echo
  echo "[13] Ingest EO Detections from CSV"

  local csv_path
  csv_path="$(prompt "Path to EO detections CSV" "data/raw/eo_detections_sample.csv")"

  local db_path
  db_path="$(prompt "DuckDB path" "data/processed/mpol.duckdb")"

  if ! run_cmd uv run python src/ingest/eo_gfw.py --csv "$csv_path" --db "$db_path"; then
    echo "Result: FAILED"
    return
  fi

  echo "Result: SUCCESS"

  if ! prompt_yes_no "Run feature matrix + scoring to verify in dashboard" "true"; then
    return
  fi

  echo
  echo "── Building feature matrix ────────────────────────────────────────────────────"
  if ! run_cmd uv run python src/features/build_matrix.py --db "$db_path" --skip-graph; then
    echo "Result: FAILED (build_matrix)"
    return
  fi

  echo
  echo "── Composite scoring + watchlist ──────────────────────────────────────────────"
  local watchlist_path="$PROJECT_ROOT/data/processed/candidate_watchlist.parquet"
  if ! run_cmd uv run python src/score/composite.py \
      --db "$db_path" \
      --output "$watchlist_path"; then
    echo "Result: FAILED (composite scoring)"
    return
  fi

  print_watchlist_summary "$watchlist_path"
  echo
  echo "── To verify in the dashboard ────────────────────────────────────────────────"
  echo "  1. Start the app:  uv run uvicorn src.api.main:app --reload"
  echo "  2. Open: http://localhost:8000 → Review tab → click a vessel"
  echo "     Look for: 'Eo Dark Count 30D'  and  'Eo Ais Mismatch Ratio'"
  echo "  3. API:  curl http://localhost:8000/api/vessels/<mmsi>/signals"
}

run_eo_feature_smoke() {
  echo
  echo "[12] EO Feature Smoke Test"

  local db_path
  db_path="$(prompt "DuckDB path (will be created fresh, OVERWRITES existing file)" "data/processed/mpol.duckdb")"

  local gap_hours
  gap_hours="$(prompt "AIS gap duration hours" "12")"

  local vessel_lat
  vessel_lat="$(prompt "Vessel last-known lat" "1.0")"

  local vessel_lon
  vessel_lon="$(prompt "Vessel last-known lon" "103.0")"

  (cd "$PROJECT_ROOT" && run_cmd uv run python scripts/smoke_eo_feature.py \
      --db "$db_path" --gap-hours "$gap_hours" --lat "$vessel_lat" --lon "$vessel_lon")
  local rc=$?
  if [[ $rc -ne 0 ]]; then
    echo "Result: FAILED"
    return
  fi
  echo "Artifact: $db_path"

  if ! prompt_yes_no "Run full pipeline + launch dashboard to verify in app" "false"; then
    return
  fi

  echo
  echo "── Building feature matrix ────────────────────────────────────────────────────"
  if ! run_cmd uv run python src/features/build_matrix.py --db "$db_path" --skip-graph; then
    echo "Result: FAILED (build_matrix)"
    return
  fi

  echo
  echo "── Scoring (composite + watchlist) ───────────────────────────────────────────"
  local watchlist_path="$PROJECT_ROOT/data/processed/candidate_watchlist.parquet"
  if ! run_cmd uv run python src/score/composite.py \
      --db "$db_path" \
      --output "$watchlist_path"; then
    echo "Result: FAILED (composite scoring)"
    return
  fi

  print_watchlist_summary "$watchlist_path"
  echo
  echo "── To verify in the dashboard ────────────────────────────────────────────────"
  echo "  1. Start the app:  docker compose up dashboard"
  echo "  2. Open: http://localhost:8000 → Review tab → vessel 123456789"
  echo "     Look for: 'Eo Dark Count 30D  2 dark detections'"
  echo "              'Eo Ais Mismatch Ratio  67% dark'"
  echo "  3. API:  curl http://localhost:8000/api/vessels/123456789/signals"
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
    echo "4) Delayed-Label Intelligence"
    echo "     What: retroactively detect precursor signals for newly confirmed vessels;"
    echo "           propagate risk labels to co-owned/managed/STS-contacted vessels"
    echo "     When: after analysts confirm new vessels (run weekly or post-batch)"
    echo "      Who: data scientist, intelligence analyst"
    echo
    echo "── DEVELOPMENT / LOCAL TESTING ─────────────────────────────────────────────────"
    echo "5) Demo/Smoke"
    echo "     What: load a fixed demo watchlist + seed dummy causal_effects.parquet +"
    echo "           inject SAR signals so the dashboard has realistic data without running"
    echo "           a full pipeline (populates watchlist, ATT causal badge, SHAP signals,"
    echo "           SAR dark-vessel detection in SHAP panel)"
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
    echo "7) Causal Analysis & Drift Check"
    echo "     What: run drift monitor (data + concept drift) and score unknown-unknown"
    echo "           evasion candidates against a given DB"
    echo "     When: after a pipeline run, or to verify issue #63 acceptance criteria"
    echo "      Who: data scientist, intelligence analyst"
    echo
    echo "8) Pre-Label Holdout Evaluation"
    echo "     What: evaluate watchlist ranking against analyst-curated pre-labels;"
    echo "           reports leading-indicator precision/recall and disagreement analysis"
    echo "           (model-high vs analyst-cleared; model-low vs analyst-suspected)"
    echo "     When: after a scoring run, or to verify issue #62 acceptance criteria"
    echo "      Who: data scientist, intelligence analyst"
    echo
    echo "── DATA SETUP (run once / when sanctions data is stale) ─────────────────────────"
    echo "9) Prepare Sanctions DB"
    echo "     What: download OpenSanctions dataset and load it into public_eval.duckdb"
    echo "     When: initial setup, or when the sanctions dataset needs refreshing (monthly)"
    echo "      Who: data engineer"
    echo
    echo "10) Build Sanctions Demo Sample"
    echo "     What: slice a small demo DuckDB from the full sanctions DB for fast UI testing"
    echo "     When: after job 9, or when the demo dataset is out of date"
    echo "      Who: developer, product"
    echo
    echo "11) SAR Feature Smoke Test"
    echo "     What: initialise a fresh DuckDB, seed one vessel with an AIS gap + three"
    echo "           unmatched SAR detections nearby, run unmatched_sar_detections_30d,"
    echo "           and verify the attribution count is correct"
    echo "     When: after changing SAR ingestion or feature logic; verifying issue #84"
    echo "      Who: developer, data engineer"
    echo
    echo "12) EO Feature Smoke Test"
    echo "     What: seed one vessel with an AIS gap + 2 dark and 1 matched EO detections,"
    echo "           run eo_dark_count_30d / eo_ais_mismatch_ratio, verify counts"
    echo "     When: after changing EO ingestion or feature logic; verifying issue #119"
    echo "      Who: developer, data engineer"
    echo
    echo "13) Ingest EO Detections from CSV"
    echo "     What: load a local EO detections CSV into eo_detections table (no API token needed)"
    echo "     When: testing EO fusion with sample or real CSV data before GFW API access"
    echo "      Who: developer, data engineer"
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
      4) run_backtracking ;;
      5) run_demo_smoke ;;
      6) run_seed_dev_data ;;
      7) run_causal_analysis ;;
      8) run_prelabel_evaluation ;;
      9) run_prepare_sanctions_db ;;
      10) run_build_sanctions_demo ;;
      11) run_sar_feature_smoke ;;
      12) run_eo_feature_smoke ;;
      13) run_ingest_eo_csv ;;
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
