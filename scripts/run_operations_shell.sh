#!/usr/bin/env bash

set -u

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Auto-detect MinIO running at localhost:9000 and configure S3 vars so
# scripts will write to MinIO (where the dashboard reads from) rather than
# the local filesystem.
if curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; then
  export S3_BUCKET="${S3_BUCKET:-arktrace}"
  export S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
  export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
  export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
  export AWS_REGION="${AWS_REGION:-us-east-1}"
  echo "[info] MinIO detected at localhost:9000 — S3 mode enabled (s3://$S3_BUCKET)"
fi

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

  if ! run_cmd uv run python scripts/run_public_backtest_batch.py --regions "$regions" "${strict_flag[@]+"${strict_flag[@]}"}"; then
    echo "Result: FAILED"
    return
  fi

  local summary_path="$PROJECT_ROOT/data/processed/backtest_public_integration_summary.json"
  local report_path="$PROJECT_ROOT/data/processed/backtest_report_public_integration.json"
  (cd "$PROJECT_ROOT" && uv run python scripts/print_backtest_report.py \
      --summary "$summary_path" --report "$report_path")
}

seed_demo_causal_effects() {
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
      --db "$db_path" "${force_download_flag[@]+"${force_download_flag[@]}"}" "${force_reload_flag[@]+"${force_reload_flag[@]}"}"; then
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
  local geo_filter_arg=()
  if [[ -f "$PROJECT_ROOT/config/geopolitical_events.json" ]]; then
    geo_filter_arg=(--geopolitical-event-filter "$PROJECT_ROOT/config/geopolitical_events.json")
  fi
  if ! run_cmd uv run python src/score/composite.py \
      --db "$db_path" \
      --output "$watchlist_path" \
      "${geo_filter_arg[@]}"; then
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

run_ingest_ais_csv() {
  echo
  echo "[14] Ingest AIS Positions from CSV / NMEA file"

  local db_path
  db_path="$(prompt "DuckDB path" "data/processed/mpol.duckdb")"

  local file_path
  file_path="$(prompt "Path to AIS file (CSV or NMEA)" "")"

  # If no file given, offer to generate a sample from the existing DB
  if [[ -z "$file_path" ]]; then
    local sample_path="$PROJECT_ROOT/data/raw/ais_sample_export.csv"
    echo
    echo "  No file specified."
    if prompt_yes_no "Generate a sample CSV from $db_path to test the ingestion path" "true"; then
      echo "  Exporting 20 rows from ais_positions → $sample_path ..."
      (cd "$PROJECT_ROOT" && uv run python -c "
import duckdb, polars as pl, sys
db = '$db_path'
try:
    con = duckdb.connect(db, read_only=True)
    df = con.execute('''
        SELECT mmsi AS MMSI, timestamp AS BaseDateTime,
               lat AS LAT, lon AS LON,
               sog AS SOG, cog AS COG,
               nav_status AS Status, ship_type AS VesselType
        FROM ais_positions LIMIT 20
    ''').pl()
    con.close()
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    sys.exit(1)
if df.is_empty():
    print('No rows in ais_positions — run Full Screening (job 1) first.', file=sys.stderr)
    sys.exit(1)
df.write_csv('$sample_path')
print(f'Exported {df.height} rows to $sample_path')
") || { echo "Result: FAILED (export)"; return; }
      file_path="$sample_path"
    else
      echo "Aborted — provide a file path to continue."
      return
    fi
  fi

  if [[ ! -f "$PROJECT_ROOT/$file_path" && ! -f "$file_path" ]]; then
    echo "Error: file not found: $file_path"
    return
  fi

  local mode="csv"
  if prompt_yes_no "Parse as NMEA 0183 VDM/VDO sentences (default: CSV)" "false"; then
    mode="nmea"
  fi

  local bbox_str
  bbox_str="$(prompt "Bounding box lat_min lon_min lat_max lon_max (leave blank for no filter)" "")"

  local cmd=(uv run python src/ingest/ais_csv.py --file "$file_path" --db "$db_path")

  if [[ "$mode" == "nmea" ]]; then
    cmd+=(--nmea)
  else
    local col_map
    col_map="$(prompt "Column map overrides key=col,... (leave blank for MarineCadastre defaults)" "")"
    if [[ -n "$col_map" ]]; then
      cmd+=(--column-map "$col_map")
    fi
  fi

  if [[ -n "$bbox_str" ]]; then
    # shellcheck disable=SC2206
    cmd+=(--bbox $bbox_str)
  fi

  if ! run_cmd "${cmd[@]}"; then
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
  local geo_filter_arg=()
  if [[ -f "$PROJECT_ROOT/config/geopolitical_events.json" ]]; then
    geo_filter_arg=(--geopolitical-event-filter "$PROJECT_ROOT/config/geopolitical_events.json")
  fi
  if ! run_cmd uv run python src/score/composite.py \
      --db "$db_path" \
      --output "$watchlist_path" \
      "${geo_filter_arg[@]}"; then
    echo "Result: FAILED (composite scoring)"
    return
  fi

  print_watchlist_summary "$watchlist_path"
  echo
  echo "── To verify in the dashboard ────────────────────────────────────────────────"
  echo "  1. Start the app:  uv run uvicorn src.api.main:app --reload"
  echo "  2. Open: http://localhost:8000 — new vessels from your file appear on the map"
  echo "  3. API:  curl http://localhost:8000/api/vessels"
}

run_precision_verification() {
  echo
  echo "[16] Precision@50 Verification"
  echo
  echo "  Choose a verification level:"
  echo "    1) Quick  — re-score + validate against OFAC labels in existing DB"
  echo "    2) Full   — run backtest with labels manifest (Precision@K, AUROC)"
  echo "    3) Public — full public OpenSanctions integration test (pytest)"
  echo
  local level
  read -r -p "  Choose [1]: " level
  level="${level:-1}"

  case "$level" in
    1)
      echo
      echo "── Option 1: Quick validate ────────────────────────────────────────────────────"
      local db_path
      db_path="$(prompt "DuckDB path" "data/processed/mpol.duckdb")"
      local watchlist_path
      watchlist_path="$(prompt "Watchlist output path" "data/processed/candidate_watchlist.parquet")"

      echo
      echo "── Re-scoring vessels ──────────────────────────────────────────────────────────"
      if ! run_cmd uv run python -m src.score.watchlist --db "$db_path" --output "$watchlist_path"; then
        echo "Result: FAILED (scoring)"
        return
      fi

      echo
      echo "── Computing validation metrics ────────────────────────────────────────────────"
      local metrics_path
      metrics_path="$(prompt "Metrics output path" "data/processed/validation_metrics.json")"
      if ! run_cmd uv run python -m src.score.validate \
          --db "$db_path" \
          --watchlist "$watchlist_path" \
          --output "$metrics_path"; then
        echo "Result: FAILED (validate)"
        return
      fi

      echo
      echo "── Results ─────────────────────────────────────────────────────────────────────"
      (cd "$PROJECT_ROOT" && uv run python -c "
import json, sys
with open('$metrics_path') as f:
    m = json.load(f)
p50 = m.get('precision_at_50', 'n/a')
r200 = m.get('recall_at_200', 'n/a')
auroc = m.get('auroc', 'n/a')
total = m.get('candidate_count', 'n/a')
pos = m.get('positive_count', 'n/a')
target = 0.68
status = '✅ PASS' if isinstance(p50, float) and p50 >= target else '❌ BELOW TARGET'
print(f'  Precision@50  : {p50:.3f}  (target ≥ {target})  {status}')
print(f'  Recall@200    : {r200:.3f}' if isinstance(r200, float) else f'  Recall@200    : {r200}')
print(f'  AUROC         : {auroc:.3f}' if isinstance(auroc, float) else f'  AUROC         : {auroc}')
print(f'  Candidates    : {total}  (positives: {pos})')
")
      ;;

    2)
      echo
      echo "── Option 2: Full backtest ──────────────────────────────────────────────────────"
      local manifest_path
      manifest_path="$(prompt "Evaluation manifest JSON" "data/processed/evaluation_manifest.json")"
      local report_path
      report_path="$(prompt "Report output path" "data/processed/backtest_report.json")"

      if [[ ! -f "$PROJECT_ROOT/$manifest_path" ]]; then
        echo "Error: manifest not found: $manifest_path"
        echo "Tip: run job 3 (Historical Backtesting) first to generate an evaluation manifest."
        return
      fi

      if ! run_cmd uv run python -m src.score.backtest \
          --manifest "$manifest_path" \
          --output "$report_path" \
          --k 25 50 100; then
        echo "Result: FAILED"
        return
      fi

      echo
      echo "── Results ─────────────────────────────────────────────────────────────────────"
      (cd "$PROJECT_ROOT" && uv run python -c "
import json
with open('$report_path') as f:
    r = json.load(f)
s = r.get('summary', {})
p50 = s.get('precision_at_50', {}).get('mean')
auroc = s.get('auroc', {}).get('mean')
target = 0.68
status = '✅ PASS' if isinstance(p50, float) and p50 >= target else '❌ BELOW TARGET'
print(f'  Precision@50 (mean): {p50:.3f}  (target ≥ {target})  {status}' if isinstance(p50, float) else f'  Precision@50: {p50}')
print(f'  AUROC        (mean): {auroc:.3f}' if isinstance(auroc, float) else f'  AUROC: {auroc}')
print(f'  Windows            : {s.get(\"window_count\", \"n/a\")}')
")
      ;;

    3)
      echo
      echo "── Option 3: Public OpenSanctions integration test ──────────────────────────────"
      local watchlist_path
      watchlist_path="$(prompt "Watchlist parquet path" "data/processed/candidate_watchlist.parquet")"
      local sanctions_db
      sanctions_db="$(prompt "Public sanctions DuckDB path" "data/processed/public_eval.duckdb")"

      local prepare_flag=""
      if [[ ! -f "$PROJECT_ROOT/$sanctions_db" ]]; then
        echo "Warning: $sanctions_db not found."
        if prompt_yes_no "Auto-prepare sanctions DB now (downloads ~150 MB)" "true"; then
          prepare_flag="1"
        else
          echo "Aborted — run job 9 (Prepare Sanctions DB) first."
          return
        fi
      fi

      echo
      echo "── Summary (Precision@50 quick check) ──────────────────────────────────────────"
      local tmp_metrics
      tmp_metrics="$(mktemp /tmp/arktrace_metrics_XXXXXX.json)"
      if (cd "$PROJECT_ROOT" && uv run python -m src.score.validate \
            --db "data/processed/mpol.duckdb" \
            --watchlist "$watchlist_path" \
            --output "$tmp_metrics") >/dev/null 2>&1; then
        (cd "$PROJECT_ROOT" && uv run python -c "
import json
with open('$tmp_metrics') as f:
    m = json.load(f)
p50   = m.get('precision_at_50', 'n/a')
r200  = m.get('recall_at_200', 'n/a')
auroc = m.get('auroc', 'n/a')
total = m.get('candidate_count', 'n/a')
pos   = m.get('positive_count', 'n/a')
target = 0.68
status = '✅ PASS' if isinstance(p50, float) and p50 >= target else '❌ BELOW TARGET'
print(f'  Precision@50  : {p50:.3f}  (target ≥ {target})  {status}' if isinstance(p50, float) else f'  Precision@50 : {p50}')
print(f'  Recall@200    : {r200:.3f}' if isinstance(r200, float) else f'  Recall@200   : {r200}')
print(f'  AUROC         : {auroc:.3f}' if isinstance(auroc, float) else f'  AUROC        : {auroc}')
print(f'  Candidates    : {total}  (OFAC positives: {pos})')
if pos == 0:
    print()
    print('  ⚠️  No OFAC positives found — watchlist is likely demo/synthetic data.')
    print('     Run job 1 (Full Screening) with real AIS + sanctions data for a meaningful score.')
")
      else
        echo "  (skipped — DB not available or validate failed)"
      fi
      rm -f "$tmp_metrics"

      echo
      echo "── Full integration test (pytest) ──────────────────────────────────────────────"
      if ! (cd "$PROJECT_ROOT" && env RUN_PUBLIC_DATA_TESTS=1 \
            PUBLIC_TEST_WATCHLIST="$watchlist_path" \
            PUBLIC_SANCTIONS_DB="$sanctions_db" \
            PREPARE_PUBLIC_DATA_IF_MISSING="${prepare_flag:-0}" \
            uv run pytest tests/test_public_data_backtest_integration.py -v -s); then
        echo "Result: FAILED"
        return
      fi

      echo "Result: SUCCESS"
      ;;

    *)
      echo "Invalid selection"
      ;;
  esac
}

run_ingest_custom_feeds() {
  echo
  echo "[15] Ingest custom feed drop-ins (_inputs/custom_feeds/)"

  local feeds_dir
  feeds_dir="$(prompt "Feeds directory" "_inputs/custom_feeds")"

  local db_path
  db_path="$(prompt "DuckDB path" "data/processed/mpol.duckdb")"

  local dry_run_flag=""
  if prompt_yes_no "Dry-run (detect feed types without inserting rows)" "false"; then
    dry_run_flag="--dry-run"
  fi

  local cmd=(uv run python src/ingest/custom_feeds.py --dir "$feeds_dir" --db "$db_path")
  if [[ -n "$dry_run_flag" ]]; then
    cmd+=("$dry_run_flag")
  fi

  if ! run_cmd "${cmd[@]}"; then
    echo "Result: FAILED"
    return
  fi

  echo "Result: SUCCESS"

  if [[ -n "$dry_run_flag" ]]; then
    return
  fi

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
  local geo_filter_arg=()
  if [[ -f "$PROJECT_ROOT/config/geopolitical_events.json" ]]; then
    geo_filter_arg=(--geopolitical-event-filter "$PROJECT_ROOT/config/geopolitical_events.json")
  fi
  if ! run_cmd uv run python src/score/composite.py \
      --db "$db_path" \
      --output "$watchlist_path" \
      "${geo_filter_arg[@]}"; then
    echo "Result: FAILED (composite scoring)"
    return
  fi

  print_watchlist_summary "$watchlist_path"
  echo
  echo "── To verify in the dashboard ────────────────────────────────────────────────"
  echo "  1. Start the app:  uv run uvicorn src.api.main:app --reload"
  echo "  2. Open: http://localhost:8000"
  echo "  3. API:  curl http://localhost:8000/api/vessels"
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
  local geo_filter_arg=()
  if [[ -f "$PROJECT_ROOT/config/geopolitical_events.json" ]]; then
    geo_filter_arg=(--geopolitical-event-filter "$PROJECT_ROOT/config/geopolitical_events.json")
  fi
  if ! run_cmd uv run python src/score/composite.py \
      --db "$db_path" \
      --output "$watchlist_path" \
      "${geo_filter_arg[@]}"; then
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
  local geo_filter_arg=()
  if [[ -f "$PROJECT_ROOT/config/geopolitical_events.json" ]]; then
    geo_filter_arg=(--geopolitical-event-filter "$PROJECT_ROOT/config/geopolitical_events.json")
  fi
  if ! run_cmd uv run python src/score/composite.py \
      --db "$db_path" \
      --output "$watchlist_path" \
      "${geo_filter_arg[@]}"; then
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

run_download_ais_marine_cadastre() {
  echo
  echo "[17] Download & Ingest Marine Cadastre AIS (NOAA — free, US coastal waters)"
  echo
  echo "  Marine Cadastre publishes annual AIS archives for US coastal waters."
  echo "  Files are ~1–4 GB zipped. Already-downloaded archives are skipped."
  echo
  echo "  ⚠️  Coverage is US coastal only — not Singapore/Malacca."
  echo "      Use bbox preset 'us-gulf' or 'us-east' to get real rows."
  echo "      Singapore preset will return 0 rows from this source."
  echo

  local year
  year="$(prompt "Year to download" "2023")"

  local db_path
  db_path="$(prompt "DuckDB path" "data/processed/mpol.duckdb")"

  echo
  echo "  Bounding box presets:"
  echo "    1) Singapore / Malacca (default arktrace AOI — 0 rows from MarineCadastre)"
  echo "    2) US Gulf of Mexico   (lat 18-32, lon -98 to -80) — good for tanker traffic"
  echo "    3) US East Coast       (lat 24-46, lon -82 to -65)"
  echo "    4) Global              (no filter — loads everything, very large)"
  echo "    5) Custom"
  echo
  local bbox_choice
  read -r -p "  Choose bbox [2]: " bbox_choice
  bbox_choice="${bbox_choice:-2}"

  local bbox_args=()
  case "$bbox_choice" in
    1) bbox_args=(--bbox -5 92 22 122) ;;
    2) bbox_args=(--bbox 18 -98 32 -80) ;;
    3) bbox_args=(--bbox 24 -82 46 -65) ;;
    4) ;;  # no --bbox = global
    5)
      local lat_min lon_min lat_max lon_max
      lat_min="$(prompt "lat_min" "-90")"
      lon_min="$(prompt "lon_min" "-180")"
      lat_max="$(prompt "lat_max" "90")"
      lon_max="$(prompt "lon_max" "180")"
      bbox_args=(--bbox "$lat_min" "$lon_min" "$lat_max" "$lon_max")
      ;;
    *)
      echo "Invalid selection — using US Gulf of Mexico"
      bbox_args=(--bbox 18 -98 32 -80)
      ;;
  esac

  echo
  echo "── Downloading and ingesting year $year ────────────────────────────────────────"
  if ! run_cmd uv run python -m src.ingest.marine_cadastre \
      --year "$year" \
      --db "$db_path" \
      "${bbox_args[@]+"${bbox_args[@]}"}"; then
    echo "Result: FAILED"
    return
  fi

  echo "Result: SUCCESS"

  if ! prompt_yes_no "Run feature matrix + scoring now to measure Precision@50?" "true"; then
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
  if ! run_cmd uv run python -m src.score.watchlist \
      --db "$db_path" \
      --output "$watchlist_path"; then
    echo "Result: FAILED (scoring)"
    return
  fi

  echo
  echo "── Precision@50 (OFAC validate) ───────────────────────────────────────────────"
  local tmp_metrics
  tmp_metrics="$(mktemp /tmp/arktrace_metrics_XXXXXX.json)"
  if (cd "$PROJECT_ROOT" && uv run python -m src.score.validate \
        --db "$db_path" \
        --watchlist "$watchlist_path" \
        --output "$tmp_metrics") >/dev/null 2>&1; then
    (cd "$PROJECT_ROOT" && uv run python -c "
import json
with open('$tmp_metrics') as f:
    m = json.load(f)
p50   = m.get('precision_at_50', 'n/a')
r200  = m.get('recall_at_200', 'n/a')
auroc = m.get('auroc', 'n/a')
total = m.get('candidate_count', 'n/a')
pos   = m.get('positive_count', 'n/a')
target = 0.68
status = '✅ PASS' if isinstance(p50, float) and p50 >= target else '❌ BELOW TARGET'
print(f'  Precision@50  : {p50:.3f}  (target ≥ {target})  {status}' if isinstance(p50, float) else f'  Precision@50 : {p50}')
print(f'  Recall@200    : {r200:.3f}' if isinstance(r200, float) else f'  Recall@200   : {r200}')
print(f'  AUROC         : {auroc:.3f}' if isinstance(auroc, float) else f'  AUROC        : {auroc}')
print(f'  Candidates    : {total}  (OFAC positives: {pos})')
if pos == 0:
    print()
    print('  ⚠️  No OFAC positives matched. OFAC vessels are mostly Middle East/Asia flagged —')
    print('     US coastal AIS data rarely overlaps. Try AISHub for Singapore/Malacca data.')
")
  fi
  rm -f "$tmp_metrics"
  print_watchlist_summary "$watchlist_path"
}

run_fetch_aishub() {
  echo
  echo "[18] Fetch AISHub Live AIS — Singapore / Malacca Strait"
  echo
  echo "  AISHub (aishub.net) provides free live vessel positions via HTTP API."
  echo "  Registration required: https://www.aishub.net/join-us"
  echo "  Set AISHUB_USERNAME in .env or enter it below."
  echo

  local username
  username="${AISHUB_USERNAME:-}"
  if [[ -z "$username" ]]; then
    username="$(prompt "AISHub username")"
  else
    echo "  Using AISHUB_USERNAME from environment: $username"
  fi

  if [[ -z "$username" ]]; then
    echo "Error: username required."
    return
  fi

  local db_path
  db_path="$(prompt "DuckDB path" "data/processed/mpol.duckdb")"

  echo
  echo "  Bounding box presets:"
  echo "    1) Singapore / Malacca Strait  (lat -2–8, lon 98–110)  [default]"
  echo "    2) Strait of Singapore only    (lat 1.0–1.5, lon 103.5–104.5)"
  echo "    3) Custom"
  echo
  local bbox_choice
  read -r -p "  Choose bbox [1]: " bbox_choice
  bbox_choice="${bbox_choice:-1}"

  local bbox_args=()
  case "$bbox_choice" in
    1) bbox_args=(--bbox -2 98 8 110) ;;
    2) bbox_args=(--bbox 1.0 103.5 1.5 104.5) ;;
    3)
      local lat_min lon_min lat_max lon_max
      lat_min="$(prompt "lat_min" "-2")"
      lon_min="$(prompt "lon_min" "98")"
      lat_max="$(prompt "lat_max" "8")"
      lon_max="$(prompt "lon_max" "110")"
      bbox_args=(--bbox "$lat_min" "$lon_min" "$lat_max" "$lon_max")
      ;;
    *)
      echo "Invalid — using Singapore / Malacca default"
      bbox_args=(--bbox -2 98 8 110)
      ;;
  esac

  echo
  echo "── Fetching live positions from AISHub ─────────────────────────────────────────"
  if ! run_cmd uv run python -m src.ingest.aishub \
      --username "$username" \
      --db "$db_path" \
      "${bbox_args[@]+"${bbox_args[@]}"}"; then
    echo "Result: FAILED"
    return
  fi

  echo "Result: SUCCESS"

  if ! prompt_yes_no "Run feature matrix + scoring + Precision@50?" "true"; then
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
  if ! run_cmd uv run python -m src.score.watchlist \
      --db "$db_path" \
      --output "$watchlist_path"; then
    echo "Result: FAILED (scoring)"
    return
  fi

  echo
  echo "── Precision@50 ───────────────────────────────────────────────────────────────"
  local tmp_metrics
  tmp_metrics="$(mktemp /tmp/arktrace_metrics_XXXXXX.json)"
  if (cd "$PROJECT_ROOT" && uv run python -m src.score.validate \
        --db "$db_path" \
        --watchlist "$watchlist_path" \
        --output "$tmp_metrics") >/dev/null 2>&1; then
    (cd "$PROJECT_ROOT" && uv run python -c "
import json
with open('$tmp_metrics') as f:
    m = json.load(f)
p50   = m.get('precision_at_50', 'n/a')
r200  = m.get('recall_at_200', 'n/a')
auroc = m.get('auroc', 'n/a')
total = m.get('candidate_count', 'n/a')
pos   = m.get('positive_count', 'n/a')
target = 0.68
status = '✅ PASS' if isinstance(p50, float) and p50 >= target else '❌ BELOW TARGET'
print(f'  Precision@50  : {p50:.3f}  (target ≥ {target})  {status}' if isinstance(p50, float) else f'  Precision@50 : {p50}')
print(f'  Recall@200    : {r200:.3f}' if isinstance(r200, float) else f'  Recall@200   : {r200}')
print(f'  AUROC         : {auroc:.3f}' if isinstance(auroc, float) else f'  AUROC        : {auroc}')
print(f'  Candidates    : {total}  (OFAC positives: {pos})')
if pos == 0:
    print()
    print('  ⚠️  No OFAC positives matched yet — live data covers recent positions only.')
    print('     Fetch repeatedly over time to build up history, then re-score.')
")
  fi
  rm -f "$tmp_metrics"
  print_watchlist_summary "$watchlist_path"
}

run_fetch_aisstream() {
  echo
  echo "[19] Fetch aisstream.io Live AIS — Singapore / Malacca Strait"
  echo
  echo "  aisstream.io is a free real-time AIS WebSocket stream (no equipment required)."
  echo "  Register instantly at https://aisstream.io to get an API key."
  echo "  Set AISSTREAM_API_KEY in .env or enter it below."
  echo
  echo "  ⚠️  Live data only — no historical. Run on a schedule to build up history."
  echo "     Tip: add to crontab to fetch every 30 min automatically."
  echo

  if [[ -z "${AISSTREAM_API_KEY:-}" ]]; then
    echo "Error: AISSTREAM_API_KEY not set. Add it to .env or export it."
    echo "  Register free at https://aisstream.io"
    return
  fi
  echo "  Using AISSTREAM_API_KEY from environment."

  local db_path
  db_path="$(prompt "DuckDB path" "data/processed/mpol.duckdb")"

  local duration
  duration="$(prompt "Collection duration seconds" "300")"

  echo
  echo "  Bounding box presets:"
  echo "    1) Singapore / Malacca Strait  (lat -2–8, lon 98–110)  [default]"
  echo "    2) Strait of Singapore only    (lat 1.0–1.5, lon 103.5–104.5)"
  echo "    3) Custom"
  echo
  local bbox_choice
  read -r -p "  Choose bbox [1]: " bbox_choice
  bbox_choice="${bbox_choice:-1}"

  local bbox_args=()
  case "$bbox_choice" in
    1) bbox_args=(--bbox -2 98 8 110) ;;
    2) bbox_args=(--bbox 1.0 103.5 1.5 104.5) ;;
    3)
      local lat_min lon_min lat_max lon_max
      lat_min="$(prompt "lat_min" "-2")"
      lon_min="$(prompt "lon_min" "98")"
      lat_max="$(prompt "lat_max" "8")"
      lon_max="$(prompt "lon_max" "110")"
      bbox_args=(--bbox "$lat_min" "$lon_min" "$lat_max" "$lon_max")
      ;;
    *)
      echo "Invalid — using Singapore / Malacca default"
      bbox_args=(--bbox -2 98 8 110)
      ;;
  esac

  echo
  echo "── Collecting live AIS from aisstream.io ───────────────────────────────────────"
  if ! run_cmd uv run python -m src.ingest.ais_stream \
      --db "$db_path" \
      --duration "$duration" \
      "${bbox_args[@]+"${bbox_args[@]}"}"; then
    echo "Result: FAILED"
    return
  fi

  echo "Result: SUCCESS"

  echo
  local total_rows
  total_rows="$(cd "$PROJECT_ROOT" && uv run python -c "
import duckdb
con = duckdb.connect('$db_path', read_only=True)
n = con.execute('SELECT count(*) FROM ais_positions').fetchone()[0]
v = con.execute('SELECT count(DISTINCT mmsi) FROM ais_positions').fetchone()[0]
con.close()
print(f'  Total positions in DB : {n}')
print(f'  Unique vessels        : {v}')
" 2>/dev/null)"
  echo "$total_rows"

  if ! prompt_yes_no "Run feature matrix + scoring + Precision@50?" "false"; then
    echo
    echo "  Tip: fetch a few more times first to build up position history, then score."
    echo "  Add to crontab:  */30 * * * * cd $PROJECT_ROOT && uv run python -m src.ingest.ais_stream --db $db_path --duration 300 ${bbox_args[*]+"${bbox_args[*]}"}"
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
  if ! run_cmd uv run python -m src.score.watchlist \
      --db "$db_path" \
      --output "$watchlist_path"; then
    echo "Result: FAILED (scoring)"
    return
  fi

  echo
  echo "── Precision@50 ───────────────────────────────────────────────────────────────"
  local tmp_metrics
  tmp_metrics="$(mktemp /tmp/arktrace_metrics_XXXXXX.json)"
  if (cd "$PROJECT_ROOT" && uv run python -m src.score.validate \
        --db "$db_path" \
        --watchlist "$watchlist_path" \
        --output "$tmp_metrics") >/dev/null 2>&1; then
    (cd "$PROJECT_ROOT" && uv run python -c "
import json
with open('$tmp_metrics') as f:
    m = json.load(f)
p50   = m.get('precision_at_50', 'n/a')
r200  = m.get('recall_at_200', 'n/a')
auroc = m.get('auroc', 'n/a')
total = m.get('candidate_count', 'n/a')
pos   = m.get('positive_count', 'n/a')
target = 0.68
status = '✅ PASS' if isinstance(p50, float) and p50 >= target else '❌ BELOW TARGET'
print(f'  Precision@50  : {p50:.3f}  (target ≥ {target})  {status}' if isinstance(p50, float) else f'  Precision@50 : {p50}')
print(f'  Recall@200    : {r200:.3f}' if isinstance(r200, float) else f'  Recall@200   : {r200}')
print(f'  AUROC         : {auroc:.3f}' if isinstance(auroc, float) else f'  AUROC        : {auroc}')
print(f'  Candidates    : {total}  (OFAC positives: {pos})')
if pos == 0:
    print()
    print('  ⚠️  No OFAC positives yet — need more position history.')
    print('     Run job 19 a few more times or set up the cron schedule.')
")
  fi
  rm -f "$tmp_metrics"
  print_watchlist_summary "$watchlist_path"
}

run_aisstream_agent() {
  echo
  echo "[20] aisstream.io launchd Agent — Install / Uninstall / Status / Logs"
  echo
  echo "  Manages a macOS launchd agent that runs src/ingest/ais_stream.py"
  echo "  continuously in the background, surviving reboots and auto-restarting"
  echo "  on crash. Logs go to ~/.arktrace/aisstream.log."
  echo
  echo "  Sub-commands:"
  echo "    1) Install / (re-)start agent"
  echo "    2) Uninstall agent"
  echo "    3) Status"
  echo "    4) View logs"
  echo

  if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "Error: launchd is macOS-only. Use a systemd unit or cron on Linux."
    return
  fi

  if [[ -z "${AISSTREAM_API_KEY:-}" ]]; then
    echo "Error: AISSTREAM_API_KEY not set. Add it to .env or export it."
    echo "  Register free at https://aisstream.io"
    return
  fi

  local sub_choice
  read -r -p "  Choose [1-4]: " sub_choice

  case "$sub_choice" in
    1)
      echo
      local db_path
      db_path="$(prompt "DuckDB path" "data/processed/mpol.duckdb")"

      echo
      echo "  Bounding box presets:"
      echo "    1) Singapore / Malacca Strait  (lat -2–8, lon 98–110)  [default]"
      echo "    2) Strait of Singapore only    (lat 1.0–1.5, lon 103.5–104.5)"
      echo "    3) Custom"
      echo
      local bbox_choice
      read -r -p "  Choose bbox [1]: " bbox_choice
      bbox_choice="${bbox_choice:-1}"

      local bbox_args=()
      case "$bbox_choice" in
        1) bbox_args=(--bbox -2 98 8 110) ;;
        2) bbox_args=(--bbox 1.0 103.5 1.5 104.5) ;;
        3)
          local lat_min lon_min lat_max lon_max
          lat_min="$(prompt "lat_min" "-2")"
          lon_min="$(prompt "lon_min" "98")"
          lat_max="$(prompt "lat_max" "8")"
          lon_max="$(prompt "lon_max" "110")"
          bbox_args=(--bbox "$lat_min" "$lon_min" "$lat_max" "$lon_max")
          ;;
        *)
          echo "Invalid — using Singapore / Malacca default"
          bbox_args=(--bbox -2 98 8 110)
          ;;
      esac

      echo
      echo "── Installing launchd agent ───────────────────────────────────────────────────"
      if ! "$PROJECT_ROOT/scripts/install_aisstream_agent.sh" install \
            --db "$db_path" \
            "${bbox_args[@]+"${bbox_args[@]}"}"; then
        echo "Result: FAILED"
        return
      fi
      echo
      echo "Agent is running. Positions will accumulate in $db_path."
      echo "Run job 16 or job 20/4 to check progress."
      ;;

    2)
      echo
      echo "── Uninstalling launchd agent ─────────────────────────────────────────────────"
      "$PROJECT_ROOT/scripts/install_aisstream_agent.sh" uninstall
      ;;

    3)
      echo
      echo "── Agent status ───────────────────────────────────────────────────────────────"
      "$PROJECT_ROOT/scripts/install_aisstream_agent.sh" status
      ;;

    4)
      echo
      local lines
      lines="$(prompt "Lines to show" "50")"
      echo
      echo "── Agent logs ─────────────────────────────────────────────────────────────────"
      "$PROJECT_ROOT/scripts/install_aisstream_agent.sh" logs --lines "$lines"
      ;;

    *)
      echo "Invalid selection"
      ;;
  esac
}

run_predemo_checklist() {
  echo
  echo "[22] Pre-Submission Demo Checklist"
  echo
  echo "  Runs the full pre-demo sequence in order:"
  echo "    Step 1 — Refresh public sanctions DB (OpenSanctions download + reload)"
  echo "    Step 2 — Check AIS data freshness (≥48h from launchd agent recommended)"
  echo "    Step 3 — Full screening pipeline (rebuild vessel_features + composite scores)"
  echo "    Step 4 — Precision@50 verification (public OpenSanctions backtest)"
  echo "    Step 5 — Remind to capture dashboard screenshot"
  echo
  echo "  Estimated time: 15–25 min (dominated by sanctions download ~5 min + pipeline ~10 min)"
  echo

  local region
  region="$(prompt "Region (singapore/japan/middleeast/europe/gulf)" "singapore")"
  region="$(tr '[:upper:]' '[:lower:]' <<< "$region")"

  local eval_db
  eval_db="$(prompt "Public eval DuckDB path" "data/processed/public_eval.duckdb")"

  local screening_db
  case "$region" in
    singapore)  screening_db="data/processed/singapore.duckdb" ;;
    japan)      screening_db="data/processed/japansea.duckdb" ;;
    middleeast) screening_db="data/processed/middleeast.duckdb" ;;
    europe)     screening_db="data/processed/europe.duckdb" ;;
    gulf)       screening_db="data/processed/gulf.duckdb" ;;
    *)          screening_db="data/processed/mpol.duckdb" ;;
  esac

  local overall_ok=true

  # ── Step 1: Refresh sanctions DB ──────────────────────────────────────────
  echo
  echo "━━ Step 1/5 — Refresh public sanctions DB ────────────────────────────────────"
  echo
  if ! run_cmd uv run python scripts/prepare_public_sanctions_db.py --db "$eval_db"; then
    echo "  ❌ FAILED — sanctions refresh failed. Aborting checklist."
    overall_ok=false
  else
    echo "  ✅ Sanctions DB refreshed: $eval_db"
  fi

  if [[ "$overall_ok" == "false" ]]; then return; fi

  # ── Step 2: Check AIS data freshness ──────────────────────────────────────
  echo
  echo "━━ Step 2/5 — Check AIS data freshness ──────────────────────────────────────"
  echo
  (cd "$PROJECT_ROOT" && uv run python - <<PYEOF
import duckdb, sys
from datetime import datetime, timedelta, timezone

db = "$screening_db"
try:
    con = duckdb.connect(db, read_only=True)
    row = con.execute(
        "SELECT COUNT(*), MIN(timestamp)::VARCHAR, MAX(timestamp)::VARCHAR FROM ais_positions"
    ).fetchone()
    con.close()
    total, earliest, latest = row
    if total == 0:
        print("  ⚠️  ais_positions is EMPTY — no AIS data loaded.")
        print("     Run job 20 to install the aisstream launchd agent, or job 19 to fetch live AIS.")
        sys.exit(1)
    latest_dt = datetime.fromisoformat(latest.replace(" ", "T")).replace(tzinfo=timezone.utc)
    age_h = (datetime.now(timezone.utc) - latest_dt).total_seconds() / 3600
    print(f"  Rows    : {total:,}")
    print(f"  Earliest: {earliest}")
    print(f"  Latest  : {latest}  ({age_h:.1f}h ago)")
    if age_h > 48:
        print(f"  ⚠️  Latest AIS position is {age_h:.0f}h old — consider restarting the launchd agent (job 20).")
    else:
        print("  ✅ AIS data is fresh.")
except Exception as e:
    print(f"  ⚠️  Could not read {db}: {e}")
    sys.exit(1)
PYEOF
  )
  local ais_rc=$?
  if [[ $ais_rc -ne 0 ]]; then
    echo
    if ! prompt_yes_no "AIS data missing or stale — continue anyway?" "false"; then
      echo "Aborting checklist."
      return
    fi
  fi

  # ── Step 3: Full screening pipeline ───────────────────────────────────────
  echo
  echo "━━ Step 3/5 — Full screening pipeline ───────────────────────────────────────"
  echo "  (rebuilds vessel_features and composite scores from current DB state)"
  echo
  if ! run_cmd uv run python scripts/run_pipeline.py \
      --region "$region" \
      --non-interactive; then
    echo "  ❌ FAILED — pipeline failed. Check output above."
    overall_ok=false
  else
    echo "  ✅ Pipeline complete."
    local watchlist_path
    case "$region" in
      singapore)  watchlist_path="$PROJECT_ROOT/data/processed/singapore_watchlist.parquet" ;;
      japan)      watchlist_path="$PROJECT_ROOT/data/processed/japansea_watchlist.parquet" ;;
      middleeast) watchlist_path="$PROJECT_ROOT/data/processed/middleeast_watchlist.parquet" ;;
      europe)     watchlist_path="$PROJECT_ROOT/data/processed/europe_watchlist.parquet" ;;
      gulf)       watchlist_path="$PROJECT_ROOT/data/processed/gulf_watchlist.parquet" ;;
      *)          watchlist_path="$PROJECT_ROOT/data/processed/candidate_watchlist.parquet" ;;
    esac
    print_watchlist_summary "$watchlist_path"
  fi

  if [[ "$overall_ok" == "false" ]]; then return; fi

  # ── Step 4: Precision@50 verification ─────────────────────────────────────
  echo
  echo "━━ Step 4/5 — Precision@50 verification (public OpenSanctions backtest) ─────"
  echo
  if [[ ! -f "$PROJECT_ROOT/$eval_db" ]]; then
    echo "  ⚠️  $eval_db not found — skipping P@50 check."
    echo "     Re-run step 1 or pull from R2: uv run python scripts/sync_r2.py pull-sanctions-db"
  else
    local watchlist_path
    case "$region" in
      singapore)  watchlist_path="data/processed/singapore_watchlist.parquet" ;;
      japan)      watchlist_path="data/processed/japansea_watchlist.parquet" ;;
      middleeast) watchlist_path="data/processed/middleeast_watchlist.parquet" ;;
      europe)     watchlist_path="data/processed/europe_watchlist.parquet" ;;
      gulf)       watchlist_path="data/processed/gulf_watchlist.parquet" ;;
      *)          watchlist_path="data/processed/candidate_watchlist.parquet" ;;
    esac

    (cd "$PROJECT_ROOT" && uv run python - <<PYEOF
import duckdb, polars as pl, sys
from pathlib import Path

watchlist_path = "$watchlist_path"
eval_db = "$eval_db"
target = 0.68

if not Path(watchlist_path).exists():
    print(f"  ⚠️  Watchlist not found: {watchlist_path}")
    sys.exit(1)

watchlist = pl.read_parquet(watchlist_path)
if "confidence" not in watchlist.columns:
    print("  ⚠️  Watchlist missing 'confidence' column.")
    sys.exit(1)

top50 = watchlist.sort("confidence", descending=True).head(50)

con = duckdb.connect(eval_db, read_only=True)
positives = pl.from_pandas(con.execute("""
    SELECT DISTINCT
        REPLACE(COALESCE(mmsi,''),'IMO','') AS mmsi,
        REPLACE(COALESCE(imo,''),'IMO','') AS imo
    FROM sanctions_entities
    WHERE (lower(COALESCE(list_source,'')) LIKE '%ofac%'
        OR lower(COALESCE(list_source,'')) LIKE '%un%'
        OR lower(COALESCE(list_source,'')) LIKE '%eu%')
      AND (COALESCE(mmsi,'') <> '' OR COALESCE(imo,'') <> '')
""").fetchdf())
con.close()

pos_mmsi = set(positives["mmsi"].to_list())
pos_imo  = set(positives["imo"].to_list())

hits = 0
for row in top50.iter_rows(named=True):
    if str(row.get("mmsi","")) in pos_mmsi or str(row.get("imo","")) in pos_imo:
        hits += 1

p50 = hits / 50
status = "✅ PASS" if p50 >= target else "❌ BELOW TARGET"
print(f"  Precision@50  : {p50:.3f}  ({hits}/50)  (target ≥ {target})  {status}")
print(f"  Watchlist size: {len(watchlist):,} vessels")
if p50 < target:
    sys.exit(1)
PYEOF
    )
    local p50_rc=$?
    if [[ $p50_rc -ne 0 ]]; then
      echo
      echo "  P@50 is below target. Check scoring weights or refresh AIS + sanctions data."
      overall_ok=false
    fi
  fi

  # ── Step 5: Dashboard screenshot reminder ─────────────────────────────────
  echo
  echo "━━ Step 5/5 — Dashboard screenshot ──────────────────────────────────────────"
  echo
  if [[ "$overall_ok" == "true" ]]; then
    echo "  ✅ All checks passed. Ready for submission demo."
  else
    echo "  ⚠️  One or more steps failed — review output above before demo."
  fi
  echo
  echo "  To view the dashboard:"
  echo "    uv run uvicorn src.api.main:app --reload"
  echo "    open http://localhost:8000"
  echo
  echo "  Capture screenshot of:"
  echo "    • Ranked watchlist table (top-50 vessels with confidence scores)"
  echo "    • At least one vessel detail panel (SHAP signals + causal badge)"
  echo "    • Geopolitical context panel if GDELT data is loaded"
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
    echo "14) Ingest AIS Positions from CSV / NMEA"
    echo "     What: load AIS positions from a CSV (any provider, configurable column map)"
    echo "           or NMEA 0183 VDM/VDO sentence file into ais_positions; then optionally"
    echo "           re-run feature matrix + scoring so new vessels appear on the dashboard"
    echo "     When: testing a new S-AIS provider feed (Spire, exactEarth, Orbcomm, etc.)"
    echo "      Who: developer, data engineer"
    echo
    echo "15) Ingest custom feed drop-ins"
    echo "     What: run all CSV files in _inputs/custom_feeds/ through the auto-detector;"
    echo "           auto-routes AIS/SAR/cargo/sanctions feeds to the appropriate DuckDB table;"
    echo "           then optionally re-run feature matrix + scoring"
    echo "     When: loading proprietary AIS, SAR detections, cargo manifests, or sanctions"
    echo "           lists without writing any ingestion code"
    echo "      Who: analyst, developer, data engineer"
    echo
    echo "16) Precision@50 Verification"
    echo "     What: verify scoring model quality after a parameter change; three levels:"
    echo "           1-Quick (re-score + OFAC validate), 2-Full (backtest manifest),"
    echo "           3-Public (OpenSanctions pytest integration test)"
    echo "     When: after changing scoring weights, contamination, or blend ratio (#186)"
    echo "      Who: ML engineer, data scientist"
    echo
    echo "17) Download & Ingest Marine Cadastre AIS"
    echo "     What: download a free NOAA annual AIS archive, filter to a bbox, ingest into"
    echo "           DuckDB, then optionally score and measure Precision@50"
    echo "     When: getting real AIS data without a commercial provider subscription"
    echo "      Who: developer, data scientist"
    echo
    echo "18) Fetch AISHub Live AIS — Singapore / Malacca Strait"
    echo "     What: pull live vessel positions from AISHub API for Singapore/Malacca bbox;"
    echo "           ingest into DuckDB then optionally score and measure Precision@50"
    echo "     When: getting real Singapore Strait AIS data (free, requires aishub.net account)"
    echo "      Who: developer, data scientist"
    echo
    echo "19) Fetch aisstream.io Live AIS — Singapore / Malacca Strait"
    echo "     What: collect live AIS via WebSocket from aisstream.io for a configurable"
    echo "           duration; ingest into DuckDB; optionally score + Precision@50"
    echo "     When: getting real AIS data immediately (free, instant signup, no equipment)"
    echo "      Who: developer, data scientist"
    echo
    echo "20) aisstream.io launchd Agent (macOS) — Install / Uninstall / Status / Logs"
    echo "     What: manage a background launchd agent that runs ais_stream.py continuously,"
    echo "           surviving reboots and auto-restarting on crash (macOS only)"
    echo "     When: setting up persistent AIS data collection on a MacBook"
    echo "      Who: developer, data engineer"
    echo
    echo "21) GFW EO API Ingest"
    echo "     What: fetch GFW Events API detections for a bbox/window and populate eo_detections"
    echo "     When: GFW_API_TOKEN is set in .env (free-tier: FISHING events; research: GAP events)"
    echo "      Who: data engineer"
    echo
    echo "22) Pre-Submission Demo Checklist"
    echo "     What: sequenced pre-demo run — refresh sanctions → check AIS freshness →"
    echo "           full pipeline → Precision@50 verification → dashboard screenshot reminder"
    echo "     When: before any submission demo or evaluation; run ~1h before the demo slot"
    echo "      Who: ops, data engineer, product"
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
      14) run_ingest_ais_csv ;;
      15) run_ingest_custom_feeds ;;
      16) run_precision_verification ;;
      17) run_download_ais_marine_cadastre ;;
      18) run_fetch_aishub ;;
      19) run_fetch_aisstream ;;
      20) run_aisstream_agent ;;
      21) run_ingest_eo_gfw ;;
      22) run_predemo_checklist ;;
      q|quit|exit)
        echo "Bye"
        return
        ;;
      *)
        echo "Invalid selection"
        ;;
    esac

    echo
    read -r -p "Press Enter to return to menu..." _
  done
}

main_menu
