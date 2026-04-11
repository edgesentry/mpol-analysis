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
