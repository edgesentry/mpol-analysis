#!/usr/bin/env bash
# aisstream_agents.sh — manage launchd agents for all AIS stream regions.
#
# Usage:
#   scripts/aisstream_agents.sh init                   # install all plists from config/launchagents/
#   scripts/aisstream_agents.sh start [region ...]     # load agent(s); omit region to start all
#   scripts/aisstream_agents.sh stop  [region ...]     # unload agent(s); omit region to stop all
#   scripts/aisstream_agents.sh status                 # show running agents + record counts
#   scripts/aisstream_agents.sh logs  <region>         # tail logs for one region
#
# Regions: singapore  japansea  persiangulf  europe  middleeast  hornofafrica  blacksea
#          gulfofguinea  gulfofaden  gulfofmexico
#
# NOTE: max 3 concurrent streams on the aisstream.io free tier — 429s will occur above that.
#
# Requirements:
#   - macOS (launchd)
#   - AISSTREAM_API_KEY in .env or exported
#   - uv installed

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG_DIR="$PROJECT_ROOT/pipeline/config/launchagents"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
LOG_DIR="$HOME/.arktrace"
DOT_ENV="$PROJECT_ROOT/.env"

LABEL_PREFIX="io.arktrace.aisstream"
ALL_REGIONS=(singapore japansea persiangulf europe middleeast hornofafrica blacksea gulfofguinea gulfofaden gulfofmexico)

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

_load_dotenv() {
  if [[ -f "$DOT_ENV" ]]; then
    while IFS='=' read -r key rest; do
      [[ -z "$key" || "$key" == \#* ]] && continue
      export "$key"="${rest}"
    done < "$DOT_ENV"
  fi
}

_check_api_key() {
  _load_dotenv
  if [[ -z "${AISSTREAM_API_KEY:-}" ]]; then
    echo "Error: AISSTREAM_API_KEY is not set." >&2
    echo "  Add it to $DOT_ENV or export it before running this script." >&2
    exit 1
  fi
}

_plist_src() { echo "$CONFIG_DIR/${LABEL_PREFIX}.${1}.plist"; }
_plist_dst() { echo "$LAUNCH_AGENTS_DIR/${LABEL_PREFIX}.${1}.plist"; }
_label()     { echo "${LABEL_PREFIX}.${1}"; }
_db_path()   { echo "$PROJECT_ROOT/data/processed/${1}.duckdb"; }
_log_file()  { echo "$LOG_DIR/${1}.log"; }
_err_file()  { echo "$LOG_DIR/${1}.err"; }

_resolve_regions() {
  # If args given, use them; otherwise use ALL_REGIONS
  if [[ $# -gt 0 ]]; then
    echo "$@"
  else
    echo "${ALL_REGIONS[@]}"
  fi
}

# --------------------------------------------------------------------------- #
# init — copy plists to ~/Library/LaunchAgents, injecting real API key
# --------------------------------------------------------------------------- #

cmd_init() {
  _check_api_key
  local api_key="$AISSTREAM_API_KEY"

  mkdir -p "$LOG_DIR"

  local installed=0
  for region in "${ALL_REGIONS[@]}"; do
    local src; src="$(_plist_src "$region")"
    local dst; dst="$(_plist_dst "$region")"

    if [[ ! -f "$src" ]]; then
      echo "  [skip] $region — no config found at $src"
      continue
    fi

    sed "s|REPLACE_WITH_AISSTREAM_API_KEY|${api_key}|g" "$src" > "$dst"
    echo "  [ok]   $region → $dst"
    (( installed++ )) || true
  done

  # Install r2sync agent if template exists and AWS credentials are available
  local r2sync_src="$CONFIG_DIR/${LABEL_PREFIX%.*}.r2sync.plist"
  local r2sync_dst="$LAUNCH_AGENTS_DIR/${LABEL_PREFIX%.*}.r2sync.plist"
  if [[ -f "$r2sync_src" ]]; then
    _load_dotenv
    local aws_key="${AWS_ACCESS_KEY_ID:-}"
    local aws_secret="${AWS_SECRET_ACCESS_KEY:-}"
    if [[ -z "$aws_key" || -z "$aws_secret" ]]; then
      echo "  [skip] r2sync — AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY not set"
    else
      sed -e "s|REPLACE_WITH_AWS_ACCESS_KEY_ID|${aws_key}|g" \
          -e "s|REPLACE_WITH_AWS_SECRET_ACCESS_KEY|${aws_secret}|g" \
          "$r2sync_src" > "$r2sync_dst"
      echo "  [ok]   r2sync → $r2sync_dst"
      (( installed++ )) || true
    fi
  fi

  echo
  echo "$installed plist(s) installed to $LAUNCH_AGENTS_DIR"
  echo "Run 'scripts/aisstream_agents.sh start <region>' to activate."
}

# --------------------------------------------------------------------------- #
# start — load agent(s)
# --------------------------------------------------------------------------- #

cmd_start() {
  local regions
  read -ra regions <<< "$(_resolve_regions "$@")"

  for region in "${regions[@]}"; do
    local dst; dst="$(_plist_dst "$region")"
    if [[ ! -f "$dst" ]]; then
      echo "  [skip] $region — plist not found (run 'init' first)"
      continue
    fi
    # Unload first to avoid "already loaded" errors
    launchctl unload "$dst" 2>/dev/null || true
    launchctl load "$dst"
    echo "  [started] $region"
  done
}

# --------------------------------------------------------------------------- #
# stop — unload agent(s)
# --------------------------------------------------------------------------- #

cmd_stop() {
  local regions
  read -ra regions <<< "$(_resolve_regions "$@")"

  for region in "${regions[@]}"; do
    local dst; dst="$(_plist_dst "$region")"
    if [[ ! -f "$dst" ]]; then
      echo "  [skip] $region — plist not installed"
      continue
    fi
    launchctl unload "$dst" 2>/dev/null || true
    echo "  [stopped] $region"
  done
}

# --------------------------------------------------------------------------- #
# status — running agents + record counts
# --------------------------------------------------------------------------- #

cmd_status() {
  printf "%-16s  %-10s  %-8s  %s\n" "REGION" "STATUS" "RECORDS" "LAST LOG LINE"
  printf "%-16s  %-10s  %-8s  %s\n" "------" "------" "-------" "-------------"

  for region in "${ALL_REGIONS[@]}"; do
    local label; label="$(_label "$region")"
    local db;    db="$(_db_path "$region")"
    local log;   log="$(_log_file "$region")"

    # launchd status
    local pid exit_code status
    pid=$(launchctl list 2>/dev/null | awk -v lbl="$label" '$3==lbl{print $1}') || true
    exit_code=$(launchctl list 2>/dev/null | awk -v lbl="$label" '$3==lbl{print $2}') || true

    if [[ -z "$pid" ]]; then
      status="not loaded"
    elif [[ "$pid" == "-" ]]; then
      status="stopped(${exit_code})"
    else
      status="running(${pid})"
    fi

    # record count
    local count="-"
    if [[ -f "$db" ]]; then
      count=$(uv run --project "$PROJECT_ROOT" python -c "
import duckdb
try:
    con = duckdb.connect('$db', read_only=True)
    print(con.execute('SELECT COUNT(*) FROM ais_positions').fetchone()[0])
    con.close()
except:
    print('-')
" 2>/dev/null) || count="-"
    fi

    # last log line
    local last_log="-"
    if [[ -f "$log" ]]; then
      last_log=$(tail -1 "$log" 2>/dev/null || echo "-")
    fi

    printf "%-16s  %-10s  %-8s  %s\n" "$region" "$status" "$count" "${last_log:0:60}"
  done
}

# --------------------------------------------------------------------------- #
# logs — tail logs for one region
# --------------------------------------------------------------------------- #

cmd_logs() {
  local region="${1:-}"
  local lines="${2:-50}"

  if [[ -z "$region" ]]; then
    echo "Usage: $(basename "$0") logs <region> [lines]" >&2
    exit 1
  fi

  local log; log="$(_log_file "$region")"
  local err; err="$(_err_file "$region")"

  if [[ -f "$log" ]]; then
    echo "=== stdout ($log) — last $lines lines ==="
    tail -"$lines" "$log"
  else
    echo "(no stdout log: $log)"
  fi

  echo
  if [[ -f "$err" ]] && [[ -s "$err" ]]; then
    echo "=== stderr ($err) — last $lines lines ==="
    tail -"$lines" "$err"
  else
    echo "(stderr empty)"
  fi
}

# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #

usage() {
  cat <<EOF
Usage: $(basename "$0") <command> [region ...] [options]

Commands:
  init              Install all region plists to ~/Library/LaunchAgents/
                    (reads AISSTREAM_API_KEY from .env or environment)
  start [region …]  Load and start agent(s). Omit region to start all.
  stop  [region …]  Unload and stop agent(s). Omit region to stop all.
  status            Show status and record counts for all regions.
  logs  <region>    Tail stdout/stderr logs for one region.

Regions: ${ALL_REGIONS[*]}

NOTE: aisstream.io free tier supports max ~3 concurrent streams.
      hornofafrica and middleeast show poor coverage on the free tier.
      Starting more will trigger HTTP 429 rate limiting.

Examples:
  scripts/aisstream_agents.sh init
  scripts/aisstream_agents.sh start singapore japansea persiangulf
  scripts/aisstream_agents.sh stop persiangulf
  scripts/aisstream_agents.sh status
  scripts/aisstream_agents.sh logs singapore
EOF
}

case "${1:-}" in
  init)   shift; cmd_init   "$@" ;;
  start)  shift; cmd_start  "$@" ;;
  stop)   shift; cmd_stop   "$@" ;;
  status) shift; cmd_status "$@" ;;
  logs)   shift; cmd_logs   "$@" ;;
  *)      usage; exit 1 ;;
esac
