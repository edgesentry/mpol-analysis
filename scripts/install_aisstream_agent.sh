#!/usr/bin/env bash
# install_aisstream_agent.sh — install / uninstall / status / logs for the
# launchd agent that runs src/ingest/ais_stream.py continuously.
#
# Usage:
#   scripts/install_aisstream_agent.sh install   [--db PATH] [--bbox LAT_MIN LON_MIN LAT_MAX LON_MAX]
#   scripts/install_aisstream_agent.sh uninstall
#   scripts/install_aisstream_agent.sh status
#   scripts/install_aisstream_agent.sh logs       [--lines N]
#
# Requirements:
#   - macOS (uses launchd)
#   - AISSTREAM_API_KEY set in .env or exported in the environment
#   - uv installed and on PATH

set -euo pipefail

LABEL="io.arktrace.aisstream"
PLIST_PATH="$HOME/Library/LaunchAgents/${LABEL}.plist"
LOG_DIR="$HOME/.arktrace"
LOG_FILE="$LOG_DIR/aisstream.log"
ERR_FILE="$LOG_DIR/aisstream.err"
NEWSYSLOG_CONF="$HOME/Library/LaunchAgents/${LABEL}.newsyslog.conf"

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DOT_ENV="$PROJECT_ROOT/.env"

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

_load_dotenv() {
  if [[ -f "$DOT_ENV" ]]; then
    # Export each non-comment, non-empty line
    while IFS='=' read -r key rest; do
      [[ -z "$key" || "$key" == \#* ]] && continue
      export "$key"="${rest}"
    done < "$DOT_ENV"
  fi
}

_uv_path() {
  # Return the absolute path to the uv binary found via PATH
  command -v uv 2>/dev/null || { echo "Error: uv not found on PATH" >&2; exit 1; }
}

_check_api_key() {
  _load_dotenv
  if [[ -z "${AISSTREAM_API_KEY:-}" ]]; then
    echo "Error: AISSTREAM_API_KEY is not set." >&2
    echo "  Add it to $DOT_ENV  or export it before running this script." >&2
    exit 1
  fi
}

# --------------------------------------------------------------------------- #
# install
# --------------------------------------------------------------------------- #

cmd_install() {
  local db_path="${DB_PATH:-$PROJECT_ROOT/data/processed/mpol.duckdb}"
  local lat_min="-2" lon_min="98" lat_max="8" lon_max="110"

  # Parse flags
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --db) db_path="$2"; shift 2 ;;
      --bbox)
        lat_min="$2"; lon_min="$3"; lat_max="$4"; lon_max="$5"
        shift 5
        ;;
      *) echo "Unknown flag: $1" >&2; exit 1 ;;
    esac
  done

  _check_api_key
  local api_key="$AISSTREAM_API_KEY"
  local uv_bin
  uv_bin="$(_uv_path)"

  # Resolve DB path to absolute
  if [[ "$db_path" != /* ]]; then
    db_path="$PROJECT_ROOT/$db_path"
  fi

  mkdir -p "$LOG_DIR"

  # Log rotation via newsyslog (runs hourly on macOS).
  # Rotate at 50 MB, keep 5 archives, compress with bzip2 (flag J).
  # Format: path  owner:group  mode  count  size(KB)  when  flags  [pid-file  sig]
  cat > "$NEWSYSLOG_CONF" <<NEWSYSLOG
# arktrace aisstream.io log rotation — managed by install_aisstream_agent.sh
${LOG_FILE}	$(id -un):$(id -gn)	644	5	51200	*	JN
${ERR_FILE}	$(id -un):$(id -gn)	644	5	51200	*	JN
NEWSYSLOG

  cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>

  <key>ProgramArguments</key>
  <array>
    <string>${uv_bin}</string>
    <string>run</string>
    <string>--project</string>
    <string>${PROJECT_ROOT}</string>
    <string>python</string>
    <string>-m</string>
    <string>pipeline.src.ingest.ais_stream</string>
    <string>--db</string>
    <string>${db_path}</string>
    <string>--bbox</string>
    <string>${lat_min}</string>
    <string>${lon_min}</string>
    <string>${lat_max}</string>
    <string>${lon_max}</string>
    <string>--duration</string>
    <string>0</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${PROJECT_ROOT}</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>AISSTREAM_API_KEY</key>
    <string>${api_key}</string>
    <key>DB_PATH</key>
    <string>${db_path}</string>
    <key>HOME</key>
    <string>${HOME}</string>
    <key>PATH</key>
    <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${HOME}/.local/bin:${HOME}/.cargo/bin</string>
  </dict>

  <!-- Restart automatically on exit / crash -->
  <key>KeepAlive</key>
  <true/>

  <!-- Wait 30 s between crash restarts to avoid tight crash loops -->
  <key>ThrottleInterval</key>
  <integer>30</integer>

  <!-- Start on login -->
  <key>RunAtLoad</key>
  <true/>

  <key>StandardOutPath</key>
  <string>${LOG_FILE}</string>

  <key>StandardErrorPath</key>
  <string>${ERR_FILE}</string>
</dict>
</plist>
PLIST

  echo "Plist written: $PLIST_PATH"

  # Unload first if already loaded (ignore errors — it may not be loaded yet)
  launchctl unload "$PLIST_PATH" 2>/dev/null || true

  launchctl load "$PLIST_PATH"
  echo "Agent loaded. aisstream.io collector is now running."
  echo
  echo "  Logs   : $LOG_FILE"
  echo "  Errors : $ERR_FILE"
  echo "  Rotation: $NEWSYSLOG_CONF (newsyslog, max 50 MB × 5 archives)"
  echo "  Status : scripts/install_aisstream_agent.sh status"
  echo
  echo "━━━  PRE-FLIGHT: keep MacBook running for 48–72 h  ━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  1. Plug into power."
  echo "  2. Disable sleep: System Settings → Battery → Options →"
  echo "       set display and disk sleep to Never while plugged in; disable Power Nap."
  echo "  3. Keep lid open (or attach external display + keyboard/mouse for clamshell)."
  echo "  4. Run as a safety net:  caffeinate -i &"
  echo "  5. Verify aisstream.io free-tier rate for Singapore/Malacca bbox;"
  echo "       narrow bbox with --bbox if messages are throttled."
  echo "  6. Confirm ais_stream.py handles WebSocket disconnects with backoff (not a tight loop)."
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

# --------------------------------------------------------------------------- #
# uninstall
# --------------------------------------------------------------------------- #

cmd_uninstall() {
  if [[ ! -f "$PLIST_PATH" ]]; then
    echo "No plist found at $PLIST_PATH — nothing to uninstall."
    return
  fi
  launchctl unload "$PLIST_PATH" 2>/dev/null || true
  rm -f "$PLIST_PATH"
  rm -f "$NEWSYSLOG_CONF"
  echo "Agent unloaded and plist removed."
  echo "Logs are kept at $LOG_DIR — delete manually if no longer needed."
}

# --------------------------------------------------------------------------- #
# status
# --------------------------------------------------------------------------- #

cmd_status() {
  echo "Plist:  $PLIST_PATH"
  if [[ ! -f "$PLIST_PATH" ]]; then
    echo "Status: NOT INSTALLED"
    return
  fi

  local pid
  pid="$(launchctl list "$LABEL" 2>/dev/null | awk 'NR==1{print $1}')" || true

  if [[ -z "$pid" ]]; then
    echo "Status: INSTALLED but not running"
  elif [[ "$pid" == "-" ]]; then
    local exit_code
    exit_code="$(launchctl list "$LABEL" 2>/dev/null | awk 'NR==1{print $2}')" || true
    echo "Status: STOPPED (last exit code: ${exit_code:-unknown})"
  else
    echo "Status: RUNNING (PID $pid)"
  fi

  echo
  echo "Log file: $LOG_FILE"
  if [[ -f "$LOG_FILE" ]]; then
    echo "Last 5 log lines:"
    tail -5 "$LOG_FILE" | sed 's/^/  /'
  else
    echo "  (no log file yet)"
  fi
}

# --------------------------------------------------------------------------- #
# logs
# --------------------------------------------------------------------------- #

cmd_logs() {
  local lines=50
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --lines|-n) lines="$2"; shift 2 ;;
      *) echo "Unknown flag: $1" >&2; exit 1 ;;
    esac
  done

  if [[ -f "$LOG_FILE" ]]; then
    echo "=== stdout ($LOG_FILE) — last $lines lines ==="
    tail -"$lines" "$LOG_FILE"
  else
    echo "(no stdout log yet: $LOG_FILE)"
  fi

  echo
  if [[ -f "$ERR_FILE" ]]; then
    echo "=== stderr ($ERR_FILE) — last $lines lines ==="
    tail -"$lines" "$ERR_FILE"
  else
    echo "(no stderr log yet: $ERR_FILE)"
  fi
}

# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #

usage() {
  cat <<EOF
Usage: $(basename "$0") <command> [options]

Commands:
  install    Install and start the launchd agent (restarts on login/crash)
             Options: --db PATH  --bbox LAT_MIN LON_MIN LAT_MAX LON_MAX
  uninstall  Stop and remove the launchd agent
  status     Show agent status and last log lines
  logs       Print log output  (--lines N, default 50)
EOF
}

case "${1:-}" in
  install)   shift; cmd_install   "$@" ;;
  uninstall) shift; cmd_uninstall "$@" ;;
  status)    shift; cmd_status    "$@" ;;
  logs)      shift; cmd_logs      "$@" ;;
  *)         usage; exit 1 ;;
esac
