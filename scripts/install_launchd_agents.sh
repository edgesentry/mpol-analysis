#!/bin/zsh
# install_launchd_agents.sh — generate and load arktrace launchd agents
#
# Generates plist files for all AIS stream collectors + the R2 sync job from
# a single source of truth, then loads them via launchctl.
#
# Usage:
#   bash scripts/install_launchd_agents.sh [--regions REGIONS] [--all] [--unload] [--status] [--dry-run]
#
# Options:
#   --regions REGIONS   Comma-separated list of regions to install (default: japansea,blacksea,middleeast)
#   --all               Install all available stream regions
#   --unload            Unload and remove all io.arktrace.* agents
#   --status            Show status of all io.arktrace.* agents
#   --dry-run           Print generated plists without writing or loading
#   --no-r2sync         Skip the r2sync agent
#   --help              Show this help
#
# Credentials are read from .env in the project root (never hardcoded here).
# Required .env keys:
#   AISSTREAM_API_KEY        — aisstream.io WebSocket key
#   AWS_ACCESS_KEY_ID        — R2 access key (for r2sync agent)
#   AWS_SECRET_ACCESS_KEY    — R2 secret key (for r2sync agent)

set -euo pipefail

# Source ~/.zshrc so credentials exported there are available to the script
# (non-interactive zsh shells do not source ~/.zshrc automatically).
[[ -f "${HOME}/.zshrc" ]] && source "${HOME}/.zshrc" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${0:A}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PIPELINE_DIR="${PROJECT_DIR}/pipeline"
PLIST_DIR="${HOME}/Library/LaunchAgents"
LOG_DIR="${HOME}/.arktrace"
UV_BIN="/opt/homebrew/bin/uv"
ENV_FILE="${PROJECT_DIR}/.env"

# ---------------------------------------------------------------------------
# Region bbox lookup: "lat_min lon_min lat_max lon_max"
# ---------------------------------------------------------------------------
ALL_REGIONS="singapore japansea blacksea middleeast europe persiangulf gulfofaden gulfofguinea gulfofmexico hornofafrica"
DEFAULT_REGIONS="japansea blacksea middleeast"

region_bbox() {
  case "$1" in
    singapore)    echo "-5 92 22 122" ;;
    japansea)     echo "25 115 48 145" ;;
    blacksea)     echo "40 27 48 42" ;;
    middleeast)   echo "12 32 32 60" ;;
    europe)       echo "30 -22 72 42" ;;
    persiangulf)  echo "20 48 30 65" ;;
    gulfofaden)   echo "10 42 16 52" ;;
    gulfofguinea) echo "-5 -5 10 10" ;;
    gulfofmexico) echo "18 -98 31 -80" ;;
    hornofafrica) echo "0 38 20 68" ;;
    *) echo "" ;;
  esac
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
REGIONS="${DEFAULT_REGIONS}"
DO_UNLOAD=0
DO_STATUS=0
DRY_RUN=0
INSTALL_R2SYNC=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --regions)   REGIONS="${2//,/ }"; shift 2 ;;
    --all)       REGIONS="${ALL_REGIONS}"; shift ;;
    --unload)    DO_UNLOAD=1; shift ;;
    --status)    DO_STATUS=1; shift ;;
    --dry-run)   DRY_RUN=1; shift ;;
    --no-r2sync) INSTALL_R2SYNC=0; shift ;;
    --help)
      grep '^#' "$0" | sed 's/^# \?//'
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
if [[ ${DO_STATUS} -eq 1 ]]; then
  echo "io.arktrace.* agents:"
  printf "  %-42s %s\n" "LABEL" "PID / EXIT"
  printf "  %-42s %s\n" "-----" "----------"
  launchctl list | awk '/io\.arktrace\./ { printf "  %-42s %s/%s\n", $3, $1, $2 }'
  exit 0
fi

# ---------------------------------------------------------------------------
# Unload
# ---------------------------------------------------------------------------
if [[ ${DO_UNLOAD} -eq 1 ]]; then
  echo "Unloading all io.arktrace.* agents..."
  for plist in "${PLIST_DIR}"/io.arktrace.*.plist; do
    [[ -f "${plist}" ]] || continue
    label=$(basename "${plist}" .plist)
    if launchctl list | grep -q "${label}"; then
      launchctl unload "${plist}"
      echo "  unloaded ${label}"
    fi
    rm -f "${plist}"
    echo "  removed ${plist}"
  done
  echo "Done."
  exit 0
fi

# ---------------------------------------------------------------------------
# Load credentials — .env file takes precedence, env vars as fallback
# (credentials exported via ~/.zshrc are picked up automatically)
# ---------------------------------------------------------------------------
_env_get() {
  local key="$1"
  # Try .env file first
  if [[ -f "${ENV_FILE}" ]]; then
    local val
    val=$(grep -E "^${key}=" "${ENV_FILE}" | head -1 | sed 's/^[^=]*=//;s/^"//;s/"$//;s/^ //;s/ $//')
    [[ -n "${val}" ]] && echo "${val}" && return
  fi
  # Fall back to already-exported environment variable
  echo "${(P)key:-}"
}

AISSTREAM_API_KEY="$(_env_get AISSTREAM_API_KEY)"
AWS_ACCESS_KEY_ID="$(_env_get AWS_ACCESS_KEY_ID)"
AWS_SECRET_ACCESS_KEY="$(_env_get AWS_SECRET_ACCESS_KEY)"

if [[ -z "${AISSTREAM_API_KEY}" ]]; then
  echo "Error: AISSTREAM_API_KEY not found in ${ENV_FILE}" >&2
  exit 1
fi
if [[ ${INSTALL_R2SYNC} -eq 1 ]] && [[ -z "${AWS_ACCESS_KEY_ID}" || -z "${AWS_SECRET_ACCESS_KEY}" ]]; then
  echo "Error: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY not found in ${ENV_FILE}" >&2
  echo "       Pass --no-r2sync to skip the R2 sync agent." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
mkdir -p "${LOG_DIR}"

_write_or_print() {
  local path="$1"
  local content="$2"
  if [[ ${DRY_RUN} -eq 1 ]]; then
    echo "=== ${path} ==="
    printf '%s\n' "${content}"
    echo
  else
    printf '%s\n' "${content}" > "${path}"
  fi
}

_load_agent() {
  local plist="$1"
  local label="$2"
  if [[ ${DRY_RUN} -eq 1 ]]; then
    echo "  [dry-run] would load ${label}"
    return
  fi
  if launchctl list | grep -q "${label}"; then
    launchctl unload "${plist}" 2>/dev/null || true
  fi
  launchctl load "${plist}"
  echo "  loaded ${label}"
}

# ---------------------------------------------------------------------------
# Generate AIS stream plists
# ---------------------------------------------------------------------------
echo "Installing AIS stream agents: ${REGIONS}"

for region in ${=REGIONS}; do
  bbox="$(region_bbox "${region}")"
  if [[ -z "${bbox}" ]]; then
    echo "  [warn] unknown region '${region}' — skipping" >&2
    continue
  fi

  read -r lat_min lon_min lat_max lon_max <<< "${bbox}"
  label="io.arktrace.aisstream.${region}"
  plist="${PLIST_DIR}/${label}.plist"
  db_path="${PROJECT_DIR}/data/processed/${region}.duckdb"

  content="<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\"
  \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
<dict>
  <key>Label</key>
  <string>${label}</string>

  <key>ProgramArguments</key>
  <array>
    <string>${UV_BIN}</string>
    <string>run</string>
    <string>--project</string>
    <string>${PROJECT_DIR}</string>
    <string>python</string>
    <string>-m</string>
    <string>src.ingest.ais_stream</string>
    <string>--db</string>
    <string>${db_path}</string>
    <string>--bbox</string>
    <string>${lat_min}</string>
    <string>${lon_min}</string>
    <string>${lat_max}</string>
    <string>${lon_max}</string>
    <string>--duration</string>
    <string>0</string>
    <string>--flush-interval</string>
    <string>60</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${PIPELINE_DIR}</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>AISSTREAM_API_KEY</key>
    <string>${AISSTREAM_API_KEY}</string>
    <key>DB_PATH</key>
    <string>${db_path}</string>
    <key>HOME</key>
    <string>${HOME}</string>
    <key>PATH</key>
    <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${HOME}/.local/bin:${HOME}/.cargo/bin</string>
    <key>PYTHONPATH</key>
    <string>${PIPELINE_DIR}</string>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
  </dict>

  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>30</integer>
  <key>RunAtLoad</key>
  <true/>

  <key>StandardOutPath</key>
  <string>${LOG_DIR}/${region}.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/${region}.err</string>
</dict>
</plist>"

  _write_or_print "${plist}" "${content}"
  _load_agent "${plist}" "${label}"
done

# ---------------------------------------------------------------------------
# Generate R2 sync plist
# ---------------------------------------------------------------------------
if [[ ${INSTALL_R2SYNC} -eq 1 ]]; then
  r2_regions="${REGIONS// /,}"
  label="io.arktrace.r2sync"
  plist="${PLIST_DIR}/${label}.plist"

  echo "Installing R2 sync agent (every 6h, regions: ${r2_regions})"

  content="<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\"
  \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">
<plist version=\"1.0\">
<dict>
  <key>Label</key>
  <string>${label}</string>

  <key>ProgramArguments</key>
  <array>
    <string>${UV_BIN}</string>
    <string>run</string>
    <string>--project</string>
    <string>${PROJECT_DIR}</string>
    <string>python</string>
    <string>${PROJECT_DIR}/scripts/sync_r2.py</string>
    <string>push-ais-dbs</string>
    <string>--regions</string>
    <string>${r2_regions}</string>
    <string>--data-dir</string>
    <string>${PROJECT_DIR}/data/processed</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${PROJECT_DIR}</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>AWS_ACCESS_KEY_ID</key>
    <string>${AWS_ACCESS_KEY_ID}</string>
    <key>AWS_SECRET_ACCESS_KEY</key>
    <string>${AWS_SECRET_ACCESS_KEY}</string>
    <key>HOME</key>
    <string>${HOME}</string>
    <key>PATH</key>
    <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${HOME}/.local/bin:${HOME}/.cargo/bin</string>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
  </dict>

  <!-- Run every 6 hours (21600 seconds) -->
  <key>StartInterval</key>
  <integer>21600</integer>

  <key>RunAtLoad</key>
  <true/>

  <key>StandardOutPath</key>
  <string>${LOG_DIR}/r2sync.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/r2sync.err</string>
</dict>
</plist>"

  _write_or_print "${plist}" "${content}"
  _load_agent "${plist}" "${label}"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
if [[ ${DRY_RUN} -eq 0 ]]; then
  echo ""
  echo "Done. Active io.arktrace.* agents:"
  launchctl list | awk '/io\.arktrace\./ { printf "  %-42s PID=%s\n", $3, $1 }'
  echo ""
  echo "Logs:   ${LOG_DIR}/<region>.log"
  echo "Status: bash scripts/install_launchd_agents.sh --status"
  echo "Unload: bash scripts/install_launchd_agents.sh --unload"
fi
