#!/usr/bin/env bash
# scripts/run_app.sh
#
# Start arktrace in native macOS dev mode.
#
#   • Data is read from local disk (pull from R2 first if not present)
#   • mlx-lm runs as a local OpenAI-compatible server (Apple Silicon, Metal)
#   • Dashboard runs natively on the host — connects to the mlx-lm server
#
# Prerequisites (one-time):
#   uv pip install mlx-lm
#
# Usage:
#   bash scripts/run_app.sh
#   bash scripts/run_app.sh --region japan
#   bash scripts/run_app.sh --model mlx-community/Qwen2.5-7B-Instruct-4bit
#   bash scripts/run_app.sh --provider anthropic   # skip local LLM entirely
#
# Options:
#   --region REGION   Region to serve: singapore|japan|middleeast|europe|gulf
#                     (default: singapore)
#   --model MODEL     mlx-community model ID or local path (overrides LLM_MODEL)
#   --provider NAME   LLM provider: openai (mlx-lm) | anthropic (overrides LLM_PROVIDER)
#   --port PORT       Port for uvicorn (default: 8000)
#   --llm-port PORT   Port for mlx-lm server (default: 8080)
#   --no-llm          Skip starting mlx-lm server (assumes it is already running)
#   --help            Show this message

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Defaults ──────────────────────────────────────────────────────────────────
PORT="${PORT:-8000}"
LLM_PORT="${LLM_PORT:-8080}"
REGION="singapore"
START_LLM=true

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --region)   REGION="$2";             shift 2 ;;
    --model)    export LLM_MODEL="$2";   shift 2 ;;
    --provider) export LLM_PROVIDER="$2"; shift 2 ;;
    --port)     PORT="$2";               shift 2 ;;
    --llm-port) LLM_PORT="$2";           shift 2 ;;
    --no-llm)   START_LLM=false;         shift   ;;
    --help|-h)
      sed -n '/^# Usage/,/^[^#]/{ /^[^#]/d; s/^# \{0,2\}//; p }' "$0"
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Validate region and set DB path ───────────────────────────────────────────
case "${REGION}" in
  singapore)  DB_FILENAME="singapore.duckdb" ;;
  japan)      DB_FILENAME="japansea.duckdb" ;;
  middleeast) DB_FILENAME="middleeast.duckdb" ;;
  europe)     DB_FILENAME="europe.duckdb" ;;
  gulf)       DB_FILENAME="gulf.duckdb" ;;
  *)
    echo "Error: unknown region '${REGION}'." >&2
    echo "Valid regions: singapore, japan, middleeast, europe, gulf" >&2
    exit 1
    ;;
esac

# ── Load .env ─────────────────────────────────────────────────────────────────
if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -o allexport
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +o allexport
fi

# ── Resolve data directory ─────────────────────────────────────────────────────
# Canonical location: ~/.arktrace/data  (override with ARKTRACE_DATA_DIR)
DATA_DIR="${ARKTRACE_DATA_DIR:-${HOME}/.arktrace/data}"
export ARKTRACE_DATA_DIR="${DATA_DIR}"
export ARKTRACE_REGION="${REGION}"
export DB_PATH="${DATA_DIR}/${DB_FILENAME}"

# ── Check local data ──────────────────────────────────────────────────────────
WATCHLIST="${DATA_DIR}/candidate_watchlist.parquet"
if [[ ! -f "${WATCHLIST}" ]]; then
  echo "⬇️  Local data not found for region '${REGION}' (${DATA_DIR})."
  echo "   Pulling from R2…"
  uv run python "${SCRIPT_DIR}/sync_r2.py" pull --region "${REGION}" --data-dir "${DATA_DIR}"
  echo ""
fi

# ── Start mlx-lm server ───────────────────────────────────────────────────────
PROVIDER="${LLM_PROVIDER:-openai}"
MODEL="${LLM_MODEL:-mlx-community/Qwen2.5-7B-Instruct-4bit}"

if [[ "${START_LLM}" == true && "${PROVIDER}" != "anthropic" ]]; then
  if ! uv run python -c "import mlx_lm" 2>/dev/null; then
    echo "⬇️  mlx-lm not found. Installing…"
    uv pip install mlx-lm
  fi

  echo "🤖 Starting mlx-lm server on port ${LLM_PORT}…"
  echo "   Model: ${MODEL}"
  uv run mlx_lm.server \
    --model "${MODEL}" \
    --port "${LLM_PORT}" \
    &
  MLX_PID=$!
  echo "   mlx-lm PID: ${MLX_PID}"
  echo "   Waiting for server to be ready…"
  for i in $(seq 1 30); do
    if curl -sf "http://localhost:${LLM_PORT}/v1/models" > /dev/null 2>&1; then
      echo "   ✅ mlx-lm ready → http://localhost:${LLM_PORT}/v1"
      break
    fi
    sleep 2
    if [[ $i -eq 30 ]]; then
      echo "   ⚠️  mlx-lm did not respond in 60s — dashboard will start anyway"
    fi
  done
  echo ""

  # Point the dashboard at the local mlx-lm server
  export LLM_PROVIDER="openai"
  export LLM_BASE_URL="http://localhost:${LLM_PORT}/v1"
  export LLM_API_KEY="${LLM_API_KEY:-local}"
  export LLM_MODEL="${MODEL}"

  # Shut down mlx-lm when the script exits
  trap 'echo ""; echo "Stopping mlx-lm (PID ${MLX_PID})…"; kill "${MLX_PID}" 2>/dev/null || true' EXIT
fi

# ── Print config summary ───────────────────────────────────────────────────────
echo "🚀 Starting dashboard"
echo "   Region        = ${REGION}"
echo "   Data dir      = ${DATA_DIR}"
echo "   DB_PATH       = ${DB_PATH}"
echo "   LLM_PROVIDER  = ${LLM_PROVIDER:-openai}"
echo "   LLM_BASE_URL  = ${LLM_BASE_URL:-http://localhost:${LLM_PORT}/v1}"
echo "   LLM_MODEL     = ${LLM_MODEL:-${MODEL}}"
echo "   Dashboard     → http://localhost:${PORT}"
echo ""

# ── Run dashboard ──────────────────────────────────────────────────────────────
cd "${REPO_ROOT}"
exec uv run uvicorn src.api.main:app \
  --host 0.0.0.0 \
  --port "${PORT}" \
  --reload
