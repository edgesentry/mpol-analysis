#!/usr/bin/env bash
# scripts/run_app.sh
#
# Start arktrace in native macOS dev mode.
#
#   • Data is read from local disk (pull from R2 first if not present)
#   • llama-server (llama.cpp) runs as a local OpenAI-compatible server
#   • Dashboard runs natively on the host — connects to the llama-server
#
# Prerequisites (one-time):
#   Install llama.cpp: https://github.com/ggml-org/llama.cpp/blob/master/docs/install.md
#   macOS (Homebrew): brew install llama.cpp
#
# Usage:
#   bash scripts/run_app.sh
#   bash scripts/run_app.sh --region japan
#   bash scripts/run_app.sh --model bartowski/Qwen2.5-7B-Instruct-GGUF
#   bash scripts/run_app.sh --provider anthropic   # skip local LLM entirely
#
# Options:
#   --region REGION   Region to serve: singapore|japan|middleeast|europe|gulf
#                     (default: singapore)
#   --model MODEL     HuggingFace repo (bartowski/...) or local .gguf path (overrides LLM_MODEL)
#   --gguf-file FILE  GGUF filename within the HF repo (default: Qwen2.5-7B-Instruct-Q4_K_M.gguf)
#   --provider NAME   LLM provider: openai (llama-server) | anthropic (overrides LLM_PROVIDER)
#   --port PORT       Port for uvicorn (default: 8000)
#   --llm-port PORT   Port for llama-server (default: 8080)
#   --no-llm          Skip starting llama-server (assumes it is already running)
#   --help            Show this message

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Defaults ──────────────────────────────────────────────────────────────────
PORT="${PORT:-8000}"
LLM_PORT="${LLM_PORT:-8080}"
REGION="singapore"
START_LLM=true
HF_MODEL="bartowski/Qwen2.5-7B-Instruct-GGUF"
GGUF_FILE="Qwen2.5-7B-Instruct-Q4_K_M.gguf"

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --region)    REGION="$2";              shift 2 ;;
    --model)     HF_MODEL="$2";            shift 2 ;;
    --gguf-file) GGUF_FILE="$2";           shift 2 ;;
    --provider)  export LLM_PROVIDER="$2"; shift 2 ;;
    --port)      PORT="$2";                shift 2 ;;
    --llm-port)  LLM_PORT="$2";            shift 2 ;;
    --no-llm)    START_LLM=false;          shift   ;;
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

# ── Start llama-server ────────────────────────────────────────────────────────
PROVIDER="${LLM_PROVIDER:-openai}"

if [[ "${START_LLM}" == true && "${PROVIDER}" != "anthropic" ]]; then
  if ! command -v llama-server &>/dev/null; then
    echo "❌ llama-server not found."
    echo ""
    echo "   Please install llama.cpp first:"
    echo "     macOS (Homebrew): brew install llama.cpp"
    echo "     Other platforms:  https://github.com/ggml-org/llama.cpp/blob/master/docs/install.md"
    echo ""
    echo "   Then re-run: bash scripts/run_app.sh"
    exit 1
  fi

  echo "🤖 Starting llama-server on port ${LLM_PORT}…"
  echo "   Model: ${HF_MODEL} (${GGUF_FILE})"
  llama-server \
    --hf-repo "${HF_MODEL}" \
    --hf-file "${GGUF_FILE}" \
    --port "${LLM_PORT}" \
    --ctx-size 4096 \
    --n-gpu-layers 99 \
    &
  LLM_PID=$!
  echo "   llama-server PID: ${LLM_PID}"
  echo "   Waiting for server to be ready…"
  LLAMA_READY=false
  for i in $(seq 1 30); do
    if curl -sf "http://localhost:${LLM_PORT}/v1/models" > /dev/null 2>&1; then
      LLAMA_READY=true
      break
    fi
    sleep 2
    if [[ $i -eq 30 ]]; then
      echo "   ⚠️  llama-server did not respond in 60s — dashboard will start anyway"
    fi
  done

  if [[ "${LLAMA_READY}" == true ]]; then
    # Verify /v1/chat/completions is available — older llama.cpp returns 404 here.
    HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
      -X POST "http://localhost:${LLM_PORT}/v1/chat/completions" \
      -H "Content-Type: application/json" \
      -d '{"model":"test","messages":[{"role":"user","content":"ping"}],"max_tokens":1}' \
      2>/dev/null || echo "000")
    if [[ "${HTTP_STATUS}" == "404" ]]; then
      echo ""
      echo "   ❌ llama-server is running but /v1/chat/completions returned 404."
      echo "      Your llama.cpp is outdated. Upgrade it and retry:"
      echo "        brew upgrade llama.cpp"
      echo "      Then re-run: bash scripts/run_app.sh"
      echo ""
      kill "${LLM_PID}" 2>/dev/null || true
      exit 1
    fi
    echo "   ✅ llama-server ready → http://localhost:${LLM_PORT}/v1"
  fi
  echo ""

  # Point the dashboard at the local llama-server
  export LLM_PROVIDER="openai"
  export LLM_BASE_URL="http://localhost:${LLM_PORT}/v1"
  export LLM_API_KEY="${LLM_API_KEY:-local}"
  export LLM_MODEL="${GGUF_FILE}"

  # Shut down llama-server when the script exits
  trap 'echo ""; echo "Stopping llama-server (PID ${LLM_PID})…"; kill "${LLM_PID}" 2>/dev/null || true' EXIT
fi

# ── Print config summary ───────────────────────────────────────────────────────
echo "🚀 Starting dashboard"
echo "   Region        = ${REGION}"
echo "   Data dir      = ${DATA_DIR}"
echo "   DB_PATH       = ${DB_PATH}"
echo "   LLM_PROVIDER  = ${LLM_PROVIDER:-openai}"
echo "   LLM_BASE_URL  = ${LLM_BASE_URL:-http://localhost:${LLM_PORT}/v1}"
echo "   LLM_MODEL     = ${LLM_MODEL:-${GGUF_FILE}}"
echo "   Dashboard     → http://localhost:${PORT}"
echo ""

# ── Run dashboard ──────────────────────────────────────────────────────────────
cd "${REPO_ROOT}"
exec uv run uvicorn src.api.main:app \
  --host 0.0.0.0 \
  --port "${PORT}" \
  --reload
