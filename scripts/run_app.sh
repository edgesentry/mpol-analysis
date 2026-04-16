#!/usr/bin/env bash
# scripts/run_app.sh
#
# Start arktrace natively (no Docker) on macOS, Linux, or Windows (Git Bash / WSL2).
#
#   • Data is read from local disk (pulled from R2 automatically if not present)
#   • llama-server (llama.cpp) runs as a local OpenAI-compatible server
#   • Dashboard runs natively on the host — connects to llama-server
#
# Prerequisites (one-time):
#   Python 3.12+ and uv: https://docs.astral.sh/uv/getting-started/installation/
#   llama.cpp (for analyst briefs — optional, dashboard works without it):
#     macOS:          brew install llama.cpp
#     Linux / WSL2:   see https://github.com/ggml-org/llama.cpp/releases/latest
#     Windows native: use Docker instead — see docs/deployment.md
#
# Usage:
#   bash scripts/run_app.sh
#   bash scripts/run_app.sh --region japan
#   bash scripts/run_app.sh --model bartowski/Qwen2.5-7B-Instruct-GGUF
#   bash scripts/run_app.sh --provider anthropic   # use Anthropic API for briefs
#   bash scripts/run_app.sh --no-llm               # skip llama-server entirely
#
# Options:
#   --region REGION   singapore|japan|middleeast|europe|persiangulf  (default: singapore)
#   --model MODEL     HuggingFace repo (bartowski/...) or local .gguf path
#   --gguf-file FILE  GGUF filename within the HF repo (default: Qwen2.5-7B-Instruct-Q4_K_M.gguf)
#   --provider NAME   openai (llama-server, default) | anthropic
#   --port PORT       uvicorn port (default: 8000)
#   --llm-port PORT   llama-server port (default: 8080)
#   --no-llm          Skip llama-server (briefs show placeholder)
#   --help            Show this message

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Detect OS ─────────────────────────────────────────────────────────────────
OS="$(uname -s)"
case "${OS}" in
  Darwin*)  OS_NAME="macOS" ;;
  Linux*)   OS_NAME="Linux" ;;
  MINGW*|MSYS*|CYGWIN*) OS_NAME="Windows (Git Bash)" ;;
  *)        OS_NAME="${OS}" ;;
esac

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

# ── Validate region ───────────────────────────────────────────────────────────
case "${REGION}" in
  singapore)  DB_FILENAME="singapore.duckdb" ;;
  japan)      DB_FILENAME="japansea.duckdb" ;;
  middleeast) DB_FILENAME="middleeast.duckdb" ;;
  europe)     DB_FILENAME="europe.duckdb" ;;
  persiangulf)   DB_FILENAME="persiangulf.duckdb" ;;
  gulfofguinea)  DB_FILENAME="gulfofguinea.duckdb" ;;
  gulfofaden)    DB_FILENAME="gulfofaden.duckdb" ;;
  gulfofmexico)  DB_FILENAME="gulfofmexico.duckdb" ;;
  *)
    echo "Error: unknown region '${REGION}'." >&2
    echo "Valid regions: singapore, japan, middleeast, europe, persiangulf, gulfofguinea, gulfofaden, gulfofmexico" >&2
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
LLM_PID=""

if [[ "${START_LLM}" == true && "${PROVIDER}" != "anthropic" ]]; then
  if ! command -v llama-server &>/dev/null; then
    echo "ℹ️  llama-server not found — analyst briefs will show a placeholder."
    echo "   To enable briefs, install llama.cpp:"
    case "${OS_NAME}" in
      macOS)
        echo "     brew install llama.cpp"
        echo "   Then re-run: bash scripts/run_app.sh"
        ;;
      "Linux")
        echo "     Download the pre-built binary for your arch from:"
        echo "     https://github.com/ggml-org/llama.cpp/releases/latest"
        echo "     e.g. llama-<tag>-bin-ubuntu-x64.zip  →  unzip, add to PATH"
        echo "   Or use Docker: docker run -p 8000:8000 -v arktrace-data:/root/.arktrace/data ghcr.io/edgesentry/arktrace:latest"
        ;;
      "Windows (Git Bash)")
        echo "     Download the Windows binary from:"
        echo "     https://github.com/ggml-org/llama.cpp/releases/latest"
        echo "     e.g. llama-<tag>-bin-win-avx2-x64.zip  →  unzip, add to PATH"
        echo "   Or use Docker (recommended on Windows):"
        echo "     docker run -p 8000:8000 -v arktrace-data:/root/.arktrace/data ghcr.io/edgesentry/arktrace:latest"
        ;;
    esac
    echo ""
    START_LLM=false
  fi

  if [[ "${START_LLM}" == true ]]; then
    echo "🤖 Starting llama-server on port ${LLM_PORT}…"
    echo "   Model: ${HF_MODEL} (${GGUF_FILE})"

    # macOS: Metal GPU acceleration. Linux/Windows: CPU (add --n-gpu-layers 99
    # with NVIDIA CUDA build of llama.cpp for GPU acceleration on Linux).
    GPU_LAYERS=99
    if [[ "${OS_NAME}" == "Linux" || "${OS_NAME}" == "Windows (Git Bash)" ]]; then
      GPU_LAYERS=0
    fi

    llama-server \
      --hf-repo "${HF_MODEL}" \
      --hf-file "${GGUF_FILE}" \
      --port "${LLM_PORT}" \
      --ctx-size 4096 \
      --n-gpu-layers "${GPU_LAYERS}" \
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
      # Verify /v1/chat/completions is available — older llama.cpp returns 404.
      HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST "http://localhost:${LLM_PORT}/v1/chat/completions" \
        -H "Content-Type: application/json" \
        -d '{"model":"test","messages":[{"role":"user","content":"ping"}],"max_tokens":1}' \
        2>/dev/null || echo "000")
      if [[ "${HTTP_STATUS}" == "404" ]]; then
        echo ""
        echo "   ❌ llama-server returned 404 for /v1/chat/completions — llama.cpp is outdated."
        case "${OS_NAME}" in
          macOS)   echo "      Upgrade: brew upgrade llama.cpp" ;;
          "Linux") echo "      Download the latest release from: https://github.com/ggml-org/llama.cpp/releases/latest" ;;
        esac
        echo ""
        kill "${LLM_PID}" 2>/dev/null || true
        exit 1
      fi
      echo "   ✅ llama-server ready → http://localhost:${LLM_PORT}/v1"
    fi
    echo ""

    export LLM_PROVIDER="openai"
    export LLM_BASE_URL="http://localhost:${LLM_PORT}/v1"
    export LLM_API_KEY="${LLM_API_KEY:-local}"
    export LLM_MODEL="${GGUF_FILE}"
  fi
fi

# ── Shutdown handler ──────────────────────────────────────────────────────────
_CLEANED_UP=false

_kill_with_timeout() {
  local pid="$1" timeout="${2:-3}"
  kill "${pid}" 2>/dev/null || return 0
  for _ in $(seq 1 "${timeout}"); do
    sleep 1
    kill -0 "${pid}" 2>/dev/null || return 0
  done
  kill -9 "${pid}" 2>/dev/null || true
}

_cleanup() {
  [[ "${_CLEANED_UP}" == true ]] && return
  _CLEANED_UP=true
  echo ""
  echo "Shutting down…"
  if [[ -n "${UVICORN_PID:-}" ]]; then
    echo "  Stopping uvicorn (PID ${UVICORN_PID})…"
    _kill_with_timeout "${UVICORN_PID}" 3
  fi
  WORKERS=$(pgrep -f "uvicorn src.api.main:app" 2>/dev/null || true)
  VENV_WORKERS=$(pgrep -f "${REPO_ROOT}/.venv/bin/python.*multiprocessing" 2>/dev/null || true)
  for PID in ${WORKERS} ${VENV_WORKERS}; do
    kill -9 "${PID}" 2>/dev/null || true
  done
  if [[ -n "${LLM_PID:-}" ]]; then
    echo "  Stopping llama-server (PID ${LLM_PID})…"
    _kill_with_timeout "${LLM_PID}" 5
  fi
  echo "Done."
}
trap '_cleanup' EXIT INT TERM

# ── Print config summary ──────────────────────────────────────────────────────
echo "🚀 Starting dashboard (${OS_NAME})"
echo "   Region        = ${REGION}"
echo "   Data dir      = ${DATA_DIR}"
echo "   DB_PATH       = ${DB_PATH}"
echo "   LLM_PROVIDER  = ${LLM_PROVIDER:-none (briefs disabled)}"
if [[ -n "${LLM_BASE_URL:-}" ]]; then
echo "   LLM_BASE_URL  = ${LLM_BASE_URL}"
fi
echo "   Dashboard     → http://localhost:${PORT}"
echo "   Press Ctrl+C to stop."
echo ""

# ── Run dashboard ─────────────────────────────────────────────────────────────
cd "${REPO_ROOT}"
uv run uvicorn src.api.main:app \
  --host 0.0.0.0 \
  --port "${PORT}" \
  --reload &
UVICORN_PID=$!
wait "${UVICORN_PID}"
