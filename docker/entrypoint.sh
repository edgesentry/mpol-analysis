#!/usr/bin/env bash
# docker/entrypoint.sh — container startup script
#
# 1. Pull demo data from R2 if not already present (no credentials needed).
# 2. Start llama-server in the background if a GGUF model is available.
# 3. Start uvicorn in the foreground.
#
# Environment variables (all optional):
#   ARKTRACE_REGION    Region to serve (default: singapore)
#   MODEL_DIR          Directory where GGUF model files are stored (default: /models)
#   LLM_MODEL          GGUF filename inside MODEL_DIR (default: Qwen2.5-7B-Instruct-Q4_K_M.gguf)
#   LLM_PORT           Port for llama-server (default: 8080)
#   LLM_PROVIDER       Set to "anthropic" to use the Anthropic API instead of llama-server
#   LLM_API_KEY        API key (for Anthropic or remote OpenAI-compatible endpoint)
#   AUTO_PULL          Set to "0" to disable automatic R2 data pull on startup

set -euo pipefail

REGION="${ARKTRACE_REGION:-singapore}"
DATA_DIR="${ARKTRACE_DATA_DIR:-/root/.arktrace/data}"
MODEL_DIR="${MODEL_DIR:-/models}"
GGUF_FILE="${LLM_MODEL:-Qwen2.5-7B-Instruct-Q4_K_M.gguf}"
LLM_PORT="${LLM_PORT:-8080}"
AUTO_PULL="${AUTO_PULL:-1}"

export ARKTRACE_REGION="${REGION}"
export ARKTRACE_DATA_DIR="${DATA_DIR}"

# ── 1. Pull demo data ──────────────────────────────────────────────────────────
if [[ "${AUTO_PULL}" != "0" ]] && [[ ! -f "${DATA_DIR}/candidate_watchlist.parquet" ]]; then
  echo "Pulling demo data from R2 (region: ${REGION})…"
  python scripts/sync_r2.py pull-demo --data-dir "${DATA_DIR}" || {
    echo "⚠️  Demo data pull failed — dashboard will start but may show no vessels."
    echo "   Check your network connection or set AUTO_PULL=0 to skip."
  }
fi

# ── 2. Start llama-server (optional) ──────────────────────────────────────────
LLM_PID=""
PROVIDER="${LLM_PROVIDER:-}"

if [[ "${PROVIDER}" != "anthropic" ]]; then
  MODEL_PATH="${MODEL_DIR}/${GGUF_FILE}"
  if command -v llama-server &>/dev/null && [[ -f "${MODEL_PATH}" ]]; then
    echo "Starting llama-server (CPU mode) on port ${LLM_PORT}…"
    echo "   Model: ${MODEL_PATH}"
    llama-server \
      --model "${MODEL_PATH}" \
      --port "${LLM_PORT}" \
      --host 0.0.0.0 \
      --ctx-size 4096 \
      --n-gpu-layers 0 \
      &
    LLM_PID=$!

    # Wait for llama-server to be ready (up to 60 s)
    for i in $(seq 1 30); do
      if curl -sf "http://localhost:${LLM_PORT}/v1/models" >/dev/null 2>&1; then
        echo "   llama-server ready."
        break
      fi
      sleep 2
    done

    export LLM_PROVIDER="openai"
    export LLM_BASE_URL="http://localhost:${LLM_PORT}/v1"
    export LLM_API_KEY="${LLM_API_KEY:-local}"
    export LLM_MODEL="${GGUF_FILE}"
  else
    echo "ℹ️  No GGUF model found at ${MODEL_PATH} — analyst briefs will show a placeholder."
    echo "   To enable briefs, mount a model volume (see docs/deployment.md)."
  fi
fi

# ── Shutdown handler ──────────────────────────────────────────────────────────
_cleanup() {
  if [[ -n "${LLM_PID:-}" ]]; then
    kill "${LLM_PID}" 2>/dev/null || true
  fi
}
trap '_cleanup' EXIT INT TERM

# ── 3. Start uvicorn ──────────────────────────────────────────────────────────
echo "Starting dashboard → http://0.0.0.0:8000"
exec uvicorn src.api.main:app --host 0.0.0.0 --port 8000
