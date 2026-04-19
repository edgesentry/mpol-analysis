#!/usr/bin/env bash
# scripts/run_llama.sh
#
# Start llama-server (llama.cpp) as a local OpenAI-compatible endpoint so the
# arktrace SPA (app/) can generate analyst briefs without a cloud API.
#
# The SPA calls: http://localhost:8080/v1/chat/completions
#
# Prerequisites (one-time):
#   macOS:          brew install llama.cpp caddy
#   Linux / WSL2:   llama.cpp — download pre-built binary from:
#                   https://github.com/ggml-org/llama.cpp/releases/latest
#                   caddy — see docs/local-llm-setup.md (apt/dnf packages)
#   Windows:        llama.cpp — download pre-built binary from:
#                   https://github.com/ggml-org/llama.cpp/releases/latest
#                   e.g. llama-<tag>-bin-win-avx2-x64.zip  →  unzip, add to PATH
#                   caddy — winget install Caddy.Caddy
#
# Usage:
#   bash scripts/run_llama.sh
#   bash scripts/run_llama.sh --model bartowski/Qwen2.5-7B-Instruct-GGUF
#   bash scripts/run_llama.sh --gguf-file Qwen2.5-7B-Instruct-Q4_K_M.gguf
#   bash scripts/run_llama.sh --port 8080
#
# Options:
#   --model MODEL     HuggingFace repo (default: bartowski/Qwen2.5-7B-Instruct-GGUF)
#   --gguf-file FILE  GGUF filename within the HF repo (default: Qwen2.5-7B-Instruct-Q4_K_M.gguf)
#   --port PORT       llama-server port (default: 8080)
#   --help            Show this message

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
HF_MODEL="bartowski/Qwen2.5-7B-Instruct-GGUF"
GGUF_FILE="Qwen2.5-7B-Instruct-Q4_K_M.gguf"
PORT="${LLM_PORT:-8080}"

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --model)     HF_MODEL="$2"; shift 2 ;;
    --gguf-file) GGUF_FILE="$2"; shift 2 ;;
    --port)      PORT="$2";     shift 2 ;;
    --help|-h)
      sed -n '/^# Usage/,/^[^#]/{ /^[^#]/d; s/^# \{0,2\}//; p }' "$0"
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Detect OS ─────────────────────────────────────────────────────────────────
OS="$(uname -s)"
case "${OS}" in
  Darwin*)             OS_NAME="macOS" ;;
  Linux*)              OS_NAME="Linux" ;;
  MINGW*|MSYS*|CYGWIN*) OS_NAME="Windows (Git Bash)" ;;
  *)                   OS_NAME="${OS}" ;;
esac

# ── Check llama-server installed ───────────────────────────────────────────────
if ! command -v llama-server &>/dev/null; then
  echo "❌  llama-server not found. Install llama.cpp first:"
  case "${OS_NAME}" in
    macOS)
      echo "    brew install llama.cpp"
      ;;
    "Linux")
      echo "    Download the pre-built binary for your arch from:"
      echo "    https://github.com/ggml-org/llama.cpp/releases/latest"
      echo "    e.g. llama-<tag>-bin-ubuntu-x64.zip  →  unzip, add to PATH"
      ;;
    "Windows (Git Bash)")
      echo "    Download the Windows binary from:"
      echo "    https://github.com/ggml-org/llama.cpp/releases/latest"
      echo "    e.g. llama-<tag>-bin-win-avx2-x64.zip  →  unzip, add to PATH"
      ;;
  esac
  exit 1
fi

# ── GPU layers ────────────────────────────────────────────────────────────────
# macOS: Metal GPU acceleration. Linux/Windows: CPU by default.
# Add --n-gpu-layers 99 with NVIDIA CUDA build of llama.cpp for GPU on Linux.
GPU_LAYERS=99
if [[ "${OS_NAME}" == "Linux" || "${OS_NAME}" == "Windows (Git Bash)" ]]; then
  GPU_LAYERS=0
fi

# ── Start llama-server ─────────────────────────────────────────────────────────
echo "🤖 Starting llama-server (${OS_NAME})"
echo "   Model     = ${HF_MODEL} (${GGUF_FILE})"
echo "   Endpoint  → http://localhost:${PORT}/v1/chat/completions"
echo "   Press Ctrl+C to stop."
echo ""

llama-server \
  --hf-repo "${HF_MODEL}" \
  --hf-file "${GGUF_FILE}" \
  --port "${PORT}" \
  --ctx-size 4096 \
  --n-gpu-layers "${GPU_LAYERS}" \
  &
LLM_PID=$!

# ── Wait for readiness ─────────────────────────────────────────────────────────
echo "   Waiting for server to be ready…"
for i in $(seq 1 30); do
  if curl -sf "http://localhost:${PORT}/v1/models" > /dev/null 2>&1; then
    # Verify /v1/chat/completions is available — older llama.cpp returns 404
    HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
      -X POST "http://localhost:${PORT}/v1/chat/completions" \
      -H "Content-Type: application/json" \
      -d '{"model":"test","messages":[{"role":"user","content":"ping"}],"max_tokens":1}' \
      2>/dev/null || echo "000")
    if [[ "${HTTP_STATUS}" == "404" ]]; then
      echo ""
      echo "   ❌ llama-server returned 404 for /v1/chat/completions — llama.cpp is outdated."
      case "${OS_NAME}" in
        macOS)   echo "      Upgrade: brew upgrade llama.cpp" ;;
        "Linux") echo "      Download latest: https://github.com/ggml-org/llama.cpp/releases/latest" ;;
      esac
      kill "${LLM_PID}" 2>/dev/null || true
      exit 1
    fi
    echo "   ✅ llama-server ready → http://localhost:${PORT}/v1"

    # ── Caddy HTTPS proxy ─────────────────────────────────────────────────────
    # The SPA defaults to https://localhost:8443 so Safari (which blocks
    # HTTPS→HTTP) and Chrome both work without any VITE_LLM_ENDPOINT override.
    # Caddy adds its local CA to the macOS keychain on first run; Safari will
    # prompt once to accept the cert — after that it's seamless.
    HTTPS_PORT=$((PORT + 363))   # 8080 → 8443
    CADDY_PID=""
    if command -v caddy &>/dev/null; then
      caddy reverse-proxy \
        --from "localhost:${HTTPS_PORT}" \
        --to   "localhost:${PORT}" \
        > /tmp/caddy-llama.log 2>&1 &
      CADDY_PID=$!
      sleep 1
      if kill -0 "${CADDY_PID}" 2>/dev/null; then
        echo "   ✅ Caddy HTTPS proxy   → https://localhost:${HTTPS_PORT}/v1"
        echo "      (Safari: accept the Caddy local-CA cert on first visit)"
      else
        echo "   ❌ Caddy failed to start — local LLM will be offline in Safari"
        echo "      Check /tmp/caddy-llama.log for details"
        echo "      Install: brew install caddy"
        CADDY_PID=""
      fi
    else
      echo "   ❌ caddy not found — local LLM will be offline in Safari"
      echo "      Install: brew install caddy"
    fi

    break
  fi
  sleep 2
  if [[ $i -eq 30 ]]; then
    echo "   ⚠️  llama-server did not respond in 60s — check logs above"
  fi
done

echo ""

# ── Shutdown handler ───────────────────────────────────────────────────────────
_cleanup() {
  echo ""
  echo "Shutting down llama-server…"
  kill "${LLM_PID}" 2>/dev/null || true
  [[ -n "${CADDY_PID}" ]] && kill "${CADDY_PID}" 2>/dev/null || true
  echo "Done."
}
trap '_cleanup' EXIT INT TERM

wait "${LLM_PID}"
