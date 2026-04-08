#!/usr/bin/env bash
# scripts/run_dev.sh
#
# Start arktrace in native macOS dev mode.
#
#   • Infra (MinIO) runs in Docker via docker-compose.infra.yml
#   • Dashboard runs natively on the host — gets Apple Metal / ANE acceleration
#     for local LLM inference (typically 5–10× faster than inside a Colima VM)
#
# Prerequisites (one-time):
#   CMAKE_ARGS="-DGGML_METAL=on" uv pip install llama-cpp-python --force-reinstall
#   uv run python scripts/download_model.py gemma-4-e4b-it
#
# Usage:
#   bash scripts/run_dev.sh
#   bash scripts/run_dev.sh --model ~/models/gemma-4-E2B-it-Q4_K_M.gguf
#   bash scripts/run_dev.sh --provider anthropic   # skip local LLM entirely
#
# Options:
#   --model PATH      Path to GGUF model file (overrides LLAMACPP_MODEL_PATH)
#   --provider NAME   LLM provider: llamacpp | anthropic | openai (overrides LLM_PROVIDER)
#   --port PORT       Port for uvicorn (default: 8000)
#   --no-infra        Skip starting Docker infra (assumes MinIO is already up)
#   --help            Show this message

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker-compose.infra.yml"

# ── Defaults ──────────────────────────────────────────────────────────────────
PORT="${PORT:-8000}"
START_INFRA=true

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --model)    export LLAMACPP_MODEL_PATH="$2"; shift 2 ;;
    --provider) export LLM_PROVIDER="$2";        shift 2 ;;
    --port)     PORT="$2";                       shift 2 ;;
    --no-infra) START_INFRA=false;               shift   ;;
    --help|-h)
      sed -n '/^# Usage/,/^[^#]/{ /^[^#]/d; s/^# \{0,2\}//; p }' "$0"
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Load .env (non-export, just source) ──────────────────────────────────────
if [[ -f "${REPO_ROOT}/.env" ]]; then
  # shellcheck disable=SC1091
  set -o allexport
  source "${REPO_ROOT}/.env"
  set +o allexport
fi

# ── Resolve python packages ────────────────────────────────────────────────────
if [[ "${LLM_PROVIDER:-llamacpp}" == "llamacpp" ]]; then
  if ! uv run python -c "import llama_cpp" 2>/dev/null; then
    echo "⬇️  llama-cpp-python not found. Compiling with Apple Metal support…"
    CMAKE_ARGS="-DGGML_METAL=on" uv pip install llama-cpp-python --force-reinstall
  fi
fi

# ── Resolve model path if not set ────────────────────────────────────────────
if [[ -z "${LLAMACPP_MODEL_PATH:-}" ]]; then
  DEFAULT_MODEL="${HOME}/.cache/arktrace/models/gemma-4-E4B-it-Q4_K_M.gguf"
  FALLBACK_MODEL="${HOME}/models/gemma-4-E4B-it-Q4_K_M.gguf"
  if [[ -f "${DEFAULT_MODEL}" ]]; then
    export LLAMACPP_MODEL_PATH="${DEFAULT_MODEL}"
  elif [[ -f "${FALLBACK_MODEL}" ]]; then
    export LLAMACPP_MODEL_PATH="${FALLBACK_MODEL}"
  elif [[ "${LLM_PROVIDER:-llamacpp}" == "llamacpp" ]]; then
    echo "⬇️  No GGUF model found. Downloading default model (gemma-4-e4b-it)…"
    uv run --with huggingface-hub python scripts/download_model.py gemma-4-e4b-it --dir "${HOME}/models"
    export LLAMACPP_MODEL_PATH="${HOME}/models/gemma-4-E4B-it-Q4_K_M.gguf"
  fi
fi

# ── Override S3 endpoint for host ────────────────────────────────────────────
# Inside Docker containers the hostname is 'minio'; on the host it's localhost.
export S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export S3_BUCKET="${S3_BUCKET:-arktrace}"

# ── Start infra ───────────────────────────────────────────────────────────────
if [[ "${START_INFRA}" == true ]]; then
  echo "🐳 Starting infra (MinIO)…"
  docker compose -f "${COMPOSE_FILE}" up -d
  echo "   MinIO console → http://localhost:9001  (minioadmin / minioadmin)"
  echo ""
fi

# ── Print config summary ──────────────────────────────────────────────────────
echo "🚀 Starting dashboard natively (Metal-accelerated LLM on Apple Silicon)"
echo "   LLM_PROVIDER     = ${LLM_PROVIDER:-llamacpp}"
echo "   LLAMACPP_MODEL   = ${LLAMACPP_MODEL_PATH:-<not set>}"
echo "   S3_ENDPOINT      = ${S3_ENDPOINT}"
echo "   Dashboard        → http://localhost:${PORT}"
echo ""

# ── Run dashboard ─────────────────────────────────────────────────────────────
cd "${REPO_ROOT}"
exec uv run uvicorn src.api.main:app \
  --host 0.0.0.0 \
  --port "${PORT}" \
  --reload
