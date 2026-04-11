#!/usr/bin/env bash
# scripts/stop_app.sh — kill the arktrace dashboard and its infra
#
# Usage:
#   bash scripts/stop_app.sh           # stop uvicorn + MinIO containers
#   bash scripts/stop_app.sh --infra   # stop MinIO containers only
#   bash scripts/stop_app.sh --app     # stop uvicorn only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker-compose.infra.yml"

STOP_APP=true
STOP_INFRA=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --app)   STOP_INFRA=false; shift ;;
    --infra) STOP_APP=false;   shift ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Stop uvicorn + its multiprocessing workers ────────────────────────────────
if [[ "${STOP_APP}" == true ]]; then
  PIDS=$(pgrep -f "uvicorn src.api.main:app" 2>/dev/null || true)
  if [[ -n "${PIDS}" ]]; then
    echo "🛑 Stopping uvicorn (PIDs: ${PIDS})…"
    kill ${PIDS}
    # Wait up to 5 s for graceful shutdown, then force-kill
    for i in $(seq 1 10); do
      sleep 0.5
      REMAINING=$(pgrep -f "uvicorn src.api.main:app" 2>/dev/null || true)
      [[ -z "${REMAINING}" ]] && break
      if [[ ${i} -eq 10 ]]; then
        echo "   Force-killing uvicorn…"
        kill -9 ${REMAINING} 2>/dev/null || true
      fi
    done
    echo "   uvicorn stopped."
  else
    echo "   uvicorn is not running."
  fi

  # ── Stop mlx-lm server ─────────────────────────────────────────────────────
  MLX_PIDS=$(pgrep -f "mlx_lm.server" 2>/dev/null || true)
  if [[ -n "${MLX_PIDS}" ]]; then
    echo "🤖 Stopping mlx-lm server (PIDs: ${MLX_PIDS})…"
    kill ${MLX_PIDS}
    sleep 1
    # Force kill if still running
    REMAINING_MLX=$(pgrep -f "mlx_lm.server" 2>/dev/null || true)
    if [[ -n "${REMAINING_MLX}" ]]; then
      echo "   Force-killing mlx-lm…"
      kill -9 ${REMAINING_MLX} 2>/dev/null || true
    fi
    echo "   mlx-lm stopped."
  fi

  # Kill any multiprocessing worker processes spawned by the venv's python
  # (these hold the DuckDB lock and survive the uvicorn parent being killed)
  pkill -9 -f "${REPO_ROOT}/.venv/bin/python" 2>/dev/null || true
fi

# ── Stop infra ────────────────────────────────────────────────────────────────
if [[ "${STOP_INFRA}" == true ]] && [[ -f "${COMPOSE_FILE}" ]]; then
  echo "🐳 Stopping infra (MinIO)…"
  docker compose -f "${COMPOSE_FILE}" down
  echo "   MinIO stopped."
fi
