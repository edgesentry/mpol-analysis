#!/usr/bin/env bash
# fetch_demo_data.sh — download the arktrace demo bundle (no credentials required)
#
# Downloads candidate_watchlist.parquet, composite_scores.parquet,
# causal_effects.parquet, and validation_metrics.json from the public
# arktrace-public.edgesentry.io custom domain.
#
# Usage:
#   bash scripts/fetch_demo_data.sh [--force]
#
#   --force    Re-download even if data is already present
#
# Data directory resolution (first match wins):
#   1. ARKTRACE_DATA_DIR env var
#   2. ~/.arktrace/data/  (standard install location)
#
# Requirements: curl, unzip — both standard on macOS/Linux, no Python/uv needed.

set -euo pipefail

BASE_URL="https://arktrace-public.edgesentry.io"
DEMO_KEY="demo.zip"
DATA_DIR="${ARKTRACE_DATA_DIR:-${HOME}/.arktrace/data}"
FORCE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=1; shift ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "${FORCE}" == "0" ]] && [[ -f "${DATA_DIR}/candidate_watchlist.parquet" ]]; then
  echo "Demo data already present in ${DATA_DIR} — skipping download."
  echo "Run with --force to re-download."
  exit 0
fi

echo "Fetching arktrace demo data → ${DATA_DIR}"
mkdir -p "${DATA_DIR}"

TMP="$(mktemp /tmp/arktrace-demo-XXXXXX.zip)"
trap 'rm -f "${TMP}"' EXIT

curl -fsSL --progress-bar "${BASE_URL}/${DEMO_KEY}" -o "${TMP}"
unzip -o -q "${TMP}" -d "${DATA_DIR}"

echo "Done. Demo data ready in ${DATA_DIR}."
echo ""
echo "Start the dashboard:"
echo "  uv run uvicorn pipeline.src.api.main:app --reload"
echo "  open http://localhost:8000"
