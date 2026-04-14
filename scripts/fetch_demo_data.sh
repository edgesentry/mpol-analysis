#!/usr/bin/env bash
# fetch_demo_data.sh — download the arktrace demo bundle from R2 (no credentials required).
#
# Downloads candidate_watchlist.parquet, composite_scores.parquet,
# causal_effects.parquet, and validation_metrics.json.
#
# Usage:
#   bash scripts/fetch_demo_data.sh [--region REGION]
#
#   REGION: singapore (default), japan, middleeast, europe, gulf
#
# Data directory resolution (first match wins):
#   1. ARKTRACE_DATA_DIR env var
#   2. data/processed/ if it exists under the current directory (repo-local dev)
#   3. ~/.arktrace/data/  (standard user-level install location)
#
# Requirements: uv must be installed (https://docs.astral.sh/uv/)
# No R2 credentials needed — the demo bundle is publicly accessible.

set -euo pipefail

REGION="${ARKTRACE_REGION:-singapore}"

# Parse --region flag
while [[ $# -gt 0 ]]; do
  case "$1" in
    --region) REGION="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

cd "$(dirname "$0")/.."

# Canonical location: ~/.arktrace/data  (override with ARKTRACE_DATA_DIR)
DATA_DIR="${ARKTRACE_DATA_DIR:-${HOME}/.arktrace/data}"

echo "Region: ${REGION}"
echo "Fetching arktrace demo data from R2 → ${DATA_DIR}"
ARKTRACE_REGION="${REGION}" uv run python scripts/sync_r2.py pull-demo --data-dir "${DATA_DIR}"

echo ""
echo "Start the dashboard:"
echo "  ARKTRACE_REGION=${REGION} uv run uvicorn src.api.main:app --reload"
echo "  open http://localhost:8000"
