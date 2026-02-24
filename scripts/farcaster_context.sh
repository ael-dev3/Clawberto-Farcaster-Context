#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATE="$(date +%Y-%m-%d)"
OUTPUT_DIR="${REPO_DIR}/data"
mkdir -p "$OUTPUT_DIR"

ARGS=(
  --source snapchain
  --collect-last-hours 24
  --timezone "${OPEN_CLAW_TIMEZONE:-UTC}"
  --query "${FC_CONTEXT_QUERY:-*}"
  --final-top-posts "${FC_CONTEXT_TOP_POSTS:-12}"
  --final-comments-per-post "${FC_CONTEXT_COMMENTS_PER_POST:-3}"
  --final-snippet-length "${FC_CONTEXT_SNIPPET_LEN:-180}"
  --exclude-themes "${FC_CONTEXT_EXCLUDE_THEMES:-}"
  --exclude-empty-records
  --exclude-promo-records
  --exclude-gm-gn-records
  --focus-themes "${FC_CONTEXT_FOCUS_THEMES:-}"
  --output "$OUTPUT_DIR/farcaster_${DATE}.txt"
  --instructions-output "$OUTPUT_DIR/llm_instructions_${DATE}.txt"
  --readable-output "$OUTPUT_DIR/farcaster_${DATE}_readable.txt"
  --final-output "$OUTPUT_DIR/farcaster_${DATE}_final.txt"
)

if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  echo "Usage: scripts/farcaster_context.sh [extra args]"
  echo "Passes through extra args to farcaster_daily_scraper.py."
  exit 0
fi

python3 "$REPO_DIR/scripts/farcaster_daily_scraper.py" "${ARGS[@]}" "$@"

echo "
Latest final context:"
tail -n 120 "$OUTPUT_DIR/farcaster_${DATE}_final.txt"
