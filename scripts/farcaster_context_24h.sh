#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TIMESTAMP="$(date -u +%Y-%m-%d_%Hh%M)"
DEFAULT_DATE="$(date -u +%Y-%m-%d)"
OUTPUT_DIR="${REPO_DIR}/data"
mkdir -p "$OUTPUT_DIR"

if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  echo "Usage: scripts/farcaster_context_24h.sh [extra args]"
  echo "Defaults: source=snapchain, collect-last-hours=24, timezone=UTC,"
  echo "themes excluded: general_chat,daily_greetings,empty by default."
  exit 0
fi

python3 "$REPO_DIR/scripts/farcaster_daily_scraper.py" \
  --source snapchain \
  --collect-last-hours 24 \
  --timezone "${OPEN_CLAW_TIMEZONE:-UTC}" \
  --query "${FC_CONTEXT_QUERY:-*}" \
  --final-top-posts "${FC_CONTEXT_TOP_POSTS:-12}" \
  --final-comments-per-post "${FC_CONTEXT_COMMENTS_PER_POST:-3}" \
  --final-snippet-length "${FC_CONTEXT_SNIPPET_LEN:-180}" \
  --exclude-themes "${FC_CONTEXT_EXCLUDE_THEMES:-general_chat,daily_greetings,empty}" \
  --exclude-empty-records \
  --exclude-promo-records \
  --exclude-gm-gn-records \
  --focus-themes "${FC_CONTEXT_FOCUS_THEMES:-}" \
  --output "$OUTPUT_DIR/farcaster_24h_${TIMESTAMP}.txt" \
  --instructions-output "$OUTPUT_DIR/llm_instructions_24h_${TIMESTAMP}.txt" \
  --readable-output "$OUTPUT_DIR/farcaster_24h_readable_${TIMESTAMP}.txt" \
  --final-output "$OUTPUT_DIR/farcaster_24h_final_${TIMESTAMP}.txt" \
  "$@"
