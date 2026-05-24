#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
EXTRA_ARGS=()

if [[ -n "${FC_CONTEXT_SITE_MAX_PAGES:-}" ]]; then
  EXTRA_ARGS+=(--max-pages "$FC_CONTEXT_SITE_MAX_PAGES")
fi
if [[ -n "${FC_CONTEXT_SITE_PAGE_SIZE:-}" ]]; then
  EXTRA_ARGS+=(--page-size "$FC_CONTEXT_SITE_PAGE_SIZE")
fi

if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  echo "Usage: scripts/farcaster_context_site.sh [extra scraper args]"
  echo "Runs the 24h Hypersnap scrape, then rebuilds generated site artifacts."
  echo "Optional env: FC_CONTEXT_SITE_MAX_PAGES, FC_CONTEXT_SITE_PAGE_SIZE."
  exit 0
fi

bash "$REPO_DIR/scripts/farcaster_context_24h.sh" "${EXTRA_ARGS[@]}" "$@"
python3 "$REPO_DIR/scripts/build_site.py"
