---
name: farcaster-context
icon: 🧠
description: "Generate Farcaster context snapshots (24h or 1h) for @clawberto with deterministic outputs and LLM-ready formatting."
---

# Farcaster Context Skill

This repo supports two operational modes backed by direct Hypersnap/Snapchain node scraping, plus a GitHub Pages context dashboard:

- Run 24-hour rolling context: `bash scripts/farcaster_context_24h.sh`
- Run 1-hour rolling context: `bash scripts/farcaster_context_last_hour.sh`
- Rebuild the static context site: `npm run refresh` (scrape + `scripts/build_site.py`)
- Live site: https://ael-dev3.github.io/Clawberto-Farcaster-Context/

Both modes talk to `/v1/info` and `/v1/events`, default to `--source hypersnap`, auto-discover shards with `--snapchain-shards auto`, and write timestamped files under `data/`:
- raw scored output
- readable ranking
- LLM instruction pack
- compact final context

Example:

```bash
bash scripts/farcaster_context_24h.sh
bash scripts/farcaster_context_last_hour.sh
```

Use `--help` on any script for extra flags.

Configure custom nodes with `--hub-urls` or `HYPERSNAP_NODE_URLS`, for example:

```bash
HYPERSNAP_NODE_URLS="http://node-a:3381,http://node-b:3381" bash scripts/farcaster_context_24h.sh
```

## Site workflow

Use the site workflow when the user asks for a hosted/readable Farcaster context page:

```bash
npm ci
npm run refresh
npm run build
```

`npm run refresh` runs the full 24h scrape, parses the latest `data/farcaster_24h_*.txt` and final context into `generated/` + `public/generated/`, rewrites `index.html` and refreshes the README snapshot/link. The dashboard style is intentionally dark purple, table-first, with linkable Farcaster usernames and cast URLs.
