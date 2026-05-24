---
name: farcaster-context
icon: 🧠
description: "Generate Farcaster context snapshots (24h or 1h) for @clawberto with deterministic outputs and LLM-ready formatting."
---

# Farcaster Context Skill

This repo supports two operational modes backed by direct Hypersnap/Snapchain node scraping:

- Run 24-hour rolling context: `bash scripts/farcaster_context_24h.sh`
- Run 1-hour rolling context: `bash scripts/farcaster_context_last_hour.sh`

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
