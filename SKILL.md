---
name: farcaster-context
icon: 🧠
description: "Generate Farcaster context snapshots (24h or 1h) for @clawberto with deterministic outputs and LLM-ready formatting."
---

# Farcaster Context Skill

This repo supports two operational modes:

- Run 24-hour rolling context: `bash scripts/farcaster_context_24h.sh`
- Run 1-hour rolling context: `bash scripts/farcaster_context_last_hour.sh`

Both modes write timestamped files under `data/`:
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
