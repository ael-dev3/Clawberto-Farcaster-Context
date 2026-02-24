---
name: farcaster-context
icon: 🧠
description: "Generate daily Farcaster context + top posts/comments summaries for @clawberto with deterministic outputs and LLM-ready formatting."
---

# Farcaster Context Skill

Use this skill to pull recent Farcaster activity, score it, and generate:
- raw scored dump (`farcaster_YYYY-MM-DD.txt`)
- LLM instruction pack (`llm_instructions_YYYY-MM-DD.txt`)
- readable ranked report (`farcaster_YYYY-MM-DD_readable.txt`)
- compact final context (`farcaster_YYYY-MM-DD_final.txt`)

## What it does

`farcaster_daily_scraper.py` collects posts/comments for a target day or rolling window, scores each cast by engagement, deduplicates/ranks signal, then emits machine-readable text artifacts.

## Default run (24h snapshot)

From repo root:

```bash
python3 scripts/farcaster_daily_scraper.py \
  --source snapchain \
  --collect-last-hours 24 \
  --query "*"
```

This defaults to timezone-local day semantics and writes files under `data/`.

## Typical skill workflows

- **Fresh context now (last 24h):**
```bash
python3 scripts/farcaster_context.sh
```
- **Rolling window by hours (example 12h, UTC):**
```bash
python3 scripts/farcaster_context.sh --collect-last-hours 12 --timezone UTC
```
- **Specific day (for backfill):**
```bash
python3 scripts/farcaster_daily_scraper.py --date 2026-02-24
```
- **Neynar mode (if needed):**
```bash
export NEYNAR_API_KEY=... && python3 scripts/farcaster_daily_scraper.py --source neynar
```

## Guardrails

- Do not pass sensitive API keys inline in chat.
- Prefer `snapchain` source by default (no key required).
- Keep default output filenames unless user requested custom paths.
