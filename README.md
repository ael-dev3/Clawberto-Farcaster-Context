# Farcaster Daily Scraper

Production-style Farcaster scraper that:
- pulls posts and comments for a target day,
- scores each cast from 1-10 using engagement-weighted signals,
- writes date-/time-stamped `.txt` outputs,
- and emits LLM-ready summaries.

Supports two modes:
- **24-hour rolling context**
- **1-hour rolling context**

## Setup

```bash
python -m pip install -r requirements.txt
```

Default source is direct Hypersnap/Snapchain node ingestion (no API key required).
The scraper talks to the node HTTP API (`/v1/info`, `/v1/events`, and username lookup endpoints), auto-discovers available shards from `/v1/info`, and fails over across multiple node URLs when configured.

Node URL configuration, in priority order:

```bash
# CLI, single or comma-separated list
python3 scripts/farcaster_daily_scraper.py --hub-urls "http://node-a:3381,http://node-b:3381" --collect-last-hours 24

# Environment fallback
export HYPERSNAP_NODE_URLS="http://node-a:3381,http://node-b:3381"
export FC_CONTEXT_SNAPCHAIN_SHARDS="auto"   # default: every shard advertised by /v1/info
```

## Local validation

Run the parser/import checks without collecting live data:

```bash
python -m compileall scripts
python -m unittest discover -s tests
python scripts/farcaster_daily_scraper.py --help
```

## Run (single-command workflows)

### 24-hour context (daily-ish)

```bash
bash scripts/farcaster_context_24h.sh
```

### Last-hour context (high-speed)

```bash
bash scripts/farcaster_context_last_hour.sh
```

You can also call the underlying script directly:

```bash
python3 scripts/farcaster_daily_scraper.py --source hypersnap --collect-last-hours 24 --timezone UTC
```

or

```bash
python3 scripts/farcaster_daily_scraper.py --source hypersnap --collect-last-hours 1 --timezone UTC
```

## Output files

24h run:
- `data/farcaster_24h_*.txt`
- `data/llm_instructions_24h_*.txt`
- `data/farcaster_24h_readable_*.txt`
- `data/farcaster_24h_final_*.txt`

1h run:
- `data/farcaster_last_hour_*.txt`
- `data/llm_instructions_last_hour_*.txt`
- `data/farcaster_last_hour_readable_*.txt`
- `data/farcaster_last_hour_final_*.txt`

## Optional: manual args

Use additional flags supported by `farcaster_daily_scraper.py`, including:
- `--query`
- `--timezone`
- `--source hypersnap` (`snapchain` remains a backwards-compatible alias)
- `--hub-url` / `--hub-urls`
- `--snapchain-shards auto` (default, reads all shards advertised by the selected node)
- `--final-top-posts`
- `--final-comments-per-post`
- `--final-snippet-length`
- `--exclude-empty-records`
- `--exclude-promo-records`
- `--exclude-gm-gn-records`
- `--focus-themes`
- `--exclude-themes`

### Theme knobs

Defaults for the bundled 24h and 1h skills exclude noisy `general_chat`, `daily_greetings`, and `empty` themes, while also filtering out empty, promo, and GM/GN entries by default.
You can tune this per run with environment variables:

```bash
export FC_CONTEXT_FOCUS_THEMES="protocol_fork,security_ops,base_culture"
export FC_CONTEXT_EXCLUDE_THEMES="general_chat,daily_greetings"
```

If you run the scraper script directly, include `--exclude-empty-records`, `--exclude-promo-records`, and `--exclude-gm-gn-records` explicitly.

Supported theme values:

- `protocol_fork`
- `token_promo`
- `security_ops`
- `apps_games`
- `base_culture`
- `daily_greetings`
- `general_chat`
- `empty`

Examples:

```bash
bash scripts/farcaster_context_24h.sh --focus-themes protocol_fork,security_ops
bash scripts/farcaster_context_24h.sh --exclude-themes token_promo,general_chat,daily_greetings
```

For full flag list, run:

```bash
python3 scripts/farcaster_daily_scraper.py --help
```
