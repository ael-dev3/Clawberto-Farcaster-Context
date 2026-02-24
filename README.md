# Farcaster Daily Scraper

Production-style Farcaster scraper that:
- pulls posts and comments for a target day,
- scores each cast from 1-10 using genuine-like weighted engagement,
- writes date-stamped `.txt` outputs,
- puts an LLM token estimate on line 1 of each generated `.txt`.

## Setup

```bash
python -m pip install -r requirements.txt
```

Default source is direct Snapchain hub ingestion (no API key required).

Optional, if you want Neynar mode:

PowerShell:
```powershell
$env:NEYNAR_API_KEY="YOUR_KEY"
```

## Run for the day script is executed

```bash
python farcaster_daily_scraper.py
```

## Run keyless direct Snapchain 24h scrape

```bash
python farcaster_daily_scraper.py --source snapchain --collect-last-hours 24 --timezone UTC
```

## Run for a specific day (example: 2/15/2026)

```bash
python farcaster_daily_scraper.py --date 2/15/2026 --timezone UTC
```

## Outputs

- `data/farcaster_YYYY-MM-DD.txt`
- `data/llm_instructions_YYYY-MM-DD.txt`
- `data/farcaster_YYYY-MM-DD_readable.txt`
- `data/farcaster_YYYY-MM-DD_final.txt` (compact daily context for bot consumption)
- each record includes `author_username_with_fid` (for example `dwr (3)`).
- each record includes `cast_url` (direct Warpcast conversation link).
- each record includes visibility fields: `visibility_tier`, `visibility_score_0_to_100`, `content_label`, `text_preview`.

Generated files start with a token-estimate header:

- `ESTIMATED_LLM_TOKENS_FOR_SCRAPED_DATA: <N>`
- `ESTIMATED_LLM_TOKENS_FOR_FINAL_CONTEXT: <N>`

Readable file sections:

- `TOP_25_BY_SCORE`
- `ALL_RECORDS_BY_RANK`

Final context file sections:

- aggregate context (`SUMMARY`, `HOT_HOURS_LOCAL`, themes/keywords, narrative clusters)
- `TOP_10_QUOTE_SUGGESTIONS_COLUMNS` with anti-spam scoring, relative engagement, and direct quote links
- `TOP_200_ENGAGEMENT_CASTS_COLUMNS`
- `THREAD_CONTEXT_COLUMNS` for high-signal replies under top-discussion posts

Useful flags for final context shaping:

- `--final-output`
- `--final-top-posts` (default `12`)
- `--final-comments-per-post` (default `3`)
- `--final-snippet-length` (default `180`)

Source selection flags:

- `--source snapchain|neynar` (default `snapchain`)
- `--hub-url` (default `http://54.157.62.17:3381`)
- `--snapchain-shards` (default `2`)
- `--snapchain-event-id-span` (default `3000000000`)

## Score definition

Current ranking rule:

- rank posts/comments by `likes_count` for casts no older than `24h` at run time,
- push older casts below fresh casts (`visibility_tier=STALE`),
- keep `algorithmic_score_1_to_10` as a normalized likes-based score for the fresh set.

Config:

- `--ranking-max-age-hours 24` (default) to adjust freshness window.

## Notes

- Cast content is protocol data distributed by Farcaster hubs (not directly stored on Ethereum L1).
- Neynar score is an off-chain reputation signal used as a quality weight.
