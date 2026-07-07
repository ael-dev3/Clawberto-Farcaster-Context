# Clawberto Farcaster Context

Static, cached Farcaster context from direct Hypersnap/Snapchain node scraping. The public site serves parsed 24-hour context tables, a compact agent-written take, linkable usernames, cast URLs, and downloadable CSV/JSON exports.

## Links

- Live context site: [https://ael-dev3.github.io/Clawberto-Farcaster-Context/](https://ael-dev3.github.io/Clawberto-Farcaster-Context/)
- Repository: [https://github.com/ael-dev3/Clawberto-Farcaster-Context](https://github.com/ael-dev3/Clawberto-Farcaster-Context)
- Generated exports: [`generated/`](generated/)

## Current snapshot

| Field | Value |
| --- | --- |
| Source | hypersnap |
| Hub URL | http://54.157.62.17:3381 |
| Readable shards | 1,2 |
| Window UTC | 2026-07-06 15:15:58 → 2026-07-07 15:15:43 |
| Total casts | 14893 |
| Posts / comments | 3763 / 11130 |
| Unique authors | 4019 |
| Total likes / recasts / replies | 13992 / 2148 / 7078 |
| Top cast author | @basedcoin |
| Top cast | https://farcaster.xyz/basedcoin/0xeb778a77 |

## Published datasets

| Table | CSV path | Rows | Downloads |
| --- | --- | --- | --- |
| summary_metrics | `generated/summary_metrics.csv` | 20 | [CSV](generated/summary_metrics.csv) / [JSON](generated/summary_metrics.json) |
| theme_summary | `generated/theme_summary.csv` | 5 | [CSV](generated/theme_summary.csv) / [JSON](generated/theme_summary.json) |
| authors | `generated/authors.csv` | 100 | [CSV](generated/authors.csv) / [JSON](generated/authors.json) |
| top_casts | `generated/top_casts.csv` | 150 | [CSV](generated/top_casts.csv) / [JSON](generated/top_casts.json) |
| posts | `generated/posts.csv` | 56 | [CSV](generated/posts.csv) / [JSON](generated/posts.json) |
| comments | `generated/comments.csv` | 124 | [CSV](generated/comments.csv) / [JSON](generated/comments.json) |
| agent_take | `generated/agent_take.csv` | 4 | [CSV](generated/agent_take.csv) / [JSON](generated/agent_take.json) |

## Data pipeline

1. `scripts/farcaster_daily_scraper.py` talks directly to Hypersnap/Snapchain HTTP nodes (`/v1/info`, `/v1/events`, and username lookup endpoints).
2. `scripts/farcaster_context_24h.sh` collects the latest rolling 24-hour context and writes raw/readable/final text outputs under ignored `data/`.
3. `scripts/build_site.py` parses the latest raw output into cached CSV/JSON datasets under `generated/` and `public/generated/`.
4. `index.html` renders the dark-purple searchable context site with linkable profiles, casts, summaries, and exports.
5. GitHub Pages deploys the static site from the Vite build artifact.

## Local development

```bash
python3 -m pip install -r requirements.txt
npm ci
npm run refresh      # scrape latest 24h + rebuild generated site artifacts
npm run build        # Vite build for GitHub Pages
npm run preview -- --host 127.0.0.1 --port 4188 --strictPort
```

For scraper-only validation:

```bash
python3 -m compileall scripts tests
python3 -m unittest discover -s tests -v
python3 scripts/farcaster_daily_scraper.py --help
```

## Node configuration

```bash
export HYPERSNAP_NODE_URLS="http://node-a:3381,http://node-b:3381"
export FC_CONTEXT_SNAPCHAIN_SHARDS="auto"
```

`--source hypersnap` is the default. `snapchain` remains a backwards-compatible alias.

## Deployment notes

If the first GitHub Pages deploy fails in `actions/configure-pages` with `Resource not accessible by integration`, Pages has not been enabled for the repo yet. Enable workflow-based Pages once, then rerun the deploy workflow:

```bash
gh api repos/ael-dev3/Clawberto-Farcaster-Context/pages -X POST -f build_type=workflow
gh run rerun <deploy-run-id>
```
