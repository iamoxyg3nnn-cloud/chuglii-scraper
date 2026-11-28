# Chuglii Scraper Workspace

This directory contains the video scraping and ingestion infrastructure for [Chuglii.in](https://chuglii.in).

## Architecture

- **Categories System**: Each folder inside `scraper/categories/` maps directly to a parent category shown in the frontend navigation (`/leak?category=<category-name>`).
- **Viral MMS**: Stored in `scraper/categories/viral-mms/` (targeting MMS sources and storing into Supabase `scraper.videos` with parent category `viral-mms`).
- **Sub-tagging**: Regional origins (`indian`, `pakistani`, `bangladeshi`) and content classifications (`desi-mms`, `bhabhi`, `viral-clips`, `web-series`, `amateur`, `village`, etc.) are preserved and extracted as searchable tags within each category.
- **Automated Cloudflare Ingestion**: Handled in production by the dedicated Cloudflare Worker `backend/workers/chuglii-scraper` running on a 15-minute cron schedule (`*/15 * * * *`).

## Directory Structure

```
scraper/
├── categories/
│   ├── viral-mms/                # Parent category: Viral MMS (Source: MMSVibeX -> scraper.videos)
│   │   ├── pipeline.py           # Core ingestion pipeline (CLI & Python API)
│   │   ├── polish.py             # Normalization, tag extraction & stream resolution
│   │   └── run.bat               # Interactive 1-click batch runner
│   │
│   ├── japanese/                 # Parent category: Japanese JAV (Source: SupJav -> scraper.japanese_videos)
│   │   ├── pipeline.py           # Core ingestion pipeline & server locker resolution
│   │   ├── polish.py             # Title cleaning, maker code extraction & formatting
│   │   └── run.bat               # Interactive 1-click batch runner
│   │
│   ├── myanmar/ onlyfans/ voyeurhouse/   # Other isolated category folders
│   │
│   └── chinese/                  # Parent category: Chinese (Source: uxj.838238.xyz -> scraper.chinese_videos)
│       ├── categories.py         # 37 whitelisted zones (JAV + livestream zones excluded)
│       ├── pipeline.py           # Per-zone crawl engine with remote-first checkpoints
│       ├── polish.py             # CJK-safe slugs, size/resolution parsing, tag extraction
│       ├── sources/uxj838238.py  # TLS-impersonated fetcher + reversed-base64 payload decoder
│       └── run.bat               # Interactive 1-click batch runner
│
├── .venv/                        # Python virtual environment (with stealth Fetcher)
├── scrapling/                    # Scrapling stealth scraping engine
├── db.py                         # Supabase database adapter (scraper.videos, scraper.japanese_videos, scraper.chinese_videos, ...)
├── schema.sql                    # PostgreSQL schema definition and RPC functions
├── apply-scraper-schema.bat      # Helper script to apply database migrations
├── pipeline.py                   # Root forwarder to categories/viral-mms/pipeline.py
└── polish.py                     # Root forwarder to categories/viral-mms/polish.py
```

## Running the Scraper Locally

### Option 1: Interactive Runner (Windows)
Double-click `categories/viral-mms/run.bat` or run:
```cmd
categories\viral-mms\run.bat
```

### Option 2: Command Line (via Virtual Environment)
```bash
# Scrape latest 2 pages of global feed (all content)
scraper\.venv\Scripts\python.exe categories/viral-mms/pipeline.py --pages 2

# Scrape specific sub-category (e.g. Pakistani, Indian)
scraper\.venv\Scripts\python.exe categories/viral-mms/pipeline.py --category pakistani --pages 1

# Scrape popular feed with deep detail enrichment
scraper\.venv\Scripts\python.exe categories/viral-mms/pipeline.py --filter popular --pages 1 --enrich
```

### Chinese Category (isolated database)

Source: `uxj.838238.xyz` (95da.com network). All records go strictly into
`scraper.chinese_videos` — never into `scraper.videos`. Zones are isolated by
`category_id` + `category_group` (`chinese` | `western`). JAV and livestream
zones are **excluded by design** and are never scraped.

Titles are stored twice: `title` keeps the original CJK source title, and
`title_en` holds the Google-translated English title (free gtx endpoint,
persistent cache in `translation_cache.json`). The frontend displays
`title_en` and site search matches both. Re-translate existing rows with
`translate_backfill.py`.

```bash
# Quick sync: page 1 of every zone, early stop on known content
scraper\.venv\Scripts\python.exe -X utf8 categories/chinese/pipeline.py --pages 1 --new-only

# Full archive backfill with checkpoints (resumable)
scraper\.venv\Scripts\python.exe -X utf8 categories/chinese/pipeline.py --full-site --new-only

# Specific zone(s) only
scraper\.venv\Scripts\python.exe -X utf8 categories/chinese/pipeline.py --categories 20100496,19920497 --pages 2

# 24/7 daemon mode
scraper\.venv\Scripts\python.exe -X utf8 categories/chinese/pipeline.py --daemon

# Unified scraper: run only the Chinese feed cycle
scraper\.venv\Scripts\python.exe -X utf8 unified_scraper.py --chinese --cn-pages 1
```

Interactive runner: `categories\chinese\run.bat`

State files (local mirrors of the remote checkpoints):
`categories/chinese/checkpoint.json`, `known_post_ids.json`, `progress.json`.

## Timeline hover previews (live, opt-in sprite storyboards)

Timeline hover shows a LIVE muted low-rendition stream playing at the
hovered position (warm hls.js decoder + 180s forward buffer + immutable
HTTP caching) - no scrape-time generation involved.

The optional sprite-storyboard generator (`scraper/thumbnails.py`) is
disabled by default and never runs in the daemon. It can still be run
manually with `python scraper/unified_scraper.py --thumbs N` if static
previews are ever wanted for a table.

## Session budgets (why a feed can no longer eat a whole run)

A fleet session is a ~280-minute GitHub Actions job that self-chains. Before
`scraper/budget.py` existed, **one feed could consume the entire session**: on
2026-11-27 the Chinese feed started at 14:50 and was still crawling when the job
timeout killed the run at 18:34. Feeds 7 (fangsung) and 8 (studios) never ran,
the checkpoint commit never happened, and the successor was never dispatched, so
24/7 chaining stopped.

How it works now:

| Layer | Guarantee |
| --- | --- |
| `unified_scraper.run_unified_sync_cycle` | Each feed gets `remaining_time / feeds_left` (floor `SCRAPER_FEED_MIN_SECONDS`, default 240s). A fast feed returns its unused time to the pool. |
| `FEED_ROTATION` + `cycle_rotation` | The feed order rotates every cycle, so a feed that could not finish its slice is first next time instead of always last. |
| `categories/*/pipeline.run_category_crawl` | Checks the deadline **before every page**. A chunk that cannot finish stops at the page it reached and the caller's cursor keeps the pages already stored. |
| `deep_backfill.run_deep_backfill` | Passes the session deadline into each chunk (it used to check only *between* chunks and overran a 60-minute budget by 28 minutes, throwing the work away). |
| `--session-minutes` | 280 by default, leaving ~45 min of the 325-min job timeout for install, the title backfills, the checkpoint commit and the dispatch. |

## Failure semantics (dead vs. unreadable vs. blocked)

Three outcomes look identical to a parser and are **not** the same fact. Getting
this wrong is what let the fleet lose live catalogue data.

| Signal | Meaning | What the scraper does |
| --- | --- | --- |
| `Page.error` (`net.is_origin_error_page`) | The *origin* answered with an error page — e.g. borntobefuck.com returned HTTP **200** with 22 bytes of `Internal server error.` for hours | Retried once, then returned as unusable. Never cached, never parsed, never treated as the end of a listing. |
| `Page.challenge` / WAF status | We were rejected (Cloudflare, 403/466/…) | Solved via Scrapling stealth, then a validated proxy, then the host is remembered as egress-blocked. |
| `ListingUnavailable` | A listing page could not be fetched | The pipeline keeps its cursor and reports a degraded feed. It never pins the catalogue as complete. |
| `stream_dead` / `dead` | The origin itself says the video is removed | Recorded in `scraper.known_post_ids` with `status='dead'` so it is never re-probed. |

`scrape_metadata_from_watch` / `player_fragment` return **`UNAVAILABLE`** (not
`None`) when the page could not be read, so the id sweep retries it instead of
writing it off.

## Ledger repair (after a site outage)

`scraper/categories/onlyfans/sweep_ids.py` has a circuit breaker: if ≥95% of the
first 40 verdicts say "dead", it stops, persists nothing and holds its cursor,
because that is the site failing, not the catalogue. The outage that motivated
it (2026-11-28) had already written 10,512 ids off before the guard existed.

```bash
# see per-hour dead/stored counts (a burst of dead with 0 stored = an outage)
python -X utf8 scraper/tools/onlyfans_ledger_repair.py --report --site borntobefuck

# delete the false "dead" rows so the sweep re-probes them, then rewind the cursor
python -X utf8 scraper/tools/onlyfans_ledger_repair.py \
    --site borntobefuck --forget-window 2026-11-28T01:00:00Z 2026-11-28T03:10:00Z --apply
```

SQL helpers live in `scraper/migrations/20261128_known_ids_repair.sql`
(`scraper_forget_known_post_ids`, `scraper_known_post_ids_summary`,
`scraper_known_post_ids_histogram`).

## Tests

```bash
python -m pytest tests -q
```

`tests/test_budget.py`, `tests/test_deadline_crawl.py`, the origin-error cases in
`tests/test_net.py` and the circuit-breaker cases in `tests/test_sweep_ids.py`
are the regression suites for the incidents above.
