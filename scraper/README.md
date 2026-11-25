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
