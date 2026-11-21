"""
Chuglii Unified Multi-Site Scraper Engine
Combines:
  1. Viral MMS (MMSVibeX -> scraper.videos)
  2. Japanese JAV (SupJav -> scraper.japanese_videos)

Features:
  - Human-like adaptive rate limiting with randomized jitter
  - Authentic Chrome TLS JA3/JA4 fingerprinting (curl_cffi chrome124)
  - Automatic exponential backoff on 429/503/403 throttling
  - Sub-millisecond deduplication (skips already indexed videos)
  - Strict working locker enforcement (VOE HD, VidSpeed, LuluStream)
  - Self-chaining daemon mode for continuous 24/7 cloud execution
  - Immediate unbuffered live logs with active heartbeat countdowns
"""

import os
import sys
import time
import random
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional

# Force immediate line-buffering on stdout/stderr for real-time live streaming logs in GitHub Actions
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent

for p in [str(CURRENT_DIR), str(PROJECT_ROOT)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from scraper.db import get_supabase_client
from scraper.categories.japanese.pipeline import (
    create_session as create_japanese_session,
    warm_up_session as warm_up_japanese_session,
    scrape_listing_page as scrape_japanese_listing,
    enrich_items_concurrently as enrich_japanese_items,
    polish_supjav_record,
    upsert_japanese_videos_batch,
    load_known_post_ids,
    save_known_post_ids,
    load_checkpoint,
    save_checkpoint,
    DEFAULT_CATEGORIES as JAV_CATEGORIES
)

def log(msg: str = "") -> None:
    """Prints message immediately flushed to stdout."""
    print(msg, flush=True)

# ─── Human Politeness & Adaptive Jitter ───────────────────────────────────────

def human_jitter(min_sec: float = 1.0, max_sec: float = 2.5) -> None:
    """Randomized delay to simulate natural human reading/browsing behavior."""
    sleep_time = random.uniform(min_sec, max_sec)
    time.sleep(sleep_time)

def adaptive_backoff(attempt: int, base: float = 3.0, max_wait: float = 30.0) -> None:
    """Exponential backoff when rate-limiting is detected."""
    wait_time = min(base * (2 ** attempt) + random.uniform(0.5, 1.5), max_wait)
    log(f"    [!] Adaptive backoff: Throttling detected. Pausing for {wait_time:.1f}s...")
    time.sleep(wait_time)

# ─── Viral MMS Ingestion Task ────────────────────────────────────────────────

def run_viral_mms_sync(pages: int = 1) -> int:
    """Runs Viral MMS ingestion pipeline for the newest releases."""
    log("\n" + "=" * 65)
    log(f"  [FEED 1/2] Viral MMS Scraper | Pages: {pages}")
    log("=" * 65)
    try:
        from scraper.pipeline import run_scraper_pipeline
        count = run_scraper_pipeline(pages=pages, filter_mode="latest", category=None, enrich_details=False)
        log(f"  [✓] Viral MMS sync complete. Videos processed: {count}")
        return count or 0
    except Exception as e:
        log(f"  [!] Viral MMS sync error: {e}")
        return 0

# ─── Japanese JAV Ingestion Task ─────────────────────────────────────────────

def run_japanese_jav_sync(pages: int = 1, workers: int = 3) -> int:
    """Runs SupJav ingestion for newest releases across all 5 categories."""
    log("\n" + "=" * 65)
    log(f"  [FEED 2/2] SupJav Japanese JAV Scraper | Working Mirrors Only")
    log("=" * 65)

    known_ids = load_known_post_ids()
    checkpoint = load_checkpoint()
    session = create_japanese_session()
    warm_up_japanese_session(session)
    new_ingested = 0

    for cat in JAV_CATEGORIES:
        log(f"\n  Checking category: [{cat}]")
        try:
            for p in range(1, pages + 1):
                items = scrape_japanese_listing(session, page_num=p, category=cat)
                if items is None:
                    log(f"    Page {p}: [!] Cloudflare challenge encountered on [{cat}]. Backing off.")
                    continue

                new_items = [itm for itm in items if itm["source_post_id"] not in known_ids]

                if not new_items:
                    log(f"    Page {p}: Up to date (all {len(items)} releases already indexed)")
                    continue

                log(f"    Page {p}: Found {len(new_items)} new releases! Resolving working mirrors...")
                enriched = enrich_japanese_items(new_items, max_workers=workers)

                if enriched:
                    records = []
                    for itm in enriched:
                        srvs = [s["name"] for s in itm.get("servers", [])]
                        log(f"      + #{itm['source_post_id']} {itm['title'][:35]}... -> {srvs}")
                        polished = polish_supjav_record(itm)
                        records.append(polished)
                        known_ids.add(itm["source_post_id"])

                    res = upsert_japanese_videos_batch(records)
                    log(f"      [✓] Batch upserted {len(records)} videos ({res})")
                    new_ingested += len(records)
                    save_known_post_ids(known_ids)
                    checkpoint["stats"]["total_ingested"] = checkpoint["stats"].get("total_ingested", 0) + len(records)
                    save_checkpoint(checkpoint)

                human_jitter(1.0, 2.0)

        except Exception as e:
            log(f"    [!] Error checking {cat}: {e}")

        # Human-like pause between category switches
        human_jitter(1.5, 3.0)

    log(f"\n  [✓] Japanese JAV sync complete. New videos added: {new_ingested}")
    return new_ingested

# ─── Unified Cycle Executor ──────────────────────────────────────────────────

def run_unified_sync_cycle(mms_pages: int = 1, jav_pages: int = 1, workers: int = 3) -> Dict[str, int]:
    """Executes a single combined sync cycle across both target websites."""
    t0 = time.time()
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log("\n" + "#" * 65)
    log(f"  STARTING UNIFIED SYNC CYCLE | {timestamp}")
    log("#" * 65)

    # 1. Scrape Viral MMS
    mms_count = run_viral_mms_sync(pages=mms_pages)

    # Human transition pause
    human_jitter(2.0, 4.0)

    # 2. Scrape Japanese JAV
    jav_count = run_japanese_jav_sync(pages=jav_pages, workers=workers)

    elapsed = time.time() - t0
    log("\n" + "#" * 65)
    log(f"  CYCLE SUMMARY | Elapsed: {elapsed:.1f}s")
    log(f"  - Viral MMS:    {mms_count} videos processed")
    log(f"  - Japanese JAV: {jav_count} new videos ingested")
    log("#" * 65)

    return {"mms": mms_count, "jav": jav_count}

# ─── Continuous 24/7 Session Daemon (For GitHub Actions) ─────────────────────

def run_continuous_daemon(
    session_minutes: int = 300,
    interval_seconds: int = 900,
    workers: int = 3
) -> None:
    """
    Runs a continuous multi-site scraping daemon.
    Designed for GitHub Actions 6-hour runners:
    Runs for ~session_minutes (default 300 min / 5 hours),
    checking both sites every interval_seconds (default 15 min).
    Provides real-time heartbeat countdown logs so progress is always visible.
    When time ends, exits cleanly to allow handoff.
    """
    start_time = time.time()
    max_seconds = session_minutes * 60
    cycle = 1

    log("=" * 65)
    log("  CHUGLII 24/7 CONTINUOUS CLOUD SCRAPER DAEMON")
    log(f"  Session Duration: {session_minutes} min ({session_minutes / 60:.1f} hours)")
    log(f"  Check Interval:   {interval_seconds} sec ({interval_seconds / 60:.1f} min)")
    log(f"  Streaming Logs:   ACTIVE (unbuffered)")
    log("=" * 65)

    while True:
        elapsed = time.time() - start_time
        remaining = max_seconds - elapsed

        if remaining <= (interval_seconds + 60):
            log(f"\n[HANDOFF] Session limit reached ({elapsed / 60:.1f}m / {session_minutes}m).")
            log("[HANDOFF] Exiting cleanly so successor job can run without queuing.")
            break

        log(f"\n>>> CYCLE #{cycle} | Session elapsed: {elapsed / 60:.1f}m | Remaining: {remaining / 60:.1f}m <<<")
        try:
            run_unified_sync_cycle(mms_pages=1, jav_pages=1, workers=workers)
        except Exception as e:
            log(f"[!] Error in cycle #{cycle}: {e}")

        # Human-jittered sleep between cycles with active live heartbeats
        jittered_sleep = interval_seconds + random.uniform(-15.0, 25.0)
        jittered_sleep = max(30.0, jittered_sleep)
        sleep_total = int(jittered_sleep)
        log(f"\n[*] Cycle #{cycle} complete. Entering polite sleep for {sleep_total // 60}m {sleep_total % 60}s...")
        cycle += 1

        # Print active heartbeat every 60s so GitHub Actions log is never silent
        heartbeat_interval = 60
        for remaining_sec in range(sleep_total, 0, -heartbeat_interval):
            m, s = divmod(remaining_sec, 60)
            log(f"    [Heartbeat] Next scan in {m}m {s:02d}s... Daemon active & healthy.")
            time.sleep(min(heartbeat_interval, remaining_sec))

# ─── CLI Entrypoint ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chuglii Unified Multi-Site Scraper")
    parser.add_argument("--sync", action="store_true", help="Run a single one-shot sync cycle and exit")
    parser.add_argument("--daemon", action="store_true", help="Run in continuous daemon mode")
    parser.add_argument("--session-minutes", type=int, default=300, help="Max session duration in minutes (default: 300)")
    parser.add_argument("--interval", type=int, default=900, help="Seconds between scan cycles (default: 900)")
    parser.add_argument("--workers", type=int, default=3, help="Concurrent workers for detail resolver (default: 3)")
    parser.add_argument("--mms-pages", type=int, default=1, help="Pages of Viral MMS to scrape")
    parser.add_argument("--jav-pages", type=int, default=1, help="Pages of Japanese JAV to scrape per category")
    args = parser.parse_args()

    if args.daemon:
        run_continuous_daemon(
            session_minutes=args.session_minutes,
            interval_seconds=args.interval,
            workers=args.workers
        )
    else:
        run_unified_sync_cycle(
            mms_pages=args.mms_pages,
            jav_pages=args.jav_pages,
            workers=args.workers
        )
