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
from typing import Dict, Any, List, Optional, Set

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

from scraper.db import get_supabase_client, wait_for_supabase, is_supabase_healthy
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

# ─── Global In-Memory Deduplication Caches ─────────────────────────────────
VIRAL_MMS_KNOWN_IDS: Set[str] = set()
JAV_KNOWN_IDS: Set[str] = set()
MYANMAR_KNOWN_IDS: Set[str] = set()
CACHES_INITIALIZED: bool = False

def init_known_id_caches() -> None:
    """Preloads existing post IDs from Supabase and local disks for zero-redundancy deduplication."""
    global VIRAL_MMS_KNOWN_IDS, JAV_KNOWN_IDS, MYANMAR_KNOWN_IDS, CACHES_INITIALIZED
    if CACHES_INITIALIZED:
        return
    log("\n[*] Initializing high-speed deduplication caches from Supabase & local...")

    # 1. Viral MMS IDs
    try:
        import importlib
        mms_mod = importlib.import_module("scraper.categories.viral-mms.pipeline")
        VIRAL_MMS_KNOWN_IDS = mms_mod.load_known_ids()
    except Exception:
        try:
            from scraper.db import get_scraper_known_post_ids
            VIRAL_MMS_KNOWN_IDS = get_scraper_known_post_ids("viral-mms")
        except Exception:
            VIRAL_MMS_KNOWN_IDS = set()
    log(f"  [✓] Viral MMS Deduplication Cache: {len(VIRAL_MMS_KNOWN_IDS)} known IDs loaded")

    # 2. Japanese JAV IDs
    try:
        JAV_KNOWN_IDS = load_known_post_ids()
    except Exception:
        try:
            from scraper.db import get_scraper_known_post_ids
            JAV_KNOWN_IDS = get_scraper_known_post_ids("japanese")
        except Exception:
            JAV_KNOWN_IDS = set()
    log(f"  [✓] Japanese JAV Deduplication Cache: {len(JAV_KNOWN_IDS)} known IDs loaded")

    # 3. Myanmar IDs
    try:
        from scraper.categories.myanmar.pipeline import load_known_ids as load_myanmar_known_ids
        MYANMAR_KNOWN_IDS = load_myanmar_known_ids()
    except Exception:
        try:
            from scraper.db import get_scraper_known_post_ids
            MYANMAR_KNOWN_IDS = get_scraper_known_post_ids("myanmar")
        except Exception:
            MYANMAR_KNOWN_IDS = set()
    log(f"  [✓] Myanmar Deduplication Cache: {len(MYANMAR_KNOWN_IDS)} known IDs loaded\n")

    CACHES_INITIALIZED = True

# ─── Viral MMS Ingestion Task ────────────────────────────────────────────────

def run_viral_mms_sync(pages: int = 1, checkpoint: Optional[Dict[str, Any]] = None) -> int:
    """Runs Viral MMS ingestion: checks Page 1 for new releases, then advances historical cursor in Supabase."""
    global VIRAL_MMS_KNOWN_IDS
    log("\n" + "=" * 65)
    log(f"  [FEED 1/3] Viral MMS Scraper | Head: Page 1 + Cursor Backfill")
    log("=" * 65)
    import importlib
    mms_mod = importlib.import_module("scraper.categories.viral-mms.pipeline")
    scrape_mms_listing = mms_mod.scrape_listing_page
    save_mms_known_ids = mms_mod.save_known_ids
    from scraper.polish import polish_scraped_record

    from scraper.db import get_scraper_checkpoint, set_scraper_checkpoint, upsert_scraped_videos_batch

    total_ingested = 0
    try:
        # 1. New incoming leaks (Page 1)
        p1_items = scrape_mms_listing(1, filter_mode="latest", category=None)
        if p1_items:
            new_p1 = [itm for itm in p1_items if str(itm.get("post_id")) not in VIRAL_MMS_KNOWN_IDS]
            if new_p1:
                log(f"    [+] Page 1: Found {len(new_p1)} brand new leaks! Ingesting...")
                polished_batch = [polish_scraped_record(itm) for itm in new_p1]
                res = upsert_scraped_videos_batch(polished_batch)
                for itm in new_p1:
                    VIRAL_MMS_KNOWN_IDS.add(str(itm.get("post_id")))
                save_mms_known_ids(VIRAL_MMS_KNOWN_IDS)
                total_ingested += len(polished_batch)
                log(f"    [✓] Page 1: Ingested {len(polished_batch)} new Viral MMS videos")
            else:
                log(f"    Page 1: Up to date (all {len(p1_items)} releases already indexed)")
        else:
            log("    Page 1: Empty response from MMSVibeX")

        # 2. Advance historical backfill cursor (Supabase-first)
        mms_state = get_scraper_checkpoint("viral-mms")
        if not mms_state or "current_page" not in mms_state:
            if checkpoint and "viral-mms" in checkpoint:
                mms_state = checkpoint["viral-mms"]
            else:
                mms_state = {"current_page": 58, "max_pages": 150}

        curr_cursor = mms_state.get("current_page", 58)
        if curr_cursor <= mms_state.get("max_pages", 150):
            log(f"  [*] Viral MMS Progress Tracing: Crawling historical page {curr_cursor}...")
            hist_items = scrape_mms_listing(curr_cursor, filter_mode="latest", category=None)
            if hist_items:
                new_hist = [itm for itm in hist_items if str(itm.get("post_id")) not in VIRAL_MMS_KNOWN_IDS]
                if new_hist:
                    log(f"    [+] Historical Page {curr_cursor}: Found {len(new_hist)} unindexed videos! Ingesting...")
                    polished_batch = [polish_scraped_record(itm) for itm in new_hist]
                    res = upsert_scraped_videos_batch(polished_batch)
                    for itm in new_hist:
                        VIRAL_MMS_KNOWN_IDS.add(str(itm.get("post_id")))
                    save_mms_known_ids(VIRAL_MMS_KNOWN_IDS)
                    total_ingested += len(polished_batch)
                    log(f"    [✓] Historical Page {curr_cursor}: Ingested {len(polished_batch)} videos")
                else:
                    log(f"    Historical Page {curr_cursor}: All {len(hist_items)} releases already indexed.")

                mms_state["current_page"] = curr_cursor + 1
                mms_state["total_ingested"] = mms_state.get("total_ingested", 0) + total_ingested
                set_scraper_checkpoint("viral-mms", mms_state)
                log(f"  [✓] Viral MMS Checkpoint: Cursor advanced to page {curr_cursor + 1} (Saved to Supabase)")
            else:
                log(f"    [!] Historical Page {curr_cursor} returned 0 items. Advancing cursor.")
                mms_state["current_page"] = curr_cursor + 1
                set_scraper_checkpoint("viral-mms", mms_state)

        log(f"  [✓] Viral MMS sync complete. New videos added: {total_ingested}")
        return total_ingested
    except Exception as e:
        log(f"  [!] Viral MMS sync error: {e}")
        return 0

# ─── Japanese JAV Ingestion Task ─────────────────────────────────────────────

def run_japanese_jav_sync(pages: int = 1, workers: int = 3, checkpoint: Optional[Dict[str, Any]] = None) -> int:
    """Runs SupJav ingestion: checks Page 1 for new releases, then advances historical cursor."""
    global JAV_KNOWN_IDS
    log("\n" + "=" * 65)
    log(f"  [FEED 2/3] SupJav Japanese JAV Scraper | Working Mirrors Only")
    log("=" * 65)

    if not checkpoint:
        checkpoint = load_checkpoint()
    session = create_japanese_session()
    warm_up_japanese_session(session)
    new_ingested = 0

    for cat in JAV_CATEGORIES:
        log(f"\n  Checking category: [{cat}]")
        try:
            # 1. Check Page 1 for brand new releases
            p1_items = scrape_japanese_listing(session, page_num=1, category=cat)
            if p1_items:
                new_p1 = [itm for itm in p1_items if str(itm["source_post_id"]) not in JAV_KNOWN_IDS]
                if new_p1:
                    log(f"    [+] Page 1: Found {len(new_p1)} brand new releases! Resolving mirrors...")
                    enriched = enrich_japanese_items(new_p1, max_workers=workers)
                    if enriched:
                        records = []
                        for itm in enriched:
                            srvs = [s["name"] for s in itm.get("servers", [])]
                            log(f"      + #{itm['source_post_id']} {itm['title'][:35]}... -> {srvs}")
                            polished = polish_supjav_record(itm)
                            records.append(polished)
                            JAV_KNOWN_IDS.add(str(itm["source_post_id"]))
                        res = upsert_japanese_videos_batch(records)
                        log(f"      [✓] Batch upserted {len(records)} videos ({res})")
                        new_ingested += len(records)
                        save_known_post_ids(JAV_KNOWN_IDS)
                else:
                    log(f"    Page 1: Up to date (all {len(p1_items)} releases already indexed)")

            # 2. Advance historical backfill cursor
            if "categories" not in checkpoint:
                checkpoint["categories"] = {}
            cat_state = checkpoint["categories"].setdefault(cat, {"current_page": 2, "max_pages": 100, "completed": False})
            curr_cursor = cat_state.get("current_page", 2)
            if curr_cursor <= 1:
                curr_cursor = 2
                cat_state["current_page"] = 2

            if curr_cursor > cat_state.get("max_pages", 99999):
                cat_state["completed"] = True
                save_checkpoint(checkpoint)
                log(f"    [✓] Category [{cat}] has completed all available pages.")

            if not cat_state.get("completed", False):
                log(f"    [*] Progress Tracing: Crawling historical page {curr_cursor} for [{cat}]...")
                hist_items = scrape_japanese_listing(session, page_num=curr_cursor, category=cat)
                if hist_items:
                    new_hist = [itm for itm in hist_items if str(itm["source_post_id"]) not in JAV_KNOWN_IDS]
                    if new_hist:
                        log(f"    [+] Historical Page {curr_cursor}: Found {len(new_hist)} unindexed videos! Resolving mirrors...")
                        enriched_hist = enrich_japanese_items(new_hist, max_workers=workers)
                        if enriched_hist:
                            records = []
                            for itm in enriched_hist:
                                srvs = [s["name"] for s in itm.get("servers", [])]
                                log(f"      + #{itm['source_post_id']} {itm['title'][:35]}... -> {srvs}")
                                polished = polish_supjav_record(itm)
                                records.append(polished)
                                JAV_KNOWN_IDS.add(str(itm["source_post_id"]))
                            res = upsert_japanese_videos_batch(records)
                            log(f"      [✓] Batch upserted {len(records)} videos ({res})")
                            new_ingested += len(records)
                            save_known_post_ids(JAV_KNOWN_IDS)
                    else:
                        log(f"    Historical Page {curr_cursor}: All {len(hist_items)} releases already indexed.")

                    # Advance cursor
                    cat_state["current_page"] = curr_cursor + 1
                    save_checkpoint(checkpoint)
                    log(f"    [✓] Progress saved: [{cat}] cursor advanced to page {curr_cursor + 1}")
                elif hist_items is not None and len(hist_items) == 0:
                    cat_state["completed"] = True
                    save_checkpoint(checkpoint)
                    log(f"    [✓] Reached end of category [{cat}]. Marked completed in checkpoint.")
                elif hist_items is None:
                    log(f"    [!] Skipping cursor advance for [{cat}] due to Cloudflare block.")

        except Exception as e:
            log(f"    [!] Error checking {cat}: {e}")

        human_jitter(1.0, 2.0)

    log(f"\n  [✓] Japanese JAV sync complete. New videos added: {new_ingested}")
    return new_ingested


# ─── Myanmar (Buu Mal) Ingestion Task ────────────────────────────────────────

def run_myanmar_sync(workers: int = 4) -> int:
    """Runs Buu Mal (Myanmar) ingestion: checks Page 1 for new releases, then advances cursor in Supabase."""
    global MYANMAR_KNOWN_IDS
    log("\n" + "=" * 65)
    log(f"  [FEED 3/3] Buu Mal Myanmar Scraper | Isolated Schema")
    log("=" * 65)
    try:
        from scraper.categories.myanmar.pipeline import (
            scrape_page,
            load_checkpoint as load_myanmar_checkpoint,
            save_checkpoint as save_myanmar_checkpoint,
            save_known_ids as save_myanmar_known_ids
        )
        from scraper.db import upsert_myanmar_videos_batch, get_scraper_checkpoint, set_scraper_checkpoint

        myanmar_state = get_scraper_checkpoint("myanmar")
        if not myanmar_state or "current_page" not in myanmar_state:
            local_cp = load_myanmar_checkpoint()
            start_p = local_cp.get("current_page", local_cp.get("last_page", 34))
            myanmar_state = {
                "current_page": max(start_p, 34),
                "max_pages": 3690,
                "total_ingested": len(MYANMAR_KNOWN_IDS)
            }

        curr_page = myanmar_state.get("current_page", 34)
        ingested = 0

        # 1. Scrape Page 1 (site-wide latest) for brand new releases
        p1_records = scrape_page(1, tag="", known_ids=MYANMAR_KNOWN_IDS, workers=workers)
        if p1_records:
            upsert_myanmar_videos_batch(p1_records)
            for r in p1_records:
                MYANMAR_KNOWN_IDS.add(str(r["source_post_id"]))
            save_myanmar_known_ids(MYANMAR_KNOWN_IDS)
            ingested += len(p1_records)
            log(f"    [✓] Page 1: Ingested {len(p1_records)} new Buu Mal releases")
        else:
            log("    Page 1: Up to date (all releases already indexed)")

        # 2. Advance historical backfill cursor (site-wide catalog)
        max_pages = myanmar_state.get("max_pages", 3690)
        if curr_page <= max_pages:
            log(f"    [*] Myanmar Historical Cursor: Crawling page {curr_page} of {max_pages}...")
            cursor_records = scrape_page(curr_page, tag="", known_ids=MYANMAR_KNOWN_IDS, workers=workers)
            if cursor_records:
                upsert_myanmar_videos_batch(cursor_records)
                for r in cursor_records:
                    MYANMAR_KNOWN_IDS.add(str(r["source_post_id"]))
                save_myanmar_known_ids(MYANMAR_KNOWN_IDS)
                ingested += len(cursor_records)
                log(f"    [✓] Historical Page {curr_page}: Ingested {len(cursor_records)} videos")
            else:
                log(f"    Historical Page {curr_page}: All releases already indexed (or page empty).")

            myanmar_state["current_page"] = curr_page + 1
            myanmar_state["total_ingested"] = myanmar_state.get("total_ingested", 0) + ingested
            set_scraper_checkpoint("myanmar", myanmar_state)
            save_myanmar_checkpoint(myanmar_state)
            log(f"  [✓] Myanmar Checkpoint: Cursor advanced to page {curr_page + 1} (Saved to Supabase)")

        log(f"  [✓] Myanmar sync complete. Total new videos: {ingested}")
        return ingested
    except Exception as e:
        log(f"  [!] Myanmar sync error: {e}")
        return 0

# ─── Unified Cycle Executor ──────────────────────────────────────────────────

def run_unified_sync_cycle(mms_pages: int = 1, jav_pages: int = 1, workers: int = 3) -> Dict[str, int]:
    """Executes a single combined sync cycle across target websites."""
    t0 = time.time()
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log("\n" + "#" * 65)
    log(f"  STARTING UNIFIED SYNC CYCLE | {timestamp}")
    log("#" * 65)

    # Supabase Health Gate: Wait up to 10m for database & Cloudflare tunnel to be fully online
    wait_for_supabase(max_wait_seconds=600, poll_interval=15)

    # Initialize in-memory deduplication caches from Supabase
    init_known_id_caches()

    checkpoint = load_checkpoint()

    # 1. Scrape Viral MMS (Head + Cursor Backfill)
    mms_count = run_viral_mms_sync(pages=mms_pages, checkpoint=checkpoint)

    # Human transition pause
    human_jitter(2.0, 3.5)

    # 2. Scrape Japanese JAV (Head + Cursor Backfill)
    jav_count = run_japanese_jav_sync(pages=jav_pages, workers=workers, checkpoint=checkpoint)

    # Human transition pause
    human_jitter(1.5, 3.0)

    # 3. Scrape Myanmar (Head + Cursor Backfill)
    myanmar_count = run_myanmar_sync(workers=workers)

    elapsed = time.time() - t0
    log("\n" + "#" * 65)
    log(f"  CYCLE SUMMARY | Elapsed: {elapsed:.1f}s")
    log(f"  - Viral MMS:    {mms_count} videos processed")
    log(f"  - Japanese JAV: {jav_count} new videos ingested")
    log(f"  - Myanmar:      {myanmar_count} new videos ingested")
    log("#" * 65)

    return {"mms": mms_count, "jav": jav_count, "myanmar": myanmar_count}

# ─── Continuous 24/7 Session Daemon (For GitHub Actions) ─────────────────────

def run_continuous_daemon(
    session_minutes: int = 300,
    interval_seconds: int = 60,
    workers: int = 3
) -> None:
    """
    Runs a continuous multi-site scraping daemon.
    Designed for GitHub Actions 6-hour runners:
    Runs for ~session_minutes (default 300 min / 5 hours),
    checking both sites every interval_seconds (default 60s).
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

        if remaining <= (interval_seconds + 30):
            log(f"\n[HANDOFF] Session limit reached ({elapsed / 60:.1f}m / {session_minutes}m).")
            log("[HANDOFF] Exiting cleanly so successor job can run without queuing.")
            break

        log(f"\n>>> CYCLE #{cycle} | Session elapsed: {elapsed / 60:.1f}m | Remaining: {remaining / 60:.1f}m <<<")
        try:
            run_unified_sync_cycle(mms_pages=1, jav_pages=1, workers=workers)
        except Exception as e:
            log(f"[!] Error in cycle #{cycle}: {e}")

        # Human-jittered sleep between cycles (~60s default) with active live heartbeats
        jittered_sleep = max(45.0, interval_seconds + random.uniform(-5.0, 15.0))
        sleep_total = int(jittered_sleep)
        log(f"\n[*] Cycle #{cycle} complete. Entering polite sleep for {sleep_total}s...")
        cycle += 1

        # Print active heartbeat every 15s so GitHub Actions log is always live
        heartbeat_interval = 15
        for remaining_sec in range(sleep_total, 0, -heartbeat_interval):
            log(f"    [Heartbeat] Next scan in {remaining_sec}s... Daemon active & healthy.")
            time.sleep(min(heartbeat_interval, remaining_sec))

# ─── CLI Entrypoint ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chuglii Unified Multi-Site Scraper")
    parser.add_argument("--sync", action="store_true", help="Run a single one-shot sync cycle and exit")
    parser.add_argument("--daemon", action="store_true", help="Run in continuous daemon mode")
    parser.add_argument("--session-minutes", type=int, default=300, help="Max session duration in minutes (default: 300)")
    parser.add_argument("--interval", type=int, default=60, help="Seconds between scan cycles (default: 60)")
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
