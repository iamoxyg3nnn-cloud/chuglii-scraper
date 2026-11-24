"""
Chuglii Unified Multi-Site Scraper Engine
Combines:
  1. Viral MMS (MMSVibeX -> scraper.videos)
  2. Japanese JAV (SupJav -> scraper.japanese_videos)
  3. Myanmar (Buu Mal -> scraper.myanmar_videos)
  4. OnlyFans Leaks (multi-site -> scraper.onlyfans_videos)
  5. Voyeur House (5 isolated websites -> per-site scraper tables)

Features:
  - Human-like adaptive rate limiting with randomized jitter
  - Authentic Chrome TLS JA3/JA4 fingerprinting (curl_cffi chrome124)
  - Automatic exponential backoff on 429/503/403 throttling
  - Sub-millisecond deduplication (skips already indexed videos)
  - Thread-safe Cloudflare session synchronization (no multi-process browser stampedes)
  - Automatic orphan browser cleanup (reaps zombie chrome/patchright processes on exit/signal)
  - Intelligent Head-check throttling (checks Page 1 every 10m, focuses continuous daemon on historical backfill)
  - Strict working locker enforcement (VOE HD, VidSpeed, LuluStream)
  - Self-chaining daemon mode for continuous 24/7 cloud execution
  - Immediate unbuffered live logs with active heartbeat countdowns
"""

import os
import sys
import time
import random
import signal
import atexit
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

# ─── Automatic Zombie / Orphan Process Cleanup ──────────────────────────────

def cleanup_orphan_browsers() -> None:
    """Terminates any leftover orphan Chrome or Patchright browser processes."""
    if sys.platform != "win32":
        try:
            import subprocess
            subprocess.run(["pkill", "-f", "chrome"], capture_output=True, timeout=5)
            subprocess.run(["pkill", "-f", "patchright"], capture_output=True, timeout=5)
        except Exception:
            pass

atexit.register(cleanup_orphan_browsers)

def handle_shutdown_signal(sig, frame):
    log(f"\n[!] Signal {sig} received. Cleaning up orphan browser processes and exiting gracefully...")
    cleanup_orphan_browsers()
    sys.exit(0)

signal.signal(signal.SIGINT, handle_shutdown_signal)
if hasattr(signal, "SIGTERM"):
    signal.signal(signal.SIGTERM, handle_shutdown_signal)

from scraper.db import (
    get_supabase_client,
    is_supabase_healthy,
    rpc_result_succeeded,
    wait_for_supabase,
)
from scraper.categories.japanese.pipeline import (
    create_session as create_japanese_session,
    warm_up_session as warm_up_japanese_session,
    scrape_listing_page as scrape_japanese_listing,
    enrich_items_concurrently as enrich_japanese_items,
    polish_supjav_record,
    upsert_japanese_videos_batch,
    load_known_post_ids as load_japanese_known_ids,
    save_known_post_ids as save_japanese_known_ids,
    load_checkpoint as load_japanese_checkpoint,
    save_checkpoint as save_japanese_checkpoint,
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
FEED_STATUS: Dict[str, str] = {}

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
            VIRAL_MMS_KNOWN_IDS = get_scraper_known_post_ids("viral-mms", limit=50000)
        except Exception:
            VIRAL_MMS_KNOWN_IDS = set()
    log(f"  [✓] Viral MMS Deduplication Cache: {len(VIRAL_MMS_KNOWN_IDS)} known IDs loaded")

    # 2. Japanese JAV IDs
    try:
        JAV_KNOWN_IDS = load_japanese_known_ids()
    except Exception:
        try:
            from scraper.db import get_scraper_known_post_ids
            JAV_KNOWN_IDS = get_scraper_known_post_ids("supjav", limit=50000)
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
            MYANMAR_KNOWN_IDS = get_scraper_known_post_ids("myanmar", limit=50000)
        except Exception:
            MYANMAR_KNOWN_IDS = set()
    log(f"  [✓] Myanmar Deduplication Cache: {len(MYANMAR_KNOWN_IDS)} known IDs loaded")

    CACHES_INITIALIZED = True

# ─── Viral MMS Ingestion Task ────────────────────────────────────────────────

def run_viral_mms_sync(check_head: bool = True, pages: int = 1) -> int:
    """Runs Viral MMS ingestion without advancing past blocked/failed pages."""
    global VIRAL_MMS_KNOWN_IDS
    log("\n" + "=" * 65)
    log("  [FEED 1/5] Viral MMS Scraper | Isolated Schema")
    log("=" * 65)
    import importlib
    mms_mod = importlib.import_module("scraper.categories.viral-mms.pipeline")
    scrape_mms = mms_mod.scrape_listing_page_with_status
    save_mms_known_ids = mms_mod.save_known_ids
    load_mms_checkpoint = mms_mod.load_checkpoint
    save_mms_checkpoint = mms_mod.save_checkpoint
    from scraper.polish import polish_scraped_record
    from scraper.db import upsert_scraped_videos_batch

    total_ingested = 0

    def ingest(items: List[Dict[str, Any]], label: str) -> bool:
        nonlocal total_ingested
        new_items = [itm for itm in items if str(itm.get("post_id")) not in VIRAL_MMS_KNOWN_IDS]
        if not new_items:
            log(f"    {label}: up to date (all {len(items)} releases already indexed)")
            return True
        batch = [polish_scraped_record(itm) for itm in new_items]
        result = upsert_scraped_videos_batch(batch)
        if not rpc_result_succeeded(result):
            log(f"    [!] {label}: upsert failed ({result}). Keeping IDs unmarked for retry.")
            return False
        confirmed = {str(itm.get("post_id")) for itm in new_items}
        VIRAL_MMS_KNOWN_IDS.update(confirmed)
        save_mms_known_ids(VIRAL_MMS_KNOWN_IDS, new_ids=confirmed, status="stored")
        total_ingested += len(batch)
        log(f"    [✓] {label}: ingested {len(batch)} new Viral MMS videos")
        return True

    try:
        if check_head:
            p1_items, fetch_ok = scrape_mms(1, filter_mode="latest", category=None)
            if not fetch_ok:
                log("    [Head] Page 1 blocked/unavailable; head check skipped.")
            elif not p1_items:
                log("    [Head] Page 1: empty response from MMSVibeX")
            else:
                ingest(p1_items, "Page 1")

        state = load_mms_checkpoint() or {"current_page": 1, "max_pages": 221, "completed": False}
        if state.get("completed"):
            log("    [✓] Viral MMS catalog is 100% indexed (backfill complete).")
        else:
            cursor = int(state.get("current_page", 1) or 1)
            max_pages = int(state.get("max_pages", 221) or 221)
            if cursor <= max_pages:
                log(f"  [*] Viral MMS Progress Tracing: crawling historical page {cursor} of {max_pages}...")
                hist_items, fetch_ok = scrape_mms(cursor, filter_mode="latest", category=None)
                if not fetch_ok:
                    log(f"    [!] Historical page {cursor} blocked/timed out. Cursor stays on {cursor}.")
                elif not hist_items:
                    log(f"    [!] Historical page {cursor} is empty. Treating as end of catalog.")
                    state["current_page"] = cursor + 1
                    state["completed"] = True
                    if save_mms_checkpoint(state):
                        log("  [✓] Viral MMS backfill marked complete.")
                elif ingest(hist_items, f"Historical page {cursor}"):
                    state["current_page"] = cursor + 1
                    state["total_ingested"] = int(state.get("total_ingested", 0) or 0) + int(
                        sum(1 for itm in hist_items if str(itm.get("post_id")) in VIRAL_MMS_KNOWN_IDS)
                    )
                    if state["current_page"] > max_pages:
                        state["completed"] = True
                        log(f"  [✓] Viral MMS Historical Backfill: complete ({max_pages} pages).")
                    if save_mms_checkpoint(state):
                        log(f"  [✓] Viral MMS Checkpoint: cursor advanced to page {state['current_page']}")
                    else:
                        log("  [!] Viral MMS checkpoint was only cached locally; remote sync must retry.")
                else:
                    log(f"    [!] Cursor stays on page {cursor} until the batch succeeds.")

        FEED_STATUS["viral-mms"] = "completed"
        log(f"  [✓] Viral MMS sync complete. New videos added: {total_ingested}")
        return total_ingested
    except Exception as e:
        FEED_STATUS["viral-mms"] = "failed"
        log(f"  [!] Viral MMS sync error: {e}")
        return total_ingested


# ─── Japanese JAV Ingestion Task ─────────────────────────────────────────────

def run_japanese_jav_sync(check_head: bool = True, pages: int = 1, workers: int = 3, checkpoint: Optional[Dict[str, Any]] = None) -> int:
    """Runs SupJav ingestion and only advances cursors after confirmed writes."""
    global JAV_KNOWN_IDS
    log("\n" + "=" * 65)
    log("  [FEED 2/5] SupJav Japanese JAV Scraper | Working Mirrors Only")
    log("=" * 65)

    if not checkpoint:
        checkpoint = load_japanese_checkpoint()

    feed_counts: Dict[str, int] = {}

    def run_feed(key: str, fn, **kwargs) -> int:
        try:
            count = int(fn(**kwargs) or 0)
            feed_counts[key] = count
            FEED_STATUS.setdefault(key, "completed")
            return count
        except Exception as e:
            FEED_STATUS[key] = "failed"
            feed_counts[key] = 0
            log(f"  [!] Feed [{key}] failed in isolation: {e}")
            return 0

    # 1. Scrape Viral MMS (Head + Cursor Backfill)
    mms_count = run_feed("viral-mms", run_viral_mms_sync, check_head=check_head, pages=mms_pages)
    human_jitter(1.5, 2.5)

    # 2. Scrape Japanese JAV (Head + Cursor Backfill)
    jav_count = run_feed("supjav", run_japanese_jav_sync, check_head=check_head, pages=jav_pages, workers=workers, checkpoint=checkpoint)
    human_jitter(1.5, 2.5)

    # 3. Scrape Myanmar (Head + Cursor Backfill)
    myanmar_count = run_feed("myanmar", run_myanmar_sync, check_head=check_head, workers=workers)
    human_jitter(1.5, 2.5)

    # 4. Scrape OnlyFans (Head + Cursor Backfill)
    of_count = run_feed("onlyfans", run_onlyfans_sync, check_head=check_head, pages_per_site=of_pages)

    # 5. Scrape Voyeur House (5 isolated websites)
    vh_count = run_feed("voyeurhouse", run_voyeurhouse_sync, check_head=check_head, pages_per_site=vh_pages)

    failed_feeds = sorted(k for k, status in FEED_STATUS.items() if status == "failed")
    global_status = "failed" if failed_feeds else "completed"
    try:
        from scraper.progress import save_progress as _save_global_progress
        _save_global_progress("unified", {
            "status": global_status,
            "failed_feeds": failed_feeds,
            "feeds": dict(FEED_STATUS),
            "counts": feed_counts,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
    except Exception as e:
        log(f"  [!] Could not persist unified cycle status: {e}")

    elapsed = time.time() - t0
    log("\n" + "#" * 65)
    log(f"  CYCLE SUMMARY | Elapsed: {elapsed:.1f}s | Status: {global_status}")
    log(f"  - Viral MMS:    {mms_count} videos processed")
    log(f"  - Japanese JAV: {jav_count} new videos ingested")
    log(f"  - Myanmar:      {myanmar_count} new videos ingested")
    log(f"  - OnlyFans:     {of_count} new videos ingested")
    log(f"  - Voyeur House: {vh_count} new videos ingested")
    if failed_feeds:
        log(f"  - Failed feeds: {', '.join(failed_feeds)}")
    log("#" * 65)

    return {
        "mms": mms_count,
        "jav": jav_count,
        "myanmar": myanmar_count,
        "onlyfans": of_count,
        "voyeurhouse": vh_count,
    }

# ─── Continuous 24/7 Session Daemon (For GitHub Actions) ─────────────────────

def run_continuous_daemon(
    session_minutes: int = 300,
    interval_seconds: int = 60,
    workers: int = 3
) -> None:
    """
    Runs a continuous multi-site scraping daemon.
    Designed for GitHub Actions 6-hour runners:
    - Runs for ~session_minutes (default 300 min / 5 hours).
    - Checks Page 1 every HEAD_CHECK_INTERVAL (10 min) for newly dropped leaks.
    - Runs rapid historical backfill between head checks to ingest archive catalogs without redundant polling.
    - Provides real-time heartbeat countdown logs so progress is always visible.
    - Exits cleanly before session deadline with zero zombie processes.
    """
    start_time = time.time()
    max_seconds = session_minutes * 60
    cycle = 1
    last_head_check = 0.0
    HEAD_CHECK_INTERVAL = 600.0  # Check Page 1 every 10 minutes

    log("=" * 65)
    log("  CHUGLII 24/7 CONTINUOUS CLOUD SCRAPER DAEMON")
    log(f"  Session Duration:    {session_minutes} min ({session_minutes / 60:.1f} hours)")
    log(f"  Check Interval:      {interval_seconds} sec ({interval_seconds / 60:.1f} min)")
    log(f"  Head Check Cadence:  Every {HEAD_CHECK_INTERVAL / 60:.0f} min (Page 1)")
    log(f"  Streaming Logs:      ACTIVE (unbuffered)")
    log("=" * 65)

    while True:
        elapsed = time.time() - start_time
        remaining = max_seconds - elapsed

        if remaining <= (interval_seconds + 30):
            log(f"\n[HANDOFF] Session limit reached ({elapsed / 60:.1f}m / {session_minutes}m).")
            log("[HANDOFF] Exiting cleanly so successor job can run without queuing.")
            cleanup_orphan_browsers()
            break

        # Check Page 1 on Cycle 1 or when interval has elapsed
        now = time.time()
        check_head = (cycle == 1) or (now - last_head_check >= HEAD_CHECK_INTERVAL)
        if check_head:
            last_head_check = now

        log(f"\n>>> CYCLE #{cycle} | Session elapsed: {elapsed / 60:.1f}m | Remaining: {remaining / 60:.1f}m <<<")
        try:
            run_unified_sync_cycle(check_head=check_head, mms_pages=1, jav_pages=1, of_pages=1, vh_pages=1, workers=workers)
        except Exception as e:
            log(f"[!] Error in cycle #{cycle}: {e}")

        # Human-jittered sleep between cycles with active live heartbeats
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
    parser.add_argument("--of-pages", type=int, default=1, help="Pages of OnlyFans to scrape per site")
    parser.add_argument("--vh-pages", type=int, default=1, help="Pages per Voyeur House website to scrape per run")
    parser.add_argument("--onlyfans", action="store_true", help="Run only the OnlyFans sync cycle")
    parser.add_argument("--voyeurhouse", action="store_true", help="Run only the Voyeur House sync cycle")
    parser.add_argument("--check-head", action="store_true", default=True, help="Check Page 1 for latest releases")
    args = parser.parse_args()

    if args.onlyfans:
        wait_for_supabase(max_wait_seconds=600, poll_interval=15)
        init_known_id_caches()
        run_onlyfans_sync(check_head=args.check_head, pages_per_site=args.of_pages)
    elif args.voyeurhouse:
        wait_for_supabase(max_wait_seconds=600, poll_interval=15)
        run_voyeurhouse_sync(check_head=args.check_head, pages_per_site=args.vh_pages)
    elif args.daemon:
        run_continuous_daemon(
            session_minutes=args.session_minutes,
            interval_seconds=args.interval,
            workers=args.workers
        )
    else:
        run_unified_sync_cycle(
            check_head=args.check_head,
            mms_pages=args.mms_pages,
            jav_pages=args.jav_pages,
            of_pages=args.of_pages,
            vh_pages=args.vh_pages,
            workers=args.workers
        )
