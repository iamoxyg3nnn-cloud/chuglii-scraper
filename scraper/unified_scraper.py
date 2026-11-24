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
                else:
                    total_before = total_ingested
                    if not ingest(hist_items, f"Historical page {cursor}"):
                        log(f"    [!] Cursor stays on page {cursor} until the batch succeeds.")
                        return total_ingested
                    state["current_page"] = cursor + 1
                    state["total_ingested"] = int(state.get("total_ingested", 0) or 0) + (total_ingested - total_before)
                    if state["current_page"] > max_pages:
                        state["completed"] = True
                        log(f"  [✓] Viral MMS Historical Backfill: complete ({max_pages} pages).")
                    if save_mms_checkpoint(state):
                        log(f"  [✓] Viral MMS Checkpoint: cursor advanced to page {state['current_page']}")
                    else:
                        log("  [!] Viral MMS checkpoint was only cached locally; remote sync must retry.")

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
    session = create_japanese_session()
    warm_up_japanese_session(session)
    new_ingested = 0
    category_errors: List[str] = []

    def commit(items: List[Dict[str, Any]], label: str) -> bool:
        nonlocal new_ingested
        if not items:
            return True
        enriched = enrich_japanese_items(items, max_workers=workers)
        if not enriched:
            log(f"      [!] {label}: no working mirrors; IDs stay unmarked for retry.")
            return True
        records = []
        confirmed_ids: Set[str] = set()
        for itm in enriched:
            srvs = [s["name"] for s in itm.get("servers", [])]
            log(f"      + #{itm['source_post_id']} {itm['title'][:35]}... -> {srvs}")
            records.append(polish_supjav_record(itm))
            confirmed_ids.add(str(itm["source_post_id"]))
        res = upsert_japanese_videos_batch(records)
        if not rpc_result_succeeded(res):
            log(f"      [!] {label}: upsert failed ({res}). Keeping IDs unmarked for retry.")
            return False
        JAV_KNOWN_IDS.update(confirmed_ids)
        save_japanese_known_ids(JAV_KNOWN_IDS, new_ids=confirmed_ids, status="stored")
        new_ingested += len(records)
        checkpoint.setdefault("stats", {})["total_ingested"] = int(checkpoint["stats"].get("total_ingested", 0) or 0) + len(records)
        save_japanese_checkpoint(checkpoint)
        log(f"      [✓] {label}: batch upserted {len(records)} videos")
        return True

    for cat in JAV_CATEGORIES:
        log(f"\n  Checking category: [{cat}]")
        try:
            cat_state = checkpoint.setdefault("categories", {}).setdefault(
                cat, {"current_page": 2, "max_pages": 100, "completed": False}
            )
            curr_cursor = max(2, int(cat_state.get("current_page", 2) or 2))
            cat_state["current_page"] = curr_cursor

            if curr_cursor > int(cat_state.get("max_pages", 99999) or 99999):
                cat_state["completed"] = True
                save_japanese_checkpoint(checkpoint)
                log(f"    [✓] Category [{cat}] has completed all available pages.")

            if check_head and not cat_state.get("completed", False):
                p1_items = scrape_japanese_listing(session, page_num=1, category=cat)
                if p1_items is None:
                    log("    [Head] Page 1 blocked/unavailable; skipping head check.")
                else:
                    new_p1 = [itm for itm in p1_items if str(itm["source_post_id"]) not in JAV_KNOWN_IDS]
                    if new_p1:
                        log(f"    [+] Page 1: Found {len(new_p1)} brand new releases! Resolving mirrors...")
                        commit(new_p1, "Head page 1")
                    else:
                        log(f"    [Head] Page 1: Up to date (all {len(p1_items)} releases already indexed)")

            if not cat_state.get("completed", False):
                max_pages = int(cat_state.get("max_pages", 99999) or 99999)
                if curr_cursor <= max_pages:
                    log(f"    [*] Progress Tracing: Crawling historical page {curr_cursor} for [{cat}]...")
                    hist_items = scrape_japanese_listing(session, page_num=curr_cursor, category=cat)
                    if hist_items is None:
                        log(f"    [!] Skipping cursor advance for [{cat}] due to Cloudflare block.")
                    elif not hist_items:
                        cat_state["completed"] = True
                        save_japanese_checkpoint(checkpoint)
                        log(f"    [✓] Reached end of category [{cat}]. Marked completed in checkpoint.")
                    else:
                        new_hist = [itm for itm in hist_items if str(itm["source_post_id"]) not in JAV_KNOWN_IDS]
                        if not new_hist:
                            log(f"    Historical Page {curr_cursor}: All {len(hist_items)} releases already indexed.")
                        elif not commit(new_hist, f"Historical page {curr_cursor}"):
                            log(f"    [!] Cursor stays on page {curr_cursor} until the batch succeeds.")
                            continue
                        cat_state["current_page"] = curr_cursor + 1
                        if cat_state["current_page"] > max_pages:
                            cat_state["completed"] = True
                        save_japanese_checkpoint(checkpoint)
                        log(f"    [✓] Progress saved: [{cat}] cursor advanced to page {cat_state['current_page']}")
        except Exception as e:
            category_errors.append(cat)
            log(f"    [!] Error checking {cat}: {e}")

        human_jitter(1.0, 2.0)

    FEED_STATUS["supjav"] = "failed" if category_errors else "completed"
    log(f"\n  [✓] Japanese JAV sync complete. New videos added: {new_ingested}")
    return new_ingested


# ─── Myanmar (Buu Mal) Ingestion Task ────────────────────────────────────────

def run_myanmar_sync(check_head: bool = True, workers: int = 4) -> int:
    """Runs Buu Mal ingestion without advancing on blocked pages or failed writes."""
    global MYANMAR_KNOWN_IDS
    log("\n" + "=" * 65)
    log("  [FEED 3/5] Buu Mal Myanmar Scraper | Isolated Schema")
    log("=" * 65)
    try:
        from scraper.categories.myanmar.pipeline import (
            scrape_page_with_status,
            load_checkpoint as load_myanmar_checkpoint,
            save_checkpoint as save_myanmar_checkpoint,
            save_known_ids as save_myanmar_known_ids,
        )
        from scraper.db import upsert_myanmar_videos_batch, get_scraper_checkpoint, set_scraper_checkpoint

        myanmar_state = get_scraper_checkpoint("myanmar")
        if not myanmar_state or "current_page" not in myanmar_state:
            local_cp = load_myanmar_checkpoint()
            start_p = int(local_cp.get("current_page", local_cp.get("last_page", 34)) or 34)
            myanmar_state = {
                "current_page": max(start_p, 34),
                "max_pages": 3690,
                "total_ingested": len(MYANMAR_KNOWN_IDS),
            }

        curr_page = int(myanmar_state.get("current_page", 34) or 34)
        ingested = 0

        def commit(records: List[Dict[str, Any]], label: str) -> bool:
            nonlocal ingested
            if not records:
                return True
            result = upsert_myanmar_videos_batch(records)
            if not rpc_result_succeeded(result):
                log(f"    [!] {label}: upsert failed ({result}). Keeping IDs unmarked for retry.")
                return False
            confirmed = {str(r["source_post_id"]) for r in records}
            MYANMAR_KNOWN_IDS.update(confirmed)
            save_myanmar_known_ids(MYANMAR_KNOWN_IDS, new_ids=confirmed, status="stored")
            delta = len(records)
            ingested += delta
            myanmar_state["total_ingested"] = int(myanmar_state.get("total_ingested", 0) or 0) + delta
            log(f"    [✓] {label}: ingested {delta} new Buu Mal videos")
            return True

        if check_head:
            p1_records, fetch_ok = scrape_page_with_status(1, "", MYANMAR_KNOWN_IDS, workers=workers)
            if not fetch_ok:
                log("    [Head] Page 1 blocked/unavailable; head check skipped.")
            elif not p1_records:
                log("    [Head] Page 1: up to date")
            else:
                commit(p1_records, "Page 1")

        max_pages = int(myanmar_state.get("max_pages", 3690) or 3690)
        if curr_page <= max_pages:
            log(f"    [*] Myanmar Historical Cursor: Crawling page {curr_page} of {max_pages}...")
            cursor_records, fetch_ok = scrape_page_with_status(curr_page, "", MYANMAR_KNOWN_IDS, workers=workers)
            if not fetch_ok:
                log(f"    [!] Historical page {curr_page} blocked/unavailable. Cursor stays on {curr_page}.")
                FEED_STATUS["myanmar"] = "failed"
                return ingested
            if cursor_records and not commit(cursor_records, f"Historical page {curr_page}"):
                log(f"    [!] Cursor stays on page {curr_page} until the batch succeeds.")
                FEED_STATUS["myanmar"] = "failed"
                return ingested
            if not cursor_records:
                log(f"    Historical Page {curr_page}: all releases already indexed (or empty).")
            myanmar_state["current_page"] = curr_page + 1
            set_scraper_checkpoint("myanmar", myanmar_state)
            save_myanmar_checkpoint(myanmar_state)
            log(f"  [✓] Myanmar Checkpoint: Cursor advanced to page {curr_page + 1} (Saved to Supabase)")

        FEED_STATUS["myanmar"] = "completed"
        log(f"  [✓] Myanmar sync complete. Total new videos: {ingested}")
        return ingested
    except Exception as e:
        FEED_STATUS["myanmar"] = "failed"
        log(f"  [!] Myanmar sync error: {e}")
        return 0


# ─── OnlyFans Leaks Ingestion Task ───────────────────────────────────────────

def run_onlyfans_sync(check_head: bool = True, pages_per_site: int = 1) -> int:
    """Runs each OnlyFans source independently with remote-first progress state."""
    log("\n" + "=" * 65)
    log("  [FEED 4/5] OnlyFans Leaks Multi-Site Fleet | Per-source isolated")
    log("=" * 65)
    try:
        from scraper.categories.onlyfans.pipeline import (
            run_site_crawl,
            load_checkpoints,
            save_checkpoints,
            load_known_ids as load_of_known_ids,
            save_known_ids as save_of_known_ids,
            load_progress as load_of_progress,
            save_progress as save_of_progress,
            SOURCES as OF_SOURCES,
        )
        from scraper.progress import finalize_status

        checkpoints = load_checkpoints()
        known_ids = load_of_known_ids()
        progress = load_of_progress()
        progress.update({
            "status": "running",
            "started_at": progress.get("started_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "mode": "head_and_backfill",
            "sites_requested": ["internetchicks", "influencersgonewild", "sheeshfans"],
        })
        save_of_progress(progress)

        total_new = 0
        failed_sites: List[str] = []
        sites = ["internetchicks", "influencersgonewild", "sheeshfans"]

        for site_key in sites:
            if site_key not in OF_SOURCES:
                continue
            site_state = checkpoints.setdefault(site_key, {"current_page": 1, "max_pages": 200})
            try:
                curr_cursor = int(site_state.get("current_page", 1) or 1)
                max_pages = int(site_state.get("max_pages", 200) or 200)

                if check_head:
                    h_stats = run_site_crawl(
                        site_key=site_key,
                        start_page=1,
                        max_pages=1,
                        known_ids=known_ids,
                        dry_run=False,
                        jitter=0.5,
                        new_only=True,
                        progress=progress,
                    )
                    total_new += int(h_stats.get("upserted", 0) or 0)

                if curr_cursor <= max_pages:
                    c_stats = run_site_crawl(
                        site_key=site_key,
                        start_page=curr_cursor,
                        max_pages=pages_per_site,
                        known_ids=known_ids,
                        dry_run=False,
                        jitter=0.8,
                        progress=progress,
                    )
                    total_new += int(c_stats.get("upserted", 0) or 0)
                    if not c_stats.get("upsert_failed"):
                        crawled = int(c_stats.get("pages_crawled", 0) or 0)
                        if crawled > 0:
                            site_state["current_page"] = curr_cursor + crawled
                    else:
                        failed_sites.append(site_key)
                    detected_max = c_stats.get("detected_max_pages")
                    if isinstance(detected_max, int) and detected_max > 0:
                        site_state["max_pages"] = max(int(site_state.get("max_pages", 0) or 0), detected_max)

                site_progress = progress.setdefault("sites", {}).setdefault(site_key, {})
                site_progress["status"] = "failed" if site_key in failed_sites else "completed"
            except Exception as e:
                failed_sites.append(site_key)
                site_progress = progress.setdefault("sites", {}).setdefault(site_key, {})
                site_progress["status"] = "failed"
                site_progress["last_error"] = str(e)
                log(f"  [!] OnlyFans site [{site_key}] failed in isolation: {e}")

            checkpoints[site_key] = site_state
            save_of_known_ids(known_ids)
            save_checkpoints(checkpoints)
            save_of_progress(progress)

        of_status = finalize_status(progress, failed_sites)
        FEED_STATUS["onlyfans"] = of_status
        progress["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        progress["totals"] = {"upserted": total_new}
        save_of_progress(progress)

        log(f"\n  [OK] OnlyFans per-source sync complete. Records appended: {total_new}")
        return total_new
    except Exception as e:
        FEED_STATUS["onlyfans"] = "failed"
        log(f"  [!] OnlyFans sync error: {e}")
        return 0


def run_voyeurhouse_sync(check_head: bool = True, pages_per_site: int = 1) -> int:
    """Runs Voyeur House with per-site remote-first state and isolated failures."""
    log("\n" + "=" * 65)
    log("  [FEED 5/5] Voyeur House | 5 isolated websites (voyeur-house.org + reallifecam.to + 3 hotscopes.net)")
    log("=" * 65)
    try:
        from scraper.categories.voyeurhouse import pipeline as vh
        from scraper.progress import finalize_status

        checkpoints = vh.load_checkpoints()
        known_ids = vh.load_known_ids()
        progress = vh.load_progress()
        progress.update({
            "status": "running",
            "started_at": progress.get("started_at") or vh._now_iso(),
            "mode": "head_and_backfill",
            "sites_requested": list(vh.SOURCES),
        })
        vh.save_progress(progress)

        total_new = 0
        failed_sites: List[str] = []

        for site_key in vh.SOURCES:
            state = checkpoints.setdefault(site_key, {
                "current_page": 1,
                "max_pages": vh._default_max_pages(site_key),
            })
            try:
                cursor = int(state.get("current_page", 1) or 1)
                max_pages = int(state.get("max_pages", vh._default_max_pages(site_key)) or vh._default_max_pages(site_key))

                if check_head:
                    head = vh.run_site_crawl(
                        site_key, 1, 1, known_ids, jitter=0.5, new_only=True, progress=progress
                    )
                    total_new += int(head.get("upserted", 0) or 0)

                if cursor <= max_pages:
                    stats = vh.run_site_crawl(
                        site_key, cursor, pages_per_site, known_ids, jitter=0.8, progress=progress
                    )
                    total_new += int(stats.get("upserted", 0) or 0)
                    if not stats.get("upsert_failed"):
                        crawled = int(stats.get("pages_crawled", 0) or 0)
                        if crawled > 0:
                            state["current_page"] = cursor + crawled
                    else:
                        failed_sites.append(site_key)
                    detected = stats.get("detected_max_pages")
                    if isinstance(detected, int) and detected > 0:
                        state["max_pages"] = max(int(state.get("max_pages", 0) or 0), detected)

                site_progress = progress.setdefault("sites", {}).setdefault(site_key, {})
                site_progress["status"] = "failed" if site_key in failed_sites else "completed"
            except Exception as e:
                failed_sites.append(site_key)
                site_progress = progress.setdefault("sites", {}).setdefault(site_key, {})
                site_progress["status"] = "failed"
                site_progress["last_error"] = str(e)
                log(f"  [!] Voyeur House site [{site_key}] failed in isolation: {e}")

            checkpoints[site_key] = state
            vh.save_known_ids(known_ids)
            vh.save_checkpoints(checkpoints)
            vh.save_progress(progress)

        vh_status = finalize_status(progress, failed_sites)
        FEED_STATUS["voyeurhouse"] = vh_status
        progress["finished_at"] = vh._now_iso()
        progress["totals"] = {"upserted": total_new}
        vh.save_progress(progress)

        log(f"\n  [OK] Voyeur House sync complete. Records appended: {total_new}")
        return total_new
    except Exception as e:
        FEED_STATUS["voyeurhouse"] = "failed"
        log(f"  [!] Voyeur House sync error: {e}")
        return 0


# ─── Unified Cycle Executor ──────────────────────────────────────────────────

def run_unified_sync_cycle(
    check_head: bool = True,
    mms_pages: int = 1,
    jav_pages: int = 1,
    of_pages: int = 1,
    vh_pages: int = 1,
    workers: int = 3,
) -> Dict[str, int]:
    """Executes a single combined sync cycle across target websites."""
    t0 = time.time()
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    mode_str = "FULL SYNC (Head + Historical)" if check_head else "FAST BACKFILL (Historical Cursors)"
    log("\n" + "#" * 65)
    log(f"  STARTING UNIFIED SYNC CYCLE | {timestamp} | {mode_str}")
    log("#" * 65)

    wait_for_supabase(max_wait_seconds=600, poll_interval=15)
    init_known_id_caches()

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

    mms_count = run_feed("viral-mms", run_viral_mms_sync, check_head=check_head, pages=mms_pages)
    human_jitter(1.5, 2.5)
    jav_count = run_feed("supjav", run_japanese_jav_sync, check_head=check_head, pages=jav_pages, workers=workers, checkpoint=checkpoint)
    human_jitter(1.5, 2.5)
    myanmar_count = run_feed("myanmar", run_myanmar_sync, check_head=check_head, workers=workers)
    human_jitter(1.5, 2.5)
    of_count = run_feed("onlyfans", run_onlyfans_sync, check_head=check_head, pages_per_site=of_pages)
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
