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
  - Supabase outage gate: waits for RPC recovery before each cycle, never scrapes offline
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
from typing import Dict, Any, List, Optional, Set, Tuple

# Force immediate line-buffering on stdout/stderr for real-time live streaming logs in GitHub Actions
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent

# ─── Deep-backfill automation policy ────────────────────────────────────────
# Deep mode grinds the archive backlog BEFORE the regular feed cycle. Left
# unbounded it consumed an entire 5-hour session and every feed starved
# (run 45742796072). It is now auto-armed when a backlog exists but hard-capped
# to a fraction of the session, so feeds always get their share.
# Feed order inside a cycle. It is rotated every cycle (see
# ``FEED_ROTATION``/``cycle_rotation`` in run_unified_sync_cycle) so a feed that
# cannot finish in its slice is first next time instead of never.
FEED_ROTATION: Tuple[str, ...] = (
    "viral-mms",
    "viral-mms-sites",
    "supjav",
    "myanmar",
    "samusar",
    "onlyfans",
    "voyeurhouse",
    "chinese",
    "fangsung",
    "studios",
)
# Smallest slice worth giving a feed (below this it is skipped, not half-run).
FEED_MIN_SECONDS = float(os.environ.get("SCRAPER_FEED_MIN_SECONDS", "240"))

DEEP_SESSION_FRACTION = float(os.environ.get("SCRAPER_DEEP_SESSION_FRACTION", "0.25"))
DEEP_AUTOARM = os.environ.get("SCRAPER_DEEP_AUTOARM", "1") not in ("0", "false", "False", "")
DEEP_AUTOARM_MIN_PAGES = int(os.environ.get("SCRAPER_DEEP_MIN_PAGES", "200"))

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

from scraper import budget
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


def wait_for_supabase_before_cycle(deadline: Optional[float] = None) -> bool:
    """Waits for Supabase until the wait deadline; never scrapes while offline."""
    max_wait = 600 if deadline is None else int(max(0, deadline - time.time()))
    if max_wait <= 0:
        log("[!] Supabase is still offline at session handoff. Successor run will retry.")
        return False
    return bool(wait_for_supabase(max_wait_seconds=max_wait, poll_interval=15))


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
            VIRAL_MMS_KNOWN_IDS = get_scraper_known_post_ids("viral-mms", limit=200000)
        except Exception:
            VIRAL_MMS_KNOWN_IDS = set()
    log(f"  [✓] Viral MMS Deduplication Cache: {len(VIRAL_MMS_KNOWN_IDS)} known IDs loaded")

    # 2. Japanese JAV IDs
    try:
        JAV_KNOWN_IDS = load_japanese_known_ids()
    except Exception:
        try:
            from scraper.db import get_scraper_known_post_ids
            JAV_KNOWN_IDS = get_scraper_known_post_ids("supjav", limit=200000)
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
            MYANMAR_KNOWN_IDS = get_scraper_known_post_ids("myanmar", limit=200000)
        except Exception:
            MYANMAR_KNOWN_IDS = set()
    log(f"  [✓] Myanmar Deduplication Cache: {len(MYANMAR_KNOWN_IDS)} known IDs loaded")

    CACHES_INITIALIZED = True

# ─── Viral MMS Ingestion Task ────────────────────────────────────────────────

def run_viral_mms_sync(
    check_head: bool = True,
    pages: int = 1,
    deadline: Optional[float] = None,
) -> int:
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


def run_viral_mms_sites_sync(
    check_head: bool = True,
    pages_per_site: int = 1,
    workers: int = 4,
    deadline: Optional[float] = None,
) -> int:
    """Runs isolated per-site Viral MMS sources (spicymms, desi49, mydesi, indianbfclips)."""
    log("\n" + "=" * 65)
    log("  [FEED 1b] Viral MMS Isolated Sites (SpicyMMS, Desi49, MyDesi, IndianBFClips)")
    log("=" * 65)
    try:
        import importlib

        mod = importlib.import_module("scraper.categories.viral-mms.sites_pipeline")
        res = mod.run_multi_site_pipeline(
            pages_per_site=pages_per_site,
            check_head=check_head,
            dry_run=False,
            workers=workers,
            deadline=deadline,
        )
        FEED_STATUS["viral-mms-sites"] = res.get("status", "completed")
        added = int(res.get("upserted", 0) or 0)
        log(f"  [OK] Viral MMS Isolated Sites complete. Added: {added}")
        return added
    except Exception as e:
        FEED_STATUS["viral-mms-sites"] = "failed"
        log(f"  [!] Viral MMS Isolated Sites sync error: {e}")
        return 0


# ─── Japanese JAV Ingestion Task ─────────────────────────────────────────────

def run_japanese_jav_sync(
    check_head: bool = True,
    pages: int = 1,
    workers: int = 3,
    checkpoint: Optional[Dict[str, Any]] = None,
    deadline: Optional[float] = None,
) -> int:
    """Runs SupJav ingestion and only advances cursors after confirmed writes."""
    global JAV_KNOWN_IDS
    log("\n" + "=" * 65)
    log("  [FEED 2/5] SupJav Japanese JAV Scraper | Working Mirrors Only")
    log("=" * 65)

    if budget.expired(deadline):
        FEED_STATUS["supjav"] = "partial"
        log("  [BUDGET] Japanese JAV feed deadline reached; resuming next cycle.")
        return 0
    if not checkpoint:
        checkpoint = load_japanese_checkpoint()
    session = create_japanese_session()
    warm_up_japanese_session(session)
    new_ingested = 0
    category_errors: List[str] = []

    def commit(items: List[Dict[str, Any]], label: str) -> Tuple[bool, Set[str]]:
        """Enriches and upserts ``items``.

        Returns ``(all_confirmed, unresolved_ids)``.  ``all_confirmed`` is False
        when enrichment or the upsert left any item unwritten, so the caller
        must not advance its page cursor.
        """
        nonlocal new_ingested
        wanted = {str(itm["source_post_id"]) for itm in items}
        if not items:
            return True, set()
        enriched = enrich_japanese_items(items, max_workers=workers)
        if not enriched:
            log(f"      [!] {label}: no working mirrors; IDs stay unmarked for retry.")
            return False, wanted
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
            return False, wanted
        JAV_KNOWN_IDS.update(confirmed_ids)
        save_japanese_known_ids(JAV_KNOWN_IDS, new_ids=confirmed_ids, status="stored")
        new_ingested += len(records)
        checkpoint.setdefault("stats", {})["total_ingested"] = int(checkpoint["stats"].get("total_ingested", 0) or 0) + len(records)
        save_japanese_checkpoint(checkpoint)
        log(f"      [✓] {label}: batch upserted {len(records)} videos")
        unresolved = wanted - confirmed_ids
        if unresolved:
            log(f"      [!] {label}: {len(unresolved)} video(s) had no working mirror.")
        return (not unresolved), unresolved

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
                        else:
                            fully_stored, unresolved = commit(new_hist, f"Historical page {curr_cursor}")
                            if not fully_stored:
                                from scraper.progress import cursor_retry_state
                                stall_count, hold = cursor_retry_state(
                                    stall_count=int(cat_state.get("stall_count", 0) or 0),
                                    unresolved=len(unresolved),
                                )
                                cat_state["stall_count"] = stall_count
                                if hold:
                                    save_japanese_checkpoint(checkpoint)
                                    log(
                                        f"    [!] Cursor stays on page {curr_cursor}; "
                                        f"{len(unresolved)} unresolved (attempt {stall_count}/5)."
                                    )
                                    continue
                                log(
                                    f"    [!] Giving up on {len(unresolved)} unresolved video(s) on "
                                    f"page {curr_cursor} after 5 attempts; marking seen and advancing."
                                )
                                JAV_KNOWN_IDS.update(unresolved)
                                save_japanese_known_ids(JAV_KNOWN_IDS, new_ids=unresolved, status="seen")
                        cat_state["current_page"] = curr_cursor + 1
                        cat_state["stall_count"] = 0
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

def run_myanmar_sync(
    check_head: bool = True,
    workers: int = 4,
    deadline: Optional[float] = None,
) -> int:
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

        if budget.expired(deadline):
            FEED_STATUS["myanmar"] = "partial"
            log("  [BUDGET] Myanmar feed deadline reached before its cursor page; resuming next cycle.")
            return 0

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
            myanmar_state["last_page"] = curr_page + 1
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


def run_samusar_sync(
    check_head: bool = True,
    pages: int = 1,
    workers: int = 4,
    deadline: Optional[float] = None,
) -> int:
    """Runs isolated Samusar Myanmar ingestion (scraper.samusar_videos)."""
    log("\n" + "=" * 65)
    log("  [FEED 3b] Samusar Myanmar Scraper | Isolated Schema")
    log("=" * 65)
    try:
        from scraper.categories.myanmar.samusar_pipeline import run_pipeline

        res = run_pipeline(
            pages=pages, check_head=check_head, workers=workers, deadline=deadline
        )
        FEED_STATUS["samusar"] = res.get("status", "completed")
        return int(res.get("upserted", 0) or 0)
    except Exception as e:
        FEED_STATUS["samusar"] = "failed"
        log(f"  [!] Samusar Myanmar sync error: {e}")
        return 0


# ─── OnlyFans Leaks Ingestion Task ───────────────────────────────────────────

def run_onlyfans_sync(
    check_head: bool = True,
    pages_per_site: int = 1,
    deadline: Optional[float] = None,
) -> int:
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
            "sites_requested": list(OF_SOURCES),
        })
        save_of_progress(progress)

        total_new = 0
        failed_sites: List[str] = []
        # Driven by the source registry, so registering a module is the only
        # step needed to add a site.
        sites = list(OF_SOURCES)
        stopped_early = False

        for site_key in sites:
            if budget.expired(deadline):
                stopped_early = True
                log(f"  [BUDGET] OnlyFans feed deadline reached; {site_key} and later sites resume next cycle.")
                break
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
                        deadline=deadline,
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
                        deadline=deadline,
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
        if stopped_early and of_status == "completed":
            of_status = "partial"
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


def run_voyeurhouse_sync(
    check_head: bool = True,
    pages_per_site: int = 1,
    deadline: Optional[float] = None,
) -> int:
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
        blocked_sites: List[str] = []
        stopped_early = False

        for site_key in vh.SOURCES:
            if budget.expired(deadline):
                stopped_early = True
                log(f"  [BUDGET] Voyeur House feed deadline reached; {site_key} and later sites resume next cycle.")
                break
            state = checkpoints.setdefault(site_key, {
                "current_page": 1,
                "max_pages": vh._default_max_pages(site_key),
            })
            try:
                cursor = int(state.get("current_page", 1) or 1)
                max_pages = int(state.get("max_pages", vh._default_max_pages(site_key)) or vh._default_max_pages(site_key))

                if check_head:
                    head = vh.run_site_crawl(
                        site_key, 1, 1, known_ids, jitter=0.5, new_only=True,
                        progress=progress, deadline=deadline,
                    )
                    total_new += int(head.get("upserted", 0) or 0)

                if cursor <= max_pages:
                    stats = vh.run_site_crawl(
                        site_key, cursor, pages_per_site, known_ids, jitter=0.8,
                        progress=progress, deadline=deadline,
                    )
                    total_new += int(stats.get("upserted", 0) or 0)
                    if not stats.get("upsert_failed"):
                        crawled = int(stats.get("pages_crawled", 0) or 0)
                        if crawled > 0:
                            state["current_page"] = cursor + crawled
                    else:
                        failed_sites.append(site_key)
                    if stats.get("fetch_failed"):
                        # The site blocked us: report it as degraded, not done.
                        blocked_sites.append(site_key)
                        state["blocked_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    detected = stats.get("detected_max_pages")
                    if isinstance(detected, int) and detected > 0:
                        state["max_pages"] = max(int(state.get("max_pages", 0) or 0), detected)

                site_progress = progress.setdefault("sites", {}).setdefault(site_key, {})
                site_progress["status"] = (
                    "failed" if site_key in failed_sites
                    else "blocked" if site_key in blocked_sites
                    else "completed"
                )
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
        if blocked_sites and vh_status == "completed":
            vh_status = "partial"
            log(f"  [!] Voyeur House sources blocked by their origin this cycle: {', '.join(blocked_sites)}")
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


# ─── Chinese Category Ingestion Task ─────────────────────────────────────

def run_chinese_sync(
    check_head: bool = True,
    pages_per_category: int = 1,
    deadline: Optional[float] = None,
) -> int:
    """Runs the Chinese category (uxj838238) per-category with remote-first state.

    JAV and livestream categories are excluded by the category whitelist.
    All records go strictly into scraper.chinese_videos.

    ``deadline`` bounds the whole feed, not just one zone: 37 zones x (head page
    + backfill page) at ~2 min/zone is over an hour, which is how this feed once
    ate an entire 4-hour session and starved fangsung/studios.
    """
    log("\n" + "=" * 65)
    log("  [FEED 6/6] Chinese Category Fleet | uxj838238 | Isolated DB")
    log("=" * 65)
    try:
        from scraper.categories.chinese.pipeline import (
            run_category_crawl,
            load_checkpoints,
            save_checkpoints,
            load_known_ids as load_cn_known_ids,
            save_known_ids as save_cn_known_ids,
            load_progress as load_cn_progress,
            save_progress as save_cn_progress,
            SITE_KEY as CN_SITE_KEY,
            DEFAULT_CATEGORIES as CN_CATEGORIES,
        )
        from scraper.categories.chinese.categories import category_info as cn_category_info
        from scraper.progress import finalize_status

        checkpoints = load_checkpoints()
        known_ids = load_cn_known_ids()
        progress = load_cn_progress()
        progress.update({
            "status": "running",
            "started_at": progress.get("started_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "mode": "head_and_backfill",
            "categories_requested": CN_CATEGORIES,
        })
        save_cn_progress(progress)

        total_new = 0
        failed_categories: List[str] = []
        stopped_early = False

        for category_id in CN_CATEGORIES:
            if budget.expired(deadline):
                stopped_early = True
                log(f"  [BUDGET] Chinese feed deadline reached; {category_id} and later zones resume next cycle.")
                break
            if not cn_category_info(category_id):
                continue
            cat_key = f"{CN_SITE_KEY}:{category_id}"
            cat_state = checkpoints.setdefault(cat_key, {"current_page": 1, "max_pages": 50})
            try:
                curr_cursor = int(cat_state.get("current_page", 1) or 1)
                max_pages = int(cat_state.get("max_pages", 50) or 50)

                if check_head:
                    h_stats = run_category_crawl(
                        site_key=CN_SITE_KEY,
                        category_id=category_id,
                        start_page=1,
                        max_pages=1,
                        known_ids=known_ids,
                        dry_run=False,
                        jitter=0.5,
                        new_only=True,
                        progress=progress,
                        deadline=deadline,
                    )
                    total_new += int(h_stats.get("upserted", 0) or 0)

                if curr_cursor <= max_pages:
                    c_stats = run_category_crawl(
                        site_key=CN_SITE_KEY,
                        category_id=category_id,
                        start_page=curr_cursor,
                        max_pages=pages_per_category,
                        known_ids=known_ids,
                        dry_run=False,
                        jitter=0.8,
                        progress=progress,
                        deadline=deadline,
                    )
                    total_new += int(c_stats.get("upserted", 0) or 0)
                    if not c_stats.get("upsert_failed"):
                        crawled = int(c_stats.get("pages_crawled", 0) or 0)
                        if crawled > 0:
                            cat_state["current_page"] = curr_cursor + crawled
                    else:
                        failed_categories.append(category_id)
                    true_max = c_stats.get("true_max_pages")
                    if isinstance(true_max, int) and true_max >= 0:
                        # The listing genuinely ended here; pin past it so this
                        # zone stops re-crawling empty pages every session. The
                        # advertised total must not undo that pin (it is often
                        # larger than the real catalogue), hence the else.
                        cat_state["max_pages"] = max(1, true_max)
                        cat_state["current_page"] = max(2, true_max + 1)
                        print(f"    [✓] {cat_key}: catalogue ends at page {true_max}; zone complete.")
                    else:
                        detected_max = c_stats.get("detected_max_pages")
                        if isinstance(detected_max, int) and detected_max > 0:
                            cat_state["max_pages"] = max(int(cat_state.get("max_pages", 0) or 0), detected_max)

                cat_progress = progress.setdefault("categories", {}).setdefault(cat_key, {})
                cat_progress["status"] = "failed" if category_id in failed_categories else "completed"
            except Exception as e:
                failed_categories.append(category_id)
                cat_progress = progress.setdefault("categories", {}).setdefault(cat_key, {})
                cat_progress["status"] = "failed"
                cat_progress["last_error"] = str(e)
                log(f"  [!] Chinese category [{category_id}] failed in isolation: {e}")

            checkpoints[cat_key] = cat_state
            save_cn_known_ids(known_ids)
            save_checkpoints(checkpoints)
            save_cn_progress(progress)

        cn_status = finalize_status(progress, failed_categories)
        if stopped_early and cn_status == "completed":
            # Stopped on the feed budget, not finished: report it as degraded so
            # the cycle summary never claims full coverage it did not reach.
            cn_status = "partial"
        FEED_STATUS["chinese"] = cn_status
        progress["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        progress["totals"] = {"upserted": total_new}
        save_cn_progress(progress)

        log(f"\n  [OK] Chinese category sync complete. Records appended: {total_new}")
        return total_new
    except Exception as e:
        FEED_STATUS["chinese"] = "failed"
        log(f"  [!] Chinese sync error: {e}")
        return 0


def run_fangsung_sync(
    check_head: bool = True,
    pages_per_category: int = 1,
    deadline: Optional[float] = None,
) -> int:
    """Runs the fangsung category (www.fangsung.com) per-region-term with
    remote-first state. All records go strictly into scraper.fangsung_videos.

    ``deadline`` stops the term loop so the feed can never outlive its slice.
    """
    log("\n" + "=" * 65)
    log("  [FEED 7/7] Fangsung Category Fleet | www.fangsung.com | Isolated DB")
    log("=" * 65)
    try:
        from scraper.categories.fangsung.pipeline import (
            run_category_crawl,
            load_checkpoints,
            save_checkpoints,
            load_known_ids as load_fs_known_ids,
            save_known_ids as save_fs_known_ids,
            load_progress as load_fs_progress,
            save_progress as save_fs_progress,
            SITE_KEY as FS_SITE_KEY,
            DEFAULT_CATEGORIES as FS_CATEGORIES,
        )
        from scraper.categories.fangsung.categories import category_info as fs_category_info
        from scraper.progress import finalize_status

        checkpoints = load_checkpoints()
        known_ids = load_fs_known_ids()
        progress = load_fs_progress()
        progress.update({
            "status": "running",
            "started_at": progress.get("started_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "mode": "head_and_backfill",
            "categories_requested": FS_CATEGORIES,
        })
        save_fs_progress(progress)

        total_new = 0
        failed_categories: List[str] = []
        stopped_early = False

        for category_id in FS_CATEGORIES:
            if budget.expired(deadline):
                stopped_early = True
                log(f"  [BUDGET] Fangsung feed deadline reached; {category_id} and later terms resume next cycle.")
                break
            if not fs_category_info(category_id):
                continue
            cat_key = f"{FS_SITE_KEY}:{category_id}"
            cat_state = checkpoints.setdefault(cat_key, {"current_page": 1, "max_pages": 50})
            try:
                curr_cursor = int(cat_state.get("current_page", 1) or 1)
                max_pages = int(cat_state.get("max_pages", 50) or 50)

                if check_head:
                    h_stats = run_category_crawl(
                        site_key=FS_SITE_KEY,
                        category_id=category_id,
                        start_page=1,
                        max_pages=1,
                        known_ids=known_ids,
                        dry_run=False,
                        jitter=0.5,
                        new_only=True,
                        progress=progress,
                        deadline=deadline,
                    )
                    total_new += int(h_stats.get("upserted", 0) or 0)

                if curr_cursor <= max_pages:
                    c_stats = run_category_crawl(
                        site_key=FS_SITE_KEY,
                        category_id=category_id,
                        start_page=curr_cursor,
                        max_pages=pages_per_category,
                        known_ids=known_ids,
                        dry_run=False,
                        jitter=0.8,
                        progress=progress,
                        deadline=deadline,
                    )
                    total_new += int(c_stats.get("upserted", 0) or 0)
                    if not c_stats.get("upsert_failed"):
                        crawled = int(c_stats.get("pages_crawled", 0) or 0)
                        if crawled > 0:
                            cat_state["current_page"] = curr_cursor + crawled
                    else:
                        failed_categories.append(category_id)
                    true_max = c_stats.get("true_max_pages")
                    if isinstance(true_max, int) and true_max >= 0:
                        # The listing genuinely ended here; pin past it so this
                        # zone stops re-crawling empty pages every session. The
                        # advertised total must not undo that pin (it is often
                        # larger than the real catalogue), hence the else.
                        cat_state["max_pages"] = max(1, true_max)
                        cat_state["current_page"] = max(2, true_max + 1)
                        print(f"    [✓] {cat_key}: catalogue ends at page {true_max}; zone complete.")
                    else:
                        detected_max = c_stats.get("detected_max_pages")
                        if isinstance(detected_max, int) and detected_max > 0:
                            cat_state["max_pages"] = max(int(cat_state.get("max_pages", 0) or 0), detected_max)

                cat_progress = progress.setdefault("categories", {}).setdefault(cat_key, {})
                cat_progress["status"] = "failed" if category_id in failed_categories else "completed"
            except Exception as e:
                failed_categories.append(category_id)
                cat_progress = progress.setdefault("categories", {}).setdefault(cat_key, {})
                cat_progress["status"] = "failed"
                cat_progress["last_error"] = str(e)
                log(f"  [!] Fangsung term [{category_id}] failed in isolation: {e}")

            checkpoints[cat_key] = cat_state
            save_fs_known_ids(known_ids)
            save_checkpoints(checkpoints)
            save_fs_progress(progress)

        fs_status = finalize_status(progress, failed_categories)
        if stopped_early and fs_status == "completed":
            fs_status = "partial"
        FEED_STATUS["fangsung"] = fs_status
        progress["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        progress["totals"] = {"upserted": total_new}
        save_fs_progress(progress)

        log(f"\n  [OK] Fangsung category sync complete. Records appended: {total_new}")
        return total_new
    except Exception as e:
        FEED_STATUS["fangsung"] = "failed"
        log(f"  [!] Fangsung sync error: {e}")
        return 0


def run_studios_sync(
    check_head: bool = True,
    pages_per_category: int = 1,
    deadline: Optional[float] = None,
) -> int:
    """Runs the studios category (multi-site: madouer.cc in Phase A) per
    whitelisted site section with remote-first state. All records go strictly
    into scraper.studios_videos."""
    log("\n" + "=" * 65)
    log("  [FEED 8/8] Studios Category Fleet | madouer.cc | Isolated DB")
    log("=" * 65)
    try:
        from scraper.categories.studios.pipeline import (
            run_category_crawl,
            load_checkpoints,
            save_checkpoints,
            load_known_ids as load_st_known_ids,
            save_known_ids as save_st_known_ids,
            load_progress as load_st_progress,
            save_progress as save_st_progress,
            ALL_ZONES as ST_ZONES,
        )
        from scraper.categories.studios.categories import category_info as st_category_info
        from scraper.progress import finalize_status

        checkpoints = load_checkpoints()
        known_ids = load_st_known_ids()
        progress = load_st_progress()
        progress.update({
            "status": "running",
            "started_at": progress.get("started_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "mode": "head_and_backfill",
            "categories_requested": [f"{sk}:{c}" for sk, c in ST_ZONES],
        })
        save_st_progress(progress)

        total_new = 0
        failed_categories: List[str] = []
        stopped_early = False

        for st_site, category_id in ST_ZONES:
            if budget.expired(deadline):
                stopped_early = True
                log(f"  [BUDGET] Studios feed deadline reached; {st_site}:{category_id} and later zones resume next cycle.")
                break
            if not st_category_info(category_id):
                continue
            cat_key = f"{st_site}:{category_id}"
            cat_state = checkpoints.setdefault(cat_key, {"current_page": 1, "max_pages": 50})
            cat_progress = progress.setdefault("categories", {}).setdefault(cat_key, {})
            try:
                curr_cursor = int(cat_state.get("current_page", 1) or 1)
                max_pages = int(cat_state.get("max_pages", 50) or 50)

                if check_head:
                    h_stats = run_category_crawl(
                        site_key=st_site,
                        category_id=category_id,
                        start_page=1,
                        max_pages=1,
                        known_ids=known_ids,
                        dry_run=False,
                        jitter=0.5,
                        new_only=True,
                        progress=progress,
                        deadline=deadline,
                    )
                    total_new += int(h_stats.get("upserted", 0) or 0)

                if curr_cursor <= max_pages:
                    c_stats = run_category_crawl(
                        site_key=st_site,
                        category_id=category_id,
                        start_page=curr_cursor,
                        max_pages=pages_per_category,
                        known_ids=known_ids,
                        dry_run=False,
                        jitter=0.8,
                        progress=progress,
                        deadline=deadline,
                    )
                    total_new += int(c_stats.get("upserted", 0) or 0)
                    if not c_stats.get("upsert_failed"):
                        crawled = int(c_stats.get("pages_crawled", 0) or 0)
                        if crawled > 0:
                            cat_state["current_page"] = curr_cursor + crawled
                    else:
                        failed_categories.append(category_id)
                    true_max = c_stats.get("true_max_pages")
                    if isinstance(true_max, int) and true_max >= 0:
                        # The listing genuinely ended here; pin past it so this
                        # zone stops re-crawling empty pages every session. The
                        # advertised total must not undo that pin (it is often
                        # larger than the real catalogue), hence the else.
                        cat_state["max_pages"] = max(1, true_max)
                        cat_state["current_page"] = max(2, true_max + 1)
                        print(f"    [✓] {cat_key}: catalogue ends at page {true_max}; zone complete.")
                    else:
                        detected_max = c_stats.get("detected_max_pages")
                        if isinstance(detected_max, int) and detected_max > 0:
                            cat_state["max_pages"] = max(int(cat_state.get("max_pages", 0) or 0), detected_max)

                cat_progress["status"] = "failed" if category_id in failed_categories else "completed"
            except Exception as e:
                failed_categories.append(category_id)
                cat_progress["status"] = "failed"
                cat_progress["last_error"] = str(e)
                log(f"  [!] Studios section [{st_site}:{category_id}] failed in isolation: {e}")

            checkpoints[cat_key] = cat_state
            save_st_known_ids(known_ids)
            save_checkpoints(checkpoints)
            save_st_progress(progress)

        st_status = finalize_status(progress, failed_categories)
        if stopped_early and st_status == "completed":
            st_status = "partial"
        FEED_STATUS["studios"] = st_status
        progress["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        progress["totals"] = {"upserted": total_new}
        save_st_progress(progress)

        log(f"\n  [OK] Studios category sync complete. Records appended: {total_new}")
        return total_new
    except Exception as e:
        FEED_STATUS["studios"] = "failed"
        log(f"  [!] Studios sync error: {e}")
        return 0


# ─── Unified Cycle Executor ───────────────────────────────────────────────────

def run_unified_sync_cycle(
    check_head: bool = True,
    mms_pages: int = 1,
    jav_pages: int = 1,
    of_pages: int = 1,
    vh_pages: int = 1,
    cn_pages: int = 1,
    fs_pages: int = 1,
    st_pages: int = 1,
    workers: int = 3,
    supabase_deadline: Optional[float] = None,
    cycle_rotation: int = 0,
) -> Dict[str, int]:
    """Executes a single combined sync cycle across target websites.

    ``supabase_deadline`` is the session handoff time; each feed receives a
    fair share of the time left (``FEED_ROTATION`` order is rotated by
    ``cycle_rotation`` so no feed is permanently last).
    """
    t0 = time.time()
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    # FEED_STATUS is process-global; a stale "failed" from a previous cycle
    # previously poisoned every later cycle's summary.
    FEED_STATUS.clear()
    mode_str = "FULL SYNC (Head + Historical)" if check_head else "FAST BACKFILL (Historical Cursors)"
    log("\n" + "#" * 65)
    log(f"  STARTING UNIFIED SYNC CYCLE | {timestamp} | {mode_str}")
    log("#" * 65)

    if not wait_for_supabase_before_cycle(supabase_deadline):
        log("[PAUSE] Supabase unavailable. No feed will run until Supabase is back online.")
        return {
            "mms": 0,
            "mms_sites": 0,
            "jav": 0,
            "myanmar": 0,
            "samusar": 0,
            "onlyfans": 0,
            "voyeurhouse": 0,
            "chinese": 0,
            "fangsung": 0,
            "studios": 0,
        }

    init_known_id_caches()

    checkpoint = load_japanese_checkpoint()
    feed_counts: Dict[str, int] = {}

    # Every feed below runs inside its own slice of the session, so no single
    # feed can starve the rest. The list is also rotated per cycle: on
    # 2026-11-27 the Chinese feed started at 14:50 and was still crawling when
    # the job timeout killed the run at 18:34, so feeds 7 and 8 (fangsung,
    # studios) never ran, the checkpoint commit never happened and the 24/7
    # self-chain stopped dead. Rotation means the feeds that went last this
    # cycle go first next cycle.
    feed_plan = list(FEED_ROTATION)
    if len(feed_plan) > 1:
        shift = int(cycle_rotation) % len(feed_plan)
        feed_plan = feed_plan[shift:] + feed_plan[:shift]

    def feed_budget(key: str, feeds_left: int) -> Optional[float]:
        """This feed's slice of what is left of the session."""
        if supabase_deadline is None:
            return None
        return budget.fair_share(supabase_deadline, feeds_left, floor_seconds=FEED_MIN_SECONDS).at

    def run_feed(key: str, fn, feeds_left: int = 1, **kwargs) -> int:
        # Handoff guard: a session that is killed mid-feed loses the checkpoint
        # commit and the successor dispatch. Past the deadline we never start
        # another feed; cursor state is persisted per page, so the next session
        # resumes cleanly.
        if supabase_deadline is not None and time.time() >= supabase_deadline - 120:
            FEED_STATUS.setdefault(key, "skipped")
            feed_counts[key] = 0
            log(f"  [HANDOFF] Deadline reached; skipping feed [{key}] so the session ends cleanly.")
            return 0
        kwargs.setdefault("deadline", feed_budget(key, feeds_left))
        left = budget.remaining(kwargs["deadline"])
        if left is not None:
            log(f"  [BUDGET] feed [{key}] has {left / 60:.1f} min of this session.")
        started = time.time()
        try:
            count = int(fn(**kwargs) or 0)
            feed_counts[key] = count
            FEED_STATUS.setdefault(key, "completed")
            log(f"  [DONE] feed [{key}] finished in {(time.time() - started) / 60:.1f} min.")
            return count
        except Exception as e:
            FEED_STATUS[key] = "failed"
            feed_counts[key] = 0
            log(f"  [!] Feed [{key}] failed in isolation: {e}")
            return 0

    # Runs are executed in the rotated plan order. ``feeds_left`` shrinks as the
    # cycle progresses, so a fast feed returns its unused slice to the pool and
    # the last feed gets everything that is left.
    runners = {
        "viral-mms": (run_viral_mms_sync, {"check_head": check_head, "pages": mms_pages}),
        "viral-mms-sites": (run_viral_mms_sites_sync, {"check_head": check_head, "pages_per_site": mms_pages, "workers": workers}),
        "supjav": (run_japanese_jav_sync, {"check_head": check_head, "pages": jav_pages, "workers": workers, "checkpoint": checkpoint}),
        "myanmar": (run_myanmar_sync, {"check_head": check_head, "workers": workers}),
        "samusar": (run_samusar_sync, {"check_head": check_head, "pages": 1, "workers": workers}),
        "onlyfans": (run_onlyfans_sync, {"check_head": check_head, "pages_per_site": of_pages}),
        "voyeurhouse": (run_voyeurhouse_sync, {"check_head": check_head, "pages_per_site": vh_pages}),
        "chinese": (run_chinese_sync, {"check_head": check_head, "pages_per_category": cn_pages}),
        "fangsung": (run_fangsung_sync, {"check_head": check_head, "pages_per_category": fs_pages}),
        "studios": (run_studios_sync, {"check_head": check_head, "pages_per_category": st_pages}),
    }
    results: Dict[str, int] = {}
    total_feeds = len(feed_plan)
    for index, key in enumerate(feed_plan):
        if key not in runners:
            continue
        fn, kwargs = runners[key]
        results[key] = run_feed(key, fn, feeds_left=max(1, total_feeds - index), **kwargs)
        human_jitter(1.0, 2.0)

    mms_count = results.get("viral-mms", 0)
    mms_sites_count = results.get("viral-mms-sites", 0)
    jav_count = results.get("supjav", 0)
    myanmar_count = results.get("myanmar", 0)
    samusar_count = results.get("samusar", 0)
    of_count = results.get("onlyfans", 0)
    vh_count = results.get("voyeurhouse", 0)
    cn_count = results.get("chinese", 0)
    fs_count = results.get("fangsung", 0)
    st_count = results.get("studios", 0)

    failed_feeds = sorted(k for k, status in FEED_STATUS.items() if status == "failed")
    # A feed skipped at the session deadline is degraded, not healthy: it must
    # show up in the summary instead of silently reading as a clean cycle.
    partial_feeds = sorted(k for k, status in FEED_STATUS.items() if status in ("partial", "skipped"))
    global_status = "failed" if failed_feeds else "partial" if partial_feeds else "completed"
    try:
        from scraper.progress import save_progress as _save_global_progress
        _save_global_progress("unified", {
            "status": global_status,
            "failed_feeds": failed_feeds,
            "degraded_feeds": partial_feeds,
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
    log(f"  - Viral Sites:  {mms_sites_count} new videos ingested")
    log(f"  - Japanese JAV: {jav_count} new videos ingested")
    log(f"  - Myanmar:      {myanmar_count} new videos ingested")
    log(f"  - Samusar:      {samusar_count} new videos ingested")
    log(f"  - OnlyFans:     {of_count} new videos ingested")
    log(f"  - Voyeur House: {vh_count} new videos ingested")
    log(f"  - Chinese:      {cn_count} new items ingested")
    log(f"  - Fangsung:     {fs_count} new videos ingested")
    if failed_feeds:
        log(f"  - Failed feeds: {', '.join(failed_feeds)}")
    if partial_feeds:
        log(f"  - Degraded feeds (items pending retry): {', '.join(partial_feeds)}")
    log("#" * 65)

    return {
        "mms": mms_count,
        "mms_sites": mms_sites_count,
        "jav": jav_count,
        "myanmar": myanmar_count,
        "samusar": samusar_count,
        "onlyfans": of_count,
        "voyeurhouse": vh_count,
        "chinese": cn_count,
        "fangsung": fs_count,
        "studios": st_count,
    }


# ─── Continuous 24/7 Session Daemon (For GitHub Actions) ─────────────────────

def deep_deadline_for_session(
    start_time: float, max_seconds: float, handoff_deadline: float
) -> float:
    """Latest timestamp deep backfill may run until this session.

    Always the earlier of the handoff guard and the session budget, so the
    regular feed cycle keeps at least ``1 - DEEP_SESSION_FRACTION`` of the run.
    """
    budget = start_time + max_seconds * max(0.0, min(DEEP_SESSION_FRACTION, 0.9))
    return min(handoff_deadline - 120, budget)


def run_continuous_daemon(
    session_minutes: int = 300,
    interval_seconds: int = 60,
    workers: int = 3,
    cn_pages: int = 1,
    fs_pages: int = 1,
    st_pages: int = 1,
    deep_chunk_pages: int = 20,
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
    # Reserve the handoff window so a Supabase wait can never consume the
    # successor's checkout time or exceed the GitHub Actions job timeout.
    supabase_handoff_deadline = start_time + max_seconds - (interval_seconds + 30)
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

    # Hard cap on how much of a session deep backfill may consume. Before this,
    # the grinder was given the whole handoff deadline, so one stuck zone could
    # (and did, on 2026-11-26) starve every regular feed for the session.
    log(
        f"  Deep budget:         {DEEP_SESSION_FRACTION:.0%} of session "
        f"({max_seconds * DEEP_SESSION_FRACTION / 60:.0f}m max), auto-arm "
        f"{'ON' if DEEP_AUTOARM else 'OFF'} at >={DEEP_AUTOARM_MIN_PAGES} pages backlog"
    )

    # Proxy fleet: harvest + validate free proxies in the background from the first
    # second of the session, so origins that reject a runner IP (madouhd 466,
    # fangsung challenge) already have a route before they are reached. Nothing
    # to configure: an operator-supplied SCRAPER_PROXY_URL just takes precedence.
    try:
        from scraper.proxy_pool import ensure_started as _ensure_proxies
        from scraper.proxy_pool import status as _proxy_status

        _started = _ensure_proxies()
        _pst = _proxy_status()
        log(
            f"  Proxy fleet:         ON (pool={_pst['pool']}, min={_pst['min_pool']}, harvesting in background)"
            if _started and _pst["enabled"]
            else "  Proxy fleet:         OFF (SCRAPER_PROXY_FALLBACK=0)"
        )
    except Exception as exc:
        log(f"  [!] Proxy fleet unavailable: {exc}")

    # Self-arming: resume the archive grind when a real backlog exists, so the
    # operator no longer has to remember `deep_backfill.py set ...`.
    if DEEP_AUTOARM:
        try:
            from scraper.deep_backfill import auto_arm_if_backlog

            auto_arm_if_backlog(min_pages=DEEP_AUTOARM_MIN_PAGES)
        except Exception as exc:
            log(f"[!] Deep auto-arm skipped: {exc}")

    # Surface the HTTP layer's state so cache/Cloudflare behaviour is visible
    # in the run log instead of being invisible background magic.
    try:
        from scraper import net as _net

        _stats = _net.cache_stats()
        log(
            f"  HTTP cache:          {_stats['entries']} entries "
            f"({_stats['live']} live, {_stats['dead']} dead)"
        )
        log(f"  Politeness:          >= {_net.HOST_MIN_INTERVAL:.2f}s between requests per host")
    except Exception as exc:
        log(f"  [!] HTTP layer status unavailable: {exc}")

    while True:
        elapsed = time.time() - start_time
        remaining = max_seconds - elapsed

        if remaining <= (interval_seconds + 30):
            log(f"\n[HANDOFF] Session limit reached ({elapsed / 60:.1f}m / {session_minutes}m).")
            log("[HANDOFF] Exiting cleanly so successor job can run without queuing.")
            cleanup_orphan_browsers()
            break

        # Never scrape while the data plane is down: rows would be lost and the
        # origin gets hammered for nothing (98 minutes of wasted requests during
        # run 45742796072). Probe at most once a minute, then pause politely.
        db_ready = True
        try:
            from scraper.db import supabase_available, wait_for_supabase

            db_ready = supabase_available()
            if not db_ready:
                log("\n[PAUSE] Supabase data plane unavailable; not scraping this cycle.")
                db_ready = wait_for_supabase(
                    max_wait_seconds=max(60, min(300, int(remaining - 60))),
                    poll_interval=15,
                )
        except Exception as exc:
            log(f"[!] Supabase health probe failed ({exc}); continuing.")

        if not db_ready:
            log("[PAUSE] Still offline. Sleeping before the next attempt.")
            time.sleep(max(30.0, min(interval_seconds, 120)))
            cycle += 1
            continue

        # Check Page 1 on Cycle 1 or when interval has elapsed
        now = time.time()
        check_head = (cycle == 1) or (now - last_head_check >= HEAD_CHECK_INTERVAL)
        if check_head:
            last_head_check = now

        # Deep-first: when the persistent flag is active, grind the archive
        # backlog BEFORE the regular feed cycle. Previously the grind only ran
        # AFTER feeds 1-7, so a newly added deep target (e.g. madouhd's 152
        # zones) was starved behind ~30+ minutes of other feeds every session
        # - and any session cancelled mid-cycle never reached it at all.
        try:
            from scraper.deep_backfill import get_flag, run_deep_backfill

            _flag = get_flag()
            if _flag.get("active"):
                _targets = _flag.get("targets") or []
                log(f"\n[DEEP] Deep backfill ACTIVE for: {', '.join(_targets) or 'none'} - grinding BEFORE feed cycle")
                run_deep_backfill(
                    targets=_targets,
                    deadline=min(
                        supabase_handoff_deadline - 120,
                        deep_deadline_for_session(start_time, max_seconds, supabase_handoff_deadline),
                    ),
                    chunk_pages=deep_chunk_pages,
                )
                if time.time() >= supabase_handoff_deadline - 180:
                    log("\n[HANDOFF] Deep backfill consumed the session. Exiting cleanly.")
                    cleanup_orphan_browsers()
                    break
        except Exception as e:
            log(f"[!] Deep backfill error: {e}")

        log(f"\n>>> CYCLE #{cycle} | Session elapsed: {elapsed / 60:.1f}m | Remaining: {remaining / 60:.1f}m <<<")
        try:
            from scraper.proxy_pool import status as _proxy_status
            from scraper.net import blocked_hosts as _blocked_hosts

            _st = _proxy_status()
            log(
                f"    [proxy] pool={_st['pool']}/{_st['min_pool']} | "
                f"coverage={_st['coverage'] or '-'} | probed={_st['stats'].get('probed', 0)} "
                f"evicted={_st['stats'].get('evicted', 0)}"
            )
            _b = _blocked_hosts()
            if _b:
                log("    [proxy] egress-blocked hosts: " + ", ".join(f"{h}({int(a)}s)" for h, a in _b.items()))
        except Exception:
            pass

        try:
            run_unified_sync_cycle(
                check_head=check_head,
                mms_pages=1,
                jav_pages=1,
                of_pages=1,
                vh_pages=1,
                cn_pages=cn_pages,
                fs_pages=fs_pages,
                st_pages=st_pages,
                workers=workers,
                supabase_deadline=supabase_handoff_deadline,
                cycle_rotation=cycle - 1,
            )
        except Exception as e:
            log(f"[!] Error in cycle #{cycle}: {e}")

        # (Deep-backfill grind is handled deep-first at the top of the cycle,
        # so no post-cycle pass is needed: if the flag is still active and
        # session time remains, the next iteration grinds again.)

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
    parser.add_argument("--cn-pages", type=int, default=1, help="Pages per Chinese category to scrape per run")
    parser.add_argument("--fs-pages", type=int, default=1, help="Pages per fangsung region term to scrape per run")
    parser.add_argument("--st-pages", type=int, default=1, help="Pages per studios site section to scrape per run")
    parser.add_argument("--deep-chunk-pages", type=int, default=20, help="Pages per zone per deep-backfill chunk (default: 20)")
    parser.add_argument("--viral-mms-sites", action="store_true", help="Run only isolated Viral MMS sites sync")
    parser.add_argument("--samusar", action="store_true", help="Run only isolated Samusar Myanmar sync")
    parser.add_argument("--onlyfans", action="store_true", help="Run only the OnlyFans sync cycle")
    parser.add_argument("--voyeurhouse", action="store_true", help="Run only the Voyeur House sync cycle")
    parser.add_argument("--chinese", action="store_true", help="Run only the Chinese category sync cycle")
    parser.add_argument("--fangsung", action="store_true", help="Run only the fangsung category sync cycle")
    parser.add_argument("--studios", action="store_true", help="Run only the studios category sync cycle")
    parser.add_argument("--thumbs", type=int, default=0, metavar="N", help="Generate sprite storyboards for N videos and exit")
    parser.add_argument("--check-head", action="store_true", default=True, help="Check Page 1 for latest releases")
    args = parser.parse_args()

    if args.thumbs:
        from scraper.thumbnails import backfill_table
        done = backfill_table("chinese_videos", limit=args.thumbs)
        log(f"[thumbs] backfill complete: {done} storyboard(s) generated")
        sys.exit(0)

    if args.viral_mms_sites:
        if wait_for_supabase_before_cycle():
            run_viral_mms_sites_sync(check_head=args.check_head, pages_per_site=args.mms_pages, workers=args.workers)
        else:
            log("[STOP] Supabase unavailable; Viral MMS sites sync did not start.")
    elif args.samusar:
        if wait_for_supabase_before_cycle():
            run_samusar_sync(check_head=args.check_head, pages=args.mms_pages, workers=args.workers)
        else:
            log("[STOP] Supabase unavailable; Samusar Myanmar sync did not start.")
    elif args.onlyfans:
        if wait_for_supabase_before_cycle():
            init_known_id_caches()
            run_onlyfans_sync(check_head=args.check_head, pages_per_site=args.of_pages)
        else:
            log("[STOP] Supabase unavailable; OnlyFans sync did not start.")
    elif args.voyeurhouse:
        if wait_for_supabase_before_cycle():
            run_voyeurhouse_sync(check_head=args.check_head, pages_per_site=args.vh_pages)
        else:
            log("[STOP] Supabase unavailable; Voyeur House sync did not start.")
    elif args.chinese:
        if wait_for_supabase_before_cycle():
            run_chinese_sync(check_head=args.check_head, pages_per_category=args.cn_pages)
        else:
            log("[STOP] Supabase unavailable; Chinese sync did not start.")
    elif args.fangsung:
        if wait_for_supabase_before_cycle():
            run_fangsung_sync(check_head=args.check_head, pages_per_category=args.fs_pages)
        else:
            log("[STOP] Supabase unavailable; Fangsung sync did not start.")
    elif args.studios:
        if wait_for_supabase_before_cycle():
            run_studios_sync(check_head=args.check_head, pages_per_category=args.st_pages)
        else:
            log("[STOP] Supabase unavailable; Studios sync did not start.")
    elif args.daemon:
        run_continuous_daemon(
            session_minutes=args.session_minutes,
            interval_seconds=args.interval,
            workers=args.workers,
            cn_pages=args.cn_pages,
            fs_pages=args.fs_pages,
            st_pages=args.st_pages,
            deep_chunk_pages=args.deep_chunk_pages,
        )
    else:
        run_unified_sync_cycle(
            check_head=args.check_head,
            mms_pages=args.mms_pages,
            jav_pages=args.jav_pages,
            of_pages=args.of_pages,
            vh_pages=args.vh_pages,
            cn_pages=args.cn_pages,
            fs_pages=args.fs_pages,
            workers=args.workers
        )
