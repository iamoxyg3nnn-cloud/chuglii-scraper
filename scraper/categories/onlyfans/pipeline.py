"""
Unified Multi-Site OnlyFans Scraper & Cross-Site Deduplication Pipeline
Targets:
  1. Internet Chicks (https://internetchicks.com)
  2. Influencers Gone Wild (https://influencersgonewild.com)
  3. SheeshFans (https://sheeshfans.com)

Features:
 - Multi-tier cross-site deduplication (Locker hash, Semantic token fingerprint, Jaccard overlap)
 - Automatic stream mirror merging (combines VOE, PlayMogo, Streamtape, DoodStream, Direct CDN MP4)
 - Multi-threaded detail enrichment (ThreadPoolExecutor)
 - Zero-dependency stealth scraping via Scrapling Fetcher
 - Resilient per-site checkpoint state persistence
 - Continuous 24/7 daemon watcher mode
"""

import os
import sys
import re
import json
import time
import random
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

CURRENT_DIR = Path(__file__).resolve().parent
CATEGORIES_DIR = CURRENT_DIR.parent
SCRAPER_DIR = CATEGORIES_DIR.parent
PROJECT_ROOT = SCRAPER_DIR.parent

for p in [str(PROJECT_ROOT), str(SCRAPER_DIR), str(CURRENT_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from scraper.categories.onlyfans.polish import polish_onlyfans_record
    from scraper.categories.onlyfans.sources import SOURCES
except ImportError:
    from polish import polish_onlyfans_record
    from sources import SOURCES

from scrapling import Fetcher

CHECKPOINT_FILE = CURRENT_DIR / "checkpoint.json"
KNOWN_IDS_FILE = CURRENT_DIR / "known_post_ids.json"
PROGRESS_FILE = CURRENT_DIR / "progress.json"

# ─────────────────────────────────────────────────────────────────────────────
# Checkpoint & Cache Persistence
# ─────────────────────────────────────────────────────────────────────────────

def load_checkpoints() -> Dict[str, Any]:
    """Loads per-site checkpoints: Supabase-first with local fallback."""
    state = {
        "internetchicks": {"current_page": 1, "max_pages": 513},
        "influencersgonewild": {"current_page": 1, "max_pages": 200},
        "sheeshfans": {"current_page": 1, "max_pages": 200},
        "pimpbunny": {"current_page": 1, "max_pages": 154},
        "total_ingested": 0,
        "total_merged": 0
    }
    if CHECKPOINT_FILE.exists():
        try:
            data = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                state.update(data)
        except Exception:
            pass
    return state

def save_checkpoints(state: Dict[str, Any]) -> None:
    """Saves per-site checkpoints locally and syncs to Supabase."""
    try:
        from scraper.db import set_scraper_checkpoint
        set_scraper_checkpoint("onlyfans_multi", state)
    except Exception:
        pass
    try:
        state["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
        CHECKPOINT_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[!] Warning writing checkpoint file: {e}")

def load_known_ids() -> Set[str]:
    """Loads post ID set formatted as site:post_id."""
    known_ids: Set[str] = set()
    if KNOWN_IDS_FILE.exists():
        try:
            data = json.loads(KNOWN_IDS_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                known_ids.update(str(x) for x in data)
        except Exception:
            pass
    return known_ids

def save_known_ids(known_ids: Set[str]) -> None:
    try:
        KNOWN_IDS_FILE.write_text(json.dumps(sorted(list(known_ids))), encoding="utf-8")
    except Exception as e:
        print(f"[!] Warning writing known IDs cache: {e}")

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_progress() -> Dict[str, Any]:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_progress(progress: Dict[str, Any]) -> None:
    try:
        progress["updated_at"] = _now_iso()
        PROGRESS_FILE.write_text(json.dumps(progress, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        print(f"[!] Failed to save progress: {e}")


def init_site_progress(progress: Dict[str, Any], site: str, *, start_page: int, end_page: int, mode: str) -> Dict[str, Any]:
    sites = progress.setdefault("sites", {})
    sites[site] = {
        "site": site,
        "mode": mode,
        "status": "running",
        "start_page": start_page,
        "end_page": end_page,
        "current_page": start_page - 1,
        "pages_crawled": 0,
        "items_found": 0,
        "items_new": 0,
        "items_merged": 0,
        "skipped_known": 0,
        "upserted": 0,
        "errors": 0,
        "last_error": None,
        "started_at": _now_iso(),
        "finished_at": None,
    }
    progress["status"] = "running"
    progress.setdefault("started_at", _now_iso())
    save_progress(progress)
    return sites[site]


def finish_site_progress(progress: Dict[str, Any], site: str, stats: Dict[str, Any], status: str = "completed") -> None:
    sites = progress.setdefault("sites", {})
    sp = sites.setdefault(site, {"site": site})
    sp.update({
        "status": status,
        "pages_crawled": stats.get("pages_crawled", sp.get("pages_crawled", 0)),
        "items_found": stats.get("items_found", sp.get("items_found", 0)),
        "items_new": stats.get("items_new", sp.get("items_new", 0)),
        "items_merged": stats.get("items_merged", sp.get("items_merged", 0)),
        "skipped_known": stats.get("skipped_known", sp.get("skipped_known", 0)),
        "upserted": stats.get("upserted", sp.get("upserted", 0)),
        "errors": stats.get("errors", sp.get("errors", 0)),
        "last_error": stats.get("last_error", sp.get("last_error")),
        "finished_at": _now_iso(),
    })
    save_progress(progress)


def log_progress(site: str, stats: Dict[str, Any], *, page_num: int = None, end_page: int = None) -> None:
    prefix = f"[PROGRESS][{site}]"
    page_part = f" page {page_num}/{end_page}" if page_num and end_page else ""
    print(
        f"{prefix}{page_part} | crawled={stats.get('pages_crawled',0)} found={stats.get('items_found',0)} "
        f"new={stats.get('items_new',0)} merged={stats.get('items_merged',0)} skipped={stats.get('skipped_known',0)} "
        f"upserted={stats.get('upserted',0)} errors={stats.get('errors',0)}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Unified Concurrent Crawler
# ─────────────────────────────────────────────────────────────────────────────

def enrich_items_concurrently(source_module, items: List[Dict[str, Any]], max_workers: int = 5) -> List[Dict[str, Any]]:
    """Fetches details for listing items concurrently."""
    if not items:
        return []
    enriched = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(source_module.enrich_detail, item): item for item in items}
        for future in as_completed(futures):
            try:
                res = future.result()
                enriched.append(res)
            except Exception as e:
                item = futures[future]
                print(f"[!] Error enriching item {item.get('detail_url')}: {e}")
                enriched.append(item)
    return enriched

def run_site_crawl(
    site_key: str,
    start_page: int,
    max_pages: int,
    known_ids: Set[str],
    dry_run: bool = False,
    jitter: float = 1.0,
    new_only: bool = False,
    progress: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Runs ingestion for a single source site. Data is isolated per site (no cross-site dedup/merge)."""
    source_mod = SOURCES.get(site_key)
    if not source_mod:
        print(f"[!] Unknown site key: {site_key}")
        return {"items_found": 0, "items_new": 0, "items_merged": 0, "errors": 1, "last_error": "unknown site"}

    mode = "new_only" if new_only else "resume"
    end_page = start_page + max_pages - 1
    if progress is not None:
        init_site_progress(progress, site_key, start_page=start_page, end_page=end_page, mode=mode)

    print(f"\n=======================================================")
    print(f"CRAWLING SOURCE: {site_key.upper()} (Pages {start_page} to {end_page}) [{mode}]")
    print(f"=======================================================")

    stats = {
        "items_found": 0,
        "items_new": 0,
        "items_merged": 0,
        "skipped_known": 0,
        "pages_crawled": 0,
        "upsert_failed": False,
        "detected_max_pages": None,
        "stopped_early": False,
        "upserted": 0,
        "errors": 0,
        "last_error": None,
    }
    known_page_streak = 0

    for page_idx in range(max_pages):
        page_num = start_page + page_idx
        cards, detected_max = source_mod.scrape_listing(page_num)
        if isinstance(detected_max, int) and detected_max > 0:
            stats["detected_max_pages"] = max(stats.get("detected_max_pages") or 0, detected_max)

        if not cards:
            print(f"[-] No cards found on {site_key} page {page_num}. Ending site crawl.")
            if progress is not None:
                sp = progress.setdefault("sites", {}).setdefault(site_key, {})
                sp["last_error"] = f"No cards on page {page_num} (possible end/404/anti-bot)"
                save_progress(progress)
            break

        stats["pages_crawled"] += 1
        stats["items_found"] += len(cards)

        # Skip items already processed from this specific site
        new_cards = [c for c in cards if f"{site_key}:{c.get('source_post_id')}" not in known_ids]
        skipped = len(cards) - len(new_cards)
        stats["skipped_known"] += skipped

        print(f"    [{site_key}] Page {page_num}: {len(cards)} cards, {len(new_cards)} new, {skipped} previously seen.")

        if not new_cards:
            known_page_streak += 1
            if new_only and known_page_streak >= 2:
                stats["stopped_early"] = True
                print(f"    [{site_key}] New-only mode: hit {known_page_streak} known pages in a row, stopping early.")
                break
            time.sleep(jitter)
            continue
        known_page_streak = 0

        # Concurrent detail enrichment
        enriched = enrich_items_concurrently(source_mod, new_cards, max_workers=5)

        # Polish and pass through Cross-Site Deduplicator
        batch_to_upsert = []
        pending_ids = []
        for raw in enriched:
            try:
                # Discard non-video entries (e.g. photo sets or galleries without video streams)
                raw_servers = raw.get("servers") or []
                raw_stream = raw.get("full_video_url")
                if not raw_stream and not raw_servers:
                    is_photo_set = raw.get("gallery_url") or any(w in raw.get("title", "").lower() for w in ["photo", "photos", "set", "gallery"])
                    if is_photo_set:
                        known_ids.add(f"{site_key}:{raw.get('source_post_id')}")
                        print(f"      [SKIP PHOTO SET] '{raw.get('title')}' is a photo set/gallery. Skipped.")
                    else:
                        print(f"      [SKIP UNRESOLVED] '{raw.get('title')}' has no stream (enrichment failed or anti-bot gated). Not marked known.")
                    continue

                polished = polish_onlyfans_record(raw)
                if not polished.get("full_video_url") and not (polished.get("raw_payload", {}).get("servers") or polished.get("servers")):
                    known_ids.add(f"{site_key}:{raw.get('source_post_id')}")
                    print(f"      [SKIP NON-VIDEO] '{polished.get('title')}' has no valid stream after polish. Skipped.")
                    continue

                processed_record = polished
                batch_to_upsert.append(processed_record)
                pending_ids.append(f"{site_key}:{raw.get('source_post_id')}")
                stats["items_new"] += 1
            except Exception as e:
                print(f"[!] Error polishing {site_key} post {raw.get('source_post_id')}: {e}")

        # Batch upsert to database (per-site isolated). Write to both canonical+per-source table.
        upsert_succeeded = not batch_to_upsert
        if not dry_run and batch_to_upsert:
            deduped_batch_dict = {}
            for r in batch_to_upsert:
                k = (r.get("source_site"), str(r.get("source_post_id")))
                deduped_batch_dict[k] = r
            final_batch = list(deduped_batch_dict.values())
            try:
                from scraper.db import upsert_onlyfans_videos_batch, sync_onlyfans_source_tables_batch
                result = upsert_onlyfans_videos_batch(final_batch)
                source_result = sync_onlyfans_source_tables_batch(final_batch)
                canon_ok = not (isinstance(result, dict) and result.get("success") is False)
                source_ok = bool(source_result.get("success"))
                upsert_succeeded = canon_ok and source_ok
                if canon_ok:
                    stats["upserted"] += len(final_batch)
                else:
                    stats["errors"] += 1
                    stats["last_error"] = str(result.get("error") or result)
                if not source_ok:
                    stats["errors"] += 1
                    stats["last_error"] = f"source_sync: {source_result.get('errors')}"
            except Exception as e:
                upsert_succeeded = False
                stats["errors"] += 1
                stats["last_error"] = str(e)
                print(f"[!] Supabase upsert error: {e}")

        # Only mark IDs as known once persistence is confirmed. A failed upsert
        # previously poisoned the cache, so rows were skipped forever despite
        # never existing in the database.
        if dry_run:
            pass  # never poison the known-ID cache during a dry run
        elif upsert_succeeded:
            known_ids.update(pending_ids)
        else:
            stats["upsert_failed"] = True
            print(f"[!] Keeping {len(pending_ids)} {site_key} IDs unmarked for retry after failed upsert.")

        save_known_ids(known_ids)
        if progress is not None:
            sp = progress.setdefault("sites", {}).setdefault(site_key, {})
            sp.update({
                "status": "running",
                "current_page": page_num,
                "pages_crawled": stats.get("pages_crawled", 0),
                "items_found": stats.get("items_found", 0),
                "items_new": stats.get("items_new", 0),
                "items_merged": stats.get("items_merged", 0),
                "skipped_known": stats.get("skipped_known", 0),
                "upserted": stats.get("upserted", 0),
                "errors": stats.get("errors", 0),
                "last_error": stats.get("last_error"),
            })
            save_progress(progress)
        log_progress(site_key, stats, page_num=page_num, end_page=end_page)
        time.sleep(jitter + random.uniform(0.3, 0.8))

    if progress is not None:
        finish_site_progress(progress, site_key, stats, status="failed" if stats.get("upsert_failed") else "completed")
    return stats

def run_unified_pipeline(
    sites: List[str],
    pages_per_site: int = 2,
    dry_run: bool = False,
    save_json: bool = False,
    jitter: float = 1.0,
    full_site: bool = False,
    new_only: bool = False,
) -> None:
    """Executes the master cross-site deduplicating pipeline across selected sites."""
    checkpoints = load_checkpoints()
    known_ids = load_known_ids()
    progress = load_progress()
    progress.update({
        "status": "running",
        "started_at": progress.get("started_at") or _now_iso(),
        "mode": "full_site" if full_site else "new_only" if new_only else "resume",
        "sites_requested": sites,
        "pages_per_site": pages_per_site,
        "dry_run": dry_run,
    })
    save_progress(progress)
    # Cross-site deduplication/merge intentionally disabled: isolate data per site.
    export_cache_file = CURRENT_DIR / "onlyfans_canonical_cache.json"

    total_stats = {"items_found": 0, "items_new": 0, "items_merged": 0, "skipped_known": 0}

    for site in sites:
        site_cp = checkpoints.setdefault(site, {"current_page": 1, "max_pages": 500})
        start_p = 1 if full_site else int(site_cp.get("current_page", 1) or 1)
        site_max = int(site_cp.get("max_pages") or 500)
        effective_pages = max(1, site_max - start_p + 1) if full_site else pages_per_site
        res = run_site_crawl(
            site_key=site,
            start_page=start_p,
            max_pages=effective_pages,
            known_ids=known_ids,
            dry_run=dry_run,
            jitter=jitter,
            new_only=new_only,
            progress=progress,
        )
        for k in total_stats:
            if k in res:
                total_stats[k] += res.get(k, 0)
        detected_max = res.get("detected_max_pages")
        if isinstance(detected_max, int) and detected_max > 0:
            site_cp["max_pages"] = max(int(site_cp.get("max_pages", 0) or 0), detected_max)
        if not res.get("upsert_failed"):
            crawled = int(res.get("pages_crawled", 0) or 0)
            if full_site:
                site_cp["last_full_crawl_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                # Keep cursor; full backfill should not force normal incremental mode to restart oddly.
            else:
                site_cp["current_page"] = start_p + crawled
        checkpoints[site] = site_cp

    checkpoints["total_ingested"] = int(checkpoints.get("total_ingested", 0) or 0) + total_stats.get("items_new", 0)
    checkpoints["total_merged"] = total_stats["items_merged"]
    save_checkpoints(checkpoints)

    # Per-site isolation mode: no cross-site canonical merge cache is written.


    progress["status"] = "completed"
    progress["finished_at"] = _now_iso()
    progress["totals"] = {**total_stats, "canonical_total": None}
    save_progress(progress)
    print("\n" + "=" * 60)
    print("UNIFIED MULTI-SITE ONLYFANS SCRAPE SUMMARY")
    print(f"Sites crawled          : {', '.join(sites)}")
    print(f"Raw items found        : {total_stats['items_found']}")
    print(f"Unique new videos      : {total_stats['items_new']}")
    print(f"Duplicates merged      : {total_stats['items_merged']}")
    print(f"Known items skipped    : {total_stats['skipped_known']}")
    print(f"Items upserted         : {total_stats.get('items_new',0)} (per-site isolated)")
    print(f"Progress file          : {PROGRESS_FILE}")
    print("=" * 60 + "\n")

def main():
    parser = argparse.ArgumentParser(description="Multi-Site OnlyFans Scraper & Deduplication Pipeline")
    parser.add_argument("--site", choices=["all", "internetchicks", "influencersgonewild", "sheeshfans", "pimpbunny"], default="all",
                        help="Target site to crawl (default: all)")
    parser.add_argument("--pages", type=int, default=2, help="Number of pages per site (default: 2)")
    parser.add_argument("--full-site", action="store_true", help="Crawl from page 1 to detected max pages for each selected site")
    parser.add_argument("--new-only", action="store_true", help="Stop a site early after a run of already-known pages (for fresh updates)")
    parser.add_argument("--dry-run", action="store_true", help="Scrape and deduplicate without writing to Supabase")
    parser.add_argument("--save-json", action="store_true", help="Save merged canonical records to JSON")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background watcher mode")
    parser.add_argument("--delay", type=float, default=1.0, help="Polite delay between pages in seconds")

    args = parser.parse_args()

    target_sites = ["internetchicks", "influencersgonewild", "sheeshfans", "pimpbunny"] if args.site == "all" else [args.site]

    if args.daemon:
        print("[*] Starting continuous 24/7 cross-site watcher daemon...")
        while True:
            # Daemon: check for new content from page 1, stop early when already-known.
            run_unified_pipeline(
                target_sites,
                pages_per_site=max(1, args.pages),
                dry_run=args.dry_run,
                save_json=args.save_json,
                jitter=args.delay,
                full_site=False,
                new_only=True,
            )
            time.sleep(600)
        return

    run_unified_pipeline(
        sites=target_sites,
        pages_per_site=args.pages,
        dry_run=args.dry_run,
        save_json=args.save_json,
        jitter=args.delay,
        full_site=args.full_site,
        new_only=args.new_only,
    )

if __name__ == "__main__":
    main()
