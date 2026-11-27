"""
Chinese Category Scraper Pipeline (isolated database, multisite-ready)
Targets:
  1. uxj.838238.xyz (95da.com network) — whitelisted video + magnet categories

Features:
 - Category-whitelist filtering (JAV and livestream zones are never scraped)
 - TLS-impersonated fetching via Scrapling (chrome124) with polite backoff
 - Obfuscated-payload decoding (reversed base64 -> j_b64 JSON) — no browser needed
 - Multi-threaded detail enrichment (ThreadPoolExecutor)
 - Per-category remote-first checkpoints + central known-ID ledger
 - Isolated persistence: strictly scraper.chinese_videos, never scraper.videos
 - Continuous daemon mode for 24/7 cloud execution
"""

import os
import sys
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
    from scraper.categories.chinese.polish import polish_chinese_record
    from scraper.categories.chinese.translate import save_cache as save_translation_cache
    from scraper.categories.chinese.sources import SOURCES
    from scraper.categories.chinese.categories import (
        ALL_ENABLED_CATEGORIES,
        DEFAULT_CATEGORIES,
        category_info,
    )
    from scraper.categories.listing_errors import ListingUnavailable
except ImportError:
    from polish import polish_chinese_record
    from translate import save_cache as save_translation_cache
    from sources import SOURCES
    from categories import ALL_ENABLED_CATEGORIES, DEFAULT_CATEGORIES, category_info
    from listing_errors import ListingUnavailable  # type: ignore

from scrapling import Fetcher

CHECKPOINT_FILE = CURRENT_DIR / "checkpoint.json"
KNOWN_IDS_FILE = CURRENT_DIR / "known_post_ids.json"
PROGRESS_FILE = CURRENT_DIR / "progress.json"

SITE_KEY = "uxj838238"
# In-memory known-ID keys look like 'uxj838238:video:<id>' / 'uxj838238:torrent:<id>'
KNOWN_ID_SITE = SITE_KEY

DEFAULT_CHECKPOINTS: Dict[str, Any] = {
    "total_ingested": 0,
}
for _cat_id in DEFAULT_CATEGORIES:
    DEFAULT_CHECKPOINTS[f"{SITE_KEY}:{_cat_id}"] = {"current_page": 1, "max_pages": 50}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Checkpoint & Cache Persistence
# ─────────────────────────────────────────────────────────────────────────────

def load_checkpoints() -> Dict[str, Any]:
    """Loads per-category checkpoints from Supabase first and local cache second."""
    from scraper.progress import load_checkpoint as _load
    state = _load("chinese_multi", CHECKPOINT_FILE, DEFAULT_CHECKPOINTS)
    for cat_id in DEFAULT_CATEGORIES:
        state.setdefault(f"{SITE_KEY}:{cat_id}", {"current_page": 1, "max_pages": 50})
    return state


def save_checkpoints(state: Dict[str, Any]) -> bool:
    """Saves per-category checkpoints remotely and mirrors them locally."""
    from scraper.progress import save_checkpoint as _save
    state["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
    return _save("chinese_multi", state, CHECKPOINT_FILE)


def load_known_ids() -> Set[str]:
    """Loads ``site:kind:id`` keys from local cache plus the Supabase ledger."""
    from scraper.progress import load_known_ids as _load
    ids: Set[str] = _load(KNOWN_ID_SITE, KNOWN_IDS_FILE, prefix=True, limit=200000)
    save_known_ids(ids)
    return ids


def save_known_ids(
    known_ids: Set[str],
    new_ids: Optional[Set[str]] = None,
    status: str = "stored",
) -> None:
    """Mirrors IDs locally and records newly-confirmed IDs in the central ledger."""
    from scraper.progress import mark_known_ids, save_known_ids as _save
    if new_ids:
        mark_known_ids(new_ids, status=status)
    _save(KNOWN_IDS_FILE, known_ids)


def load_progress() -> Dict[str, Any]:
    """Loads per-run Chinese progress from Supabase first and local cache second."""
    from scraper.progress import load_progress as _load
    return _load("chinese_progress", PROGRESS_FILE, {"status": "idle", "categories": {}})


def save_progress(progress: Dict[str, Any]) -> bool:
    """Persists per-run progress remotely and mirrors it locally."""
    from scraper.progress import save_progress as _save
    progress["updated_at"] = _now_iso()
    return _save("chinese_progress", progress, PROGRESS_FILE)


def init_category_progress(progress: Dict[str, Any], cat_key: str, *, start_page: int, end_page: int, mode: str) -> Dict[str, Any]:
    cats = progress.setdefault("categories", {})
    cats[cat_key] = {
        "category": cat_key,
        "mode": mode,
        "status": "running",
        "start_page": start_page,
        "end_page": end_page,
        "current_page": start_page - 1,
        "pages_crawled": 0,
        "items_found": 0,
        "items_new": 0,
        "skipped_known": 0,
        "skipped_dead": 0,
        "skipped_unresolved": 0,
        "upserted": 0,
        "errors": 0,
        "last_error": None,
        "started_at": _now_iso(),
        "finished_at": None,
    }
    progress["status"] = "running"
    progress.setdefault("started_at", _now_iso())
    save_progress(progress)
    return cats[cat_key]


def finish_category_progress(progress: Dict[str, Any], cat_key: str, stats: Dict[str, Any], status: str = "completed") -> None:
    cats = progress.setdefault("categories", {})
    cp = cats.setdefault(cat_key, {"category": cat_key})
    cp.update({
        "status": status,
        "pages_crawled": stats.get("pages_crawled", cp.get("pages_crawled", 0)),
        "items_found": stats.get("items_found", cp.get("items_found", 0)),
        "items_new": stats.get("items_new", cp.get("items_new", 0)),
        "skipped_known": stats.get("skipped_known", cp.get("skipped_known", 0)),
        "skipped_dead": stats.get("skipped_dead", cp.get("skipped_dead", 0)),
        "skipped_unresolved": stats.get("skipped_unresolved", cp.get("skipped_unresolved", 0)),
        "upserted": stats.get("upserted", cp.get("upserted", 0)),
        "errors": stats.get("errors", cp.get("errors", 0)),
        "last_error": stats.get("last_error", cp.get("last_error")),
        "finished_at": _now_iso(),
    })
    save_progress(progress)


def log_progress(cat_key: str, stats: Dict[str, Any], *, page_num=None, end_page=None) -> None:
    prefix = f"[PROGRESS][{cat_key}]"
    page_part = f" page {page_num}/{end_page}" if page_num and end_page else ""
    print(
        f"{prefix}{page_part} | crawled={stats.get('pages_crawled',0)} found={stats.get('items_found',0)} "
        f"new={stats.get('items_new',0)} skipped={stats.get('skipped_known',0)} dead={stats.get('skipped_dead',0)} "
        f"unresolved={stats.get('skipped_unresolved',0)} upserted={stats.get('upserted',0)} errors={stats.get('errors',0)}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Concurrent Enrichment
# ─────────────────────────────────────────────────────────────────────────────

def enrich_items_concurrently(source_module, items: List[Dict[str, Any]], max_workers: int = 5) -> List[Dict[str, Any]]:
    """Fetches detail pages for listing items concurrently."""
    if not items:
        return []
    enriched = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(source_module.enrich_detail, item): item for item in items}
        for future in as_completed(futures):
            item = futures[future]
            try:
                res = future.result()
                enriched.append(res)
            except Exception as e:
                print(f"[!] Error enriching item {item.get('detail_url')}: {e}")
                enriched.append(item)
    return enriched


# ─────────────────────────────────────────────────────────────────────────────
# Per-Category Crawl
# ─────────────────────────────────────────────────────────────────────────────

def run_category_crawl(
    site_key: str,
    category_id: str,
    start_page: int,
    max_pages: int,
    known_ids: Set[str],
    dry_run: bool = False,
    jitter: float = 1.0,
    new_only: bool = False,
    progress: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Runs ingestion for one (site, category) pair. Data is isolated per site and
    strictly written to scraper.chinese_videos.
    """
    source_mod = SOURCES.get(site_key)
    if not source_mod:
        print(f"[!] Unknown site key: {site_key}")
        return {"items_found": 0, "items_new": 0, "errors": 1, "last_error": "unknown site"}

    info = category_info(category_id)
    if not info:
        print(f"[!] Category {category_id} is not whitelisted; skipping.")
        return {"items_found": 0, "items_new": 0, "errors": 0}

    cat_key = f"{site_key}:{category_id}"
    content_type = info.get("content_type", "video")
    mode = "new_only" if new_only else "resume"
    end_page = start_page + max_pages - 1
    if progress is not None:
        init_category_progress(progress, cat_key, start_page=start_page, end_page=end_page, mode=mode)

    print(f"\n=======================================================")
    print(
        f"CRAWLING {site_key.upper()} | {info.get('cn')} / {info.get('en')} [{category_id}] "
        f"({content_type}) | Pages {start_page} to {end_page} [{mode}]"
    )
    print(f"=======================================================")

    stats = {
        "items_found": 0,
        "items_new": 0,
        "skipped_known": 0,
        "skipped_dead": 0,
        "skipped_unresolved": 0,
        "pages_crawled": 0,
        "upsert_failed": False,
        "detected_max_pages": None,
        "stopped_early": False,
        "upserted": 0,
        "errors": 0,
        "last_error": None,
        "true_max_pages": None,
    }
    known_page_streak = 0

    for page_idx in range(max_pages):
        page_num = start_page + page_idx
        try:
            cards, detected_max = source_mod.scrape_listing(category_id, page_num)
        except ListingUnavailable as e:
            # Transient block/timeout: keep the cursor here, do not end the zone.
            print(f"[!] {cat_key} page {page_num} unavailable ({e}); zone left incomplete for retry.")
            stats["errors"] += 1
            stats["last_error"] = f"listing unavailable on page {page_num}: {e}"
            stats["listing_unavailable"] = True
            if progress is not None:
                cp = progress.setdefault("categories", {}).setdefault(cat_key, {})
                cp["last_error"] = stats["last_error"]
                save_progress(progress)
            break
        if isinstance(detected_max, int) and detected_max > 0:
            stats["detected_max_pages"] = max(stats.get("detected_max_pages") or 0, detected_max)

        if not cards:
            # A real 200 page with no cards is the true end of this listing. Pin
            # it so the cursor never re-crawls the same empty pages: the site's
            # advertised total is frequently larger than the real catalogue.
            # Never claim pages we did not fetch, and never exceed the total the
            # site itself reported on this page.
            end = max(0, page_num - 1)
            if isinstance(detected_max, int) and 0 < detected_max < end:
                end = detected_max
            stats["true_max_pages"] = end
            print(f"[-] No cards on {cat_key} page {page_num}; true end = page {page_num - 1}. Ending category crawl.")
            if progress is not None:
                cp = progress.setdefault("categories", {}).setdefault(cat_key, {})
                cp["last_error"] = f"end of listing at page {page_num}"
                save_progress(progress)
            break

        stats["pages_crawled"] += 1
        stats["items_found"] += len(cards)

        new_cards = [
            c for c in cards
            if f"{site_key}:{c.get('content_type', content_type)}:{c.get('source_post_id')}" not in known_ids
        ]
        skipped = len(cards) - len(new_cards)
        stats["skipped_known"] += skipped

        print(f"    [{cat_key}] Page {page_num}: {len(cards)} cards, {len(new_cards)} new, {skipped} previously seen.")

        if not new_cards:
            known_page_streak += 1
            if new_only and known_page_streak >= 2:
                stats["stopped_early"] = True
                print(f"    [{cat_key}] New-only mode: hit {known_page_streak} known pages in a row, stopping early.")
                break
            time.sleep(jitter)
            continue
        known_page_streak = 0

        enriched = enrich_items_concurrently(source_mod, new_cards, max_workers=5)

        batch_to_upsert = []
        pending_ids: Set[str] = set()
        confirmed_ids: Set[str] = set()
        seen_ids: Set[str] = set()
        dead_ids: Set[str] = set()
        unresolved_titles: List[str] = []
        for raw in enriched:
            item_key = f"{site_key}:{raw.get('content_type', content_type)}:{raw.get('source_post_id')}"
            if raw.get("dead"):
                # Confirmed soft-404 on the site: content removed -> mark seen.
                dead_ids.add(item_key)
                seen_ids.add(item_key)
                stats["skipped_dead"] += 1
                continue

            has_payload = bool(
                raw.get("full_video_url")
                or raw.get("magnet_uri")
                or raw.get("cdn_endpoints")
                or raw.get("torrent_file_url")
            )
            if not has_payload:
                # Detail fetch failed (anti-bot/transient): keep unmarked for retry.
                unresolved_titles.append(str(raw.get("title") or "untitled"))
                continue

            try:
                polished = polish_chinese_record(raw)
                # English title is produced inside polish_chinese_record()
                # (single Google Translate call). Re-translating here doubled
                # the request count for every item for no benefit.
                # Videos must have a playable stream; torrents must have a magnet or torrent file.
                if polished.get("content_type") == "video" and not polished.get("full_video_url"):
                    unresolved_titles.append(str(polished.get("title") or "untitled"))
                    continue
                if polished.get("content_type") == "torrent" and not (polished.get("magnet_uri") or polished.get("torrent_file_url")):
                    unresolved_titles.append(str(polished.get("title") or "untitled"))
                    continue
                batch_to_upsert.append(polished)
                pending_ids.add(item_key)
                stats["items_new"] += 1
            except Exception as e:
                print(f"[!] Error polishing {cat_key} post {raw.get('source_post_id')}: {e}")

        if unresolved_titles:
            stats["skipped_unresolved"] += len(unresolved_titles)
            samples = "; ".join(unresolved_titles[:3])
            if len(unresolved_titles) > 3:
                samples += f"; +{len(unresolved_titles) - 3} more"
            print(
                f"      [SKIP UNRESOLVED] {len(unresolved_titles)} {cat_key} items had no stream/magnet "
                f"(enrichment failed or anti-bot gated). Kept unmarked for retry. Samples: {samples}"
            )

        # Batch upsert to the isolated chinese table.
        upsert_succeeded = not batch_to_upsert
        if not dry_run and batch_to_upsert:
            deduped = {}
            for r in batch_to_upsert:
                k = (r.get("source_site"), str(r.get("source_post_id")))
                deduped[k] = r
            final_batch = list(deduped.values())
            try:
                from scraper.db import rpc_result_succeeded, upsert_chinese_videos_batch
                result = upsert_chinese_videos_batch(final_batch)
                upsert_succeeded = rpc_result_succeeded(result)
                if upsert_succeeded:
                    stats["upserted"] += len(final_batch)
                    # Persist the translation cache so titles are never
                    # re-translated across runs.
                    try:
                        save_translation_cache()
                    except Exception:
                        pass
                else:
                    stats["errors"] += 1
                    stats["last_error"] = str(result.get("error") or result)
            except Exception as e:
                upsert_succeeded = False
                stats["errors"] += 1
                stats["last_error"] = str(e)
                print(f"[!] Supabase upsert error: {e}")

        # Only mark IDs as known once persistence is confirmed (or confirmed dead).
        if dry_run:
            pass  # never poison the known-ID cache during a dry run
        else:
            if seen_ids:
                known_ids.update(seen_ids)
                save_known_ids(known_ids, new_ids=seen_ids, status="seen")
            if upsert_succeeded:
                confirmed_ids.update(pending_ids)
                known_ids.update(confirmed_ids)
                save_known_ids(known_ids, new_ids=confirmed_ids, status="stored")
            elif pending_ids:
                stats["upsert_failed"] = True
                print(f"[!] Keeping {len(pending_ids)} {cat_key} IDs unmarked for retry after failed upsert.")

        save_known_ids(known_ids)
        if progress is not None:
            cp = progress.setdefault("categories", {}).setdefault(cat_key, {})
            cp.update({
                "status": "running",
                "current_page": page_num,
                "pages_crawled": stats.get("pages_crawled", 0),
                "items_found": stats.get("items_found", 0),
                "items_new": stats.get("items_new", 0),
                "skipped_known": stats.get("skipped_known", 0),
                "skipped_dead": stats.get("skipped_dead", 0),
                "skipped_unresolved": stats.get("skipped_unresolved", 0),
                "upserted": stats.get("upserted", 0),
                "errors": stats.get("errors", 0),
                "last_error": stats.get("last_error"),
            })
            save_progress(progress)
        log_progress(cat_key, stats, page_num=page_num, end_page=end_page)
        time.sleep(jitter + random.uniform(0.3, 0.8))

    if progress is not None:
        finish_category_progress(progress, cat_key, stats, status="failed" if stats.get("upsert_failed") else "completed")
    return stats


def run_chinese_pipeline(
    categories: Optional[List[str]] = None,
    pages_per_category: int = 2,
    dry_run: bool = False,
    jitter: float = 1.0,
    full_site: bool = False,
    new_only: bool = False,
) -> None:
    """Executes the crawl across whitelisted categories (one isolated table)."""
    categories = [c for c in (categories or DEFAULT_CATEGORIES) if category_info(c)]
    checkpoints = load_checkpoints()
    known_ids = load_known_ids()
    progress = load_progress()
    progress.update({
        "status": "running",
        "started_at": progress.get("started_at") or _now_iso(),
        "mode": "full_site" if full_site else "new_only" if new_only else "resume",
        "categories_requested": categories,
        "pages_per_category": pages_per_category,
        "dry_run": dry_run,
    })
    save_progress(progress)

    total_stats = {
        "items_found": 0,
        "items_new": 0,
        "skipped_known": 0,
        "skipped_dead": 0,
        "upserted": 0,
        "errors": 0,
    }
    failed_categories: List[str] = []

    for category_id in categories:
        cat_key = f"{SITE_KEY}:{category_id}"
        try:
            cat_cp = checkpoints.setdefault(cat_key, {"current_page": 1, "max_pages": 50})
            start_p = 1 if full_site else int(cat_cp.get("current_page", 1) or 1)
            cat_max = int(cat_cp.get("max_pages") or 50)
            effective_pages = max(1, cat_max - start_p + 1) if full_site else pages_per_category
            res = run_category_crawl(
                site_key=SITE_KEY,
                category_id=category_id,
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
                    total_stats[k] += int(res.get(k, 0) or 0)
            detected_max = res.get("detected_max_pages")
            if not dry_run:
                if isinstance(detected_max, int) and detected_max > 0:
                    cat_cp["max_pages"] = max(int(cat_cp.get("max_pages", 0) or 0), detected_max)
                cat_status = "completed"
                true_max = res.get("true_max_pages")
                if isinstance(true_max, int) and true_max >= 0 and not res.get("listing_unavailable"):
                    # The listing genuinely ended here: pin the cursor past it so
                    # this zone is never re-crawled (nor its empty pages asked
                    # for again) in a later session.
                    cat_cp["max_pages"] = max(1, true_max)
                    cat_cp["current_page"] = max(2, true_max + 1)
                    cat_cp["stall_count"] = 0
                    print(f"    [✓] {cat_key}: catalogue ends at page {true_max}; zone complete.")
                    progress.setdefault("categories", {}).setdefault(cat_key, {})["status"] = "completed"
                    save_checkpoints(checkpoints)
                    save_progress(progress)
                    continue
                if not res.get("upsert_failed"):
                    crawled = int(res.get("pages_crawled", 0) or 0)
                    unresolved = int(res.get("skipped_unresolved", 0) or 0)
                    if unresolved:
                        from scraper.progress import cursor_retry_state
                        stall, hold = cursor_retry_state(
                            stall_count=int(cat_cp.get("stall_count", 0) or 0), unresolved=unresolved
                        )
                        cat_cp["stall_count"] = stall
                        if hold:
                            cat_status = "partial"
                            print(f"    [!] Holding {cat_key} cursor for retry ({unresolved} unresolved, attempt {stall}/5).")
                        else:
                            print(f"    [!] Giving up on {unresolved} unresolved {cat_key} items after 5 attempts.")
                    if cat_status == "completed":
                        cat_cp["stall_count"] = 0
                        if full_site:
                            cat_cp["last_full_crawl_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                        else:
                            cat_cp["current_page"] = start_p + crawled
                else:
                    failed_categories.append(category_id)
                    cat_status = "failed"
                progress.setdefault("categories", {}).setdefault(cat_key, {})["status"] = cat_status
        except Exception as exc:
            failed_categories.append(category_id)
            progress.setdefault("categories", {}).setdefault(cat_key, {})["status"] = "failed"
            progress["categories"][cat_key]["last_error"] = str(exc)
            print(f"[!] Chinese category [{category_id}] failed in isolation: {exc}")
        save_checkpoints(checkpoints)
        save_progress(progress)

    checkpoints["total_ingested"] = int(checkpoints.get("total_ingested", 0) or 0) + int(total_stats.get("upserted", 0) or 0)
    save_checkpoints(checkpoints)

    from scraper.progress import finalize_status
    finalize_status(progress, failed_categories)
    progress["finished_at"] = _now_iso()
    progress["totals"] = {**total_stats}
    save_progress(progress)
    print("\n" + "=" * 60)
    print("CHINESE CATEGORY SCRAPE SUMMARY")
    print(f"Categories crawled     : {len(categories)}")
    print(f"Raw items found        : {total_stats['items_found']}")
    print(f"Unique new items       : {total_stats['items_new']}")
    print(f"Known items skipped    : {total_stats['skipped_known']}")
    print(f"Dead items skipped     : {total_stats['skipped_dead']}")
    print(f"Items upserted         : {total_stats.get('upserted',0)} (isolated scraper.chinese_videos)")
    print(f"Progress file          : {PROGRESS_FILE}")
    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Chinese Category Scraper (isolated DB, multisite-ready)")
    parser.add_argument("--categories", type=str, default="", help="Comma-separated category ids (default: all whitelisted)")
    parser.add_argument("--pages", type=int, default=2, help="Number of pages per category (default: 2)")
    parser.add_argument("--full-site", action="store_true", help="Crawl from page 1 to detected max pages for each category")
    parser.add_argument("--new-only", action="store_true", help="Stop a category early after a run of already-known pages")
    parser.add_argument("--dry-run", action="store_true", help="Scrape without writing to Supabase")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background watcher mode")
    parser.add_argument("--delay", type=float, default=1.0, help="Polite delay between pages in seconds")

    args = parser.parse_args()

    categories = [c.strip() for c in args.categories.split(",") if c.strip()] or None

    if args.daemon:
        print("[*] Starting continuous 24/7 Chinese category watcher daemon...")
        while True:
            run_chinese_pipeline(
                categories=categories,
                pages_per_category=max(1, args.pages),
                dry_run=args.dry_run,
                jitter=args.delay,
                full_site=False,
                new_only=True,
            )
            time.sleep(600)
        return

    run_chinese_pipeline(
        categories=categories,
        pages_per_category=args.pages,
        dry_run=args.dry_run,
        jitter=args.delay,
        full_site=args.full_site,
        new_only=args.new_only,
    )


if __name__ == "__main__":
    main()
