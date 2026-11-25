"""
Fangsung category pipeline (www.fangsung.com -> scraper.fangsung_videos).

Same shape as the chinese pipeline: per-category (region term) crawl with
remote-first checkpoints, concurrent detail enrichment (stream extraction +
ffprobe quality probing), English translation at ingest, isolated upserts.
Video-only site: no torrents, no cdn_endpoints.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

CURRENT_DIR = Path(__file__).resolve().parent
SOURCES_DIR = CURRENT_DIR / "sources"
SCRAPER_DIR = CURRENT_DIR.parent
PROJECT_ROOT = SCRAPER_DIR.parent

import sys

for p in [str(PROJECT_ROOT), str(SCRAPER_DIR), str(CURRENT_DIR), str(SOURCES_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from scraper.categories.fangsung.sources import SOURCES
    from scraper.categories.fangsung.categories import (
        DEFAULT_CATEGORIES,
        category_info,
    )
    from scraper.categories.fangsung.polish import polish_fangsung_record
    from scraper.categories.fangsung.translate import (
        save_cache as save_translation_cache,
    )
except ImportError:
    from sources import SOURCES  # type: ignore
    from categories import DEFAULT_CATEGORIES, category_info  # type: ignore
    from polish import polish_fangsung_record  # type: ignore
    from translate import save_cache as save_translation_cache  # type: ignore

CHECKPOINT_FILE = CURRENT_DIR / "checkpoint.json"
KNOWN_IDS_FILE = CURRENT_DIR / "known_post_ids.json"
PROGRESS_FILE = CURRENT_DIR / "progress.json"

SITE_KEY = "fangsung"
KNOWN_ID_SITE = SITE_KEY

DEFAULT_CHECKPOINTS: Dict[str, Any] = {
    "total_ingested": 0,
}
for _term in DEFAULT_CATEGORIES:
    DEFAULT_CHECKPOINTS[f"{SITE_KEY}:{_term}"] = {"current_page": 1, "max_pages": 50}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Checkpoint & Cache Persistence
# ─────────────────────────────────────────────────────────────────────────────

def load_checkpoints() -> Dict[str, Any]:
    from scraper.progress import load_checkpoint as _load
    state = _load("fangsung_multi", CHECKPOINT_FILE, DEFAULT_CHECKPOINTS)
    for term in DEFAULT_CATEGORIES:
        state.setdefault(f"{SITE_KEY}:{term}", {"current_page": 1, "max_pages": 50})
    return state


def save_checkpoints(state: Dict[str, Any]) -> bool:
    from scraper.progress import save_checkpoint as _save
    state["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
    return _save("fangsung_multi", state, CHECKPOINT_FILE)


def load_known_ids() -> Set[str]:
    from scraper.progress import load_known_ids as _load
    ids: Set[str] = _load(KNOWN_ID_SITE, KNOWN_IDS_FILE, prefix=True, limit=200000)
    save_known_ids(ids)
    return ids


def save_known_ids(
    known_ids: Set[str],
    new_ids: Optional[Set[str]] = None,
    status: str = "stored",
) -> None:
    from scraper.progress import mark_known_ids, save_known_ids as _save
    if new_ids:
        mark_known_ids(new_ids, status=status)
    _save(KNOWN_IDS_FILE, known_ids)


def load_progress() -> Dict[str, Any]:
    from scraper.progress import load_progress as _load
    return _load("fangsung_progress", PROGRESS_FILE, {"status": "idle", "categories": {}})


def save_progress(progress: Dict[str, Any]) -> bool:
    from scraper.progress import save_progress as _save
    progress["updated_at"] = _now_iso()
    return _save("fangsung_progress", progress, PROGRESS_FILE)


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
    Runs ingestion for one (site, region-term) pair. Data is isolated and
    strictly written to scraper.fangsung_videos.
    """
    source_mod = SOURCES.get(site_key)
    if not source_mod:
        print(f"[!] Unknown site key: {site_key}")
        return {"items_found": 0, "items_new": 0, "errors": 1, "last_error": "unknown site"}

    info = category_info(category_id)
    if not info:
        print(f"[!] Category '{category_id}' is not whitelisted; skipping.")
        return {"items_found": 0, "items_new": 0, "errors": 0}

    cat_key = f"{site_key}:{category_id}"
    mode = "new_only" if new_only else "resume"
    end_page = start_page + max_pages - 1
    if progress is not None:
        init_category_progress(progress, cat_key, start_page=start_page, end_page=end_page, mode=mode)

    print(f"\n=======================================================")
    print(
        f"CRAWLING {site_key.upper()} | {info.get('cn')} / {info.get('en')} "
        f"| Pages {start_page} to {end_page} [{mode}]"
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
    }
    known_page_streak = 0

    for page_idx in range(max_pages):
        page_num = start_page + page_idx
        cards, detected_max = source_mod.scrape_listing(category_id, page_num)
        if isinstance(detected_max, int) and detected_max > 0:
            stats["detected_max_pages"] = max(stats.get("detected_max_pages") or 0, detected_max)

        if not cards:
            print(f"[-] No cards found on {cat_key} page {page_num}. Ending category crawl.")
            if progress is not None:
                cp = progress.setdefault("categories", {}).setdefault(cat_key, {})
                cp["last_error"] = f"No cards on page {page_num} (possible end/404/anti-bot)"
                save_progress(progress)
            break

        stats["pages_crawled"] += 1
        stats["items_found"] += len(cards)

        new_cards = [
            c for c in cards
            if f"{site_key}:video:{c.get('source_post_id')}" not in known_ids
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
        unresolved_titles: List[str] = []
        for raw in enriched:
            item_key = f"{site_key}:video:{raw.get('source_post_id')}"
            if raw.get("dead"):
                # Detail fetch failed (anti-bot/transient): keep unmarked for retry.
                unresolved_titles.append(str(raw.get("title") or "untitled"))
                continue

            try:
                polished = polish_fangsung_record(raw)
                # Videos must have a playable stream.
                if not polished.get("full_video_url"):
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
                f"      [SKIP UNRESOLVED] {len(unresolved_titles)} {cat_key} items had no stream "
                f"(enrichment failed or anti-bot gated). Kept unmarked for retry. Samples: {samples}"
            )

        # Batch upsert to the isolated fangsung table.
        upsert_succeeded = not batch_to_upsert
        if not dry_run and batch_to_upsert:
            deduped = {}
            for r in batch_to_upsert:
                k = (r.get("source_site"), str(r.get("source_post_id")))
                deduped[k] = r
            final_batch = list(deduped.values())
            try:
                from scraper.db import rpc_result_succeeded, upsert_fangsung_videos_batch
                result = upsert_fangsung_videos_batch(final_batch)
                upsert_succeeded = rpc_result_succeeded(result)
                if upsert_succeeded:
                    stats["upserted"] += len(final_batch)
                    confirmed_ids.update(pending_ids)
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

        if upsert_succeeded:
            seen_ids.update(pending_ids)
            # Mark ALL ids on successfully processed pages (including known
            # ones) so re-crawls skip them without refetching details.
        else:
            # Upsert failed: leave ids unmarked so the next run retries them.
            print(f"    [{cat_key}] Upsert failed; ids left unmarked for retry.")

        new_keys = {f"{site_key}:video:{c.get('source_post_id')}" for c in new_cards}
        if upsert_succeeded:
            known_ids.update(new_keys)
            if confirmed_ids:
                save_known_ids(known_ids, new_ids=confirmed_ids)

        if progress is not None:
            cp = progress.setdefault("categories", {}).setdefault(cat_key, {})
            cp["current_page"] = page_num
            save_progress(progress)
        log_progress(cat_key, stats, page_num=page_num, end_page=end_page)

        time.sleep(jitter)

    if progress is not None:
        status = "failed" if stats["errors"] and not stats["upserted"] else "completed"
        finish_category_progress(progress, cat_key, stats, status=status)

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_fangsung_pipeline(
    check_head: bool = True,
    pages_per_category: int = 1,
    dry_run: bool = False,
    categories: Optional[List[str]] = None,
    max_workers: int = 5,
) -> Dict[str, Any]:
    """Convenience runner for local/testing use (Actions uses unified_scraper)."""
    checkpoints = load_checkpoints()
    known_ids = load_known_ids()
    progress = load_progress()
    progress.update({
        "status": "running",
        "started_at": progress.get("started_at") or _now_iso(),
        "mode": "head_and_backfill",
        "categories_requested": categories or DEFAULT_CATEGORIES,
    })
    save_progress(progress)

    terms = categories or DEFAULT_CATEGORIES
    total_new = 0
    for term in terms:
        if not category_info(term):
            continue
        cat_state = checkpoints.setdefault(f"{SITE_KEY}:{term}", {"current_page": 1, "max_pages": 50})
        if check_head:
            h = run_category_crawl(
                site_key=SITE_KEY, category_id=term, start_page=1, max_pages=1,
                known_ids=known_ids, dry_run=dry_run, jitter=0.5, new_only=True, progress=progress,
            )
            total_new += int(h.get("upserted", 0) or 0)
            h_max = h.get("detected_max_pages")
            if isinstance(h_max, int) and h_max > 0:
                cat_state["max_pages"] = max(int(cat_state.get("max_pages", 0) or 0), h_max)
        curr = int(cat_state.get("current_page", 1) or 1)
        s = run_category_crawl(
            site_key=SITE_KEY, category_id=term, start_page=curr, max_pages=pages_per_category,
            known_ids=known_ids, dry_run=dry_run, jitter=0.8, progress=progress,
        )
        total_new += int(s.get("upserted", 0) or 0)
        if not s.get("upsert_failed") and int(s.get("pages_crawled", 0) or 0) > 0:
            cat_state["current_page"] = curr + int(s.get("pages_crawled", 0) or 0)
        s_max = s.get("detected_max_pages")
        if isinstance(s_max, int) and s_max > 0:
            cat_state["max_pages"] = max(int(cat_state.get("max_pages", 0) or 0), s_max)
        checkpoints[f"{SITE_KEY}:{term}"] = cat_state
        save_known_ids(known_ids)
        save_checkpoints(checkpoints)
        save_progress(progress)

    progress["status"] = "completed" if total_new >= 0 else "failed"
    progress["finished_at"] = _now_iso()
    progress["totals"] = {"upserted": total_new}
    save_progress(progress)
    return {"upserted": total_new, "progress": progress}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Fangsung category pipeline")
    parser.add_argument("--pages", type=int, default=1, help="Pages per category")
    parser.add_argument("--categories", type=str, default="", help="Comma-separated region terms (default: all)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-head", action="store_true", help="Skip the new-only head pass")
    args = parser.parse_args()
    cats = [c.strip() for c in args.categories.split(",") if c.strip()] or None
    out = run_fangsung_pipeline(
        check_head=not args.no_head,
        pages_per_category=args.pages,
        dry_run=args.dry_run,
        categories=cats,
    )
    print(f"[DONE] upserted={out['upserted']}")


if __name__ == "__main__":
    main()
