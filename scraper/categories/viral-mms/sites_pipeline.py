"""
Multi-site isolated Viral MMS pipeline for:
  - spicymms (https://www.spicymms.com)
  - desi49 (https://desi49.buzz)
  - mydesi (https://mydesi.sbs)
  - indianbfclips (https://www.indianbfclips.com)

Each site stores records strictly into its own isolated Supabase table
(scraper.<site>_videos) and verifies stream/embed liveness before insertion
so dead videos are never scraped.
"""
import argparse
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

CURRENT_DIR = Path(__file__).resolve().parent
CATEGORIES_DIR = CURRENT_DIR.parent
SCRAPER_DIR = CATEGORIES_DIR.parent
PROJECT_ROOT = SCRAPER_DIR.parent

for p in [str(PROJECT_ROOT), str(SCRAPER_DIR), str(CURRENT_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from polish import polish_scraped_record  # noqa: E402
from sources import SOURCES  # noqa: E402
from scraper import budget  # noqa: E402

VIRAL_MMS_SITES = ("spicymms", "desi49", "mydesi", "indianbfclips")
CHECKPOINT_FILE = CURRENT_DIR / "sites_checkpoint.json"
KNOWN_IDS_FILE = CURRENT_DIR / "sites_known_post_ids.json"
PROGRESS_FILE = CURRENT_DIR / "sites_progress.json"

DEFAULT_CHECKPOINTS: Dict[str, Any] = {
    "spicymms": {"current_page": 1, "max_pages": 250},
    "desi49": {"current_page": 1, "max_pages": 460},
    "mydesi": {"current_page": 1, "max_pages": 60},
    "indianbfclips": {"current_page": 1, "max_pages": 250},
    "total_ingested": 0,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_checkpoints() -> Dict[str, Any]:
    from scraper.progress import load_checkpoint as _load

    state = _load("viral_mms_sites", CHECKPOINT_FILE, DEFAULT_CHECKPOINTS)
    for site, defaults in DEFAULT_CHECKPOINTS.items():
        if isinstance(defaults, dict):
            cur = state.get(site)
            if not isinstance(cur, dict):
                state[site] = dict(defaults)
            else:
                for k, v in defaults.items():
                    cur.setdefault(k, v)
    return state


def save_checkpoints(state: Dict[str, Any]) -> bool:
    from scraper.progress import save_checkpoint as _save

    state["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
    return _save("viral_mms_sites", state, CHECKPOINT_FILE)


def load_known_ids() -> Set[str]:
    from scraper.progress import load_known_ids as _load

    ids: Set[str] = set()
    for site in VIRAL_MMS_SITES:
        ids.update(_load(site, KNOWN_IDS_FILE, prefix=True, limit=100000))
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

    return _load("viral_mms_sites_progress", PROGRESS_FILE, {"status": "idle", "sites": {}})


def save_progress(progress: Dict[str, Any]) -> bool:
    from scraper.progress import save_progress as _save

    progress["updated_at"] = _now_iso()
    return _save("viral_mms_sites_progress", progress, PROGRESS_FILE)


def enrich_items_concurrently(
    source_mod: Any, items: List[Dict[str, Any]], max_workers: int = 4
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Returns ``(alive_items, dead_items, transient_error_items)``."""
    if not items:
        return [], [], []
    alive: List[Dict[str, Any]] = []
    dead: List[Dict[str, Any]] = []
    errored: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(source_mod.enrich_detail, card): card for card in items}
        for future in as_completed(future_map):
            orig = future_map[future]
            try:
                enriched, status = future.result()
                if status == "ok" and enriched:
                    alive.append(enriched)
                elif status == "dead":
                    dead.append(orig)
                else:
                    errored.append(orig)
            except Exception as exc:
                print(f"    [!] Worker error enriching {orig.get('url')}: {exc}")
                errored.append(orig)
    return alive, dead, errored


def run_site_crawl(
    site_key: str,
    start_page: int,
    max_pages: int,
    known_ids: Set[str],
    *,
    dry_run: bool = False,
    jitter: float = 0.8,
    new_only: bool = False,
    workers: int = 4,
    progress: Optional[Dict[str, Any]] = None,
    deadline: Optional[float] = None,
) -> Dict[str, Any]:
    source_mod = SOURCES.get(site_key)
    if not source_mod:
        return {"items_found": 0, "upserted": 0, "errors": 1, "last_error": f"unknown site {site_key}"}

    mode = "new_only" if new_only else "resume"
    end_page = start_page + max_pages - 1
    print(f"\n  [Viral MMS :: {site_key}] Pages {start_page}..{end_page} ({mode})")

    stats: Dict[str, Any] = {
        "items_found": 0,
        "items_alive": 0,
        "skipped_known": 0,
        "skipped_dead": 0,
        "skipped_unresolved": 0,
        "first_unresolved_page": None,
        "pages_crawled": 0,
        "detected_max_pages": None,
        "upserted": 0,
        "upsert_failed": False,
        "fetch_failed": False,
        "deadline_hit": False,
        "errors": 0,
        "last_error": None,
    }
    known_streak = 0

    for idx in range(max_pages):
        page_num = start_page + idx
        if budget.expired(deadline):
            stats["deadline_hit"] = True
            print(f"    [BUDGET] {site_key}: stopping at page {page_num} (feed deadline reached).")
            break
        cards, detected_max, fetch_ok = source_mod.scrape_listing(page_num)
        if isinstance(detected_max, int) and detected_max > 0:
            stats["detected_max_pages"] = max(stats.get("detected_max_pages") or 0, detected_max)

        if not fetch_ok:
            stats["fetch_failed"] = True
            stats["errors"] += 1
            stats["last_error"] = f"listing fetch blocked/failed at page {page_num}"
            print(f"    [{site_key}] Page {page_num}: fetch blocked/failed; keeping cursor.")
            break

        if not cards:
            print(f"    [{site_key}] Page {page_num}: empty/end of catalog.")
            break

        stats["pages_crawled"] += 1
        stats["items_found"] += len(cards)

        new_cards = [c for c in cards if f"{site_key}:{c.get('post_id')}" not in known_ids]
        skipped = len(cards) - len(new_cards)
        stats["skipped_known"] += skipped
        print(f"    [{site_key}] Page {page_num}: {len(cards)} cards, {len(new_cards)} new, {skipped} known")

        if not new_cards:
            known_streak += 1
            if new_only and known_streak >= 2:
                print(f"    [{site_key}] New-only streak reached; stopping early.")
                break
            time.sleep(jitter)
            continue
        known_streak = 0

        alive_items, dead_items, errored_items = enrich_items_concurrently(
            source_mod, new_cards, max_workers=workers
        )
        stats["skipped_dead"] += len(dead_items)
        stats["skipped_unresolved"] += len(errored_items)
        if errored_items and stats["first_unresolved_page"] is None:
            stats["first_unresolved_page"] = page_num

        if dead_items:
            print(f"    [{site_key}] Dropped {len(dead_items)} dead/unplayable video(s)")
        if errored_items:
            print(f"    [{site_key}] {len(errored_items)} transient fetch error(s) kept unmarked for retry")

        batch_to_upsert: List[Dict[str, Any]] = []
        pending_ids: List[str] = []
        for item in alive_items:
            try:
                polished = polish_scraped_record(item, source_site=site_key)
                if not polished.get("full_video_url"):
                    dead_items.append(item)
                    continue
                batch_to_upsert.append(polished)
                pending_ids.append(f"{site_key}:{polished['source_post_id']}")
            except Exception as exc:
                stats["errors"] += 1
                stats["last_error"] = str(exc)

        stats["items_alive"] += len(batch_to_upsert)

        upsert_ok = True
        if not dry_run and batch_to_upsert:
            from scraper.db import rpc_result_succeeded, upsert_viral_mms_site_videos_batch

            deduped = {(r["source_site"], r["source_post_id"]): r for r in batch_to_upsert}
            final_batch = list(deduped.values())
            res = upsert_viral_mms_site_videos_batch(site_key, final_batch)
            if rpc_result_succeeded(res):
                stats["upserted"] += len(final_batch)
                print(f"    [{site_key}] [✓] Upserted {len(final_batch)} verified live videos")
            else:
                upsert_ok = False
                stats["upsert_failed"] = True
                stats["errors"] += 1
                stats["last_error"] = str(res)
                print(f"    [{site_key}] [!] Upsert failed: {res}")

        if not dry_run:
            dead_keys = {f"{site_key}:{d.get('post_id')}" for d in dead_items if d.get("post_id")}
            if dead_keys:
                known_ids.update(dead_keys)
                save_known_ids(known_ids, new_ids=dead_keys, status="seen")
            if upsert_ok and pending_ids:
                stored_keys = set(pending_ids)
                known_ids.update(stored_keys)
                save_known_ids(known_ids, new_ids=stored_keys, status="stored")

        if progress is not None:
            sp = progress.setdefault("sites", {}).setdefault(site_key, {})
            sp.update({
                "site": site_key,
                "status": "failed" if stats["upsert_failed"] else "running",
                "current_page": page_num,
                "pages_crawled": stats["pages_crawled"],
                "items_found": stats["items_found"],
                "upserted": stats["upserted"],
                "skipped_dead": stats["skipped_dead"],
                "skipped_unresolved": stats["skipped_unresolved"],
                "errors": stats["errors"],
                "last_error": stats["last_error"],
            })
            save_progress(progress)

        time.sleep(jitter + random.uniform(0.2, 0.6))

    if progress is not None:
        sp = progress.setdefault("sites", {}).setdefault(site_key, {})
        sp["status"] = "failed" if stats.get("upsert_failed") else "completed"
        sp["finished_at"] = _now_iso()
        save_progress(progress)

    return stats


def run_multi_site_pipeline(
    sites: Optional[List[str]] = None,
    *,
    pages_per_site: int = 1,
    check_head: bool = True,
    dry_run: bool = False,
    workers: int = 4,
    deadline: Optional[float] = None,
) -> Dict[str, Any]:
    target_sites = [s for s in (sites or list(VIRAL_MMS_SITES)) if s in SOURCES]
    checkpoints = load_checkpoints()
    known_ids = load_known_ids()
    progress = load_progress()
    progress.update({
        "status": "running",
        "started_at": progress.get("started_at") or _now_iso(),
        "sites_requested": target_sites,
    })
    if not dry_run:
        save_progress(progress)

    total_upserted = 0
    failed_sites: List[str] = []

    for site_key in target_sites:
        if budget.expired(deadline):
            print("  [BUDGET] Viral MMS sites feed deadline reached; remaining sites resume next cycle.")
            break
        state = checkpoints.setdefault(site_key, {"current_page": 1, "max_pages": 250})
        try:
            if check_head:
                head_res = run_site_crawl(
                    site_key,
                    1,
                    1,
                    known_ids,
                    dry_run=dry_run,
                    jitter=0.5,
                    new_only=True,
                    workers=workers,
                    progress=None if dry_run else progress,
                    deadline=deadline,
                )
                total_upserted += int(head_res.get("upserted", 0) or 0)

            cursor = int(state.get("current_page", 1) or 1)
            max_pages = int(state.get("max_pages", 250) or 250)
            if cursor <= max_pages:
                hist_res = run_site_crawl(
                    site_key,
                    cursor,
                    pages_per_site,
                    known_ids,
                    dry_run=dry_run,
                    jitter=0.8,
                    new_only=False,
                    workers=workers,
                    progress=None if dry_run else progress,
                    deadline=deadline,
                )
                total_upserted += int(hist_res.get("upserted", 0) or 0)
                if not hist_res.get("upsert_failed") and not hist_res.get("fetch_failed"):
                    crawled = int(hist_res.get("pages_crawled", 0) or 0)
                    unresolved = int(hist_res.get("skipped_unresolved", 0) or 0)
                    first_unres = hist_res.get("first_unresolved_page")
                    if unresolved and isinstance(first_unres, int):
                        from scraper.progress import cursor_retry_state

                        stall, hold = cursor_retry_state(
                            stall_count=int(state.get("stall_count", 0) or 0),
                            unresolved=unresolved,
                        )
                        state["stall_count"] = stall
                        if hold:
                            state["current_page"] = max(1, first_unres)
                        else:
                            state["stall_count"] = 0
                            state["current_page"] = cursor + crawled
                    elif crawled > 0:
                        state["stall_count"] = 0
                        state["current_page"] = cursor + crawled
                    elif hist_res.get("pages_crawled", 0) == 0:
                        # Reached verified end of catalog
                        state["current_page"] = cursor + 1
                elif hist_res.get("upsert_failed"):
                    failed_sites.append(site_key)

                det_max = hist_res.get("detected_max_pages")
                if isinstance(det_max, int) and det_max > 0:
                    state["max_pages"] = max(int(state.get("max_pages", 0) or 0), det_max)
        except Exception as exc:
            failed_sites.append(site_key)
            print(f"  [!] Viral MMS [{site_key}] failed in isolation: {exc}")

        if not dry_run:
            checkpoints[site_key] = state
            save_checkpoints(checkpoints)
            save_known_ids(known_ids)

    from scraper.progress import finalize_status

    status = finalize_status(progress, failed_sites)
    if not dry_run:
        checkpoints["total_ingested"] = int(checkpoints.get("total_ingested", 0) or 0) + total_upserted
        save_checkpoints(checkpoints)
        progress["finished_at"] = _now_iso()
        progress["totals"] = {"upserted": total_upserted}
        save_progress(progress)

    return {"status": status, "upserted": total_upserted, "failed_sites": failed_sites}


def main():
    parser = argparse.ArgumentParser(description="Isolated Multi-Site Viral MMS Scraper")
    parser.add_argument("--site", choices=["all", *VIRAL_MMS_SITES], default="all")
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--no-head", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sites = list(VIRAL_MMS_SITES) if args.site == "all" else [args.site]
    res = run_multi_site_pipeline(
        sites,
        pages_per_site=args.pages,
        check_head=not args.no_head,
        dry_run=args.dry_run,
        workers=args.workers,
    )
    print(f"\nDone: {res}")


if __name__ == "__main__":
    main()
