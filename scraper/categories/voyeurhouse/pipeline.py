"""Voyeur House isolated multi-site pipeline (per-site checkpoints + progress)."""

import argparse
import json
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

CURRENT_DIR = Path(__file__).resolve().parent
SCRAPER_DIR = CURRENT_DIR.parent.parent
PROJECT_ROOT = SCRAPER_DIR.parent
for p in (str(PROJECT_ROOT), str(SCRAPER_DIR), str(CURRENT_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

from scraper import budget  # noqa: E402
from scraper.categories.listing_errors import ListingUnavailable  # noqa: E402

try:
    from scraper.categories.voyeurhouse.polish import polish_voyeurhouse_record
    from scraper.categories.voyeurhouse.polish_avs import polish_avs_record
    from scraper.categories.voyeurhouse.sources import SOURCES
except ImportError:  # pragma: no cover
    from polish import polish_voyeurhouse_record
    from polish_avs import polish_avs_record
    from sources import SOURCES

CHECKPOINT_FILE = CURRENT_DIR / "checkpoint.json"
KNOWN_IDS_FILE = CURRENT_DIR / "known_post_ids.json"
PROGRESS_FILE = CURRENT_DIR / "progress.json"
DEFAULT_MAX_PAGES = 786


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _default_max_pages(site: str) -> int:
    """Per-website catalog depth (the original voyeur-house.org stays at 786)."""
    module = SOURCES.get(site)
    return int(getattr(module, "DEFAULT_MAX_PAGES", DEFAULT_MAX_PAGES) or DEFAULT_MAX_PAGES) if module else DEFAULT_MAX_PAGES


def _polish(site: str, record: Dict[str, Any]) -> Dict[str, Any]:
    """Voyeur-house.org uses its native polisher; AVS-family sites use the AVS polisher."""
    return polish_avs_record(record) if site != "voyeurhouseorg" else polish_voyeurhouse_record(record)


def _upsert(site: str, batch: List[Dict[str, Any]]) -> Any:
    """Writes ONLY to the isolated table belonging to ``site``."""
    from scraper import db
    if site == "voyeurhouseorg":
        return db.upsert_voyeurhouse_videos_batch(batch)
    return db.upsert_voyeurhouse_site_videos_batch(site, batch)


def load_checkpoints() -> Dict[str, Any]:
    """Loads cursor state from Supabase first and the atomic local cache second."""
    from scraper.progress import load_checkpoint as _load
    defaults: Dict[str, Any] = {
        site: {"current_page": 1, "max_pages": _default_max_pages(site)} for site in SOURCES
    }
    state = _load("voyeurhouse", CHECKPOINT_FILE, defaults)
    for site, default in defaults.items():
        state.setdefault(site, dict(default))
    return state


def save_checkpoints(state: Dict[str, Any]) -> bool:
    """Saves cursor state remotely and mirrors it locally."""
    from scraper.progress import save_checkpoint as _save
    state["timestamp"] = _now_iso()
    return _save("voyeurhouse", state, CHECKPOINT_FILE)


def load_known_ids() -> Set[str]:
    """Loads ``site:post_id`` keys from local cache plus each site's Supabase ledger."""
    from scraper.progress import load_known_ids as _load
    known: Set[str] = set()
    for site in SOURCES:
        known.update(_load(site, KNOWN_IDS_FILE, prefix=True, limit=50000))
    save_known_ids(known)
    return known


def save_known_ids(
    known: Set[str],
    new_ids: Optional[Set[str]] = None,
    status: str = "stored",
) -> None:
    """Mirrors IDs locally and records newly-confirmed IDs in the central ledger."""
    from scraper.progress import mark_known_ids, save_known_ids as _save
    if new_ids:
        mark_known_ids(new_ids, status=status)
    _save(KNOWN_IDS_FILE, known)


def load_progress() -> Dict[str, Any]:
    """Loads per-run progress from Supabase first and local cache second."""
    from scraper.progress import load_progress as _load
    return _load("voyeurhouse_progress", PROGRESS_FILE, {"status": "idle", "sites": {}})


def save_progress(progress: Dict[str, Any]) -> bool:
    """Persists per-run progress remotely and mirrors it locally."""
    from scraper.progress import save_progress as _save
    progress["updated_at"] = _now_iso()
    return _save("voyeurhouse_progress", progress, PROGRESS_FILE)


def _init_site_progress(progress: Dict[str, Any], site: str, start_page: int, end_page: int, mode: str) -> None:
    progress.setdefault("sites", {})[site] = {
        "site": site,
        "status": "running",
        "mode": mode,
        "start_page": start_page,
        "end_page": end_page,
        "current_page": start_page,
        "pages_crawled": 0,
        "items_found": 0,
        "items_new": 0,
        "skipped_known": 0,
        "skipped_dead": 0,
        "upserted": 0,
        "errors": 0,
        "last_error": None,
        "started_at": _now_iso(),
        "finished_at": None,
    }
    progress["status"] = "running"
    save_progress(progress)


def _log(site: str, stats: Dict[str, Any], page: int, end_page: int) -> None:
    print(
        f"[PROGRESS][voyeurhouse:{site}] page {page}/{end_page} | crawled={stats['pages_crawled']} "
        f"found={stats['items_found']} new={stats['items_new']} skipped={stats['skipped_known']} "
        f"dead={stats['skipped_dead']} unresolved={stats['skipped_unresolved']} "
        f"upserted={stats['upserted']} errors={stats['errors']}",
        flush=True,
    )


def _enrich(
    items: List[Dict[str, Any]], source_module: Any, workers: int = 5
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Returns ``(enriched, unresolved)`` items.

    A transient worker failure is not proof that a post is dead, so failed
    items are returned separately and their IDs stay unmarked for retry.
    """
    if not items:
        return [], []
    result: List[Dict[str, Any]] = []
    unresolved: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {executor.submit(source_module.enrich_detail, item): item for item in items}
        for future in as_completed(futures):
            try:
                result.append(future.result())
            except Exception as exc:
                item = futures[future]
                print(f"[!] Voyeur House enrich error {item.get('detail_url')}: {exc}")
                unresolved.append(item)
    return result, unresolved


def run_site_crawl(
    site_key: str,
    start_page: int,
    max_pages: int,
    known_ids: Set[str],
    *,
    dry_run: bool = False,
    jitter: float = 1.0,
    new_only: bool = False,
    workers: int = 5,
    progress: Optional[Dict[str, Any]] = None,
    deadline: Optional[float] = None,
) -> Dict[str, Any]:
    source = SOURCES.get(site_key)
    if not source:
        return {"items_found": 0, "items_new": 0, "errors": 1, "last_error": "unknown site"}

    end_page = start_page + max_pages - 1
    if progress is not None:
        _init_site_progress(progress, site_key, start_page, end_page, "new_only" if new_only else "resume")

    stats: Dict[str, Any] = {
        "pages_crawled": 0,
        "items_found": 0,
        "items_new": 0,
        "skipped_known": 0,
        "skipped_dead": 0,
        "skipped_unresolved": 0,
        "unresolved_keys": [],
        "first_unresolved_page": None,
        "upserted": 0,
        "upsert_failed": False,
        "detected_max_pages": None,
        "fetch_failed": False,
        "deadline_hit": False,
        "errors": 0,
        "last_error": None,
    }
    known_streak = 0

    for page_num in range(start_page, end_page + 1):
        if budget.expired(deadline):
            stats["deadline_hit"] = True
            print(f"    [BUDGET] {site_key}: stopping at page {page_num} (feed deadline reached).")
            break
        try:
            cards, detected_max = source.scrape_listing(page_num)
        except ListingUnavailable as exc:
            # Blocked/timed out: NOT the end of the catalogue. Keep the cursor on
            # this page and report the site as degraded instead of "completed".
            stats["errors"] += 1
            stats["fetch_failed"] = True
            stats["last_error"] = f"listing unavailable on page {page_num}: {exc}"
            print(f"    [!] {site_key} page {page_num} unavailable ({exc}); cursor stays here.")
            break
        if isinstance(detected_max, int) and detected_max > 0:
            stats["detected_max_pages"] = max(int(stats.get("detected_max_pages") or 0), detected_max)
        if not cards:
            print(f"[-] No Voyeur House cards on page {page_num}. Ending crawl.")
            break

        stats["pages_crawled"] += 1
        stats["items_found"] += len(cards)
        fresh = [c for c in cards if f"{site_key}:{c.get('source_post_id')}" not in known_ids]
        stats["skipped_known"] += len(cards) - len(fresh)
        if not fresh:
            known_streak += 1
            if new_only and known_streak >= 2:
                break
            time.sleep(jitter)
            continue
        known_streak = 0

        enriched, unresolved_items = _enrich(fresh, source, workers=workers)
        # A detail fetch that failed because of a bot wall / 5xx must be
        # retryable.  Only confirmed 404/410/no-embed pages are dead.
        transient_ids = {
            id(item) for item in enriched
            if str(item.get("status") or "").lower() == "unresolved"
            or item.get("dead_reason") in {"detail_fetch_failed", "detail_unavailable"}
        }
        retry_items = unresolved_items + [item for item in enriched if id(item) in transient_ids]
        unresolved_keys = [
            f"{site_key}:{item.get('source_post_id')}"
            for item in retry_items
            if item.get("source_post_id")
        ]
        if unresolved_keys:
            stats["skipped_unresolved"] += len(unresolved_keys)
            stats["unresolved_keys"].extend(unresolved_keys)
            if stats["first_unresolved_page"] is None:
                stats["first_unresolved_page"] = page_num
        enriched = [item for item in enriched if id(item) not in transient_ids]
        batch: List[Dict[str, Any]] = []
        pending: List[str] = []
        seen_ids: Set[str] = set()
        for raw in enriched:
            polished = _polish(site_key, raw)
            key = f"{site_key}:{polished['source_post_id']}"
            if polished["status"] == "dead":
                stats["skipped_dead"] += 1
                known_ids.add(key)
                seen_ids.add(key)
                continue
            batch.append(polished)
            pending.append(key)
            stats["items_new"] += 1

        upsert_ok = True
        if batch and not dry_run:
            try:
                from scraper.db import rpc_result_succeeded
                result = _upsert(site_key, batch)
                if not rpc_result_succeeded(result):
                    upsert_ok = False
                    stats["errors"] += 1
                    stats["last_error"] = str(result.get("error") or result)
                else:
                    stats["upserted"] += len(batch)
            except Exception as exc:
                upsert_ok = False
                stats["errors"] += 1
                stats["last_error"] = str(exc)
                print(f"[!] Voyeur House upsert error: {exc}")

        if dry_run:
            pass
        else:
            if seen_ids:
                known_ids.update(seen_ids)
                save_known_ids(known_ids, new_ids=seen_ids, status="seen")
            if upsert_ok:
                confirmed_ids = set(pending)
                known_ids.update(confirmed_ids)
                save_known_ids(known_ids, new_ids=confirmed_ids, status="stored")
            elif pending:
                stats["upsert_failed"] = True
                print(f"[!] Keeping {len(pending)} Voyeur House IDs unmarked for retry.")
        save_known_ids(known_ids)

        if progress is not None:
            sp = progress.setdefault("sites", {}).setdefault(site_key, {})
            sp.update({
                "status": "running",
                "current_page": page_num,
                "pages_crawled": stats["pages_crawled"],
                "items_found": stats["items_found"],
                "items_new": stats["items_new"],
                "skipped_known": stats["skipped_known"],
                "skipped_dead": stats["skipped_dead"],
                "skipped_unresolved": stats["skipped_unresolved"],
                "upserted": stats["upserted"],
                "errors": stats["errors"],
                "last_error": stats["last_error"],
            })
            save_progress(progress)
        _log(site_key, stats, page_num, end_page)
        time.sleep(jitter + random.uniform(0.2, 0.6))

    if progress is not None:
        sp = progress.setdefault("sites", {}).setdefault(site_key, {})
        sp.update({"status": "failed" if stats["upsert_failed"] else "completed", "finished_at": _now_iso(), **stats})
        save_progress(progress)
    return stats


def run_pipeline(
    sites: List[str],
    pages_per_site: int = 2,
    *,
    dry_run: bool = False,
    jitter: float = 1.0,
    full_site: bool = False,
    new_only: bool = False,
    workers: int = 5,
) -> Dict[str, Any]:
    checkpoints = load_checkpoints()
    known_ids = load_known_ids()
    progress = load_progress()
    progress.update({
        "status": "running",
        "started_at": _now_iso(),
        "mode": "full_site" if full_site else "new_only" if new_only else "resume",
        "sites_requested": sites,
        "pages_per_site": pages_per_site,
        "dry_run": dry_run,
    })
    save_progress(progress)

    totals: Dict[str, Any] = {
        "items_found": 0,
        "items_new": 0,
        "skipped_known": 0,
        "skipped_dead": 0,
        "skipped_unresolved": 0,
        "upserted": 0,
        "errors": 0,
    }
    failed_sites: List[str] = []

    for site in sites:
        try:
            default_max = _default_max_pages(site)
            cp = checkpoints.setdefault(site, {"current_page": 1, "max_pages": default_max})
            start = 1 if full_site else int(cp.get("current_page", 1) or 1)
            max_pages = max(1, int(cp.get("max_pages") or default_max) - start + 1) if full_site else max(1, pages_per_site)
            stats = run_site_crawl(site, start, max_pages, known_ids, dry_run=dry_run, jitter=jitter, new_only=new_only, workers=workers, progress=progress)
            for key in totals:
                totals[key] += int(stats.get(key, 0) or 0)
            if stats.get("detected_max_pages"):
                cp["max_pages"] = max(int(cp.get("max_pages") or 0), int(stats["detected_max_pages"]))
            site_status = "completed"
            if stats.get("upsert_failed"):
                failed_sites.append(site)
                site_status = "failed"
            elif not full_site:
                crawled = int(stats.get("pages_crawled", 0) or 0)
                unresolved = int(stats.get("skipped_unresolved", 0) or 0)
                first_unresolved = stats.get("first_unresolved_page")
                if unresolved and isinstance(first_unresolved, int):
                    from scraper.progress import cursor_retry_state
                    stall, hold = cursor_retry_state(
                        stall_count=int(cp.get("stall_count", 0) or 0), unresolved=unresolved
                    )
                    cp["stall_count"] = stall
                    if hold:
                        cp["current_page"] = max(1, first_unresolved)
                        site_status = "partial"
                        print(
                            f"    [!] Holding {site} cursor at page {first_unresolved} for retry "
                            f"({unresolved} unresolved, attempt {stall}/5)."
                        )
                    else:
                        unresolved_keys = list(stats.get("unresolved_keys") or [])
                        if unresolved_keys:
                            known_ids.update(unresolved_keys)
                            save_known_ids(
                                known_ids, new_ids=set(unresolved_keys), status="seen"
                            )
                        print(
                            f"    [!] Giving up on {unresolved} unresolved {site} posts after 5 "
                            "attempts; marking seen and advancing."
                        )
                if site_status == "completed":
                    cp["stall_count"] = 0
                    cp["current_page"] = start + crawled
            progress.setdefault("sites", {}).setdefault(site, {})["status"] = site_status
        except Exception as exc:
            failed_sites.append(site)
            progress.setdefault("sites", {}).setdefault(site, {})["status"] = "failed"
            progress["sites"][site]["last_error"] = str(exc)
            print(f"[!] Voyeur House site [{site}] failed in isolation: {exc}")
        save_checkpoints(checkpoints)
        save_progress(progress)

    from scraper.progress import finalize_status
    finalize_status(progress, failed_sites)
    progress["finished_at"] = _now_iso()
    progress["totals"] = totals
    save_progress(progress)

    print("\n" + "=" * 60)
    print("VOYEUR HOUSE ISOLATED SCRAPE SUMMARY")
    for key, value in totals.items():
        print(f"{key:16}: {value}")
    print("=" * 60 + "\n")
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(description="Voyeur House isolated multi-site scraper")
    parser.add_argument("--site", default="voyeurhouseorg", choices=["all", *SOURCES.keys()])
    parser.add_argument("--pages", type=int, default=2)
    parser.add_argument("--full-site", action="store_true")
    parser.add_argument("--new-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--workers", type=int, default=5)
    args = parser.parse_args()

    sites = list(SOURCES.keys()) if args.site == "all" else [args.site]
    run_pipeline(
        sites,
        pages_per_site=args.pages,
        dry_run=args.dry_run,
        jitter=args.delay,
        full_site=args.full_site,
        new_only=args.new_only,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
