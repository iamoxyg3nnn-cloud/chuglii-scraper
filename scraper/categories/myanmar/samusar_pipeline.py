"""
Isolated Samusar Pipeline (Myanmar Category -> scraper.samusar_videos)

Follows the project's centralized progress + ledger design:
  - Cursor checkpoint: Supabase 'samusar' + local samusar_checkpoint.json
  - Known/dead ID ledger: Supabase known_post_ids (site='samusar') + local samusar_known_post_ids.json
  - Progress state: Supabase 'samusar_progress' + local samusar_progress.json
  - Strict dead-video filtering: dead links marked 'seen', never inserted.
"""
import argparse
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

CURRENT_DIR = Path(__file__).resolve().parent
CATEGORIES_DIR = CURRENT_DIR.parent
SCRAPER_DIR = CATEGORIES_DIR.parent
PROJECT_ROOT = SCRAPER_DIR.parent

for p in [str(CURRENT_DIR), str(SCRAPER_DIR), str(PROJECT_ROOT)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from samusar import enrich_and_polish_detail, scrape_listing  # noqa: E402

SITE_KEY = "samusar"
CHECKPOINT_FILE = CURRENT_DIR / "samusar_checkpoint.json"
KNOWN_IDS_FILE = CURRENT_DIR / "samusar_known_post_ids.json"
PROGRESS_FILE = CURRENT_DIR / "samusar_progress.json"

DEFAULT_CHECKPOINT: Dict[str, Any] = {
    "current_page": 1,
    "max_pages": 1987,
    "total_ingested": 0,
    "stall_count": 0,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_checkpoint() -> Dict[str, Any]:
    from scraper.progress import load_checkpoint as _load

    state = _load(SITE_KEY, CHECKPOINT_FILE, DEFAULT_CHECKPOINT)
    for k, v in DEFAULT_CHECKPOINT.items():
        state.setdefault(k, v)
    return state


def save_checkpoint(state: Dict[str, Any]) -> bool:
    from scraper.progress import save_checkpoint as _save

    state["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
    return _save(SITE_KEY, state, CHECKPOINT_FILE)


def load_known_ids() -> Set[str]:
    from scraper.progress import load_known_ids as _load

    ids = _load(SITE_KEY, KNOWN_IDS_FILE, prefix=False, limit=150000)
    save_known_ids(ids)
    return ids


def save_known_ids(
    known_ids: Set[str],
    new_ids: Optional[Set[str]] = None,
    status: str = "stored",
) -> None:
    from scraper.progress import mark_known_ids, save_known_ids as _save

    if new_ids:
        mark_known_ids(new_ids, default_site=SITE_KEY, status=status)
    _save(KNOWN_IDS_FILE, known_ids)


def load_progress() -> Dict[str, Any]:
    from scraper.progress import load_progress as _load

    return _load(f"{SITE_KEY}_progress", PROGRESS_FILE, {"status": "idle"})


def save_progress(progress: Dict[str, Any]) -> bool:
    from scraper.progress import save_progress as _save

    progress["updated_at"] = _now_iso()
    return _save(f"{SITE_KEY}_progress", progress, PROGRESS_FILE)


def run_samusar_crawl(
    start_page: int,
    pages_to_crawl: int,
    known_ids: Set[str],
    *,
    dry_run: bool = False,
    new_only: bool = False,
    workers: int = 4,
    jitter: float = 0.8,
) -> Dict[str, Any]:
    stats: Dict[str, Any] = {
        "pages_crawled": 0,
        "items_found": 0,
        "upserted": 0,
        "skipped_known": 0,
        "skipped_dead": 0,
        "skipped_unresolved": 0,
        "first_unresolved_page": None,
        "detected_max_pages": None,
        "fetch_failed": False,
        "upsert_failed": False,
        "errors": 0,
        "last_error": None,
    }

    mode = "new_only" if new_only else "resume"
    print(f"\n  [Myanmar :: samusar] Pages {start_page}..{start_page + pages_to_crawl - 1} ({mode})")
    known_streak = 0

    for page_num in range(start_page, start_page + pages_to_crawl):
        cards, max_page, fetch_ok = scrape_listing(page_num)
        if isinstance(max_page, int) and max_page > 0:
            prev = stats["detected_max_pages"]
            stats["detected_max_pages"] = max(prev, max_page) if isinstance(prev, int) else max_page

        if not fetch_ok:
            stats["fetch_failed"] = True
            stats["errors"] += 1
            stats["last_error"] = f"listing fetch failed at page {page_num}"
            print(f"    [samusar] Page {page_num}: fetch failed; keeping cursor.")
            break

        if not cards:
            print(f"    [samusar] Page {page_num}: empty/end of catalog.")
            break

        stats["pages_crawled"] += 1
        stats["items_found"] += len(cards)

        new_cards = [c for c in cards if str(c.get("source_post_id")) not in known_ids]
        skipped = len(cards) - len(new_cards)
        stats["skipped_known"] += skipped
        print(f"    [samusar] Page {page_num}: {len(cards)} cards, {len(new_cards)} new, {skipped} known")

        if not new_cards:
            known_streak += 1
            if new_only and known_streak >= 2:
                break
            time.sleep(jitter)
            continue
        known_streak = 0

        records: List[Dict[str, Any]] = []
        dead_ids: Set[str] = set()
        unresolved = 0

        with ThreadPoolExecutor(max_workers=workers) as pool:
            fmap = {pool.submit(enrich_and_polish_detail, c): c for c in new_cards}
            for fut in as_completed(fmap):
                c = fmap[fut]
                pid = str(c.get("source_post_id") or "")
                try:
                    rec, st = fut.result()
                    if st == "ok" and rec and rec.get("full_video_url"):
                        records.append(rec)
                    elif st == "dead":
                        if pid:
                            dead_ids.add(pid)
                    else:
                        unresolved += 1
                except Exception:
                    unresolved += 1

        stats["skipped_dead"] += len(dead_ids)
        stats["skipped_unresolved"] += unresolved
        if unresolved and stats["first_unresolved_page"] is None:
            stats["first_unresolved_page"] = page_num

        if dead_ids:
            print(f"    [samusar] Dropped {len(dead_ids)} dead/404 video(s)")
        if unresolved:
            print(f"    [samusar] {unresolved} transient error(s) kept unmarked for retry")

        upsert_ok = True
        if not dry_run and records:
            from scraper.db import rpc_result_succeeded, upsert_samusar_videos_batch

            deduped = {(r["source_site"], r["source_post_id"]): r for r in records}
            final_batch = list(deduped.values())
            res = upsert_samusar_videos_batch(final_batch)
            if rpc_result_succeeded(res):
                stats["upserted"] += len(final_batch)
                print(f"    [samusar] [✓] Upserted {len(final_batch)} verified live videos")
            else:
                upsert_ok = False
                stats["upsert_failed"] = True
                stats["errors"] += 1
                stats["last_error"] = str(res)
                print(f"    [samusar] [!] Upsert failed: {res}")

        if not dry_run:
            if dead_ids:
                known_ids.update(dead_ids)
                save_known_ids(known_ids, new_ids=dead_ids, status="seen")
            if upsert_ok and records:
                stored_ids = {str(r["source_post_id"]) for r in records}
                known_ids.update(stored_ids)
                save_known_ids(known_ids, new_ids=stored_ids, status="stored")

        time.sleep(jitter + random.uniform(0.2, 0.5))

    return stats


def run_pipeline(
    *,
    pages: int = 1,
    check_head: bool = True,
    dry_run: bool = False,
    workers: int = 4,
) -> Dict[str, Any]:
    state = load_checkpoint()
    known_ids = load_known_ids()
    progress = load_progress()
    progress.update({
        "status": "running",
        "site": SITE_KEY,
        "started_at": progress.get("started_at") or _now_iso(),
    })
    if not dry_run:
        save_progress(progress)

    total_upserted = 0
    failed = False

    if check_head:
        head_res = run_samusar_crawl(
            1, 1, known_ids, dry_run=dry_run, new_only=True, workers=workers, jitter=0.5
        )
        total_upserted += int(head_res.get("upserted", 0) or 0)
        if head_res.get("upsert_failed"):
            failed = True

    cursor = int(state.get("current_page", 1) or 1)
    max_pages = int(state.get("max_pages", 1987) or 1987)
    if cursor <= max_pages and not failed:
        hist_res = run_samusar_crawl(
            cursor, pages, known_ids, dry_run=dry_run, new_only=False, workers=workers, jitter=0.8
        )
        total_upserted += int(hist_res.get("upserted", 0) or 0)
        if hist_res.get("upsert_failed"):
            failed = True
        elif not hist_res.get("fetch_failed"):
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
            else:
                state["current_page"] = cursor + 1

        det_max = hist_res.get("detected_max_pages")
        if isinstance(det_max, int) and det_max > 0:
            state["max_pages"] = max(int(state.get("max_pages", 0) or 0), det_max)

        progress.update({
            "current_page": state["current_page"],
            "pages_crawled": hist_res.get("pages_crawled", 0),
            "items_found": hist_res.get("items_found", 0),
            "upserted": total_upserted,
            "skipped_dead": hist_res.get("skipped_dead", 0),
            "skipped_unresolved": hist_res.get("skipped_unresolved", 0),
            "errors": hist_res.get("errors", 0),
            "last_error": hist_res.get("last_error"),
        })

    if not dry_run:
        state["total_ingested"] = int(state.get("total_ingested", 0) or 0) + total_upserted
        save_checkpoint(state)
        save_known_ids(known_ids)
        progress["status"] = "failed" if failed else "completed"
        progress["finished_at"] = _now_iso()
        progress["total_ingested"] = state["total_ingested"]
        save_progress(progress)

    return {"status": "failed" if failed else "completed", "upserted": total_upserted}


def main():
    parser = argparse.ArgumentParser(description="Isolated Samusar Myanmar Scraper")
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--no-head", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    res = run_pipeline(
        pages=args.pages,
        check_head=not args.no_head,
        dry_run=args.dry_run,
        workers=args.workers,
    )
    print(f"\nDone: {res}")


if __name__ == "__main__":
    main()
