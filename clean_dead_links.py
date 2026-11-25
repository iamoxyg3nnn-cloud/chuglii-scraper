"""
clean_dead_links.py — Streaming batch parallel cleanup of dead embed servers.

Processes records one page at a time (fetch → parallel-check → upsert → next page).
Uses constant memory regardless of total record count.

Usage:
    python clean_dead_links.py                   # dry-run (no DB writes)
    python clean_dead_links.py --commit          # actually update DB
    python clean_dead_links.py --commit --workers 20 --page-size 150
"""

import sys
import os
import copy
import json
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
from pathlib import Path

# ── environment ───────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent / "scraper"))
from dotenv import load_dotenv
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

from db import get_supabase_client, upsert_onlyfans_videos_batch, rpc_result_succeeded

# ── constants ─────────────────────────────────────────────────────────────────
BASE_URL = "https://internetchicks.com"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

DEAD_HOST_SUBSTRINGS = (
    "sbfull.", "sbfast.", "sblongvu.", "sbchill.", "sbbrisk.", "sbthe.",
    "sbanh.", "sblanh.", "sbspeed.", "sblona.", "sbrapid.", "sbface.",
    "sbhight.", "sbrity.", "sbsonic.", "likessb.", "lvturbo.",
    "slwatch.", "sltube.", "slmaxed.", "streamlare.",
    "fembed.", "upvideo.", "short.ink", "mixdrop.vc", "mixdrop.ch",
)

CF_CHALLENGED_HOSTS = (
    "dood", "d000d", "d0000d", "d0o0d", "dooood", "do0od", "dooodster",
    "ds2play", "ds2video", "dsvplay", "do7go", "d-s.io", "doply",
    "myvidplay", "vidply", "vide0", "vvide0", "playmogo",
)

HOST_PRIORITY = [
    "streamtape", "filemoon", "voe.sx", "bysezejataos",
    "hgcloud", "mixdrop", "mxdrop", "dood", "playmogo",
]

_UPSERT_FIELDS = {
    "source_site", "source_post_id", "title", "slug", "detail_url", "full_video_url",
    "video_mime_type", "trailer_preview_url", "thumbnail_url", "clean_thumbnail_url",
    "quality_label", "quality_score", "duration_seconds", "duration_formatted",
    "views_count", "views_raw", "rating_score", "rating_raw", "categories", "tags",
    "actors", "status", "published_at", "raw_payload",
}

_print_lock = threading.Lock()


def log(*args):
    with _print_lock:
        print(*args, flush=True)


# ── reusable HTTP session ─────────────────────────────────────────────────────
import requests as _req
_session = _req.Session()
_session.headers.update({"User-Agent": UA, "Referer": BASE_URL + "/"})


def _is_embed_alive(embed_url: str) -> bool:
    low = embed_url.lower()
    if any(dead in low for dead in DEAD_HOST_SUBSTRINGS):
        return False
    if any(cf in low for cf in CF_CHALLENGED_HOSTS):
        return True
    try:
        r = _session.get(embed_url, timeout=8, allow_redirects=True)
        if r.status_code in (404, 410):
            return False
        body = (r.text or "")[:8000]
        if "streamtape" in low and ('"fileid":"nofile"' in body or "Video not found!" in body):
            return False
        if ("vidoza" in low or "videzz" in low) and ("File was deleted" in body or "404 Not Found" in body):
            return False
        if "parklogic" in r.url.lower() or "parklogic" in body.lower():
            return False
    except Exception:
        return True
    return True


def _host_score(embed_url: str) -> int:
    low = embed_url.lower()
    for i, h in enumerate(HOST_PRIORITY):
        if h in low:
            return i
    return -1


def _best_embed_url(servers: List[Dict]) -> str:
    if not servers:
        return ""
    return sorted(servers, key=lambda s: _host_score(s.get("embed_url", "")), reverse=True)[0].get("embed_url", "")


def _process_record(record: Dict) -> Optional[Dict]:
    raw = record.get("raw_payload") or {}
    servers: List[Dict] = raw.get("servers", [])
    if not servers:
        return None

    alive = [s for s in servers if _is_embed_alive((s.get("embed_url") or "").strip())]
    if len(alive) == len(servers):
        return None  # all alive

    updated = copy.deepcopy(record)
    updated["raw_payload"] = {**raw, "servers": alive}
    updated["full_video_url"] = _best_embed_url(alive)
    return updated


def _to_upsert(record: Dict) -> Dict:
    return {k: v for k, v in record.items() if k in _UPSERT_FIELDS}


def fetch_page(supabase, source_site: str, offset: int, limit: int) -> List[Dict]:
    params = {
        "p_source_site": source_site,
        "p_tag": None,
        "p_category": "onlyfans",
        "p_search": None,
        "p_sort": "newest",
        "p_limit": limit,
        "p_offset": offset,
    }
    try:
        resp = supabase.rpc("scraper_get_onlyfans_videos", params).execute()
        return resp.data or []
    except Exception as e:
        log(f"[fetch] ERROR at offset {offset}: {e}")
        return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--upsert-batch", type=int, default=25)
    parser.add_argument("--source", default="internetchicks")
    args = parser.parse_args()

    mode = "COMMIT" if args.commit else "DRY-RUN"
    log(f"{'='*62}")
    log(f"  Dead-link cleaner  |  {mode}  |  workers={args.workers}  page={args.page_size}")
    log(f"{'='*62}\n")

    supabase = get_supabase_client()

    # Totals
    total_fetched = 0
    total_checked = 0
    total_needs_update = 0
    total_dead_removed = 0
    total_upserted = 0
    total_upsert_errors = 0

    t_start = time.time()
    page_num = 0
    offset = 0

    while True:
        page_num += 1
        rows = fetch_page(supabase, args.source, offset, args.page_size)
        if not rows:
            log(f"[fetch] No more rows at offset {offset}. Done fetching.")
            break

        total_fetched += len(rows)
        log(f"[page {page_num:>3}] fetched {len(rows)} rows (offset {offset}, total {total_fetched})")

        # --- parallel-check this page ---
        to_update: List[Dict] = []
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(_process_record, r): r for r in rows}
            for fut in as_completed(futures):
                orig = futures[fut]
                total_checked += 1
                try:
                    result = fut.result()
                except Exception as exc:
                    log(f"  [!] worker error on {orig.get('id','?')[:8]}: {exc}")
                    continue

                if result is not None:
                    orig_srv = (orig.get("raw_payload") or {}).get("servers", [])
                    new_srv  = (result.get("raw_payload") or {}).get("servers", [])
                    pruned = len(orig_srv) - len(new_srv)
                    total_dead_removed += pruned
                    total_needs_update += 1
                    to_update.append(result)
                    log(f"  PRUNE {orig.get('slug','?')[:45]:<46}  {len(orig_srv)}→{len(new_srv)} (-{pruned})")

        log(f"[page {page_num:>3}] {len(to_update)} records need update on this page")

        # --- upsert this page's changes ---
        if to_update and args.commit:
            for i in range(0, len(to_update), args.upsert_batch):
                chunk = [_to_upsert(r) for r in to_update[i:i + args.upsert_batch]]
                res = upsert_onlyfans_videos_batch(chunk)
                if rpc_result_succeeded(res):
                    written = res.get("count", len(chunk)) if isinstance(res, dict) else len(chunk)
                    total_upserted += written
                    log(f"  [db] upserted {written} records (running total: {total_upserted})")
                else:
                    total_upsert_errors += 1
                    log(f"  [db] UPSERT FAILED: {res}")
                time.sleep(0.15)

        if len(rows) < args.page_size:
            break
        offset += args.page_size
        time.sleep(0.25)

    elapsed = time.time() - t_start
    log(f"\n{'='*62}")
    log(f"  SUMMARY  ({mode})")
    log(f"  Fetched  : {total_fetched}")
    log(f"  Checked  : {total_checked}")
    log(f"  To update: {total_needs_update}")
    log(f"  Dead srvs removed: {total_dead_removed}")
    if args.commit:
        log(f"  Upserted : {total_upserted}  errors: {total_upsert_errors}")
    else:
        log(f"  [DRY-RUN] Re-run with --commit to apply changes.")
    log(f"  Time     : {elapsed:.1f}s")
    log(f"{'='*62}")


if __name__ == "__main__":
    main()
