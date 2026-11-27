"""Backfill: translate existing fangsung titles to English.

Reads scraper.fangsung_videos rows with a missing/empty title_en, translates
each title via Google Translate, and writes title_en back through the
scraper_update_fangsung_title_en RPC in batches.

Usage:
    python -X utf8 scraper/categories/fangsung/translate_backfill.py [--limit N]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from scraper.categories.fangsung.translate import save_cache, translate_to_en  # noqa: E402
from scraper.db import call_supabase_rpc, get_supabase_client  # noqa: E402


def fetch_pending(limit: int) -> List[Dict[str, Any]]:
    """Fetches rows whose title_en is missing/empty via the get RPC."""
    supabase = get_supabase_client()
    pending: List[Dict[str, Any]] = []
    batch = 500
    offset = 0
    scanned = 0
    while len(pending) < limit:
        res = (
            supabase.rpc(
                "scraper_get_fangsung_videos",
                {
                    "p_source_site": None,
                    "p_category": None,
                    "p_tag": None,
                    "p_search": None,
                    "p_sort": "oldest",
                    "p_limit": batch,
                    "p_offset": offset,
                },
            )
            .execute()
            .data
            or []
        )
        if not res:
            break
        scanned += len(res)
        offset += len(res)
        pending.extend(r for r in res if not (r.get("title_en") or "").strip())
        if len(res) < batch:
            break
    print(f"[*] scanned {scanned} row(s); {len(pending)} need an English title")
    return pending[:limit]


def update_title_en(items: List[Dict[str, Any]]) -> int:
    res = call_supabase_rpc("scraper_update_fangsung_title_en", {"items": items})
    if isinstance(res, dict):
        return int(res.get("updated") or 0)
    return len(items)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill English titles for fangsung videos")
    parser.add_argument("--limit", type=int, default=100000, help="Max rows to process")
    args = parser.parse_args()

    pending = fetch_pending(args.limit)
    print(f"[*] {len(pending)} rows need English titles")
    if not pending:
        return 0

    translated: List[Dict[str, Any]] = []
    ok = fail = 0
    for i, row in enumerate(pending, 1):
        title = str(row.get("title") or "")
        en = translate_to_en(title)
        if en:
            translated.append(
                {
                    # The update RPC matches on id or source_post_id.
                    "id": row.get("id"),
                    "source_site": row.get("source_site") or "fangsung",
                    "source_post_id": str(row.get("source_post_id") or ""),
                    "title_en": en[:255],
                }
            )
            ok += 1
        else:
            fail += 1
        if i % 25 == 0 or i == len(pending):
            print(f"    [{i}/{len(pending)}] translated={ok} failed={fail}")
        time.sleep(0.15)  # polite pacing for the free endpoint

        if len(translated) >= 50:
            n = update_title_en(translated)
            print(f"    [DB] wrote {n} title_en values")
            translated = []
            save_cache()

    if translated:
        n = update_title_en(translated)
        print(f"    [DB] wrote {n} title_en values")
    save_cache()
    print(f"[DONE] translated={ok} failed={fail}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
