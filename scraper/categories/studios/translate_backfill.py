"""Backfill: translate existing studios titles to English.

Reads scraper.madouer_videos / scraper.dasj91_videos rows with a missing or
empty title_en, translates each title via Google Translate, and writes
title_en back through the per-site scraper_update_{site}_title_en RPCs.

Usage:
    python -X utf8 scraper/categories/studios/translate_backfill.py [--limit N]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from scraper.categories.studios.translate import save_cache, translate_to_en  # noqa: E402
from scraper.db import call_supabase_rpc, get_supabase_client  # noqa: E402


def fetch_pending(limit: int, site: str = "madouer") -> List[Dict[str, Any]]:
    """Fetches rows whose title_en is missing/empty via the per-site get RPC."""
    supabase = get_supabase_client()
    rows: List[Dict[str, Any]] = []
    batch = 500
    offset = 0
    while len(rows) < limit:
        res = (
            supabase.rpc(
                f"scraper_get_{site}_videos",
                {
                    "p_category": None,
                    "p_tag": None,
                    "p_search": None,
                    "p_sort": "oldest",
                    "p_limit": min(batch, limit - len(rows)),
                    "p_offset": offset,
                },
            )
            .execute()
            .data
            or []
        )
        if not res:
            break
        rows.extend(res)
        offset += len(res)
    pending = [r for r in rows if not (r.get("title_en") or "").strip()]
    return pending


def update_title_en(items: List[Dict[str, Any]], site: str = "madouer") -> int:
    res = call_supabase_rpc(f"scraper_update_{site}_title_en", {"items": items})
    if isinstance(res, dict):
        return int(res.get("updated") or 0)
    return len(items)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill English titles for studios videos")
    parser.add_argument("--limit", type=int, default=100000, help="Max rows to process")
    args = parser.parse_args()

    total_ok = total_fail = 0
    for site in ("madouer", "dasj91", "madouhd"):
        pending = fetch_pending(args.limit, site)
        print(f"[*] [{site}] {len(pending)} rows need English titles")
        if not pending:
            continue
        ok, fail = _run_site(site, pending)
        total_ok += ok
        total_fail += fail
    print(f"[DONE] translated={total_ok} failed={total_fail}")
    return 0


def _run_site(site: str, pending: List[Dict[str, Any]]) -> tuple:

    translated: List[Dict[str, Any]] = []
    ok = fail = 0
    for i, row in enumerate(pending, 1):
        title = str(row.get("title") or "")
        en = translate_to_en(title)
        if en:
            translated.append(
                {
                    "source_site": row.get("source_site") or site,
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
            n = update_title_en(translated, site)
            print(f"    [DB] wrote {n} title_en values")
            translated = []
            save_cache()

    if translated:
        n = update_title_en(translated, site)
        print(f"    [DB] wrote {n} title_en values")
    save_cache()
    return ok, fail


if __name__ == "__main__":
    raise SystemExit(main())
