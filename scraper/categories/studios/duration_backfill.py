"""
Backfill: duration for studios rows missing it.

For every row in scraper.madouer_videos / scraper.dasj91_videos with a NULL
duration_seconds, re-fetches the detail page, extracts the first part m3u8,
sums its EXTINF values, and writes duration back through the per-site
scraper_update_{site}_quality RPC (which now carries duration fields).

Usage:
    python -X utf8 scraper/categories/studios/duration_backfill.py [--limit N] [--site madouer|dasj91]
"""
from __future__ import annotations

import argparse
import re
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from scraper.db import call_supabase_rpc, get_supabase_client  # noqa: E402

_RE_EXTINF = re.compile(r"#EXTINF:([\d.]+)")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"


def _sum_playlist_duration(m3u8_url: str) -> int | None:
    try:
        req = urllib.request.Request(m3u8_url, headers={"User-Agent": UA})
        playlist = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "replace")
        total = sum(float(x) for x in _RE_EXTINF.findall(playlist))
        return int(round(total)) if total > 0 else None
    except Exception:
        return None


def fetch_pending(site: str, limit: int) -> List[Dict[str, Any]]:
    supabase = get_supabase_client()
    params: Dict[str, Any] = {
        "p_category": None,
        "p_tag": None,
        "p_search": None,
        "p_sort": "oldest",
        "p_limit": 500,
        "p_offset": 0,
    }
    if site == "madouer":
        params["p_source_site"] = None
        params["p_content_type"] = None
    rows: List[Dict[str, Any]] = []
    while len(rows) < limit:
        res = (
            supabase.rpc(f"scraper_get_{site}_videos", params).execute().data or []
        )
        if not res:
            break
        rows.extend(res)
        params["p_offset"] += len(res)
    return [r for r in rows if not r.get("duration_seconds")]


def update_durations(site: str, items: List[Dict[str, Any]]) -> int:
    res = call_supabase_rpc(f"scraper_update_{site}_quality", {"items": items})
    if isinstance(res, dict) and res.get("updated") is not None:
        return int(res["updated"])
    return 0


def run_site(site: str, limit: int) -> None:
    if site == "dasj91":
        from scraper.categories.studios.sources import dasj91 as src
    else:
        from scraper.categories.studios.sources import madouer as src

    pending = fetch_pending(site, limit)
    print(f"[*] [{site}] {len(pending)} rows missing duration")
    batch: List[Dict[str, Any]] = []
    ok = fail = 0
    for i, row in enumerate(pending, 1):
        dur: int | None = None
        try:
            if site == "dasj91":
                dur = src.enrich_detail(dict(row)).get("duration_seconds")
            elif site in ("madouer", "madouhd"):
                # Both store a direct (tokenless) m3u8 in full_video_url -
                # probe it directly rather than refetching the detail page.
                dur = _sum_playlist_duration(str(row.get("full_video_url") or ""))
        except Exception:
            dur = None
        if dur:
            mins, secs = divmod(int(dur), 60)
            fmt = f"{mins}:{secs:02d}" if mins < 60 else f"{mins // 60}:{mins % 60:02d}:{secs:02d}"
            batch.append({
                "id": str(row.get("id") or ""),
                "duration_seconds": int(dur),
                "duration_formatted": fmt,
            })
            ok += 1
        else:
            fail += 1
        if i % 20 == 0 or i == len(pending):
            print(f"    [{i}/{len(pending)}] ok={ok} fail={fail}")
        if len(batch) >= 40:
            print(f"    [DB] wrote {update_durations(site, batch)} durations")
            batch = []
        time.sleep(0.2)
    if batch:
        print(f"    [DB] wrote {update_durations(site, batch)} durations")
    print(f"[DONE] [{site}] ok={ok} fail={fail}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill durations for studios videos")
    parser.add_argument("--limit", type=int, default=100000)
    parser.add_argument("--site", type=str, default="", help="madouer | dasj91 | empty for both")
    args = parser.parse_args()
    sites = [args.site] if args.site else ["madouer", "dasj91", "madouhd"]
    for site in sites:
        run_site(site, args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
