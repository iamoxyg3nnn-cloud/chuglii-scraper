"""One-off backfill: probe real resolutions for existing chinese_videos rows.

The scraper originally stamped a hardcoded "720p HD" label; the site actually
serves a single rendition per video whose real resolution varies (probed via
ffprobe). This script probes every row missing raw_payload.probed_resolution
and updates resolution / quality_label / quality_score.

Usage:
    python -m scraper.categories.chinese.quality_backfill [--limit N] [--workers 8]
"""

from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

try:
    from db import get_supabase_client  # direct run from scraper/
except ImportError:
    from scraper.db import get_supabase_client

from scraper.categories.chinese.sources.uxj838238 import _probe_hls_resolution


def _load_rows(limit: int) -> List[Dict[str, Any]]:
    client = get_supabase_client()
    res = client.rpc("scraper_get_chinese_videos", {"p_limit": limit, "p_offset": 0}).execute()
    rows = res.data or []
    # Rows with a stored probe are re-labelled only (no re-probe); rows
    # without one get probed fresh.
    return [
        {"id": r["id"], "full_video_url": r.get("full_video_url"), "raw_payload": r.get("raw_payload") or {}}
        for r in rows
        if r.get("full_video_url")
    ]


def _label(w: int, h: int) -> tuple[str, int]:
    # "p" convention = the smaller dimension
    return f"{min(w, h)}p", min(w, h)


def _probe_row(row: Dict[str, Any]) -> Dict[str, Any] | None:
    stored = (row["raw_payload"] or {}).get("probed_resolution")
    if isinstance(stored, list) and len(stored) == 2:
        w, h = int(stored[0]), int(stored[1])  # already probed: relabel only
    else:
        probed = _probe_hls_resolution(row["full_video_url"])
        if not probed:
            return None
        w, h = probed
    payload = dict(row["raw_payload"])
    payload["probed_resolution"] = [w, h]
    label, score = _label(w, h)
    return {
        "id": row["id"],
        "resolution": f"{w}x{h}",
        "quality_label": label,
        "quality_score": score,
        "raw_payload": payload,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill real resolutions for chinese_videos")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    rows = _load_rows(args.limit)
    print(f"[quality-backfill] {len(rows)} row(s) to probe")
    if not rows:
        return

    updates: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(_probe_row, row): row["id"] for row in rows}
        for fut in as_completed(futures):
            try:
                upd = fut.result()
            except Exception as e:
                print(f"[quality-backfill] {futures[fut][:8]}: error {e}")
                continue
            if upd:
                updates.append(upd)
                print(f"[quality-backfill] {upd['id'][:8]}: {upd['resolution']} -> {upd['quality_label']}")
            else:
                print(f"[quality-backfill] {futures[fut][:8]}: probe failed")

    if updates:
        client = get_supabase_client()
        for i in range(0, len(updates), 200):
            chunk = updates[i:i + 200]
            client.rpc("scraper_update_chinese_quality", {"items": chunk}).execute()
        print(f"[quality-backfill] updated {len(updates)} row(s)")


if __name__ == "__main__":
    sys.exit(main())
