"""One-off repair: re-derive uncut-x quality labels from the real files.

Context: uncut-x exposes a single ``video_url`` per row and no resolution
field, so every row was stored as "1080p Full HD" / score 1080. Measured across
the catalogue that was wrong for most rows and, worse, it ranked the genuinely
best files (1440p) BELOW 1080p ones.

New ingests are measured (``uncutx._probe_media``). This repairs rows that were
stored before that change:

  * filename-tagged files (``_720p.mp4``) are trusted -- exact and free
  * untagged files (``video.mp4``) are probed with ffprobe, 6 at a time

Run: ``python -X utf8 scraper/tools/repair_uncutx_quality.py [--dry-run] [--limit N]``
"""
from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scraper.categories.onlyfans.sources import uncutx  # noqa: E402

CHUNK = 400  # rows per UPDATE; keeps the statement small


def filename_height(url: str) -> Optional[int]:
    """Height encoded in the filename (``547290_720p.mp4``), if any."""
    import re

    match = re.search(r"_(\d{3,4})p\.mp4", url.rsplit("/", 1)[-1], re.I)
    return int(match.group(1)) if match else None


def resolve_height(row: Dict[str, str]) -> Optional[int]:
    url = row["video_url"]
    tagged = filename_height(url)
    if tagged:
        return tagged
    _, measured = uncutx._probe_media(url)
    return measured


def fetch_rows() -> List[Dict[str, str]]:
    import httpx

    anon = uncutx.get_anon_key()
    headers = {"apikey": anon, "authorization": f"Bearer {anon}"}
    rows: List[Dict[str, str]] = []
    offset = 0
    while True:
        res = httpx.get(
            f"{uncutx.API}/videos?select=id,video_url"
            f"&video_url=like.*{uncutx.ALIVE_MEDIA_HOST}*&limit=1000&offset={offset}",
            headers=headers,
            timeout=60,
        )
        res.raise_for_status()
        batch = res.json()
        rows.extend({"id": r["id"], "video_url": r["video_url"]} for r in batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def apply(label_map: List[Tuple[str, str, int]]) -> None:
    """Apply (post_id, label, score) triples through the dual-write RPC.

    Goes through scraper_onlyfans_set_quality, never raw SQL: the public feed
    reads the GLOBAL scraper.onlyfans_videos table while the archive of record
    is the per-site table, so a single-table update is invisible on the site.
    """
    from scraper.db import call_supabase_rpc

    for start in range(0, len(label_map), CHUNK):
        chunk = label_map[start:start + CHUNK]
        items = [
            {"post_id": pid, "label": label, "score": score}
            for pid, label, score in chunk
        ]
        try:
            result = call_supabase_rpc(
                "scraper_onlyfans_set_quality",
                {"p_site": "uncutx", "p_items": items},
                retries=2,
                timeout=90.0,
            )
        except Exception as exc:
            raise SystemExit(f"quality update failed on chunk {start // CHUNK + 1}: {exc}")
        print(f"  chunk {start // CHUNK + 1}: {len(chunk)} rows -> {result}")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Repair uncut-x quality labels")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args(argv)

    rows = fetch_rows()
    if args.limit:
        rows = rows[: args.limit]
    print(f"[repair] {len(rows)} live uncut-x rows")

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        heights = list(pool.map(resolve_height, rows))

    label_map: List[Tuple[str, str, int]] = []
    unknown = 0
    for row, height in zip(rows, heights):
        if not height:
            unknown += 1
            continue
        label, score = uncutx._quality_for_height(height)
        label_map.append((row["id"], label, score))

    tally: Dict[str, int] = {}
    for _, label, _score in label_map:
        tally[label] = tally.get(label, 0) + 1
    print(f"  measured: {tally} | unmeasurable: {unknown}")

    if args.dry_run:
        print("  dry-run: no updates written")
        return 0

    apply(label_map)
    print("[repair] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
