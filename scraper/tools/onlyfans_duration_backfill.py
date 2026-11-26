"""Drain the OnlyFans duration backlog.

Some sources cannot tell us how long a video is (influencersgonewild publishes
no duration anywhere; sheeshfans only sometimes has ``og:video:duration``), so
their rows used to be stored with the table default 600 s and every card read
"10:00". Ingest now measures what it can, and this tool heals the rest on every
maintenance run instead of a human running a one-off script.

It is deliberately boring:

* the database decides what needs work (``duration_checked_at IS NULL``),
* a duration is read from the media itself -- MP4 ``mvhd`` first (two small
  ranged requests, no ffmpeg), else one header-only ffprobe,
* a row is marked checked either way, so media that cannot be measured does not
  churn forever,
* the run is bounded by ``--minutes`` and by a per-batch limit, so it is safe to
  schedule and safe to interrupt.

Usage:
    python -X utf8 scraper/tools/onlyfans_duration_backfill.py --minutes 10
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scraper.categories.onlyfans.media_probe import (  # noqa: E402
    mp4_duration_from_header,
    probe_media,
)
from scraper.db import call_supabase_rpc  # noqa: E402

#: Sources whose pages cannot be trusted for duration. Verified against live
#: pages: influencersgonewild carries no duration metadata at all, and
#: sheeshfans only sometimes does.
SITE_REFERERS = {
    "influencersgonewild": "https://influencersgonewild.com/",
    "sheeshfans": "https://sheeshfans.com/",
    "borntobefuck": "https://borntobefuck.com/",
    "uncutx": "https://uncut-x.com/",
}

DEFAULT_BATCH = 200
DEFAULT_WORKERS = 12


def fetch_backlog(site: str, limit: int) -> List[Dict[str, Any]]:
    res = call_supabase_rpc(
        "scraper_onlyfans_duration_backlog",
        {"p_site": site, "p_limit": int(limit)},
        retries=2,
        timeout=90.0,
    )
    items = (res or {}).get("items") or []
    return [i for i in items if i.get("post_id") and i.get("url")]


def measure(url: str, referer: Optional[str]) -> Optional[int]:
    """Duration in seconds from the media itself, or None."""
    seconds = mp4_duration_from_header(url, referer=referer)
    if not seconds:
        seconds, _height = probe_media(url, referer=referer, timeout=12)
    return seconds or None


def drain_site(
    site: str,
    *,
    minutes: float,
    batch: int = DEFAULT_BATCH,
    workers: int = DEFAULT_WORKERS,
    dry_run: bool = False,
) -> Dict[str, Any]:
    referer = SITE_REFERERS.get(site)
    deadline = time.time() + max(0.5, float(minutes)) * 60.0
    measured = 0
    unknown = 0
    written = 0
    batches = 0

    while time.time() < deadline:
        rows = fetch_backlog(site, batch)
        if not rows:
            break
        batches += 1

        def work(row: Dict[str, Any]) -> Tuple[str, Optional[int]]:
            return str(row["post_id"]), measure(row["url"], referer)

        with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
            results = list(pool.map(work, rows))

        found = [{"post_id": pid, "seconds": secs} for pid, secs in results if secs]
        missing = [pid for pid, secs in results if not secs]
        measured += len(found)
        unknown += len(missing)

        if dry_run:
            print(f"  [{site}] dry-run batch {batches}: {len(found)} measurable, {len(missing)} not")
            break

        if found:
            res = call_supabase_rpc(
                "scraper_onlyfans_set_duration",
                {"p_site": site, "p_items": found},
                retries=2,
                timeout=120.0,
            )
            written += int((res or {}).get("site_rows") or 0)
        if missing:
            # Marked checked so a dead or exotic file is not re-probed forever.
            call_supabase_rpc(
                "scraper_onlyfans_mark_duration_checked",
                {"p_site": site, "p_ids": missing},
                retries=2,
                timeout=90.0,
            )

        print(
            f"  [{site}] batch {batches}: measured {len(found)}, "
            f"unmeasurable {len(missing)}, written {written}"
        )

    return {
        "site": site,
        "batches": batches,
        "measured": measured,
        "unknown": unknown,
        "written": written,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill OnlyFans video durations")
    parser.add_argument("--minutes", type=float, default=10.0, help="time budget per site")
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--sites", default="", help="comma list (default: all sources)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    sites = [s.strip() for s in args.sites.split(",") if s.strip()] or list(SITE_REFERERS)
    started = time.time()
    totals = {"measured": 0, "unknown": 0, "written": 0}

    for site in sites:
        try:
            summary = drain_site(
                site,
                minutes=args.minutes,
                batch=args.batch,
                workers=args.workers,
                dry_run=args.dry_run,
            )
        except Exception as exc:                     # one bad source must not stop the rest
            print(f"  [!] {site} duration backfill failed: {exc}")
            continue
        for key in totals:
            totals[key] += int(summary.get(key) or 0)

    print(
        f"\n[durations] sites={len(sites)} measured={totals['measured']} "
        f"unmeasurable={totals['unknown']} written={totals['written']} "
        f"elapsed={time.time() - started:.0f}s"
    )
    # Never fail the maintenance run over a backfill; the health gate decides.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
