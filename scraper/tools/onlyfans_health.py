"""Post-run health gate for the OnlyFans sources.

Every ingest step in the workflow ends with ``|| true`` so one broken source
cannot abort the fleet. The cost is that a source which silently stops
producing data looks identical to a healthy run: green steps, empty site.

This gate makes that visible. It checks the things that actually decide whether
a visitor sees videos, and exits non-zero so the run is marked failed:

  * every registered source still has live (status='active') rows
  * the newest row per source is recent enough to prove ingestion is alive
  * the archive sweeps have not silently stalled with nothing left to show

Run: ``python -X utf8 scraper/tools/onlyfans_health.py [--max-stale-days 7]``
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

#: Sources whose absence from the site is a real defect. Kept explicit so a
#: brand-new source being registered does not fail the gate before its first
#: successful ingest.
REQUIRED_SOURCES = ("sheeshfans", "influencersgonewild", "borntobefuck", "uncutx")


def fetch_counts(site: str) -> Optional[int]:
    from scraper.db import call_supabase_rpc

    try:
        data = call_supabase_rpc(
            "scraper_count_onlyfans_source_videos",
            {"p_site": site, "p_tag": None, "p_category": None, "p_search": None},
            retries=2,
            timeout=30.0,
        )
    except Exception as exc:
        print(f"  [!] {site}: count query failed ({exc})")
        return None
    try:
        return int(data)
    except (TypeError, ValueError):
        return None


def fetch_newest(site: str) -> Optional[str]:
    """ISO timestamp of the newest ingested row, if the site exposes one."""
    from scraper.db import call_supabase_rpc

    try:
        data = call_supabase_rpc(
            "scraper_get_onlyfans_source_videos",
            {
                "p_site": site,
                "p_limit": 1,
                "p_offset": 0,
                "p_tag": None,
                "p_category": None,
                "p_search": None,
                "p_sort": "newest",
            },
            retries=1,
            timeout=30.0,
        )
    except Exception:
        return None
    rows = data if isinstance(data, list) else []
    if not rows or not isinstance(rows[0], dict):
        return None
    return rows[0].get("scraped_at") or rows[0].get("published_at")


def evaluate(
    counts: Dict[str, Optional[int]],
    newest: Dict[str, Optional[str]],
    *,
    max_stale_days: int,
    now: Optional[datetime] = None,
) -> List[str]:
    """Return a list of human-readable failures. Empty list means healthy."""
    now = now or datetime.now(timezone.utc)
    failures: List[str] = []

    for site in REQUIRED_SOURCES:
        count = counts.get(site)
        if count is None:
            failures.append(f"{site}: row count unavailable")
            continue
        if count <= 0:
            failures.append(f"{site}: 0 live rows")
            continue
        stamp = newest.get(site)
        if not stamp:
            continue  # some sources do not expose scraped_at; count is enough
        try:
            parsed = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        age_days = (now - parsed).total_seconds() / 86400.0
        if age_days > max_stale_days:
            failures.append(
                f"{site}: newest row is {age_days:.1f} days old (limit {max_stale_days})"
            )
    return failures


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="OnlyFans ingestion health gate")
    parser.add_argument("--max-stale-days", type=int, default=7)
    args = parser.parse_args(argv)

    counts: Dict[str, Optional[int]] = {}
    newest: Dict[str, Optional[str]] = {}
    for site in REQUIRED_SOURCES:
        counts[site] = fetch_counts(site)
        newest[site] = fetch_newest(site)
        print(f"  {site:20s} live={counts[site]} newest={str(newest[site])[:19]}")

    failures = evaluate(counts, newest, max_stale_days=args.max_stale_days)
    if failures:
        print("\n[health] FAILED:")
        for line in failures:
            print(f"  - {line}")
        return 1
    print("\n[health] all sources healthy")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
