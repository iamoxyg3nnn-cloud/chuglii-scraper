"""Layer 2 of the onlyfans dead-video gate: nightly revalidation.

Layer 1 (``stream_check.verify_stream`` at ingest) stops a dead video from being
stored. Layer 2 exists because streams die *after* ingest: uncut-x's catalogue
lost 3,080 Cloudflare R2 objects, and every one of those rows was ingestible
before the gate existed. Nothing at ingest time can predict that.

What it does
------------
1. Pulls the least-recently-verified live rows per site
   (``scraper_onlyfans_revalidation_batch``).
2. Re-probes each stream with the same strict gate used at ingest.
3. Stamps survivors with ``last_verified_at`` and soft-deletes the rest
   (``status='dead'``), so they vanish from the public feed without losing the
   row (DMCA / re-enable stay addressable).

Run it nightly, e.g. ``python -X utf8 scraper/categories/onlyfans/verify_dead.py --limit 200``.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Allow `python scraper/categories/onlyfans/verify_dead.py` (the workflow calls
# it that way, from the repo root) without requiring -m.
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scraper.categories.onlyfans.stream_check import verify_stream  # noqa: E402

DEFAULT_MAX_AGE_DAYS = 7
DEFAULT_LIMIT = 200
DEFAULT_WORKERS = 5

#: Sites whose streams expire and are worth re-checking, with the referer their
#: CDN demands. Sources not listed here are skipped (their media is embedded and
#: liveness is decided at play time).
SITE_REFERERS = {
    "borntobefuck": "https://borntobefuck.com/watch/{post_id}",
    "uncutx": None,
}


def referer_for(site: str, post_id: str) -> Optional[str]:
    """The Referer a site's CDN requires, or None."""
    template = SITE_REFERERS.get(site, "missing")
    if not template:
        return None
    if template == "missing":
        return None
    return template.format(post_id=post_id)


def kind_for(url: str) -> str:
    lowered = (url or "").lower()
    if ".m3u8" in lowered:
        return "hls"
    return "mp4"


def fetch_batch(site: str, limit: int, max_age_days: int) -> List[Dict[str, Any]]:
    """Rows due a re-check, least recently verified first."""
    from scraper.db import call_supabase_rpc

    data = call_supabase_rpc(
        "scraper_onlyfans_revalidation_batch",
        {"p_site": site, "p_limit": int(limit), "p_max_age_days": int(max_age_days)},
        retries=2,
        timeout=30.0,
    )
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def mark(site: str, alive_ids: Iterable[str], dead_ids: Iterable[str]) -> Dict[str, Any]:
    """Persist the verdicts. Returns the RPC's counts."""
    from scraper.db import call_supabase_rpc

    alive = sorted({str(i) for i in alive_ids if i})
    dead = sorted({str(i) for i in dead_ids if i})
    if not alive and not dead:
        return {"success": True, "alive": 0, "dead": 0}
    data = call_supabase_rpc(
        "scraper_onlyfans_mark_verified",
        {"p_site": site, "p_alive_ids": alive, "p_dead_ids": dead},
        retries=2,
        timeout=30.0,
    )
    return data if isinstance(data, dict) else {"success": False, "error": str(data)}


def probe_row(site: str, row: Dict[str, Any]) -> Tuple[str, bool, int, str]:
    """(post_id, alive, http_status, detail) for one row. Never raises."""
    post_id = str(row.get("source_post_id") or "")
    url = str(row.get("full_video_url") or "")
    if not post_id or not url:
        return post_id, False, 0, "missing id or url"
    try:
        alive, status, detail = verify_stream(
            url,
            kind=kind_for(url),
            referer=referer_for(site, post_id),
            source_site=site,
            source_post_id=post_id,
            record=False,          # last_verified_at is the authority here
        )
        return post_id, bool(alive), int(status or 0), detail
    except Exception as exc:        # pragma: no cover - verify_stream swallows
        return post_id, False, 0, f"{type(exc).__name__}: {exc}"


def revalidate_site(
    site: str,
    *,
    limit: int = DEFAULT_LIMIT,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
    workers: int = DEFAULT_WORKERS,
    dry_run: bool = False,
) -> Dict[str, Any]:
    rows = fetch_batch(site, limit, max_age_days)
    if not rows:
        print(f"  [{site}] nothing due for revalidation.")
        return {"site": site, "checked": 0, "alive": 0, "dead": 0, "errors": 0}

    alive_ids: List[str] = []
    dead_ids: List[str] = []
    errors = 0
    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
        for post_id, alive, status, detail in pool.map(lambda r: probe_row(site, r), rows):
            if alive:
                alive_ids.append(post_id)
            else:
                dead_ids.append(post_id)
                if status == 0:
                    errors += 1
                print(f"      [{site}] DEAD {post_id} (HTTP {status}: {detail})")

    if dry_run:
        print(f"  [{site}] dry-run: would mark {len(alive_ids)} alive, {len(dead_ids)} dead.")
        result = {"success": True, "alive": len(alive_ids), "dead": len(dead_ids)}
    else:
        result = mark(site, alive_ids, dead_ids)

    print(
        f"  [{site}] checked {len(rows)} | alive {len(alive_ids)} | "
        f"dead {len(dead_ids)} | transport errors {errors} | rpc={result}"
    )
    return {
        "site": site,
        "checked": len(rows),
        "alive": len(alive_ids),
        "dead": len(dead_ids),
        "errors": errors,
        "rpc": result,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Revalidate OnlyFans stream liveness")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="rows per site")
    parser.add_argument("--max-age-days", type=int, default=DEFAULT_MAX_AGE_DAYS)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--sites", default="", help="comma list (default: all revalidatable)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    sites = [s.strip() for s in args.sites.split(",") if s.strip()] or list(SITE_REFERERS)
    started = time.time()
    summaries = []
    failures = 0

    for site in sites:
        try:
            summaries.append(
                revalidate_site(
                    site,
                    limit=args.limit,
                    max_age_days=args.max_age_days,
                    workers=args.workers,
                    dry_run=args.dry_run,
                )
            )
        except Exception as exc:
            failures += 1
            print(f"  [!] {site} revalidation failed: {exc}")

    total_dead = sum(s.get("dead", 0) for s in summaries)
    print(
        f"\n[revalidate] sites={len(summaries)} checked={sum(s.get('checked', 0) for s in summaries)} "
        f"dead={total_dead} elapsed={time.time() - started:.0f}s"
    )
    print(json.dumps(summaries, default=str)[:500])
    # A failing site must not fail the whole run: the next night retries.
    return 1 if failures and not summaries else 0


if __name__ == "__main__":
    raise SystemExit(main())
