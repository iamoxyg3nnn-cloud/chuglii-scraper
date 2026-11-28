"""Repair the known-post-id ledger after a *site outage*.

Why this exists
---------------
``sweep_ids.py`` probes borntobefuck ids directly. While the origin was down
(run 46093407022, 2026-11-28) every watch page answered HTTP 200 with 22 bytes
of ``Internal server error.``, so no page had a title and every probe was
recorded as a **dead video**:

    2026-11-27 16:00-19:00   5,184 dead   0 stored
    2026-11-28 01:00-03:00   5,328 dead   0 stored
    (healthy hours)            ~35% dead  with thousands stored

The ledger is what ``scraper_get_known_post_ids`` returns, and it ignores
``status``: a false "dead" row permanently blocks that video from being
ingested. The sweep now refuses to write those verdicts (circuit breaker) and
never treats an unreadable page as death, but the rows written *before* that
guard have to be removed.

What it does
------------
1. ``--report``: per-hour dead/stored histogram, so an outage window is visible.
2. ``--forget-window START END``: delete ``status='dead'`` rows whose
   ``last_seen_at`` falls in the window, so the sweep re-probes them.
3. ``--forget-ids``: delete specific ids.

Dry run by default. Nothing is deleted unless ``--apply`` is passed.

Usage
-----
    python -X utf8 scraper/tools/onlyfans_ledger_repair.py --report
    python -X utf8 scraper/tools/onlyfans_ledger_repair.py \\
        --site borntobefuck --forget-window 2026-11-27T16:00Z 2026-11-27T19:00Z
    python -X utf8 scraper/tools/onlyfans_ledger_repair.py \\
        --site borntobefuck --forget-window 2026-11-27T16:00Z 2026-11-27T19:00Z --apply
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scraper.db import call_supabase_rpc  # noqa: E402

#: Windows where the origin was measurably broken (see the module docstring).
#: Only used by ``--repair-known-outages``.
KNOWN_OUTAGES = (
    ("borntobefuck", "2026-11-27T16:00:00Z", "2026-11-27T19:00:00Z"),
    ("borntobefuck", "2026-11-28T01:00:00Z", "2026-11-28T03:10:00Z"),
)


def report(site: str = "") -> List[Dict[str, Any]]:
    """Per-hour ledger histogram (from the SQL helper)."""
    data = call_supabase_rpc("scraper_known_post_ids_histogram", {"p_site": site})
    return data if isinstance(data, list) else []


def summary(site: str = "") -> Dict[str, Any]:
    data = call_supabase_rpc("scraper_known_post_ids_summary", {"p_site": site})
    return data if isinstance(data, dict) else {}


def forget(
    site: str,
    *,
    since: Optional[str] = None,
    until: Optional[str] = None,
    ids: Optional[List[str]] = None,
    status: Optional[str] = "dead",
    apply: bool = False,
) -> Dict[str, Any]:
    """Delete ledger rows so the ids can be probed again."""
    if not apply:
        return {
            "success": True,
            "dry_run": True,
            "site": site,
            "since": since,
            "until": until,
            "ids": len(ids or []),
            "status": status,
        }
    data = call_supabase_rpc(
        "scraper_forget_known_post_ids",
        {
            "p_site": site,
            "p_ids": ids or [],
            "p_from": None,
            "p_to": None,
            "p_status": status,
            "p_since": since,
            "p_until": until,
        },
    )
    return data if isinstance(data, dict) else {"success": False, "error": str(data)}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Repair the known-post-id ledger after an outage")
    parser.add_argument("--site", default="", help="site key (empty = every site)")
    parser.add_argument("--report", action="store_true", help="print the per-hour histogram and exit")
    parser.add_argument("--summary", action="store_true", help="print per-status totals and exit")
    parser.add_argument("--forget-window", nargs=2, metavar=("START", "END"), help="ISO timestamps")
    parser.add_argument("--forget-ids", nargs="+", help="explicit ids to forget")
    parser.add_argument("--status", default="dead", help="only forget rows with this status")
    parser.add_argument("--repair-known-outages", action="store_true",
                        help="forget the dead rows recorded during the known outage windows")
    parser.add_argument("--apply", action="store_true", help="actually delete (default: dry run)")
    args = parser.parse_args(argv)

    if args.report:
        for row in report(args.site):
            print(f"  {row.get('hour')}  {row.get('status'):<7} {row.get('n')}")
        return 0

    if args.summary or not (args.forget_window or args.forget_ids or args.repair_known_outages):
        print(json.dumps(summary(args.site), indent=2, default=str))
        if not (args.forget_window or args.forget_ids or args.repair_known_outages):
            return 0

    jobs: List[Dict[str, Any]] = []
    if args.forget_window:
        start, end = args.forget_window
        jobs.append({"site": args.site, "since": start, "until": end})
    if args.forget_ids:
        jobs.append({"site": args.site, "ids": list(args.forget_ids)})
    if args.repair_known_outages:
        for site, start, end in KNOWN_OUTAGES:
            jobs.append({"site": site, "since": start, "until": end})

    for job in jobs:
        result = forget(
            job["site"],
            since=job.get("since"),
            until=job.get("until"),
            ids=job.get("ids"),
            status=args.status,
            apply=args.apply,
        )
        print(json.dumps({"job": job, "result": result}, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
