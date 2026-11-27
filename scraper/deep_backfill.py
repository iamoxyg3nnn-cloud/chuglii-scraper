"""Deep-backfill mode for the scraper fleet.

Normal fleet cycles advance every zone by only one listing page per pass —
great for freshness, far too slow to reach the end of a multi-thousand-page
archive. Deep backfill flips the fleet into "grind" mode: after each normal
cycle it keeps crawling every pending zone, chunk after chunk, until the
session deadline — and the mode persists across chained Actions sessions
(stored in the remote ``scraper.checkpoints`` store under the key
``deep_backfill``) until every zone reports complete.

Usage (CLI):
    python -X utf8 scraper/deep_backfill.py set all        # or chinese|fangsung
    python -X utf8 scraper/deep_backfill.py clear
    python -X utf8 scraper/deep_backfill.py status         # live coverage report
"""

from __future__ import annotations

import importlib
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

FLAG_KEY = "deep_backfill"

# Hard ceiling on how many crawl passes one zone may burn per deep-backfill
# call. A legitimate zone needs at most ceil(max_pages / chunk_pages) passes
# (50/20 -> 3); anything beyond this is a cursor bug and must not eat the
# session. Added after run 45742796072 looped a dead dasj91 page 13,185 times.
_MAX_ITERATIONS_PER_ZONE = 40

TARGETS = {
    "chinese": {"pipeline": "scraper.categories.chinese.pipeline", "site_key": "uxj838238", "label": "Chinese (uxj838238)"},
    "fangsung": {"pipeline": "scraper.categories.fangsung.pipeline", "site_key": "fangsung", "label": "Fangsung (www.fangsung.com)"},
    "studios": {
        "pipeline": "scraper.categories.studios.pipeline",
        "site_key": "madouer",
        "label": "Studios (madouer + dasj91)",
        # Multi-site target: explicit zone list (site_key, category) pairs.
        "zones": "LAZY:studios_zones",
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Persistent flag (survives runner loss; lives in scraper.checkpoints)
# ─────────────────────────────────────────────────────────────────────────────

def get_flag() -> Dict[str, Any]:
    from scraper.db import call_supabase_rpc
    data = call_supabase_rpc("scraper_get_checkpoint", {"p_key": FLAG_KEY})
    if isinstance(data, dict) and data.get("active"):
        return data
    return {"active": False, "targets": []}


def set_flag(targets: Optional[List[str]] = None, active: bool = True) -> bool:
    from scraper.db import call_supabase_rpc
    state = {
        "active": active,
        "targets": sorted(targets or []),
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    data = call_supabase_rpc("scraper_set_checkpoint", {"p_key": FLAG_KEY, "p_state": state})
    return isinstance(data, dict) and data.get("success") is True


def _load_target(target: str):
    cfg = TARGETS[target]
    mod = importlib.import_module(cfg["pipeline"])
    return cfg, mod


def _target_zones(target: str, mod) -> List[tuple]:
    """(site_key, term) pairs for a target. Targets with an explicit 'zones'
    config (multi-site categories) use it; single-site targets fall back to
    site_key + DEFAULT_CATEGORIES."""
    cfg = TARGETS[target]
    if cfg.get("zones") == "LAZY:studios_zones":
        from scraper.categories.studios.categories import ALL_ZONES
        return list(ALL_ZONES)
    if "zones" in cfg:
        return [(sk, term) for sk, term in cfg["zones"]]
    return [(cfg["site_key"], term) for term in mod.DEFAULT_CATEGORIES]


def collect_zone_states(target: str) -> List[Dict[str, Any]]:
    """Snapshot of every zone's cursor for one target (no crawling)."""
    _, mod = _load_target(target)
    checkpoints = mod.load_checkpoints()
    zones: List[Dict[str, Any]] = []
    for site_key, term in _target_zones(target, mod):
        key = f"{site_key}:{term}"
        state = checkpoints.get(key) or {"current_page": 1, "max_pages": 50}
        cur = int(state.get("current_page", 1) or 1)
        mx = int(state.get("max_pages", 50) or 50)
        zones.append({"key": key, "term": term, "current_page": cur, "max_pages": mx})
    return zones


def coverage_report(targets: Optional[List[str]] = None) -> str:
    """Human-readable per-zone coverage report (used by `status` and logs)."""
    targets = targets or list(TARGETS)
    lines: List[str] = []
    for target in targets:
        _, mod = _load_target(target)
        label = TARGETS[target]["label"]
        try:
            zones = collect_zone_states(target)
        except Exception as exc:
            lines.append(f"[{label}] unavailable: {exc}")
            continue
        done = [z for z in zones if z["current_page"] > z["max_pages"]]
        pages_left = sum(max(0, z["max_pages"] - z["current_page"] + 1) for z in zones)
        lines.append(
            f"[{label}] zones: {len(zones)} | complete: {len(done)} | pages remaining: {pages_left}"
        )
        for z in sorted(zones, key=lambda z: -(z["max_pages"] - z["current_page"])):
            mark = "DONE" if z["current_page"] > z["max_pages"] else f"p {z['current_page']}/{z['max_pages']}"
            lines.append(f"    {z['term']:<20} {mark}")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# The grind loop
# ─────────────────────────────────────────────────────────────────────────────

def _supabase_ready(wait_seconds: int = 300) -> bool:
    """True when scrape results can actually be persisted.

    Scraping while the data plane is down burns requests, invites Cloudflare
    blocks and produces nothing, which is exactly what happened for 98 minutes
    during run 45742796072. The health answer is cached (60s) so this is cheap
    to call inside the grind loop.
    """
    try:
        from scraper.db import supabase_available, wait_for_supabase
        from scraper import db as _db
    except Exception:  # pragma: no cover - db module is always present
        return True
    # Unconfigured (local/dev) runs have nothing to persist: never pause.
    if not getattr(_db, "SUPABASE_URL", None):
        return True
    try:
        if supabase_available():
            return True
    except Exception:
        return True
    print("[DEEP] Supabase data plane unavailable; pausing crawl (results could not persist).")
    try:
        if wait_for_supabase(max_wait_seconds=wait_seconds, poll_interval=15):
            return True
    except Exception:
        return True
    return False


def backlog_pages_remaining(targets: Optional[List[str]] = None) -> int:
    """Total pages still unfetched across targets, from local checkpoints."""
    total = 0
    for target in (targets or list(TARGETS)):
        try:
            zones = collect_zone_states(target)
        except Exception:
            continue
        total += sum(
            max(0, int(z["max_pages"]) - int(z["current_page"]) + 1) for z in zones
        )
    return total


def auto_arm_if_backlog(
    min_pages: int = 200,
    targets: Optional[List[str]] = None,
) -> bool:
    """Arm deep backfill automatically when a real backlog exists.

    Deep mode used to be manual-only, which meant the archive grind stalled
    until somebody remembered to run the CLI. It is now self-arming, but only
    from code that also enforces a per-session budget (see
    ``DEEP_SESSION_FRACTION`` in unified_scraper) and the stall/iteration
    guards in :func:`run_deep_backfill`, so arming can no longer starve feeds.

    Returns True only when the flag was just turned on.
    """
    try:
        flag = get_flag()
    except Exception:
        return False
    if flag.get("active"):
        return False                       # already grinding; leave it alone

    remaining = backlog_pages_remaining(targets)
    if remaining < max(1, int(min_pages)):
        return False

    chosen = sorted(targets or TARGETS)
    try:
        ok = set_flag(targets=chosen, active=True)
    except Exception:
        return False
    if ok:
        print(
            f"[DEEP] Auto-armed deep backfill for {', '.join(chosen)} "
            f"({remaining} pages of backlog >= {min_pages} threshold)."
        )
    return bool(ok)


def run_deep_backfill(
    targets: List[str],
    deadline: float,
    chunk_pages: int = 20,
    jitter: float = 0.6,
    min_chunk_seconds: float = 120.0,
) -> Dict[str, Any]:
    """Crawls every pending zone of every target, chunk by chunk, until either
    everything is complete or ``deadline`` (unix time) is reached.

    Checkpoints are saved after every chunk, so a killed session loses at most
    one chunk of cursor progress (dedupe makes re-crawls cheap anyway).
    """
    summary: Dict[str, Any] = {"targets": {}, "stopped_at": None}
    active_targets = [t for t in targets if t in TARGETS]

    for target in active_targets:
        cfg, mod = _load_target(target)
        checkpoints = mod.load_checkpoints()
        known_ids = mod.load_known_ids()
        progress = mod.load_progress()
        zones = _target_zones(target, mod)
        for zsite, zterm in zones:
            checkpoints.setdefault(f"{zsite}:{zterm}", {"current_page": 1, "max_pages": 50})

        zone_keys = sorted(f"{sk}:{t}" for sk, t in zones)
        pending = []
        for key in zone_keys:
            state = checkpoints[key]
            cur = int(state.get("current_page", 1) or 1)
            mx = int(state.get("max_pages", 50) or 50)
            if cur <= mx:
                pending.append(key)

        # CHEAPEST FIRST. The old order was the alphabetical checkpoint key, so
        # every pass started at 19920497 (the 5,743-page magnet zone) and the
        # session deadline always hit before the loop reached another zone:
        # 'zones pending: 37' never shrank, and 12,000+ pages of smaller zones
        # were never touched. Sorting by remaining pages retires the small
        # archives first, so the pending list shrinks every session and the
        # giant zone is ground last instead of starving everything behind it.
        def _remaining(key: str) -> int:
            st = checkpoints[key]
            return max(
                0,
                int(st.get("max_pages", 50) or 50)
                - int(st.get("current_page", 1) or 1)
                + 1,
            )

        pending.sort(key=_remaining)
        for key in pending:
            checkpoints[key]["remaining_pages"] = _remaining(key)
        pages_left = sum(_remaining(k) for k in pending)
        print(f"[DEEP] [{cfg['label']}] zones pending: {len(pending)} | pages remaining: {pages_left}")
        if pending:
            preview = ", ".join(f"{k}={_remaining(k)}" for k in pending[:5])
            print(f"[DEEP] [{cfg['label']}] pass order (fewest pages first): {preview}")

        crawled_pages = 0
        stalls: Dict[str, int] = {}
        offline = False
        for key in pending:
            if offline:
                break
            zsite, term = key.split(":", 1)
            iterations = 0
            while time.time() < deadline - min_chunk_seconds:
                iterations += 1
                if not _supabase_ready():
                    offline = True
                    summary["stopped_at"] = "supabase_offline"
                    print("[DEEP] Stopping pass: Supabase is offline, so nothing would persist.")
                    break
                if iterations > _MAX_ITERATIONS_PER_ZONE:
                    # Belt and braces: no cursor bug may ever spin a whole session.
                    print(
                        f"[DEEP] [{cfg['label']}] {term}: hit {_MAX_ITERATIONS_PER_ZONE} "
                        "iteration cap; abandoning zone this pass."
                    )
                    break
                state = checkpoints[key]
                cur = int(state.get("current_page", 1) or 1)
                mx = int(state.get("max_pages", 50) or 50)
                if cur > mx:
                    break
                chunk = min(chunk_pages, mx - cur + 1)
                print(f"[DEEP] [{cfg['label']}] {term}: pages {cur}-{cur + chunk - 1} of {mx}")
                try:
                    stats = mod.run_category_crawl(
                        site_key=zsite,
                        category_id=term,
                        start_page=cur,
                        max_pages=chunk,
                        known_ids=known_ids,
                        dry_run=False,
                        jitter=jitter,
                        progress=progress,
                    )
                except Exception as exc:
                    print(f"[DEEP] [{cfg['label']}] {term}: chunk failed, skipping zone this pass: {exc}")
                    break
                crawled = int(stats.get("pages_crawled", 0) or 0)
                true_max = stats.get("true_max_pages")
                detected = stats.get("detected_max_pages")
                progressed = False
                if not stats.get("upsert_failed"):
                    if isinstance(true_max, int) and true_max >= 0:
                        # The listing genuinely ended at true_max (empty page /
                        # 404 past the real end). Pin the cursor past it so the
                        # zone reads as COMPLETE instead of re-crawling a dead
                        # page forever - the site's advertised total is inflated.
                        state["max_pages"] = max(1, true_max)
                        state["current_page"] = max(2, true_max + 1)
                        crawled_pages += crawled
                        progressed = True
                    elif crawled > 0:
                        state["current_page"] = cur + crawled
                        crawled_pages += crawled
                        progressed = True
                if isinstance(detected, int) and detected > 0:
                    # Never let an inflated advertised total pull the cursor back.
                    state["max_pages"] = max(
                        int(state.get("max_pages", 0) or 0), detected
                    )
                checkpoints[key] = state
                mod.save_known_ids(known_ids)
                mod.save_checkpoints(checkpoints)
                mod.save_progress(progress)

                new_cur = int(state.get("current_page", 1) or 1)
                if new_cur > int(state.get("max_pages", 50) or 50):
                    print(f"[DEEP] [{cfg['label']}] {term}: COMPLETE ({new_cur - 1} pages indexed)")
                if not progressed:
                    # No page was indexed and nothing advanced: one dead zone
                    # must never starve the rest of the backlog for the session.
                    stall = stalls.get(key, 0) + 1
                    stalls[key] = stall
                    if stall >= 2:
                        print(
                            f"[DEEP] [{cfg['label']}] {term}: no progress after "
                            f"{stall} attempts; abandoning zone this pass."
                        )
                        break
                else:
                    stalls[key] = 0
                if time.time() >= deadline - min_chunk_seconds:
                    summary["stopped_at"] = "deadline"
                    break
            if time.time() >= deadline - min_chunk_seconds:
                break
            if offline:
                break

        # Re-snapshot: is this target fully covered now?
        zones = collect_zone_states(target)
        remaining = sum(max(0, z["max_pages"] - z["current_page"] + 1) for z in zones)
        summary["targets"][target] = {
            "pages_crawled": crawled_pages,
            "pages_remaining": remaining,
            "complete": remaining <= 0,
        }
        if time.time() >= deadline - min_chunk_seconds:
            break

    # Completion: every target fully covered → clear the persistent flag so
    # the fleet returns to normal freshness-only operation.
    all_done = all(info.get("complete") for info in summary["targets"].values()) and bool(summary["targets"])
    if all_done and time.time() < deadline - min_chunk_seconds:
        if set_flag(active=False):
            print("[DEEP] All targets fully indexed. Deep-backfill mode deactivated.")
            summary["completed"] = True
    return summary


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    import sys

    if len(sys.argv) < 2 or sys.argv[1] not in {"set", "clear", "status"}:
        print(__doc__)
        raise SystemExit(2)
    cmd = sys.argv[1]

    if cmd == "status":
        flag = get_flag()
        print(f"Deep backfill active: {flag.get('active')}")
        print(f"Targets: {', '.join(flag.get('targets') or []) or '-'}")
        targets = flag.get("targets") if flag.get("active") else None
        print(coverage_report(targets or list(TARGETS)))
        return

    if cmd == "clear":
        ok = set_flag(active=False)
        print("Deep backfill flag cleared." if ok else "Failed to clear flag (Supabase error?).")
        raise SystemExit(0 if ok else 1)

    # set
    raw = (sys.argv[2] if len(sys.argv) > 2 else "all").strip().lower()
    if raw == "all":
        targets = sorted(TARGETS)
    else:
        targets = [t.strip() for t in raw.split(",") if t.strip() in TARGETS]
    if not targets:
        print(f"No valid targets in '{raw}'. Valid: {', '.join(TARGETS)} or 'all'.")
        raise SystemExit(2)
    if set_flag(targets=targets, active=True):
        print(f"Deep backfill ACTIVATED for: {', '.join(targets)}")
        print("The fleet will grind these archives every session until complete,")
        print("then deactivate itself automatically.")
    else:
        print("Failed to set flag (Supabase error?).")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
