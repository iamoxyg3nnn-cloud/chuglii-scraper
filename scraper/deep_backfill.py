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

TARGETS = {
    "chinese": {"pipeline": "scraper.categories.chinese.pipeline", "site_key": "uxj838238", "label": "Chinese (uxj838238)"},
    "fangsung": {"pipeline": "scraper.categories.fangsung.pipeline", "site_key": "fangsung", "label": "Fangsung (www.fangsung.com)"},
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


def set_flag(targets: List[str], active: bool = True) -> bool:
    from scraper.db import call_supabase_rpc
    state = {
        "active": active,
        "targets": sorted(targets),
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    data = call_supabase_rpc("scraper_set_checkpoint", {"p_key": FLAG_KEY, "p_state": state})
    return isinstance(data, dict) and data.get("success") is True


def _load_target(target: str):
    cfg = TARGETS[target]
    mod = importlib.import_module(cfg["pipeline"])
    return cfg, mod


def collect_zone_states(target: str) -> List[Dict[str, Any]]:
    """Snapshot of every zone's cursor for one target (no crawling)."""
    _, mod = _load_target(target)
    checkpoints = mod.load_checkpoints()
    prefix = f"{TARGETS[target]['site_key']}:"
    zones: List[Dict[str, Any]] = []
    for term in mod.DEFAULT_CATEGORIES:
        key = f"{prefix}{term}"
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
        site_key = cfg["site_key"]
        checkpoints = mod.load_checkpoints()
        known_ids = mod.load_known_ids()
        progress = mod.load_progress()
        prefix = f"{site_key}:"
        for term in mod.DEFAULT_CATEGORIES:
            checkpoints.setdefault(f"{prefix}{term}", {"current_page": 1, "max_pages": 50})

        zone_keys = sorted(k for k in checkpoints if k.startswith(prefix))
        pending = []
        for key in zone_keys:
            state = checkpoints[key]
            cur = int(state.get("current_page", 1) or 1)
            mx = int(state.get("max_pages", 50) or 50)
            if cur <= mx:
                pending.append(key)
        pages_left = sum(
            max(0, int(checkpoints[k].get("max_pages", 50) or 50) - int(checkpoints[k].get("current_page", 1) or 1) + 1)
            for k in pending
        )
        print(f"[DEEP] [{cfg['label']}] zones pending: {len(pending)} | pages remaining: {pages_left}")

        crawled_pages = 0
        for key in pending:
            term = key.split(":", 1)[1]
            while time.time() < deadline - min_chunk_seconds:
                state = checkpoints[key]
                cur = int(state.get("current_page", 1) or 1)
                mx = int(state.get("max_pages", 50) or 50)
                if cur > mx:
                    break
                chunk = min(chunk_pages, mx - cur + 1)
                print(f"[DEEP] [{cfg['label']}] {term}: pages {cur}-{cur + chunk - 1} of {mx}")
                try:
                    stats = mod.run_category_crawl(
                        site_key=site_key,
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
                if not stats.get("upsert_failed") and crawled > 0:
                    state["current_page"] = cur + crawled
                    crawled_pages += crawled
                detected = stats.get("detected_max_pages")
                if isinstance(detected, int) and detected > 0:
                    state["max_pages"] = max(int(state.get("max_pages", 0) or 0), detected)
                checkpoints[key] = state
                mod.save_known_ids(known_ids)
                mod.save_checkpoints(checkpoints)
                mod.save_progress(progress)

                new_cur = int(state.get("current_page", 1) or 1)
                if new_cur > int(state.get("max_pages", 50) or 50):
                    print(f"[DEEP] [{cfg['label']}] {term}: COMPLETE ({new_cur - 1} pages indexed)")
                if time.time() >= deadline - min_chunk_seconds:
                    summary["stopped_at"] = "deadline"
                    break
            if time.time() >= deadline - min_chunk_seconds:
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
