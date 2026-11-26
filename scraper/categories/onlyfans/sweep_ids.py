"""P6: sweep the borntobefuck id space that listings never reach.

``/videos`` is a rolling ~1-month window (597 ids) and ``?sort=oldest`` is a
separate window (961 ids, one shared id), so the two listing modes together see
under 2% of the ~23k live ids. Every id from MIN_STREAMABLE_ID up is
addressable directly, which is the only way to reach the archive.

Cost control
------------
The cursor lives in Supabase (``progress.save_checkpoint``), not on disk, so it
survives the ephemeral CI runner. Each run works for ``--minutes`` and stops
where it got to; the next run continues. A run that is killed still saves its
cursor on the ids it finished.

Correctness
------------
Nothing is stored without passing the Layer-1 gate: ``enrich_detail`` resolves
the player fragment and verifies master -> variant -> real segment bytes. Ids
that resolve to nothing are recorded as known-dead so they are never re-probed.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scraper.categories.onlyfans.sources import borntobefuck as btb  # noqa: E402

SITE = "borntobefuck"
CHECKPOINT_KEY = "onlyfans_sweep_borntobefuck"
BATCH_SIZE = 40
MAX_ID_BUFFER = 2000


def _known_max_id(known_ids: Set[str]) -> int:
    best = 0
    prefix = f"{SITE}:"
    for key in known_ids:
        if key.startswith(prefix):
            tail = key[len(prefix):]
            if tail.isdigit():
                best = max(best, int(tail))
    return best


def load_state(known_ids: Set[str], min_id: int, max_id: Optional[int]) -> Dict[str, Any]:
    """Cursor from Supabase, defaulting to the whole addressable range."""
    from scraper.progress import load_checkpoint

    defaults = {
        "next_id": int(min_id),
        "min_id": int(min_id),
        "max_id": int(max_id or (_known_max_id(known_ids) + MAX_ID_BUFFER)),
        "probed": 0,
        "ingested": 0,
        "dead": 0,
        "runs": 0,
    }
    try:
        state = load_checkpoint(CHECKPOINT_KEY, None, None)
    except Exception:
        state = None
    if not isinstance(state, dict):
        return defaults
    for key, value in defaults.items():
        state.setdefault(key, value)
    # Honour an explicit --max-id / --min-id over whatever was stored.
    if max_id:
        state["max_id"] = int(max_id)
    state["min_id"] = int(min_id)
    # A stored cursor below the floor (older plan) is clamped forward.
    state["next_id"] = max(int(state.get("next_id") or min_id), int(min_id))
    return state


def save_state(state: Dict[str, Any]) -> bool:
    from scraper.progress import save_checkpoint

    try:
        return save_checkpoint(CHECKPOINT_KEY, state)
    except Exception as exc:  # pragma: no cover - network
        print(f"  [!] could not persist sweep cursor: {exc}")
        return False


def _flush(records: List[Dict[str, Any]]) -> int:
    """Polish + upsert a batch. Returns how many rows were stored."""
    if not records:
        return 0
    from scraper.categories.onlyfans.polish import polish_onlyfans_record
    from scraper.db import sync_onlyfans_source_tables_batch, upsert_onlyfans_videos_batch

    polished = [p for p in (polish_onlyfans_record(r) for r in records) if p]
    if not polished:
        return 0
    upsert_onlyfans_videos_batch(polished)
    sync_onlyfans_source_tables_batch(polished)
    return len(polished)


def sweep(
    *,
    minutes: float = 45.0,
    min_id: Optional[int] = None,
    max_id: Optional[int] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    from scraper.categories.onlyfans.pipeline import load_known_ids, save_known_ids

    floor = int(min_id or btb.MIN_STREAMABLE_ID)
    known_ids = load_known_ids()
    state = load_state(known_ids, floor, max_id)
    deadline = time.time() + max(0.1, float(minutes)) * 60.0

    start_id = int(state["next_id"])
    stop_at = int(state["max_id"])
    batch: List[Dict[str, Any]] = []
    new_dead: Set[str] = set()
    new_live: Set[str] = set()
    stored = 0
    probed = 0
    last_processed = None      # last id handled end-to-end; cursor = it + 1
    interrupted = False

    print(
        f"[sweep] {SITE} ids {start_id}..{stop_at} | budget {minutes:.0f} min | "
        f"limit {limit or 'none'} | dry_run={dry_run}"
    )

    for vid_num in range(start_id, stop_at + 1):
        if time.time() >= deadline:
            interrupted = True
            break
        if limit is not None and probed >= int(limit):
            interrupted = True
            break

        key = f"{SITE}:{vid_num}"
        if key in known_ids:
            last_processed = vid_num
            continue

        card = btb.scrape_metadata_from_watch(str(vid_num))
        probed += 1
        if not card:
            # No watch page = nothing to ingest. Remember it so the range does
            # not cost a request on every future run.
            new_dead.add(key)
            last_processed = vid_num
            continue

        enriched = btb.enrich_detail(card)
        if enriched.get("stream_dead"):
            new_dead.add(key)
            last_processed = vid_num
            continue

        duration = btb.hls_duration(enriched.get("full_video_url") or "", referer=enriched.get("stream_referer"))
        if duration:
            enriched["duration_seconds"] = duration

        new_live.add(key)
        batch.append(enriched)

        last_processed = vid_num

        if len(batch) >= BATCH_SIZE:
            flushed = len(batch) if dry_run else _flush(batch)
            stored += flushed
            state["ingested"] = state.get("ingested", 0) + flushed
            batch = []
            state["next_id"] = (last_processed or start_id) + 1
            state["probed"] = state.get("probed", 0) + probed
            state["dead"] = state.get("dead", 0) + len(new_dead)
            probed = 0
            known_ids |= new_dead | new_live
            if not dry_run:
                if new_live:
                    save_known_ids(known_ids, new_live, status="stored")
                if new_dead:
                    save_known_ids(known_ids, new_dead, status="dead")
            new_dead = set()
            new_live = set()
            save_state(state)
            print(f"  [sweep] at id {vid_num} | stored so far {stored}")

    if batch:
        flushed = len(batch) if dry_run else _flush(batch)
        stored += flushed
        state["ingested"] = state.get("ingested", 0) + flushed
    if not dry_run:
        if new_live:
            save_known_ids(known_ids | new_live, new_live, status="stored")
        if new_dead:
            save_known_ids(known_ids | new_dead, new_dead, status="dead")

    # The cursor only moves past ids that were fully handled: if the budget runs
    # out on the very first id, nothing was handled and it must not advance.
    if last_processed is not None:
        state["next_id"] = last_processed + 1
    else:
        state["next_id"] = start_id
    state["probed"] = state.get("probed", 0) + probed
    state["dead"] = state.get("dead", 0) + len(new_dead)
    state["runs"] = state.get("runs", 0) + 1
    if not dry_run:
        save_state(state)

    summary = {
        "site": SITE,
        "cursor": state["next_id"],
        "max_id": stop_at,
        "probed": probed,
        "stored": stored,
        "dead": len(new_dead),
        "interrupted": interrupted,
        "remaining": max(0, stop_at - state["next_id"] + 1),
        "cumulative": {k: state.get(k) for k in ("probed", "ingested", "dead", "runs")},
    }
    print(
        f"[sweep] done: probed {probed} stored {stored} dead {len(new_dead)} | "
        f"cursor {state['next_id']} | remaining {summary['remaining']}"
    )
    return summary


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Sweep the borntobefuck id space")
    parser.add_argument("--minutes", type=float, default=45.0, help="wall-clock budget")
    parser.add_argument("--min-id", type=int, default=None)
    parser.add_argument("--max-id", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None, help="max ids to probe")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    sweep(
        minutes=args.minutes,
        min_id=args.min_id,
        max_id=args.max_id,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
