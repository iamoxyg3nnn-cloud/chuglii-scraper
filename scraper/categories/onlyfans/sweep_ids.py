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
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scraper.categories.onlyfans.sources import borntobefuck as btb  # noqa: E402

SITE = "borntobefuck"
CHECKPOINT_KEY = "onlyfans_sweep_borntobefuck"
BATCH_SIZE = 40
MAX_ID_BUFFER = 2000
DEFAULT_WORKERS = 6


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


def _probe_one(vid_num: int) -> Tuple[str, Optional[Dict[str, Any]]]:
    """(known-id key, enriched record or None if the id is dead).

    Runs in a worker thread: pure reads plus the Layer-1 gate, no shared state.
    """
    key = f"{SITE}:{vid_num}"
    card = btb.scrape_metadata_from_watch(str(vid_num))
    if not card:
        return key, None
    enriched = btb.enrich_detail(card)
    if enriched.get("stream_dead"):
        return key, None
    duration = btb.hls_duration(
        enriched.get("full_video_url") or "", referer=enriched.get("stream_referer")
    )
    if duration:
        enriched["duration_seconds"] = duration
    return key, enriched

def sweep(
    *,
    minutes: float = 45.0,
    min_id: Optional[int] = None,
    max_id: Optional[int] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    workers: int = DEFAULT_WORKERS,
) -> Dict[str, Any]:
    from scraper.categories.onlyfans.pipeline import load_known_ids, save_known_ids

    floor = int(min_id or btb.MIN_STREAMABLE_ID)
    known_ids = load_known_ids()
    state = load_state(known_ids, floor, max_id)
    deadline = time.time() + max(0.1, float(minutes)) * 60.0

    start_id = int(state["next_id"])
    stop_at = int(state["max_id"])
    workers = max(1, int(workers))
    window = workers * 3

    batch: List[Dict[str, Any]] = []
    new_dead: Set[str] = set()
    new_live: Set[str] = set()
    stored = 0
    probed = 0
    dead_total = 0
    last_processed: Optional[int] = None
    interrupted = False

    print(
        f"[sweep] {SITE} ids {start_id}..{stop_at} | budget {minutes:.0f} min | "
        f"workers {workers} | limit {limit or 'none'} | dry_run={dry_run}"
    )

    def _flush_batch() -> None:
        nonlocal batch, stored
        flushed = len(batch) if dry_run else _flush(batch)
        stored += flushed
        state["ingested"] = state.get("ingested", 0) + flushed
        batch = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        window_start = start_id
        while window_start <= stop_at:
            if time.time() >= deadline:
                interrupted = True
                break
            if limit is not None and probed >= int(limit):
                interrupted = True
                break

            window_ids = list(range(window_start, min(window_start + window, stop_at + 1)))
            if limit is not None:
                window_ids = window_ids[: max(0, int(limit) - probed)]
            if not window_ids:
                interrupted = True
                break

            todo = [i for i in window_ids if f"{SITE}:{i}" not in known_ids]
            verdicts = {
                int(key.split(":")[1]): (key, record)
                for key, record in pool.map(_probe_one, todo)
            }
            probed += len(todo)

            for vid_num in window_ids:
                found = verdicts.get(vid_num)
                if found is not None:
                    key, record = found
                    if record is None:
                        new_dead.add(key)
                    else:
                        new_live.add(key)
                        batch.append(record)
                        if len(batch) >= BATCH_SIZE:
                            _flush_batch()
                last_processed = vid_num

            known_ids |= new_dead | new_live
            if not dry_run:
                if new_live:
                    save_known_ids(known_ids, new_live, status="stored")
                if new_dead:
                    save_known_ids(known_ids, new_dead, status="dead")
            # The cursor only covers ids fully handled in this window.
            state["next_id"] = (last_processed if last_processed is not None else start_id) + 1
            state["probed"] = state.get("probed", 0) + len(todo)
            state["dead"] = state.get("dead", 0) + len(new_dead)
            dead_total += len(new_dead)
            if not dry_run:
                save_state(state)
            print(
                f"  [sweep] {window_ids[0]}..{window_ids[-1]} | stored {stored} "
                f"dead {len(new_dead)} | cursor {state['next_id']}"
            )
            new_dead = set()
            new_live = set()
            window_start = window_ids[-1] + 1

    if batch:
        _flush_batch()

    state["runs"] = state.get("runs", 0) + 1
    if not dry_run:
        save_state(state)

    summary = {
        "site": SITE,
        "cursor": state["next_id"],
        "max_id": stop_at,
        "probed": probed,
        "stored": stored,
        "dead": dead_total,
        "interrupted": interrupted,
        "remaining": max(0, stop_at - state["next_id"] + 1),
        "cumulative": {k: state.get(k) for k in ("probed", "ingested", "dead", "runs")},
    }
    print(
        f"[sweep] done: probed {probed} stored {stored} dead {dead_total} | cursor {state['next_id']} | "
        f"remaining {summary['remaining']}"
    )
    return summary


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Sweep the borntobefuck id space")
    parser.add_argument("--minutes", type=float, default=45.0, help="wall-clock budget")
    parser.add_argument("--min-id", type=int, default=None)
    parser.add_argument("--max-id", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None, help="max ids to probe")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    sweep(
        minutes=args.minutes,
        min_id=args.min_id,
        max_id=args.max_id,
        limit=args.limit,
        dry_run=args.dry_run,
        workers=args.workers,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
