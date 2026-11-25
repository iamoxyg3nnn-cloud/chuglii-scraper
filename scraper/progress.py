"""Centralised, Supabase-backed progress + deduplication state for every scraper.

Why this exists
---------------
Before this module every website kept its cursor in one place and its known
post IDs in another:

* ``scraper.checkpoints`` (Supabase) held cursors for MMS / SupJav / Myanmar;
* OnlyFans and Voyeur House wrote cursors to local ``checkpoint.json`` files;
* known post IDs were written to local ``known_post_ids.json`` files only;
* ``progress.json`` was local-only and was never updated by the unified daemon.

A fresh GitHub runner (or a cancelled run) therefore restarted from page 1,
re-scraped thousands of pages and could permanently skip IDs whose upsert had
failed.  This module makes Supabase the source of truth for all three kinds of
state while keeping local JSON files as atomic caches for the workflow's
``git add`` step.

All public functions are defensive: a Supabase outage degrades to the local
cache, never to an empty state that would silently restart a backfill.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Set

from scraper.db import (
    add_scraper_known_post_ids,
    get_scraper_checkpoint,
    get_scraper_known_post_ids,
    set_scraper_checkpoint,
)

_CHUNK_SIZE = 500


# ─────────────────────────────────────────────────────────────────────────────
# Atomic local cache helpers
# ─────────────────────────────────────────────────────────────────────────────

def read_json_file(path: Optional[Path], default: Any) -> Any:
    """Reads a JSON cache without ever raising on corrupt/missing files."""
    if not path:
        return default
    try:
        text = Path(path).read_text(encoding="utf-8")
        value = json.loads(text)
        return value if value is not None else default
    except (OSError, ValueError, TypeError):
        return default


def write_json_file(path: Optional[Path], value: Any) -> bool:
    """Atomically replaces a JSON cache (temp file + os.replace)."""
    if not path:
        return False
    path = Path(path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(value, handle, indent=2, ensure_ascii=False, sort_keys=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, path)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        return True
    except (OSError, TypeError, ValueError) as exc:
        print(f"[!] Failed to write state cache {path}: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Checkpoints (cursors)
# ─────────────────────────────────────────────────────────────────────────────

def load_checkpoint(
    key: str,
    local_file: Optional[Path] = None,
    defaults: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Loads a checkpoint: Supabase first, local atomic cache second.

    Remote values win so a fresh runner always resumes where the daemon left
    off.  The merged result is written back to the local cache so the existing
    workflow's ``git add`` step still sees a valid file.
    """
    state: Dict[str, Any] = dict(defaults or {})
    local = read_json_file(local_file, {})
    if isinstance(local, dict):
        state.update(local)

    try:
        remote = get_scraper_checkpoint(key)
    except Exception as exc:  # pragma: no cover - network failure
        print(f"[!] Could not load remote checkpoint '{key}': {exc}")
        remote = {}

    if isinstance(remote, dict) and remote:
        state.update(remote)
        write_json_file(local_file, state)
    return state


def save_checkpoint(key: str, state: Dict[str, Any], local_file: Optional[Path] = None) -> bool:
    """Persists a checkpoint remotely and mirrors it locally.

    Returns True only when Supabase accepted the write; callers use this to
    decide whether it is safe to keep advancing a cursor.
    """
    write_json_file(local_file, state)
    try:
        ok = bool(set_scraper_checkpoint(key, state))
    except Exception as exc:  # pragma: no cover - network failure
        print(f"[!] Could not save remote checkpoint '{key}': {exc}")
        ok = False
    if not ok:
        print(f"[!] Checkpoint '{key}' is only cached locally; remote sync must be retried.")
    return ok


# ─────────────────────────────────────────────────────────────────────────────
# Known post IDs (deduplication)
# ─────────────────────────────────────────────────────────────────────────────

def _normalise_local_ids(local_file: Optional[Path], site: str, prefix: bool) -> Set[str]:
    raw = read_json_file(local_file, [])
    if not isinstance(raw, list):
        return set()
    ids: Set[str] = set()
    for item in raw:
        value = str(item).strip()
        if not value:
            continue
        if prefix:
            if ":" not in value:
                value = f"{site}:{value}"
        ids.add(value)
    return ids


def load_known_ids(
    site: str,
    local_file: Optional[Path] = None,
    *,
    prefix: bool = False,
    limit: int = 200000,
) -> Set[str]:
    """Loads known IDs from the local cache and the central Supabase ledger.

    ``prefix=True`` is used by the multi-site OnlyFans / Voyeur House pipelines
    whose in-memory keys are ``site:post_id``.  The central ledger always holds
    bare post IDs, so the prefix is added only on read and stripped on write.
    """
    site_key = str(site or "").strip().lower()
    ids = _normalise_local_ids(local_file, site_key, prefix)
    if not site_key:
        return ids

    try:
        remote = get_scraper_known_post_ids(site_key, limit=limit)
    except Exception as exc:  # pragma: no cover - network failure
        print(f"[!] Could not load remote known IDs for '{site_key}': {exc}")
        remote = set()

    for item in remote or ():
        value = str(item).strip()
        if not value:
            continue
        ids.add(f"{site_key}:{value}" if prefix else value)
    return ids


def mark_known_ids(
    ids: Iterable[str],
    *,
    default_site: str = "",
    status: str = "stored",
) -> int:
    """Writes newly-confirmed IDs to the central ledger, grouped by site.

    Only call this *after* a successful upsert (or for an ID confirmed to be a
    dead/photo page).  ``status='seen'`` is used for IDs that were evaluated but
    intentionally not stored; ``status='stored'`` means the row exists in the
    database.
    """
    grouped: Dict[str, Set[str]] = {}
    for raw in ids:
        value = str(raw or "").strip()
        if not value:
            continue
        if ":" in value:
            site_key, post_id = value.split(":", 1)
            site_key = site_key.strip().lower()
        else:
            site_key = str(default_site or "").strip().lower()
            post_id = value
        post_id = post_id.strip()
        if not site_key or not post_id:
            continue
        grouped.setdefault(site_key, set()).add(post_id)

    written = 0
    for site_key, post_ids in grouped.items():
        ordered = sorted(post_ids)
        for i in range(0, len(ordered), _CHUNK_SIZE):
            chunk = ordered[i:i + _CHUNK_SIZE]
            try:
                result = add_scraper_known_post_ids(site_key, chunk, status=status)
                if isinstance(result, dict) and result.get("success") is False:
                    raise RuntimeError(str(result.get("error") or result))
                written += len(chunk)
            except Exception as exc:  # pragma: no cover - network failure
                print(f"[!] Could not record {len(chunk)} known IDs for '{site_key}': {exc}")
    return written


def save_known_ids(local_file: Optional[Path], ids: Iterable[str]) -> bool:
    """Mirrors the full in-memory ID set to its local atomic cache."""
    return write_json_file(local_file, sorted({str(x) for x in ids if str(x).strip()}))


# ─────────────────────────────────────────────────────────────────────────────
# Per-run progress (the old local-only progress.json files)
# ─────────────────────────────────────────────────────────────────────────────

def load_progress(
    key: str,
    local_file: Optional[Path] = None,
    defaults: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Loads per-run progress (status, per-site stats) from Supabase + local."""
    return load_checkpoint(key, local_file, defaults)


def save_progress(key: str, progress: Dict[str, Any], local_file: Optional[Path] = None) -> bool:
    """Persists per-run progress under its own checkpoint key."""
    return save_checkpoint(key, progress, local_file)


def cursor_retry_state(
    *,
    stall_count: int = 0,
    unresolved: int = 0,
    max_stalls: int = 5,
) -> tuple[int, bool]:
    """Returns ``(next_stall_count, hold_cursor)`` for unresolved items.

    A page with unresolved items is retried for up to ``max_stalls`` cycles,
    then released so a permanently broken page cannot block the backfill.
    """
    if int(unresolved or 0) <= 0:
        return 0, False
    stall = int(stall_count or 0) + 1
    return stall, stall < max(1, max_stalls)


def finalize_status(progress: Dict[str, Any], failed_sites: Iterable[str]) -> str:
    """Derives an honest global status from the per-site outcomes."""
    failures = list(failed_sites)
    sites = progress.get("sites")
    if not isinstance(sites, dict):
        sites = {}
    if isinstance(sites, dict):
        failures.extend(
            site for site, state in sites.items()
            if isinstance(state, dict) and state.get("status") == "failed"
        )
    partial = [
        site for site, state in sites.items()
        if isinstance(state, dict) and state.get("status") == "partial"
    ]
    if failures:
        status = "failed"
    elif partial:
        status = "partial"
    else:
        status = "completed"
    progress["status"] = status
    progress["failed_sites"] = sorted(set(failures))
    progress["degraded_sites"] = sorted(set(partial))
    return status
