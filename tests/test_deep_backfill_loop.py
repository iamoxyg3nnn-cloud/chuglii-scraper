"""Regression tests for the deep-backfill grind loop.

Reproduces the 2026-11-26 production incident (run 45742796072): a zone whose
real listing end (page 19) is behind the advertised total (50 pages) returned
`pages_crawled=0` + `true_max_pages=19`. The cursor never advanced past the
dead page 20, so `run_deep_backfill` re-crawled the same 404 page 13,185 times
for 3h20m and starved every regular feed cycle.

Also covers `deep_backfill clear`, which was un-callable because `set_flag`
required a `targets` argument, so the persistent flag could never be turned off.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper import deep_backfill  # noqa: E402


class FakePipeline:
    """Minimal stand-in for scraper.categories.studios.pipeline."""

    def __init__(self, responses):
        self.responses = responses          # list of stats dicts, consumed in order
        self.calls = []
        self.checkpoints = {}
        self.known_ids = {}
        self.progress = {}

    def run_category_crawl(self, **kwargs):
        self.calls.append(kwargs)
        idx = min(len(self.calls) - 1, len(self.responses) - 1)
        return self.responses[idx]

    def load_checkpoints(self):
        return self.checkpoints

    def load_known_ids(self):
        return self.known_ids

    def load_progress(self):
        return self.progress

    def save_checkpoints(self, cp):
        self.checkpoints = cp

    def save_known_ids(self, ids):
        self.known_ids = ids

    def save_progress(self, p):
        self.progress = p


@pytest.fixture
def fake_target(monkeypatch):
    """Wire run_deep_backfill to one synthetic deadline-proof target."""
    # Never let a test touch the live Supabase health probe.
    monkeypatch.setattr(deep_backfill, "_supabase_ready", lambda *a, **k: True)

    def _install(responses, initial=None):
        mod = FakePipeline(responses)
        mod.checkpoints = initial or {"dasj91:aifl": {"current_page": 20, "max_pages": 50}}
        monkeypatch.setitem(
            deep_backfill.TARGETS,
            "fake",
            {
                "pipeline": "scraper.categories.studios.pipeline",
                "site_key": "dasj91",
                "label": "Fake",
                "zones": [("dasj91", "aifl")],
            },
        )
        monkeypatch.setattr(deep_backfill, "_load_target", lambda t: (deep_backfill.TARGETS["fake"], mod))
        monkeypatch.setattr(deep_backfill.time, "sleep", lambda s: None)
        return mod
    return _install


def test_true_end_pins_cursor_and_completes_zone(fake_target):
    """A dead page past the real end must complete the zone, not loop."""
    mod = fake_target(
        [{"pages_crawled": 0, "true_max_pages": 19, "items_found": 0, "upserted": 0}]
    )
    summary = deep_backfill.run_deep_backfill(
        targets=["fake"], deadline=time.time() + 600, min_chunk_seconds=1
    )

    assert len(mod.calls) == 1, f"expected a single crawl, got {len(mod.calls)} (infinite loop)"
    state = mod.checkpoints["dasj91:aifl"]
    assert state["max_pages"] == 19
    assert state["current_page"] == 20
    assert summary["targets"]["fake"]["complete"] is True


def test_empty_category_is_marked_complete(fake_target):
    """true_max_pages=0 (nothing on page 1) must complete, never divide by 0."""
    mod = fake_target([{"pages_crawled": 0, "true_max_pages": 0}])
    deep_backfill.run_deep_backfill(
        targets=["fake"],
        deadline=time.time() + 600,
        min_chunk_seconds=1,
        chunk_pages=1,
    )
    assert len(mod.calls) == 1
    state = mod.checkpoints["dasj91:aifl"]
    assert state["current_page"] > state["max_pages"]


def test_progress_advances_cursor(fake_target):
    mod = fake_target([{"pages_crawled": 5, "detected_max_pages": 40}, {"pages_crawled": 0}])
    deep_backfill.run_deep_backfill(
        targets=["fake"], deadline=time.time() + 600, min_chunk_seconds=1, chunk_pages=5
    )
    assert mod.checkpoints["dasj91:aifl"]["current_page"] == 25


def test_silent_zone_is_abandoned_after_two_stalls(fake_target):
    """No progress and no end signal must break out, not spin for the session."""
    mod = fake_target([{"pages_crawled": 0, "upsert_failed": False}])
    deep_backfill.run_deep_backfill(
        targets=["fake"], deadline=time.time() + 600, min_chunk_seconds=1
    )
    assert len(mod.calls) == 2, f"stall guard did not trip, {len(mod.calls)} calls"


def test_upsert_failure_does_not_advance_cursor(fake_target):
    mod = fake_target([{"pages_crawled": 5, "upsert_failed": True}])
    deep_backfill.run_deep_backfill(
        targets=["fake"], deadline=time.time() + 600, min_chunk_seconds=1, chunk_pages=5
    )
    assert mod.checkpoints["dasj91:aifl"]["current_page"] == 20


def test_offline_supabase_stops_the_pass_without_crawling(fake_target):
    """A dead data plane must stop the pass, not scrape rows that cannot persist."""
    mod = fake_target([{"pages_crawled": 5}])
    deep_backfill._supabase_ready = lambda *a, **k: False
    try:
        summary = deep_backfill.run_deep_backfill(
            targets=["fake"], deadline=time.time() + 600, min_chunk_seconds=1
        )
    finally:
        deep_backfill._supabase_ready = lambda *a, **k: True

    assert mod.calls == [], "crawl ran while Supabase was offline"
    assert summary["stopped_at"] == "supabase_offline"


def test_auto_arm_engages_only_with_real_backlog(monkeypatch):
    """Deep mode now re-arms itself, but never on a trivial backlog."""
    state = {"flag": {"active": False, "targets": []}, "set": []}

    monkeypatch.setattr(deep_backfill, "get_flag", lambda: dict(state["flag"]))
    monkeypatch.setattr(
        deep_backfill,
        "set_flag",
        lambda targets=None, active=True: state["set"].append((sorted(targets or []), active)) or True,
    )

    monkeypatch.setattr(deep_backfill, "backlog_pages_remaining", lambda targets=None: 50)
    assert deep_backfill.auto_arm_if_backlog(min_pages=200) is False
    assert state["set"] == [], "armed on a 50-page backlog"

    monkeypatch.setattr(deep_backfill, "backlog_pages_remaining", lambda targets=None: 31962)
    assert deep_backfill.auto_arm_if_backlog(min_pages=200) is True
    assert state["set"] and state["set"][0][1] is True


def test_auto_arm_leaves_an_active_flag_alone(monkeypatch):
    monkeypatch.setattr(deep_backfill, "get_flag", lambda: {"active": True, "targets": ["studios"]})
    monkeypatch.setattr(deep_backfill, "set_flag", lambda *a, **k: pytest.fail("re-armed an active flag"))
    monkeypatch.setattr(deep_backfill, "backlog_pages_remaining", lambda targets=None: 999999)
    assert deep_backfill.auto_arm_if_backlog() is False


def test_auto_arm_survives_flag_backend_failure(monkeypatch):
    def boom():
        raise RuntimeError("supabase down")

    monkeypatch.setattr(deep_backfill, "get_flag", boom)
    assert deep_backfill.auto_arm_if_backlog() is False


def test_clear_flag_does_not_require_targets(monkeypatch):
    """`deep_backfill clear` called set_flag(active=False) and raised TypeError."""
    captured = {}

    def fake_rpc(name, payload):
        captured.update(payload)
        return {"success": True}

    monkeypatch.setattr("scraper.db.call_supabase_rpc", fake_rpc)

    assert deep_backfill.set_flag(active=False) is True
    assert captured["p_state"]["active"] is False
    assert captured["p_state"]["targets"] == []
