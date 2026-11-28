"""Every feed must get a turn in every cycle.

Regression guard for runs 46004601681 / 46050520954: the Chinese feed started at
14:50 and was still crawling when the job timeout killed the run at 18:34, so
feeds 7 (fangsung) and 8 (studios) never ran at all, the checkpoint commit never
happened and the 24/7 self-chain stopped.

The contract pinned here:
  * a feed that overruns still cannot stop the ones behind it;
  * a feed is handed a deadline no later than its fair slice;
  * the order rotates, so no feed is permanently last.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scraper.unified_scraper as unified  # noqa: E402


@pytest.fixture()
def cycle_env(monkeypatch):
    """Stub everything a cycle touches except the feed scheduling itself."""
    monkeypatch.setattr(unified, "wait_for_supabase_before_cycle", lambda deadline=None: True)
    monkeypatch.setattr(unified, "init_known_id_caches", lambda: None)
    monkeypatch.setattr(unified, "load_japanese_checkpoint", lambda: {})
    monkeypatch.setattr(unified, "human_jitter", lambda a=0.0, b=0.0: None)

    calls = []
    deadlines = {}

    def make(name, sleep_for=0.0):
        def runner(**kwargs):
            deadlines[name] = kwargs.get("deadline")
            calls.append(name)
            if sleep_for:
                time.sleep(sleep_for)
            return 1
        return runner

    for name in unified.FEED_ROTATION:
        monkeypatch.setattr(
            unified, _runner_name(name), make(name, sleep_for=0.2 if name == "chinese" else 0.0)
        )
    monkeypatch.setitem(unified.FEED_STATUS, "x", "x")
    return calls, deadlines


def _runner_name(feed: str) -> str:
    return {
        "viral-mms": "run_viral_mms_sync",
        "viral-mms-sites": "run_viral_mms_sites_sync",
        "supjav": "run_japanese_jav_sync",
        "myanmar": "run_myanmar_sync",
        "samusar": "run_samusar_sync",
        "onlyfans": "run_onlyfans_sync",
        "voyeurhouse": "run_voyeurhouse_sync",
        "chinese": "run_chinese_sync",
        "fangsung": "run_fangsung_sync",
        "studios": "run_studios_sync",
    }[feed]


def test_every_feed_runs_in_one_cycle(cycle_env):
    calls, _ = cycle_env
    unified.run_unified_sync_cycle(
        check_head=True,
        workers=1,
        supabase_deadline=time.time() + 3600,
        cycle_rotation=0,
    )
    assert sorted(calls) == sorted(unified.FEED_ROTATION), (
        "a cycle must reach every feed; missing: "
        f"{set(unified.FEED_ROTATION) - set(calls)}"
    )


def test_a_slow_feed_cannot_starve_the_ones_behind_it(cycle_env):
    """Every feed starts even when the budget per feed is smaller than the floor."""
    calls, deadlines = cycle_env
    unified.run_unified_sync_cycle(
        check_head=True,
        workers=1,
        supabase_deadline=time.time() + 600,      # 10 min across 10 feeds
        cycle_rotation=0,
    )
    assert sorted(calls) == sorted(unified.FEED_ROTATION)
    # Every feed was handed a real (future) deadline, not an expired one.
    now = time.time()
    for feed in unified.FEED_ROTATION:
        assert deadlines[feed] is None or deadlines[feed] > now - 1, feed


def test_feeds_are_given_a_deadline_not_the_whole_session(cycle_env):
    _, deadlines = cycle_env
    end = time.time() + 4000
    unified.run_unified_sync_cycle(
        check_head=True, workers=1, supabase_deadline=end, cycle_rotation=0
    )
    first = deadlines[unified.FEED_ROTATION[0]]
    assert first is not None
    assert first <= end
    # The first feed must not be given the entire remaining session.
    assert first < end - 60


def test_feed_order_rotates_between_cycles(cycle_env):
    calls, _ = cycle_env
    unified.run_unified_sync_cycle(
        check_head=True, workers=0, supabase_deadline=time.time() + 3600, cycle_rotation=3
    )
    expected = list(unified.FEED_ROTATION[3:]) + list(unified.FEED_ROTATION[:3])
    assert calls == expected, f"rotation ignored: {calls}"


def test_a_failed_feed_is_isolated(cycle_env, monkeypatch):
    calls, _ = cycle_env

    def boom(**kwargs):
        calls.append("chinese")
        raise RuntimeError("kaboom")

    monkeypatch.setattr(unified, "run_chinese_sync", boom)
    result = unified.run_unified_sync_cycle(
        check_head=True, workers=1, supabase_deadline=time.time() + 3600, cycle_rotation=0
    )
    assert result["chinese"] == 0
    assert unified.FEED_STATUS.get("chinese") == "failed"
    # Everything after the failing feed still ran.
    assert "studios" in calls


def test_every_callee_the_cycle_hands_a_deadline_accepts_one():
    """Pins the call contract.

    The first post-fix production run (46114735791) failed the samusar feed with
    ``run_pipeline() got an unexpected keyword argument 'deadline'``: the cycle
    hands every runner a deadline, so every runner (and every crawl it forwards
    it to) must accept one.
    """
    import importlib
    import inspect

    runners = {
        "viral-mms": unified.run_viral_mms_sync,
        "viral-mms-sites": unified.run_viral_mms_sites_sync,
        "supjav": unified.run_japanese_jav_sync,
        "myanmar": unified.run_myanmar_sync,
        "samusar": unified.run_samusar_sync,
        "onlyfans": unified.run_onlyfans_sync,
        "voyeurhouse": unified.run_voyeurhouse_sync,
        "chinese": unified.run_chinese_sync,
        "fangsung": unified.run_fangsung_sync,
        "studios": unified.run_studios_sync,
    }
    for name, fn in runners.items():
        assert "deadline" in inspect.signature(fn).parameters, name

    crawls = {
        "scraper.categories.chinese.pipeline:run_category_crawl",
        "scraper.categories.fangsung.pipeline:run_category_crawl",
        "scraper.categories.studios.pipeline:run_category_crawl",
        "scraper.categories.onlyfans.pipeline:run_site_crawl",
        "scraper.categories.voyeurhouse.pipeline:run_site_crawl",
        "scraper.categories.viral-mms.sites_pipeline:run_site_crawl",
        "scraper.categories.viral-mms.sites_pipeline:run_multi_site_pipeline",
        "scraper.categories.myanmar.samusar_pipeline:run_pipeline",
    }
    for ref in crawls:
        mod_name, func_name = ref.split(":")
        mod = importlib.import_module(mod_name)
        fn = getattr(mod, func_name)
        assert "deadline" in inspect.signature(fn).parameters, ref
