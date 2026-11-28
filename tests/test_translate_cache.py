"""Regression tests for the title-translation layer.

Incident: translate_to_en() wrote ``None`` into the *persisted* cache on any
exception, so one Google Translate outage permanently marked thousands of CJK
titles as untranslatable. chinese_videos had 5,492 rows with an empty
title_en while every backfill run reported "19 rows need English titles"
(it only ever scanned the oldest `limit` rows).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scraper.categories.chinese import translate as tr  # noqa: E402


@pytest.fixture(autouse=True)
def clean_state(monkeypatch, tmp_path):
    monkeypatch.setattr(tr, "_CACHE_PATH", tmp_path / "cache.json")
    monkeypatch.setattr(tr, "_cache", {})
    monkeypatch.setattr(tr, "_cache_loaded", True)
    monkeypatch.setattr(tr, "_cache_dirty", False)
    monkeypatch.setattr(tr, "_fail_streak", 0)
    monkeypatch.setattr(tr, "_endpoint_down_until", 0.0)
    monkeypatch.setattr(tr, "_last_request_at", 0.0)
    monkeypatch.setattr(tr.time, "sleep", lambda s: None)
    yield


def _raising_urlopen(*a, **k):
    raise OSError("network unreachable")


def test_transport_failure_is_not_cached(monkeypatch):
    monkeypatch.setattr(tr.urllib.request, "urlopen", _raising_urlopen)
    assert tr.translate_to_en("国产精品第一页") is None
    assert "国产精品第一页" not in tr._cache, "a transient outage poisoned the cache"


def test_transport_failure_is_retried_next_session(monkeypatch):
    monkeypatch.setattr(tr.urllib.request, "urlopen", _raising_urlopen)
    tr.translate_to_en("可爱妹子")
    calls = []

    class Resp:
        def read(self):
            return b'{"sentences":[{"trans":"Cute girl"}]}'

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def ok(*a, **k):
        calls.append(a)
        return Resp()

    monkeypatch.setattr(tr.urllib.request, "urlopen", ok)
    assert tr.translate_to_en("可爱妹子") == "Cute girl"
    assert calls, "the title was never re-sent after a transient failure"


def test_endpoint_circuit_breaker_cools_down_then_retries(monkeypatch):
    """A rate limit is transient: cool down, then keep translating.

    Run 46114735791 translated 697 titles and then gave up on the remaining 767
    because the breaker was a permanent boolean. A throttled endpoint must not
    turn into a lost backfill.
    """
    monkeypatch.setattr(tr.urllib.request, "urlopen", _raising_urlopen)
    for i in range(tr._FAIL_STREAK_LIMIT):
        tr.translate_to_en(f"国产标题{i}")
    assert tr._endpoint_down_until > 0, "the breaker must pause, not latch off"
    assert tr.translate_to_en("冷却中的标题") is None

    # Past the cooldown window the endpoint is tried again and recovers.
    monkeypatch.setattr(tr, "_endpoint_down_until", 0.0)

    class Resp:
        def read(self):
            return b'{"sentences":[{"trans":"Recovered title"}]}'

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(tr.urllib.request, "urlopen", lambda *a, **k: Resp())
    assert tr.translate_to_en("恢复标题") == "Recovered title"
    assert tr._endpoint_down_until == 0.0, "a success must clear the cooldown"


def test_empty_translation_is_cached_as_deterministic(monkeypatch):
    class Resp:
        def read(self):
            return b'{"sentences":[]}'

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(tr.urllib.request, "urlopen", lambda *a, **k: Resp())
    assert tr.translate_to_en("无翻译标题") is None
    assert tr._cache["无翻译标题"] is None


def test_latin_titles_pass_through_without_a_request(monkeypatch):
    def must_not_run(*a, **k):
        raise AssertionError("requested a translation for an already-English title")

    monkeypatch.setattr(tr.urllib.request, "urlopen", must_not_run)
    assert tr.translate_to_en("Blacked.26.10.30.Vanna.Bardot") == "Blacked.26.10.30.Vanna.Bardot"
