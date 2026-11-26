"""Tests for the OnlyFans ingestion health gate."""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper.tools import onlyfans_health as health  # noqa: E402

NOW = datetime(2026, 11, 26, 12, 0, tzinfo=timezone.utc)
FRESH = (NOW - timedelta(hours=2)).isoformat().replace("+00:00", "Z")


def _counts(**over):
    base = {s: 100 for s in health.REQUIRED_SOURCES}
    base.update(over)
    return base


def _newest(**over):
    base = {s: FRESH for s in health.REQUIRED_SOURCES}
    base.update(over)
    return base


def test_all_sources_healthy():
    assert health.evaluate(_counts(), _newest(), max_stale_days=7, now=NOW) == []


def test_empty_source_fails():
    failures = health.evaluate(_counts(uncutx=0), _newest(), max_stale_days=7, now=NOW)
    assert len(failures) == 1
    assert "uncutx" in failures[0] and "0 live rows" in failures[0]


def test_unavailable_count_fails_loudly():
    """A query error must not be mistaken for a healthy zero."""
    failures = health.evaluate(_counts(borntobefuck=None), _newest(), max_stale_days=7, now=NOW)
    assert len(failures) == 1
    assert "unavailable" in failures[0]


def test_stale_ingestion_fails():
    old = (NOW - timedelta(days=9)).isoformat().replace("+00:00", "Z")
    failures = health.evaluate(_counts(), _newest(sheeshfans=old), max_stale_days=7, now=NOW)
    assert len(failures) == 1
    assert "sheeshfans" in failures[0] and "days old" in failures[0]


def test_missing_timestamp_is_not_a_failure():
    """Some sources never expose scraped_at; the row count still proves life."""
    counts = _counts()
    newest = _newest(sheeshfans=None)
    assert health.evaluate(counts, newest, max_stale_days=7, now=NOW) == []


def test_malformed_timestamp_is_ignored():
    assert health.evaluate(_counts(), _newest(uncutx="not-a-date"), max_stale_days=7, now=NOW) == []


def test_required_sources_cover_the_two_new_sites():
    assert "borntobefuck" in health.REQUIRED_SOURCES
    assert "uncutx" in health.REQUIRED_SOURCES
