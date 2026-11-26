"""Duration truth for the OnlyFans sources.

Two defaults were inventing durations:

* ``duration_formatted DEFAULT '10:00'`` (fixed earlier), and
* ``duration_seconds DEFAULT 600`` + ``COALESCE(x.duration_seconds, 600)``.

influencersgonewild publishes no duration metadata anywhere, so *every* one of
its 4,284 rows was stored as 600 s and the whole source displayed "10:00".
sheeshfans stored 600 for 1,008 rows whose embed endpoint is gone (HTTP 410).

These tests pin the replacement behaviour: measure the media, or stay unknown.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper.categories.onlyfans import media_probe  # noqa: E402
from scraper.categories.onlyfans.polish import format_duration  # noqa: E402


class _Proc:
    def __init__(self, stdout: str = "") -> None:
        self.stdout = stdout


def test_probe_reads_duration_and_height_in_one_call(monkeypatch):
    calls = []

    def fake_run(cmd, capture_output=True, text=True, timeout=None):
        calls.append(cmd)
        return _Proc(json.dumps({
            "format": {"duration": "108.525083"},
            "streams": [{"height": 720}, {"height": 1280}],
        }))

    monkeypatch.setattr(media_probe.shutil, "which", lambda _n: "ffprobe")
    monkeypatch.setattr(media_probe.subprocess, "run", fake_run)

    duration, height = media_probe.probe_media("https://cdn/x.mp4", referer="https://site/")
    assert duration == 109           # rounded, not truncated
    assert height == 1280            # tallest stream
    assert len(calls) == 1           # one invocation, not two
    assert any("Referer: https://site/" in str(c) for c in calls[0])


def test_probe_returns_none_when_the_container_is_unreadable(monkeypatch):
    monkeypatch.setattr(media_probe.shutil, "which", lambda _n: "ffprobe")
    monkeypatch.setattr(media_probe.subprocess, "run", lambda *a, **k: _Proc(""))
    assert media_probe.probe_media("https://cdn/dead.mp4") == (None, None)


def test_probe_returns_none_without_ffprobe(monkeypatch):
    monkeypatch.setattr(media_probe.shutil, "which", lambda _n: None)
    assert media_probe.probe_media("https://cdn/x.mp4") == (None, None)


def test_probe_swallows_timeouts(monkeypatch):
    def boom(*a, **k):
        raise TimeoutError("took too long")

    monkeypatch.setattr(media_probe.shutil, "which", lambda _n: "ffprobe")
    monkeypatch.setattr(media_probe.subprocess, "run", boom)
    assert media_probe.probe_media("https://cdn/slow.mp4") == (None, None)


@pytest.mark.parametrize("seconds,expected", [
    (2, "00:02"),
    (67, "01:07"),   # minutes are zero-padded, matching the rest of the site
    (600, "10:00"),
    (2293, "38:13"),
    (6684, "1:51:24"),      # sheeshfans used to render this as "111:24"
    (19784, "5:29:44"),
])
def test_format_duration_never_overflows_the_minutes_field(seconds, expected):
    assert format_duration(seconds) == expected


def test_format_duration_rejects_unknown():
    assert format_duration(0) is None
    assert format_duration(None) is None


def test_influencersgonewild_asks_the_media_instead_of_defaulting(monkeypatch):
    """The source has no duration metadata, so it must probe the mp4."""
    from scraper.categories.onlyfans.sources import influencersgonewild as igw

    seen = {}

    def fake_probe(url, referer=None, timeout=12):
        seen["url"] = url
        seen["referer"] = referer
        return 108, 720

    monkeypatch.setattr(igw, "probe_media", fake_probe)
    monkeypatch.setattr(igw, "fetch_page", lambda *a, **k: None)   # no detail page

    card = {"source_post_id": "1", "title": "t", "detail_url": "https://x/y", "thumbnail_url": "https://x/t.jpg"}
    merged = igw.enrich_detail(card)
    # No page, so no mp4 could be resolved -> nothing invented.
    assert merged.get("duration_seconds") in (None, card.get("duration_seconds"))
