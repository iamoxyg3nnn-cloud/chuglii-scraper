"""Quality labelling for uncut-x (measured, not assumed)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper.categories.onlyfans.sources import uncutx  # noqa: E402


def test_quality_mapping_is_monotonic():
    """Higher pixels must never score lower -- ranking depends on it."""
    heights = [360, 480, 720, 1080, 1440, 2160]
    scores = [uncutx._quality_for_height(h)[1] for h in heights]
    assert scores == sorted(scores)
    assert len(set(scores)) == len(scores)  # every tier is distinguishable


def test_unknown_height_is_mid_range_not_top():
    """An unmeasured file must not outrank a verified 1080p."""
    assert uncutx._quality_for_height(None)[1] == 720


def test_quality_labels():
    assert uncutx._quality_for_height(2160)[0] == "4K UHD"
    assert uncutx._quality_for_height(1440)[0] == "1440p QHD"
    assert uncutx._quality_for_height(1080)[0] == "1080p Full HD"
    assert uncutx._quality_for_height(720)[0] == "720p HD"
    assert uncutx._quality_for_height(480)[0] == "480p SD"


def test_unknown_height_does_not_claim_1080p():
    """The old code hardcoded 1080p for every row; most are really 720p."""
    assert uncutx._quality_for_height(None)[1] < 1080


def test_probe_media_parses_duration_and_height(monkeypatch):
    class R:
        stdout = json.dumps(
            {"format": {"duration": "1896.4"}, "streams": [{"height": 720}, {"height": 1080}]}
        )
        returncode = 0

    monkeypatch.setattr(uncutx.shutil, "which", lambda _: "/usr/bin/ffprobe")
    monkeypatch.setattr(uncutx.subprocess, "run", lambda *a, **k: R())

    duration, height = uncutx._probe_media("https://media.uncut-x.com/x.mp4")
    assert duration == 1896
    assert height == 1080  # the tallest stream wins


def test_probe_media_is_resilient(monkeypatch):
    """Bad ffprobe output must degrade to (None, None), never raise."""
    class R:
        stdout = "not json"
        returncode = 0

    monkeypatch.setattr(uncutx.shutil, "which", lambda _: "/usr/bin/ffprobe")
    monkeypatch.setattr(uncutx.subprocess, "run", lambda *a, **k: R())
    assert uncutx._probe_media("https://x/y.mp4") == (None, None)


def test_probe_media_missing_ffprobe(monkeypatch):
    monkeypatch.setattr(uncutx.shutil, "which", lambda _: None)
    assert uncutx._probe_media("https://x/y.mp4") == (None, None)
