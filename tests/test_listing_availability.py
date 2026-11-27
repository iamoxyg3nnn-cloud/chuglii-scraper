"""Regression tests: "listing unavailable" is not "end of catalogue".

Incident (runs 45904626049 / 45810354942): the Chinese (95da) and fangsung
sources returned ``([], None)`` both when a page genuinely ended *and* when the
fetch was blocked or timed out. The pipelines therefore ended a zone on a
transient error (fangsung: 40 blocked fetches per session, 0 rows ingested) and
never pinned a real end, so short zones re-crawled 30+ empty pages every
session (checkpoints sat at "p12/50" forever).
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper.categories import listing_errors  # noqa: E402


# ─── Source level: a failed fetch must raise, not return [] ─────────────────

def test_chinese_source_raises_when_listing_fetch_fails(monkeypatch):
    from scraper.categories.chinese.sources import uxj838238 as uxj

    monkeypatch.setattr(uxj, "fetch_page", lambda url, **kw: None)
    with pytest.raises(listing_errors.ListingUnavailable):
        uxj.scrape_listing("20110496", 1)


def test_fangsung_source_raises_when_listing_fetch_fails(monkeypatch):
    from scraper.categories.fangsung.sources import fangsung as fs

    monkeypatch.setattr(fs, "fetch_page", lambda url, **kw: None)
    with pytest.raises(listing_errors.ListingUnavailable):
        fs.scrape_listing("日本AV", 1)


def test_chinese_source_returns_empty_only_for_a_real_soft_404(monkeypatch):
    from scraper.categories.chinese.sources import uxj838238 as uxj

    class Resp:
        status = 200
        challenge = False
        html_content = "<html><body>removed</body></html>"   # no payload

    monkeypatch.setattr(uxj, "fetch_page", lambda url, **kw: Resp())
    items, detected = uxj.scrape_listing("20110496", 999)
    assert items == [] and detected is None


# ─── Pipeline level: pin a real end, keep the cursor on a failure ───────────

def _fake_source(scrape_listing):
    return SimpleNamespace(
        scrape_listing=scrape_listing,
        enrich_detail=lambda card: card,
    )


def _run_chinese(monkeypatch, fake_scrape, start_page=1, max_pages=3):
    from scraper.categories.chinese import pipeline as cn

    monkeypatch.setitem(cn.SOURCES, "uxj838238", _fake_source(fake_scrape))
    monkeypatch.setattr(cn, "save_known_ids", lambda *a, **k: None)
    monkeypatch.setattr(cn.time, "sleep", lambda s: None)
    return cn.run_category_crawl(
        site_key="uxj838238",
        category_id="20110496",
        start_page=start_page,
        max_pages=max_pages,
        known_ids=set(),
        jitter=0.0,
    )


def test_chinese_empty_page_pins_true_end(monkeypatch):
    calls = []

    def fake_scrape(cat, page):
        calls.append(page)
        return ([], None)          # real 200 page with no cards

    stats = _run_chinese(monkeypatch, fake_scrape)
    assert calls == [1]
    assert stats["true_max_pages"] == 0
    assert stats.get("listing_unavailable") is None


def test_chinese_unavailable_page_does_not_pin_true_end(monkeypatch):
    def fake_scrape(cat, page):
        raise listing_errors.ListingUnavailable("HTTP 403 on listing")

    stats = _run_chinese(monkeypatch, fake_scrape)
    assert stats["true_max_pages"] is None
    assert stats["listing_unavailable"] is True
    assert stats["errors"] == 1


def test_chinese_pipeline_pins_cursor_after_true_end(monkeypatch):
    """A completed catalogue must not be re-crawled with a stale 50-page total."""
    from scraper.categories.chinese import pipeline as cn

    checkpoints = {"uxj838238:20110496": {"current_page": 19, "max_pages": 50}}
    saved = {}

    monkeypatch.setattr(cn, "load_checkpoints", lambda: checkpoints)
    monkeypatch.setattr(cn, "save_checkpoints", lambda cp: saved.update(cp))
    monkeypatch.setattr(cn, "load_known_ids", lambda: set())
    monkeypatch.setattr(cn, "save_known_ids", lambda *a, **k: None)
    monkeypatch.setattr(cn, "load_progress", lambda: {"categories": {}})
    monkeypatch.setattr(cn, "save_progress", lambda p: None)
    monkeypatch.setitem(
        cn.SOURCES,
        "uxj838238",
        _fake_source(lambda cat, page: ([], None)),   # page 19 is the real end
    )

    cn.run_chinese_pipeline(categories=["20110496"], pages_per_category=5, dry_run=False)

    state = saved["uxj838238:20110496"]
    assert state["max_pages"] == 18, "the real end (page 18) must replace the stale 50"
    assert state["current_page"] == 19, "the cursor must sit past the real end"


def test_true_end_never_exceeds_the_site_reported_total(monkeypatch):
    """Page 30 is empty but the site says the catalogue ends at 22.

    Pinning 29 would mark pages 23-29 as crawled although they were never
    fetched; the pin must be min(page-1, reported total).
    """

    def fake_scrape(cat, page):
        if page == 30:
            return ([], 22)         # payload present, zero cards
        return ([{"source_post_id": str(page), "title": "t", "content_type": "video"}], 22)

    stats = _run_chinese(monkeypatch, fake_scrape, start_page=30, max_pages=1)
    assert stats["true_max_pages"] == 22


def test_fangsung_runner_pin_survives_the_advertised_total(monkeypatch):
    """Pinning the confirmed end must not be undone by the site's own total."""
    from scraper.categories.fangsung import pipeline as fp

    cps = {"fangsung:香港": {"current_page": 3, "max_pages": 50}}
    monkeypatch.setattr(fp, "load_checkpoints", lambda: cps)
    monkeypatch.setattr(fp, "load_known_ids", lambda: set())
    monkeypatch.setattr(fp, "save_known_ids", lambda *a, **k: None)
    monkeypatch.setattr(fp, "load_progress", lambda: {})
    monkeypatch.setattr(fp, "save_progress", lambda p: None)
    monkeypatch.setattr(fp, "save_checkpoints", lambda cp: None)
    monkeypatch.setattr(fp, "category_info", lambda t: {"en": t})
    monkeypatch.setattr(
        fp,
        "run_category_crawl",
        lambda **kw: {
            "pages_crawled": 0,
            "true_max_pages": 2,
            "detected_max_pages": 360,   # advertised, and much larger
            "upserted": 0,
        },
    )

    fp.run_fangsung_pipeline(check_head=False, pages_per_category=1, categories=["香港"])
    state = cps["fangsung:香港"]
    assert state["max_pages"] == 2, "the advertised total undid the confirmed end"
    assert state["current_page"] == 3
