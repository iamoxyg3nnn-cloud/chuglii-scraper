"""Deadline enforcement inside the per-zone crawl loops.

Regression guard for runs 46004601681 and 46050520954: a single feed (chinese)
started its crawl and never came back, so the job timeout killed the session at
5h25m. The feeds queued behind it never ran, the checkpoint commit and the
successor dispatch never executed, and 24/7 self-chaining stopped.

The contract these tests pin:
  * an expired deadline crawls **zero** pages and reports ``deadline_hit``;
  * a deadline that expires mid-run stops at the page reached, so the caller's
    cursor keeps the pages already stored;
  * an unreadable listing page is never treated as the end of the catalogue.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scraper.categories.chinese.pipeline as cn  # noqa: E402
import scraper.categories.fangsung.pipeline as fs  # noqa: E402
import scraper.categories.studios.pipeline as st  # noqa: E402
from scraper.categories.listing_errors import ListingUnavailable  # noqa: E402


class FakeSource:
    """A listing source that returns one card per page and counts calls."""

    def __init__(self, *, raise_unavailable_on=None):
        self.pages = []
        self.raise_on = set(raise_unavailable_on or ())

    def scrape_listing(self, category_id, page_num):
        self.pages.append((category_id, page_num))
        if page_num in self.raise_on:
            raise ListingUnavailable("HTTP 403")
        card = {
            "source_site": "fake",
            "source_post_id": str(page_num),
            "content_type": "video",
            "title": f"t{page_num}",
            "detail_url": f"https://x.test/v/{page_num}",
            "thumbnail_url": "",
        }
        return [card], 50

    def enrich_detail(self, card):
        card = dict(card)
        card["full_video_url"] = "https://cdn.test/v.m3u8"
        return card


def _progress():
    return {"status": "idle", "categories": {}}


@pytest.fixture()
def chinese_source(monkeypatch):
    src = FakeSource()
    monkeypatch.setattr(cn, "SOURCES", {"uxj838238": src})
    monkeypatch.setitem(
        cn.SOURCES, "uxj838238", src
    )
    # The polish/translate/DB layers are not what these tests are about.
    monkeypatch.setattr(cn, "enrich_items_concurrently", lambda mod, items, **kw: list(items))
    monkeypatch.setattr(cn, "polish_chinese_record", lambda raw: dict(raw))
    monkeypatch.setattr(cn, "save_progress", lambda progress: True)
    monkeypatch.setattr(cn, "save_known_ids", lambda *a, **k: None)
    return src


def test_expired_deadline_crawls_nothing(chinese_source):
    stats = cn.run_category_crawl(
        site_key="uxj838238",
        category_id="20110496",
        start_page=5,
        max_pages=10,
        known_ids=set(),
        dry_run=True,
        jitter=0.0,
        progress=_progress(),
        deadline=time.time() - 1,
    )
    assert chinese_source.pages == []
    assert stats["pages_crawled"] == 0
    assert stats["deadline_hit"] is True


def test_deadline_stops_mid_run_and_keeps_the_pages_already_crawled(chinese_source):
    # Expire shortly after the first page so the loop cannot reach page 2.
    def slow_listing(category_id, page_num):
        time.sleep(0.05)
        return FakeSource.scrape_listing(chinese_source, category_id, page_num)

    chinese_source.scrape_listing = slow_listing

    stats = cn.run_category_crawl(
        site_key="uxj838238",
        category_id="20110496",
        start_page=1,
        max_pages=10,
        known_ids=set(),
        dry_run=True,
        jitter=0.0,
        progress=_progress(),
        deadline=time.time() + 0.08,
    )
    assert stats["pages_crawled"] >= 1, "pages already crawled must be reported"
    assert stats["pages_crawled"] < 10
    assert stats["deadline_hit"] is True


def test_unavailable_listing_does_not_end_the_zone(chinese_source):
    chinese_source.raise_on = {3}
    stats = cn.run_category_crawl(
        site_key="uxj838238",
        category_id="20110496",
        start_page=3,
        max_pages=2,
        known_ids=set(),
        dry_run=True,
        jitter=0.0,
        progress=_progress(),
        deadline=None,
    )
    assert stats.get("true_max_pages") is None, "a block must not pin the catalogue end"
    assert stats["errors"] >= 1
    assert stats["pages_crawled"] == 0


@pytest.mark.parametrize(
    "module,sources_attr,site_key",
    [
        (fs, "SOURCES", "fangsung"),
        (st, "SOURCES", "madouer"),
    ],
)
def test_category_fleets_honour_an_expired_deadline(monkeypatch, module, sources_attr, site_key):
    src = FakeSource()
    monkeypatch.setattr(module, sources_attr, {site_key: src})
    monkeypatch.setattr(module, "enrich_items_concurrently", lambda mod, items, **kw: list(items))
    monkeypatch.setattr(module, "save_progress", lambda progress: True)
    monkeypatch.setattr(module, "save_known_ids", lambda *a, **k: None)
    monkeypatch.setattr(module, "polish_studios_record", lambda raw: dict(raw), raising=False)
    monkeypatch.setattr(module, "polish_fangsung_record", lambda raw: dict(raw), raising=False)

    stats = module.run_category_crawl(
        site_key=site_key,
        category_id="1" if site_key == "madouer" else "日本AV",
        start_page=1,
        max_pages=3,
        known_ids=set(),
        dry_run=True,
        jitter=0.0,
        progress=_progress(),
        deadline=time.time() - 1,
    )
    assert src.pages == []
    assert stats["pages_crawled"] == 0
    assert stats["deadline_hit"] is True
