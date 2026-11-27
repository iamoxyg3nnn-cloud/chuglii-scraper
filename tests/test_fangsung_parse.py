"""Regression tests for the fangsung listing parser.

Two independent failures made scraper.fangsung_videos stop growing on
2026-11-25:

1. The site switched detail links from the percent-encoded form
   (/a%E7%89%87/226761/FNS-259) to the raw UTF-8 form (/a片/226761/FNS-259).
   The card regex only matched the encoded form, so every listing parsed as
   "0 cards" even with a perfect 200 response.
2. GitHub runner IPs were served a Cloudflare challenge, which needs the
   Scrapling/proxy escalation in scraper.net (covered by tests/test_net.py).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper.categories.fangsung.sources import fangsung  # noqa: E402


CARD_TMPL = (
    '<a target="_blank" href="{base}/a{seg}/{pid}/{slug}">'
    '<div class="card bg-dark mb-3">'
    '<img class="card-img-top lazy" data-src="https://cdn52.apianmeng.com/kk55/japan/2026-11-27/{pid}.jpg" '
    'alt="{title}">'
    "</div></a>"
)


def _page(seg: str, count: int = 3) -> str:
    cards = "\n".join(
        CARD_TMPL.format(
            base="https://www.fangsung.com",
            seg=seg,
            pid=226761 - i,
            slug=f"SLUG-{i}",
            title=f"テスト タイトル {i}",
        )
        for i in range(count)
    )
    # Real listings are >5k; scrape_listing rejects suspiciously short pages.
    filler = "<!-- " + "x" * 6000 + " -->"
    return f'<html><body><div class="row">{cards}</div><span>共 7187</span>{filler}</body></html>'


def _install_page(monkeypatch, html: str):
    class Resp:
        status = 200
        via = "fetcher"
        challenge = False
        html_content = html

    monkeypatch.setattr(fangsung, "fetch_page", lambda url, **kw: Resp())


def test_raw_utf8_detail_links_parse(monkeypatch):
    """The current site shape: /a片/{id}/{slug} in the raw page."""
    _install_page(monkeypatch, _page("片"))
    items, detected_max = fangsung.scrape_listing("日本AV", 1)
    assert len(items) == 3, "raw UTF-8 detail links were not matched"
    assert items[0]["source_post_id"] == "226761"
    assert items[0]["detail_url"].endswith("/a片/226761/SLUG-0")
    assert detected_max == 360          # ceil(7187 / 20)


def test_percent_encoded_detail_links_still_parse(monkeypatch):
    """The older site shape must keep working (no regression)."""
    _install_page(monkeypatch, _page("%E7%89%87"))
    items, _ = fangsung.scrape_listing("日本AV", 1)
    assert len(items) == 3
    assert items[0]["source_post_id"] == "226761"


def test_both_encodings_in_one_page(monkeypatch):
    _install_page(monkeypatch, _page("片") + _page("%E7%89%87", 1))
    items, _ = fangsung.scrape_listing("日本AV", 1)
    ids = [i["source_post_id"] for i in items]
    assert "226761" in ids and len(ids) == len(set(ids)), "duplicate IDs leaked through"
