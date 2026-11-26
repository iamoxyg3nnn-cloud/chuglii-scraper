"""Parser tests for the borntobefuck listing cards.

Regression context: the first live backfill parsed only half the cards because
the split pattern assumed `class="video ranked"`, while the site also emits
`class="video"` (and other modifiers) for the same card. Pages advertised 12
videos; the parser returned 6.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper.categories.onlyfans.sources import borntobefuck as b  # noqa: E402


def _card(video_id: str, wrapper: str, title: str, model: str, handle: str, country: str) -> str:
    return f"""
    <div class="{wrapper}">
      <div class="top"><a class="card-country flag_country_video" href="https://borntobefuck.com/categories/{country}"></a></div>
      <a href="https://borntobefuck.com/watch/{video_id}" class="cardVideo-top-link">
        <img src="https://content.zavastream.com/t/{video_id}/thumbnail.jpg"
             alt="{title}" title="{title}"
             class="thumbnail"
             data-preview-url="https://content.zavastream.com/t/{video_id}/preview.webp">
        <div id="video-duration" class="time" data-duration="290"></div>
      </a>
      <div class="bottom">
        <a href="https://borntobefuck.com/@{handle}" class="profil channel_avatar_video_card"></a>
        <div class="details">
          <div class="tlwr">
            <a href="https://borntobefuck.com/watch/{video_id}"><h3 class="video-title">{title} Porn Video BornToBeFuck</h3></a>
          </div>
          <p class="video-channel channel_name_video_card">{model}</p>
          <p class="views" data-views="839119"></p>
          <p class="date" data-published="1794240000"></p>
        </div>
      </div>
    </div>"""


def test_parses_every_card_modifier():
    """`video ranked` and plain `video` wrappers must both be parsed."""
    html = (
        _card("45523", "video ranked ", "Title One", "Sonia Bellucci", "sonia-bellucci", "france")
        + _card("46660", "video ", "Title Two", "Lena The Plug", "lena-the-plug", "american")
    )
    cards = b._parse_cards(html)
    assert [c["source_post_id"] for c in cards] == ["45523", "46660"]


def test_card_fields_are_extracted():
    cards = b._parse_cards(_card("45523", "video ranked ", "Title One", "Sonia Bellucci", "sonia-bellucci", "france"))
    assert len(cards) == 1
    card = cards[0]
    assert card["title"] == "Title One"          # suffix stripped
    assert card["models"] == ["Sonia Bellucci"]
    assert card["duration_seconds"] == 290
    assert card["views_count"] == 839119
    assert card["published_at"].startswith("2026-11-09")
    assert card["thumbnail_url"].endswith("/thumbnail.jpg")   # class comes after src
    assert card["preview_url"].endswith("/preview.webp")
    assert card["categories_raw"] == ["france"]
    assert card["detail_url"].endswith("/watch/45523")


def test_duplicate_video_ids_are_collapsed():
    one = _card("45523", "video ranked ", "Title One", "A", "a", "france")
    cards = b._parse_cards(one + one)
    assert len(cards) == 1


def test_cards_without_title_are_skipped():
    broken = '<div class="video ranked "><a href="https://borntobefuck.com/watch/9"></a></div>'
    assert b._parse_cards(broken) == []


def test_archive_ids_below_cutoff_never_fetch(monkeypatch):
    """The 2023-24 archive has no stream; do not spend a request to find out."""
    called = {"n": 0}

    def must_not_call(*a, **k):
        called["n"] += 1
        raise AssertionError("player fragment requested for a known-unstreamable id")

    monkeypatch.setattr(b, "fetch_page", must_not_call)
    assert b.player_fragment("12248") is None
    assert b.resolve_stream("12248") == (None, [])
    assert called["n"] == 0


# ─── Removed-video detection (P6) ────────────────────────────────────────────
# A removed watch page still ships a working player, so a stream check alone
# reports "Video Removed" pages as alive. Three such rows were ingested by the
# first sweep before this check existed.

REMOVED_PAGE = """<html><head><title>Video Removed - BornToBeFuck</title></head>
<body><div class="active-video-container">
<p>This video has been removed and is no longer available.</p></div></body></html>"""


def test_is_removed_page_detects_the_notice():
    assert b.is_removed_page(REMOVED_PAGE) is True


def test_is_removed_page_ignores_a_normal_page():
    assert b.is_removed_page('<html><title>Some Sextape</title><body>ok</body></html>') is False


def test_removed_page_is_not_ingested(monkeypatch):
    """The metadata path must return None so the sweep records it dead."""

    class R:
        status = 200
        html_content = REMOVED_PAGE

    monkeypatch.setattr(b, "fetch_page", lambda url: R())
    assert b.scrape_metadata_from_watch("16462") is None


# ─── Quality class (short side), not raw height ──────────────────────────────

def _master(payload: str):
    class Page:
        status = 200
        html_content = payload
    return Page()


def test_parse_master_scores_landscape_by_short_side(monkeypatch):
    """1920x1080 must be 1080p, not 1080-by-height luck."""
    master = (
        '#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080\n1080p/index.m3u8\n'
        '#EXT-X-STREAM-INF:BANDWIDTH=1350000,RESOLUTION=1280x720\n720p/index.m3u8\n'
    )
    monkeypatch.setattr(b._net, "fetch", lambda *a, **k: _master(master))
    servers = b.parse_master("https://cdn.test/tenant/uuid/index.m3u8")
    assert servers[0]["quality_score"] == 1080
    assert servers[0]["quality_label"] == "1080p Full HD"
    assert servers[-1]["quality_score"] == 720


def test_parse_master_portrait_is_not_promoted_above_landscape_1080(monkeypatch):
    """A portrait 720x1280 clip used to score 1280 -- above real 1080p."""
    master = '#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1350000,RESOLUTION=720x1280\n720p/index.m3u8\n'
    monkeypatch.setattr(b._net, "fetch", lambda *a, **k: _master(master))
    servers = b.parse_master("https://cdn.test/tenant/uuid/index.m3u8")
    assert servers[0]["quality_score"] == 720
    assert servers[0]["quality_label"] == "720p HD"
