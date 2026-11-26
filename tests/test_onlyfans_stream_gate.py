"""Tests for the OnlyFans dead-video gate and the uncut-x source adapter.

Regression context: 3,080 of uncut-x's 4,445 videos point at Cloudflare R2
objects that are gone. Before this gate, a 400/404 on the media host was stored
as if it were a playable video.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper.categories.onlyfans import stream_check  # noqa: E402
from scraper.categories.onlyfans.sources import uncutx  # noqa: E402


@pytest.fixture(autouse=True)
def _clear_memo():
    stream_check.clear_memo()
    yield
    stream_check.clear_memo()


def fake_get(responses):
    """responses: url-substring -> (status, headers, body)."""
    def _get(url, headers=None, timeout=15.0):
        for needle, value in responses.items():
            if needle in url:
                return value
        return 0, {}, b""
    return _get


# ─── HLS ────────────────────────────────────────────────────────────────────

MASTER = b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1350000,RESOLUTION=1280x720\n720p/video.m3u8\n"
VARIANT = b"#EXTM3U\n#EXTINF:4.0,\nseg_00000.ts\n#EXTINF:4.0,\nseg_00001.ts\n"


def test_hls_alive_when_master_variant_and_segment_all_serve(monkeypatch):
    monkeypatch.setattr(
        stream_check, "_get",
        fake_get({
            "index.m3u8": (200, {"content-type": "application/vnd.apple.mpegurl"}, MASTER),
            "720p/video.m3u8": (200, {"content-type": "application/vnd.apple.mpegurl"}, VARIANT),
            "seg_00000.ts": (206, {"content-type": "video/mp2t"}, b"\x47" * 64),
        }),
    )
    alive, status, detail = stream_check.probe_hls("https://cdn.test/x/index.m3u8")
    assert alive is True and status == 200 and "ok" in detail


def test_hls_media_playlist_is_probed_directly(monkeypatch):
    """A per-rendition URL (segments, no STREAM-INF) is valid input.

    Regression: sources that store the rendition rather than the master had
    their .ts lines read as 'variants', so every one of them was reported dead
    with 'variant has no segments'.
    """
    monkeypatch.setattr(
        stream_check, "_get",
        fake_get({
            "720p/video.m3u8": (200, {"content-type": "application/vnd.apple.mpegurl"}, VARIANT),
            "seg_00000.ts": (206, {"content-type": "video/mp2t"}, b"\x47" * 64),
        }),
    )
    alive, status, detail = stream_check.probe_hls("https://cdn.test/x/720p/video.m3u8")
    assert alive is True and status == 200 and detail == "hls ok"


def test_hls_dead_when_master_404(monkeypatch):
    """The R2 case: a signed URL that resolves to a missing object."""
    monkeypatch.setattr(stream_check, "_get", fake_get({"index.m3u8": (404, {}, b"")}))
    alive, status, _ = stream_check.probe_hls("https://cdn.test/x/index.m3u8")
    assert alive is False and status == 404


def test_hls_dead_when_master_is_html(monkeypatch):
    monkeypatch.setattr(
        stream_check, "_get",
        fake_get({"index.m3u8": (200, {"content-type": "text/html"}, b"<html>blocked</html>")}),
    )
    alive, _, detail = stream_check.probe_hls("https://cdn.test/x/index.m3u8")
    assert alive is False and "not a playlist" in detail


def test_hls_dead_when_segment_404(monkeypatch):
    """Half-dead playlist: 200 master + 200 variant but missing segments."""
    monkeypatch.setattr(
        stream_check, "_get",
        fake_get({
            "index.m3u8": (200, {}, MASTER),
            "720p/video.m3u8": (200, {}, VARIANT),
            "seg_00000.ts": (404, {}, b""),
        }),
    )
    alive, status, detail = stream_check.probe_hls("https://cdn.test/x/index.m3u8")
    assert alive is False and status == 404 and "segment" in detail


def test_hls_dead_when_variant_has_no_segments(monkeypatch):
    monkeypatch.setattr(
        stream_check, "_get",
        fake_get({
            "index.m3u8": (200, {}, MASTER),
            "720p/video.m3u8": (200, {}, b"#EXTM3U\n#EXT-X-ENDLIST\n"),
            "seg": (200, {"content-type": "video/mp2t"}, b"x"),
        }),
    )
    alive, _, detail = stream_check.probe_hls("https://cdn.test/x/index.m3u8")
    assert alive is False and "no segments" in detail


# ─── MP4 ────────────────────────────────────────────────────────────────────

def test_mp4_alive_on_partial_content(monkeypatch):
    monkeypatch.setattr(
        stream_check, "_get",
        fake_get({"v.mp4": (206, {"content-type": "video/mp4"}, b"\x00" * 512)}),
    )
    alive, status, _ = stream_check.probe_mp4("https://media.test/v.mp4")
    assert alive is True and status == 206


@pytest.mark.parametrize("status", [400, 403, 404])
def test_mp4_dead_on_error_status(monkeypatch, status):
    """400 must never be read as alive -- that is how the R2 bucket lied."""
    monkeypatch.setattr(
        stream_check, "_get",
        fake_get({"v.mp4": (status, {"content-type": "application/xml"}, b"<Error/>")}),
    )
    alive, got, _ = stream_check.probe_mp4("https://media.test/v.mp4")
    assert alive is False and got == status


def test_mp4_dead_on_wrong_content_type(monkeypatch):
    monkeypatch.setattr(
        stream_check, "_get",
        fake_get({"v.mp4": (200, {"content-type": "text/html"}, b"<html>nope</html>")}),
    )
    alive, _, detail = stream_check.probe_mp4("https://media.test/v.mp4")
    assert alive is False and "content-type" in detail


# ─── verify_stream ──────────────────────────────────────────────────────────

def test_verify_stream_is_memoised(monkeypatch):
    calls = {"n": 0}

    def counting(url, headers=None, timeout=15.0):
        calls["n"] += 1
        return 206, {"content-type": "video/mp4"}, b"x" * 32

    monkeypatch.setattr(stream_check, "_get", counting)
    url = "https://media.test/once.mp4"
    assert stream_check.verify_stream(url, record=False)[0] is True
    assert stream_check.verify_stream(url, record=False)[0] is True
    assert calls["n"] == 1, "probe repeated for the same URL"


def test_verify_stream_never_raises(monkeypatch):
    def boom(url, headers=None, timeout=15.0):
        raise RuntimeError("network exploded")

    monkeypatch.setattr(stream_check, "_get", boom)
    alive, status, detail = stream_check.verify_stream("https://x.test/a.mp4", record=False)
    assert alive is False and "RuntimeError" in detail


def test_infer_kind():
    assert stream_check.infer_kind("https://a/b/index.m3u8") == "hls"
    assert stream_check.infer_kind("https://a/b/v.mp4") == "mp4"
    assert stream_check.infer_kind("https://a/b/v.m4v") == "mp4"
    assert stream_check.infer_kind("https://a/b/embed", declared="mp4") == "mp4"


# ─── uncut-x adapter ────────────────────────────────────────────────────────

def test_is_alive_row_rejects_dead_r2():
    dead = {
        "video_url": "https://uncut.abc.r2.cloudflarestorage.com/sophie/0587.mp4",
        "title": "some real title",
    }
    assert uncutx._is_alive_row(dead) is False


def test_is_alive_row_rejects_uuid_titles():
    row = {
        "video_url": "https://media.uncut-x.com/cards/x/video.mp4",
        "title": "0587eef1-7df1-4db2-98fb-9f99be230e86",
    }
    assert uncutx._is_alive_row(row) is False


def test_is_alive_row_accepts_live_host():
    row = {
        "video_url": "https://media.uncut-x.com/cards/Jakara%20Bella/video.mp4",
        "title": "Jakara Bella Gets What She Wants",
    }
    assert uncutx._is_alive_row(row) is True


def test_scrape_listing_filters_dead_rows_and_maps_fields(monkeypatch):
    rows = [
        {
            "id": "a1", "slug": "s1", "title": "Real Title", "title_en": None,
            "video_url": "https://media.uncut-x.com/cards/x/video.mp4",
            "thumbnail_url": "https://media.uncut-x.com/cards/x/thumb.jpg",
            "preview_url": None, "duration": None, "orientation": None,
            "model_name": "Model One", "model_names": ["Model One"],
            "categories": [], "tags": ["tag1"], "views": 42, "likes": 1,
            "created_at": "2026-03-09T12:00:00+00:00", "source": "admin",
        },
        {   # dead R2 row that somehow slipped through the query filter
            "id": "a2", "slug": "s2", "title": "Another",
            "video_url": "https://uncut.abc.r2.cloudflarestorage.com/x.mp4",
            "views": 0, "created_at": "2026-03-09T12:00:00+00:00",
        },
    ]
    monkeypatch.setattr(uncutx, "_api_get", lambda query, timeout=30.0: (rows, 1365))

    items, detected_max = uncutx.scrape_listing(1)

    assert [i["source_post_id"] for i in items] == ["a1"]
    assert items[0]["models"] == ["Model One"]
    assert items[0]["views_count"] == 42
    assert items[0]["published_at"].startswith("2026-03-09")
    assert detected_max == 2          # ceil(1365 / 1000)


def test_scrape_listing_query_excludes_dead_host_server_side(monkeypatch):
    captured = {}

    def fake_api(query, timeout=30.0):
        captured["q"] = query
        return [], None

    monkeypatch.setattr(uncutx, "_api_get", fake_api)
    uncutx.scrape_listing(1)
    assert "video_url=like.*media.uncut-x.com*" in captured["q"], (
        "dead R2 rows must be filtered in the query, not after download"
    )
    assert "offset=0" in captured["q"]


def test_enrich_detail_drops_unplayable_stream(monkeypatch):
    monkeypatch.setattr(uncutx, "verify_stream", lambda *a, **k: (False, 404, "gone"))
    card = {"source_post_id": "a1", "video_url": "https://media.uncut-x.com/x.mp4", "title": "t"}
    out = uncutx.enrich_detail(card)
    assert not out.get("servers"), "a dead stream must not produce servers"


def test_enrich_detail_builds_servers_for_live_stream(monkeypatch):
    monkeypatch.setattr(uncutx, "verify_stream", lambda *a, **k: (True, 206, "mp4 ok"))
    # One header-only ffprobe call now yields BOTH duration and height, so the
    # quality label reflects the real file instead of a hardcoded 1080p.
    monkeypatch.setattr(uncutx, "_probe_media", lambda url: (754, 1080))
    card = {"source_post_id": "a1", "video_url": "https://media.uncut-x.com/x.mp4", "title": "t"}
    out = uncutx.enrich_detail(card)
    assert out["servers"][0]["type"] == "direct_mp4"
    assert out["servers"][0]["quality_score"] == 1080
    assert out["full_video_url"] == "https://media.uncut-x.com/x.mp4"
    assert out["duration_seconds"] == 754
    assert out["duration_formatted"] == "12:34"


def test_enrich_detail_labels_a_720p_file_as_720p(monkeypatch):
    """Regression: every uncut-x row claimed '1080p Full HD'; most are 720p."""
    monkeypatch.setattr(uncutx, "verify_stream", lambda *a, **k: (True, 206, "mp4 ok"))
    monkeypatch.setattr(uncutx, "_probe_media", lambda url: (100, 720))
    card = {"source_post_id": "a2", "video_url": "https://media.uncut-x.com/y.mp4", "title": "t"}
    out = uncutx.enrich_detail(card)
    assert out["servers"][0]["quality_label"] == "720p HD"
    assert out["servers"][0]["quality_score"] == 720


def test_fmt_duration():
    assert uncutx._fmt_duration(754) == "12:34"
    assert uncutx._fmt_duration(3725) == "1:02:05"
    assert uncutx._fmt_duration(None) is None
    assert uncutx._fmt_duration(0) is None


# ─── Rendition selection must follow MEASURED quality ────────────────────────

def test_polish_picks_the_highest_measured_rendition():
    """A portrait 1280-wide master must beat a genuine 720p entry.

    Regression: polish scored renditions by name tokens only, so label "1280p"
    matched nothing and fell through to a default -- tying with "720p HD" and
    letting incidental list order decide the PRIMARY stream.
    """
    from scraper.categories.onlyfans.polish import prioritize_servers

    servers = [
        {"name": "BornToBeFuck 720p HD", "server": "b", "embed_url": "https://cdn/a/720p/index.m3u8",
         "type": "hls", "quality_label": "720p HD", "quality_score": 720},
        {"name": "BornToBeFuck 1280p", "server": "b", "embed_url": "https://cdn/a/1280p/index.m3u8",
         "type": "hls", "quality_label": "1280p", "quality_score": 1280},
    ]
    primary, ordered, label, score = prioritize_servers(servers)

    assert "1280p" in primary
    assert ordered[0]["embed_url"].endswith("1280p/index.m3u8")
    assert score == 1280


def test_polish_keeps_token_sniffing_when_no_measurement():
    """Sources that do not measure (legacy embeds) must still rank by name."""
    from scraper.categories.onlyfans.polish import prioritize_servers

    servers = [
        {"name": "Server SD", "server": "x", "embed_url": "https://h/sd", "type": "embed"},
        {"name": "Server 1080p Full HD", "server": "x", "embed_url": "https://h/fhd", "type": "embed"},
    ]
    primary, _ordered, _label, score = prioritize_servers(servers)
    assert primary == "https://h/fhd"
    assert score == 1080
