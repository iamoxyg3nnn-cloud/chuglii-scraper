"""Tests for the Layer-2 dead-stream revalidation job."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper.categories.onlyfans import verify_dead as vd  # noqa: E402


def test_referer_for_borntobefuck_is_the_watch_page():
    """content.zavastream.com serves 403 without this exact referer."""
    assert vd.referer_for("borntobefuck", "46660") == "https://borntobefuck.com/watch/46660"


def test_referer_for_uncutx_is_none():
    assert vd.referer_for("uncutx", "a1") is None


def test_referer_for_unknown_site_is_none():
    assert vd.referer_for("sheeshfans", "1") is None


def test_kind_for():
    assert vd.kind_for("https://cdn/x/index.m3u8") == "hls"
    assert vd.kind_for("https://cdn/x/video.mp4") == "mp4"
    assert vd.kind_for("https://cdn/x/stream?token=1") == "mp4"


def test_probe_row_uses_site_referer_and_kind(monkeypatch):
    captured = {}

    def fake_verify(url, **kwargs):
        captured.update({"url": url, **kwargs})
        return True, 200, "ok"

    monkeypatch.setattr(vd, "verify_stream", fake_verify)
    row = {"source_post_id": "46660", "full_video_url": "https://cdn/x/master.m3u8"}
    post_id, alive, status, _ = vd.probe_row("borntobefuck", row)

    assert (post_id, alive, status) == ("46660", True, 200)
    assert captured["kind"] == "hls"
    assert captured["referer"] == "https://borntobefuck.com/watch/46660"
    assert captured["record"] is False


def test_probe_row_handles_missing_fields():
    post_id, alive, status, detail = vd.probe_row("uncutx", {"source_post_id": "a1"})
    assert post_id == "a1" and alive is False and status == 0
    assert "missing" in detail


def test_revalidate_site_partitions_and_marks(monkeypatch):
    rows = [
        {"source_post_id": "alive1", "full_video_url": "https://m/x.mp4"},
        {"source_post_id": "dead1", "full_video_url": "https://m/y.mp4"},
        {"source_post_id": "dead2", "full_video_url": "https://m/z.mp4"},
    ]
    monkeypatch.setattr(vd, "fetch_batch", lambda *a, **k: rows)

    verdicts = {
        "alive1": (True, 206, "mp4 ok"),
        "dead1": (False, 404, "mp4 HTTP 404"),
        "dead2": (False, 0, "transport"),
    }

    def fake_probe(site, row):
        alive, status, detail = verdicts[row["source_post_id"]]
        return row["source_post_id"], alive, status, detail

    monkeypatch.setattr(vd, "probe_row", fake_probe)
    marked = {}

    def fake_mark(site, alive_ids, dead_ids):
        marked.update({"site": site, "alive": sorted(alive_ids), "dead": sorted(dead_ids)})
        return {"success": True}

    monkeypatch.setattr(vd, "mark", fake_mark)
    summary = vd.revalidate_site("uncutx", limit=10)

    assert summary["checked"] == 3
    assert summary["alive"] == 1 and summary["dead"] == 2
    assert summary["errors"] == 1          # the status-0 probe counts as transport
    assert marked == {"site": "uncutx", "alive": ["alive1"], "dead": ["dead1", "dead2"]}


def test_dry_run_never_marks(monkeypatch):
    monkeypatch.setattr(vd, "fetch_batch", lambda *a, **k: [
        {"source_post_id": "d1", "full_video_url": "https://m/y.mp4"},
    ])
    monkeypatch.setattr(vd, "probe_row", lambda site, row: ("d1", False, 404, "gone"))
    monkeypatch.setattr(vd, "mark", lambda *a, **k: pytest.fail("dry-run must not mark"))

    summary = vd.revalidate_site("uncutx", dry_run=True)
    assert summary["dead"] == 1


def test_revalidate_site_with_nothing_due(monkeypatch):
    monkeypatch.setattr(vd, "fetch_batch", lambda *a, **k: [])
    summary = vd.revalidate_site("uncutx")
    assert summary == {"site": "uncutx", "checked": 0, "alive": 0, "dead": 0, "errors": 0}


def test_main_reports_a_failing_site_without_crashing(monkeypatch, capsys):
    def boom(site, **kwargs):
        raise RuntimeError("rpc down")

    monkeypatch.setattr(vd, "revalidate_site", boom)
    code = vd.main(["--sites", "uncutx"])
    assert code == 1
    assert "revalidation failed" in capsys.readouterr().out


def test_main_returns_zero_when_a_site_succeeds(monkeypatch):
    monkeypatch.setattr(
        vd, "revalidate_site",
        lambda site, **k: {"site": site, "checked": 1, "alive": 1, "dead": 0, "errors": 0},
    )
    assert vd.main(["--sites", "uncutx"]) == 0
