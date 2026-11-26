"""The scraper heals its own durations; nobody runs a repair script by hand.

influencersgonewild cannot tell us a duration, so its rows inherited the table
default 600 s. Ingest was fixed to measure the media, and this tool drains what
is already stored. These tests pin the two properties that make it safe to
schedule: it only touches rows the database says need work, and it always marks
a row checked so unmeasurable media cannot churn forever.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper.tools import onlyfans_duration_backfill as tool  # noqa: E402


class Recorder:
    def __init__(self, backlog):
        self.backlog = list(backlog)
        self.calls = []

    def rpc(self, name, params, retries=2, timeout=None):
        self.calls.append((name, params))
        if name == "scraper_onlyfans_duration_backlog":
            items, self.backlog = self.backlog, []
            return {"success": True, "items": items}
        if name == "scraper_onlyfans_set_duration":
            return {"success": True, "site_rows": len(params["p_items"])}
        if name == "scraper_onlyfans_mark_duration_checked":
            return {"success": True, "site_rows": len(params["p_ids"])}
        return {"success": True}


def _patch(monkeypatch, recorder, measure):
    monkeypatch.setattr(tool, "call_supabase_rpc", recorder.rpc)
    monkeypatch.setattr(tool, "measure", measure)


def test_measures_and_writes_what_it_found(monkeypatch):
    rec = Recorder([
        {"post_id": "1", "url": "https://cdn/a.mp4"},
        {"post_id": "2", "url": "https://cdn/b.mp4"},
    ])
    _patch(monkeypatch, rec, lambda url, referer: 108 if url.endswith("a.mp4") else 335)

    summary = tool.drain_site("influencersgonewild", minutes=1, batch=10, workers=2)

    assert summary["measured"] == 2
    assert summary["written"] == 2
    written = [c for c in rec.calls if c[0] == "scraper_onlyfans_set_duration"][0][1]
    assert {i["post_id"]: i["seconds"] for i in written["p_items"]} == {"1": 108, "2": 335}


def test_unmeasurable_rows_are_marked_checked_not_left_to_retry(monkeypatch):
    rec = Recorder([
        {"post_id": "1", "url": "https://cdn/a.mp4"},
        {"post_id": "2", "url": "https://cdn/dead.mp4"},
    ])
    _patch(monkeypatch, rec, lambda url, referer: 108 if url.endswith("a.mp4") else None)

    summary = tool.drain_site("influencersgonewild", minutes=1, batch=10, workers=2)

    assert summary["measured"] == 1
    assert summary["unknown"] == 1
    marked = [c for c in rec.calls if c[0] == "scraper_onlyfans_mark_duration_checked"]
    assert marked and marked[0][1]["p_ids"] == ["2"]


def test_an_empty_backlog_costs_one_query(monkeypatch):
    rec = Recorder([])
    _patch(monkeypatch, rec, lambda url, referer: 100)

    summary = tool.drain_site("uncutx", minutes=1)

    assert summary == {"site": "uncutx", "batches": 0, "measured": 0, "unknown": 0, "written": 0}
    assert [c[0] for c in rec.calls] == ["scraper_onlyfans_duration_backlog"]


def test_dry_run_never_writes(monkeypatch):
    rec = Recorder([{"post_id": "1", "url": "https://cdn/a.mp4"}])
    _patch(monkeypatch, rec, lambda url, referer: 42)

    tool.drain_site("sheeshfans", minutes=1, batch=10, dry_run=True)

    assert [c[0] for c in rec.calls] == ["scraper_onlyfans_duration_backlog"]


def test_one_broken_site_does_not_stop_the_others(monkeypatch, capsys):
    def boom(*a, **k):
        raise RuntimeError("supabase down")

    monkeypatch.setattr(tool, "call_supabase_rpc", boom)
    # main() must swallow it and still exit 0: a backfill must never fail the
    # maintenance run (the health gate is the authority on quality).
    assert tool.main(["--minutes", "0.01", "--sites", "sheeshfans,uncutx"]) == 0
    assert "failed" in capsys.readouterr().out


def test_measure_prefers_the_header_parser(monkeypatch):
    calls = []
    monkeypatch.setattr(tool, "mp4_duration_from_header", lambda url, referer=None: calls.append("header") or 300)
    monkeypatch.setattr(tool, "probe_media", lambda *a, **k: calls.append("ffprobe") or (0, None))

    assert tool.measure("https://cdn/a.mp4", "https://site/") == 300
    assert calls == ["header"]          # ffprobe not needed when the header answers


def test_measure_falls_back_to_ffprobe(monkeypatch):
    monkeypatch.setattr(tool, "mp4_duration_from_header", lambda url, referer=None: None)
    monkeypatch.setattr(tool, "probe_media", lambda *a, **k: (144, 720))

    assert tool.measure("https://cdn/a.ts", "https://site/") == 144
