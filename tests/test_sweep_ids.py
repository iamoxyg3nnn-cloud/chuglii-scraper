"""Tests for the borntobefuck id-space sweep (P6)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper.categories.onlyfans import sweep_ids  # noqa: E402
from scraper.categories.onlyfans.sources import borntobefuck as btb  # noqa: E402


class Harness:
    """Records what the sweep asks for, without touching network or DB."""

    def __init__(self, monkeypatch, *, known=(), watch=None, dead_stream=(), duration=100):
        self.known = set(known)
        self.asked = []
        self.flushed = []
        self.saved_states = []
        self.dead_marks = []
        self.watch = watch if watch is not None else (lambda vid: {"source_post_id": str(vid), "title": f"v{vid}"})
        self.dead_stream = set(dead_stream)
        self.duration = duration

        monkeypatch.setattr(sweep_ids, "load_state", lambda known_ids, floor, max_id: {
            "next_id": floor, "min_id": floor, "max_id": max_id or 110,
            "probed": 0, "ingested": 0, "dead": 0, "runs": 0,
        })
        monkeypatch.setattr(sweep_ids, "save_state", lambda st: self.saved_states.append(dict(st)) or True)

        from scraper.categories.onlyfans import pipeline

        monkeypatch.setattr(pipeline, "load_known_ids", lambda: set(self.known))
        monkeypatch.setattr(
            pipeline, "save_known_ids",
            lambda known, new_ids=None, status="stored": self.dead_marks.append((tuple(sorted(new_ids or ())), status)),
        )

        def fake_watch(vid):
            self.asked.append(vid)
            return self.watch(vid)

        monkeypatch.setattr(btb, "scrape_metadata_from_watch", fake_watch)
        monkeypatch.setattr(
            btb, "enrich_detail",
            lambda card: {**card, "stream_dead": True} if str(card["source_post_id"]) in self.dead_stream
            else {**card, "full_video_url": "https://cdn/x/index.m3u8", "stream_referer": "ref"},
        )
        monkeypatch.setattr(btb, "hls_duration", lambda url, referer=None: self.duration)
        monkeypatch.setattr(sweep_ids, "_flush", lambda batch: self.flushed.append(list(batch)) or len(batch))


def test_known_ids_cost_no_requests(monkeypatch):
    h = Harness(monkeypatch, known={f"borntobefuck:{i}" for i in (100, 101, 102, 103)})
    summary = sweep_ids.sweep(minutes=5, min_id=100, max_id=110)

    assert sorted(int(v) for v in h.asked) == list(range(104, 111))
    assert summary["probed"] == 7
    assert summary["stored"] == 7


def test_ids_below_floor_are_never_touched(monkeypatch):
    h = Harness(monkeypatch)
    sweep_ids.sweep(minutes=5, min_id=100, max_id=105)
    assert min(int(v) for v in h.asked) == 100


def test_missing_watch_page_is_recorded_dead_not_stored(monkeypatch):
    h = Harness(monkeypatch, watch=lambda vid: None if str(vid) == "101" else {"source_post_id": vid, "title": "t"})
    summary = sweep_ids.sweep(minutes=5, min_id=100, max_id=102)

    assert summary["dead"] == 1
    assert ("borntobefuck:101",) in [m[0] for m in h.dead_marks]
    stored_ids = [r["source_post_id"] for batch in h.flushed for r in batch]
    assert "101" not in stored_ids


def test_dead_stream_is_recorded_dead(monkeypatch):
    h = Harness(monkeypatch, dead_stream={"102"})
    summary = sweep_ids.sweep(minutes=5, min_id=100, max_id=103)

    assert summary["dead"] == 1
    stored_ids = [r["source_post_id"] for batch in h.flushed for r in batch]
    assert "102" not in stored_ids
    assert sorted(int(v) for v in stored_ids) == [100, 101, 103]


def test_duration_from_playlist_is_applied(monkeypatch):
    h = Harness(monkeypatch, duration=2867)
    sweep_ids.sweep(minutes=5, min_id=100, max_id=100)

    assert h.flushed[0][0]["duration_seconds"] == 2867


def test_cursor_advances_past_handled_ids(monkeypatch):
    h = Harness(monkeypatch)
    summary = sweep_ids.sweep(minutes=5, min_id=100, max_id=104)

    assert summary["cursor"] == 105          # 104 handled, cursor is 104+1
    assert h.saved_states[-1]["next_id"] == 105


def test_limit_stops_early_and_resumes_there(monkeypatch):
    h = Harness(monkeypatch)
    summary = sweep_ids.sweep(minutes=5, min_id=100, max_id=500, limit=3)

    assert summary["interrupted"] is True
    assert len(h.asked) == 3
    assert summary["cursor"] == 103
    assert summary["remaining"] > 0


def test_budget_expiring_on_first_id_does_not_advance_cursor(monkeypatch):
    h = Harness(monkeypatch)
    monkeypatch.setattr(sweep_ids.time, "time", lambda: 1e18)   # always past deadline
    sweep_ids.sweep(minutes=1, min_id=100, max_id=200)

    assert h.asked == []
    assert h.saved_states[-1]["next_id"] == 100


def test_dry_run_stores_and_marks_nothing(monkeypatch):
    h = Harness(monkeypatch, dead_stream={"101"})
    summary = sweep_ids.sweep(minutes=5, min_id=100, max_id=102, dry_run=True)

    assert h.flushed == []
    assert h.dead_marks == []
    assert summary["stored"] == 2          # counted, not written


def test_batches_flush_at_the_batch_size(monkeypatch):
    h = Harness(monkeypatch)
    monkeypatch.setattr(sweep_ids, "BATCH_SIZE", 5)
    sweep_ids.sweep(minutes=5, min_id=100, max_id=111)

    assert [len(b) for b in h.flushed] == [5, 5, 2]


def test_sweep_writes_the_display_string_it_measured(monkeypatch):
    """A measured duration must arrive with its display string.

    The DB upsert falls back to COALESCE(duration_formatted, '10:00'), so a
    record carrying only duration_seconds got stamped "10:00" -- the site then
    showed ten minutes for a 38-minute video. 596 of 897 borntobefuck rows were
    wrong this way before the sweep supplied the formatted value itself.
    """
    h = Harness(monkeypatch, duration=2293)
    sweep_ids.sweep(minutes=5, min_id=100, max_id=101)

    record = h.flushed[0][0]
    assert record["duration_seconds"] == 2293
    assert record["duration_formatted"] == "38:13"


def test_sweep_formats_past_an_hour_with_hours(monkeypatch):
    h = Harness(monkeypatch, duration=6684)
    sweep_ids.sweep(minutes=5, min_id=100, max_id=101)

    assert h.flushed[0][0]["duration_formatted"] == "1:51:24"
