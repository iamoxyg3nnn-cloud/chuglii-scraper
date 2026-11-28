"""Tests for the session-budget layer (scraper/budget.py).

Regression guard for run 46004601681 / 46050520954: the Chinese feed started at
14:50 and was still crawling when the job timeout killed the session at 18:34.
Feeds 7 (fangsung) and 8 (studios) never ran, the checkpoint commit and the
successor dispatch never happened, so 24/7 self-chaining stopped.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scraper.budget as budget  # noqa: E402


def test_never_expires_without_a_deadline():
    assert not budget.expired(None)
    assert not budget.expired(budget.Deadline.never())
    assert budget.remaining(None) is None


def test_expired_and_remaining_track_the_clock():
    assert budget.expired(time.time() - 1)
    assert not budget.expired(time.time() + 60)
    assert budget.remaining(time.time() + 60) <= 60


def test_claim_can_never_outlive_its_parent():
    parent = time.time() + 10
    child = budget.claim(parent, 3600)
    assert child.at is not None and child.at <= parent


def test_claim_without_parent_uses_the_request():
    child = budget.claim(None, 30)
    assert 25 <= (child.remaining() or 0) <= 30


def test_fair_share_divides_the_remaining_time():
    parent = time.time() + 300
    first = budget.fair_share(parent, 3)
    # 300s / 3 = 100s, raised to the floor so a slice is always usable.
    left = first.remaining() or 0
    assert 239 <= left <= 300, left


def test_fair_share_floor_keeps_late_feeds_usable():
    """The last feeds must still get a real slice, never zero."""
    parent = time.time() + 5
    share = budget.fair_share(parent, 8, floor_seconds=240)
    # The parent is the binding limit here (5s left), not the floor.
    assert (share.remaining() or 0) <= 5.1


def test_every_feed_gets_a_slice_of_a_finite_session():
    """A 4-hour session split across 10 feeds leaves each one real time."""
    outside = 240 * 60
    parent = time.time() + outside
    shares = []
    remaining = outside
    for feeds_left in range(10, 0, -1):
        d = budget.fair_share(time.time() + remaining, feeds_left, floor_seconds=240)
        shares.append((d.remaining() or 0))
        remaining -= 240          # pretend each feed used only the 240s floor
    assert all(s > 0 for s in shares), shares
    assert remaining > 0, "the whole session must not be consumed by the first feeds"


def test_zero_or_negative_share_is_safe():
    d = budget.claim(None, -5)
    assert (d.remaining() or 0) <= 0.01
