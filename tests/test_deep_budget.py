"""Tests for the deep-backfill budget policy in unified_scraper.

Regression guard for run 45742796072: deep backfill was handed the whole
handoff deadline, so a single stuck zone consumed the entire 5-hour session and
all eight feeds starved.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scraper.unified_scraper as unified  # noqa: E402


def test_budget_reserves_time_for_feeds():
    start = 1_000_000.0
    session = 300 * 60          # 5 hours
    handoff = start + session - 90

    deadline = unified.deep_deadline_for_session(start, session, handoff)

    assert deadline < handoff, "deep must stop before the handoff guard"
    assert deadline - start < session, "deep must not get the whole session"
    # At least half the session must remain for the regular feed cycle.
    assert (handoff - deadline) >= session * 0.5


def test_budget_is_fraction_of_session():
    start = 0.0
    session = 1000.0
    handoff = 10_000.0          # far away: the fraction must be the binding limit
    deadline = unified.deep_deadline_for_session(start, session, handoff)
    assert deadline == pytest.approx(session * unified.DEEP_SESSION_FRACTION)


def test_budget_never_exceeds_handoff_guard():
    start = 0.0
    deadline = unified.deep_deadline_for_session(start, 1000.0, 50.0)
    assert deadline <= 50.0 - 120 or deadline == min(50.0 - 120, 1000.0 * unified.DEEP_SESSION_FRACTION)


def test_fraction_is_sane():
    assert 0.0 < unified.DEEP_SESSION_FRACTION <= 0.9, "an out-of-range fraction would starve feeds"


def test_fraction_clamped_when_env_is_absurd(monkeypatch):
    monkeypatch.setattr(unified, "DEEP_SESSION_FRACTION", 5.0)
    deadline = unified.deep_deadline_for_session(0.0, 1000.0, 10_000.0)
    assert deadline <= 1000.0 * 0.9, "absurd fractions must be clamped"
