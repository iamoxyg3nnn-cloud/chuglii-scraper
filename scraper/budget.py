"""Wall-clock budgets for the scraper fleet.

Why this module exists
----------------------
The fleet is a chain of 4-hour GitHub Actions sessions. Before this module a
**single feed** could consume a whole session: on 2026-11-27 the Chinese feed
started at 14:50 and was still crawling at 18:34, when the job timeout killed
it. Feeds 7 (fangsung) and 8 (studios) never ran at all, the checkpoint commit
and the successor dispatch never happened, and 24/7 self-chaining stopped.

The fix is a deadline that every long loop honours:

* ``unified_scraper`` hands each feed a *fair share* of the remaining session
  (``remaining / feeds_left``), so every feed is guaranteed a turn.
* Each ``run_*_sync`` passes its deadline down to the per-zone crawl.
* Each ``run_category_crawl`` / ``run_site_crawl`` checks it **before every
  page**, so a chunk that cannot finish still saves the pages it did crawl.

Everything here is transport-free and accepts either a ``Deadline`` or a bare
unix timestamp (``None`` = no limit), so callers can stay simple.
"""
from __future__ import annotations

import time
from typing import Optional, Union

__all__ = ["Deadline", "expired", "remaining", "claim", "fair_share"]

TimeLike = Union[None, float, int, "Deadline"]


class Deadline:
    """A unix timestamp other code may run until (``None`` = no limit)."""

    __slots__ = ("at",)

    def __init__(self, at: Optional[float] = None) -> None:
        self.at = None if at is None else float(at)

    # ─── constructors ───────────────────────────────────────────────────────
    @classmethod
    def never(cls) -> "Deadline":
        return cls(None)

    @classmethod
    def in_seconds(cls, seconds: float) -> "Deadline":
        return cls(time.time() + max(0.0, float(seconds)))

    # ─── queries ────────────────────────────────────────────────────────────
    @property
    def expired(self) -> bool:
        return self.at is not None and time.time() >= self.at

    def remaining(self) -> Optional[float]:
        if self.at is None:
            return None
        return max(0.0, self.at - time.time())

    def leaves(self, seconds: float) -> bool:
        """True when fewer than ``seconds`` remain (start nothing new)."""
        left = self.remaining()
        return left is not None and left <= float(seconds)

    def min(self, other: TimeLike) -> "Deadline":
        """The earlier of the two deadlines (used to nest budgets)."""
        other_at = _as_at(other)
        if self.at is None:
            return Deadline(other_at)
        if other_at is None:
            return Deadline(self.at)
        return Deadline(min(self.at, other_at))

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        left = self.remaining()
        return "<Deadline never>" if left is None else f"<Deadline {left:.0f}s left>"


def _as_at(value: TimeLike) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, Deadline):
        return value.at
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def expired(deadline: TimeLike) -> bool:
    """True when the budget is spent. ``None`` never expires."""
    at = _as_at(deadline)
    return at is not None and time.time() >= at


def remaining(deadline: TimeLike) -> Optional[float]:
    """Seconds left, or ``None`` when unbounded."""
    at = _as_at(deadline)
    return None if at is None else max(0.0, at - time.time())


def claim(deadline: TimeLike, seconds: float) -> Deadline:
    """A deadline ``seconds`` from now, capped by the parent ``deadline``.

    This is how a chunk of work gets a slice of the session: it may end early
    on its own, but it can never outlive the parent budget.
    """
    at = _as_at(deadline)
    want = time.time() + max(0.0, float(seconds))
    if at is None:
        return Deadline(want)
    return Deadline(min(at, want))


def fair_share(deadline: TimeLike, work_units_left: int, floor_seconds: float = 240.0) -> Deadline:
    """An equal slice of the remaining session for one of ``work_units_left``.

    Used to guarantee that every feed runs even when an earlier one is slow: a
    feed that finishes early returns its unused time to the pool, because the
    next feed is sized from the *live* remaining budget.
    """
    left = remaining(deadline)
    if left is None:
        return Deadline.never()
    share = max(float(floor_seconds), left / max(1, int(work_units_left)))
    return claim(deadline, share)
