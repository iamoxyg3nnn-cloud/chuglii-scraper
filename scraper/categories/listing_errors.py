"""Shared listing-availability signal for the Chinese and fangsung categories.

Every one of those sources returns ``([], None)`` when a listing page cannot be
fetched. The pipelines could not tell that apart from "this is the end of the
catalogue", so a Cloudflare block, a timeout or a 5xx silently ended a zone,
and the cursor was left stuck on a page that never actually answered
(fangsung logged 40 failed fetches per session and ingested 0 rows).

Raising :class:`ListingUnavailable` makes the distinction explicit: the caller
must NOT treat it as the end of the listing and must NOT pin a cursor past it.

``scraper.categories.studios.errors`` keeps its own identical class for the
studios sources; both mean the same thing to their pipelines.
"""

from __future__ import annotations


class ListingUnavailable(Exception):
    """A listing page could not be fetched (transient, anti-bot or server error)."""
