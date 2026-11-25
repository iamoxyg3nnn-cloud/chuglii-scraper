"""Shared exceptions for the studios sources."""


class ListingUnavailable(Exception):
    """A listing page could not be fetched (rate limit, 5xx, anti-bot).

    Distinct from a genuine end-of-listing (404 / empty marker): the zone
    must be marked incomplete and retried later, never ended.
    """
