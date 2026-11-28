"""Isolated source adapter: reallifecam.to/videos."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from scraper.categories.voyeurhouse.sources import avs_base

SOURCE_SITE = "reallifecamto"
LABEL = "RealLifeCam.to"
DEFAULT_CATEGORY = "reallifecamto"
DEFAULT_MAX_PAGES = 560
BASE_URL = "https://reallifecam.to/"
LISTING_TEMPLATE = "https://reallifecam.to/videos"


def listing_url(page: int) -> str:
    return avs_base.listing_url(LISTING_TEMPLATE, page)


def scrape_listing(page_num: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    url = listing_url(page_num)
    html, _final, status = avs_base.fetch_html(url, referer=BASE_URL)
    if not html or status == 0:
        # A block/timeout is not the end of the catalogue: hotscopes.net answers
        # 403 to this runner on every page, and "no cards" used to be read as
        # "listing finished" (which silently retired zones and burned 9 minutes
        # of proxy churn per cycle). The caller keeps its cursor and retries.
        raise avs_base.ListingUnavailable(f"listing unavailable (HTTP {status or 'no-response'}) for {url}")
    return avs_base.scrape_listing(html, BASE_URL, SOURCE_SITE, DEFAULT_CATEGORY)


def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    return avs_base.enrich_detail(card, BASE_URL, DEFAULT_CATEGORY)
