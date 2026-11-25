"""Category definitions for the fangsung category (www.fangsung.com).

The site's "categories" are search terms. Region terms are the primary crawl
targets (they partition the catalog); actress/tag terms are too numerous and
are picked up from detail-page tag chips instead.
"""

from __future__ import annotations

from typing import Dict, Optional

SITE_KEY = "fangsung"

# Primary region terms — partition the catalog, so crawling these covers it.
REGION_TERMS: Dict[str, Dict[str, str]] = {
    "日本AV": {"en": "Japanese AV", "group": "fangsung"},
    "台湾": {"en": "Taiwanese", "group": "fangsung"},
    "中国AV": {"en": "Chinese AV", "group": "fangsung"},
    "国产": {"en": "Chinese Domestic", "group": "fangsung"},
    "欧美": {"en": "Western", "group": "fangsung"},
    "韩国": {"en": "Korean", "group": "fangsung"},
    "香港": {"en": "Hong Kong", "group": "fangsung"},
    "泰国": {"en": "Thai", "group": "fangsung"},
    "越南": {"en": "Vietnamese", "group": "fangsung"},
    "大陆": {"en": "Mainland", "group": "fangsung"},
}

DEFAULT_CATEGORIES = list(REGION_TERMS.keys())


def category_info(term: str) -> Optional[Dict[str, str]]:
    """Returns metadata for a whitelisted region term, else None."""
    t = (term or "").strip()
    info = REGION_TERMS.get(t)
    if not info:
        return None
    return {"cn": t, "en": info["en"], "group": info["group"], "content_type": "video"}


def is_enabled(term: str) -> bool:
    return (term or "").strip() in REGION_TERMS
