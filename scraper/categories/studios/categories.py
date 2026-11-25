"""Category definitions for the studios category (multi-site).

Phase A site: madouer.cc (MacCMS). Phase B/C sites (dasj91, madouhd) register
here too so the pipeline/UI are stable, but their sources are added later.

Zone policy (hard rule): only whitelisted site sections are crawled. Real-victim
zones (hidden-cam / leak / 裸贷-style) are NEVER whitelisted on any site.
"""

from __future__ import annotations

from typing import Dict, Optional

MADOUER_TYPES: Dict[str, Dict[str, str]] = {
    "1": {"cn": "麻豆视频", "en": "Madou Videos", "group": "studios"},
    "2": {"cn": "日本视频", "en": "Japanese Videos", "group": "studios"},
    "3": {"cn": "欧美视频", "en": "Western Videos", "group": "studios"},
    "4": {"cn": "动漫视频", "en": "Anime Videos", "group": "studios"},
    "5": {"cn": "国产视频", "en": "Chinese Amateur", "group": "studios"},
}

DEFAULT_CATEGORIES = list(MADOUER_TYPES.keys())

# Sites registered in this category. Only sites with a live source module are
# crawlable; the pipeline skips the rest.
SITES: Dict[str, Dict[str, str]] = {
    "madouer": {"base_url": "https://madouer.cc", "label": "Madou"},
    # Phase B/C:
    # "dasj91": {"base_url": "https://91dasj.com", "label": "91 Dasj"},
    # "madouhd": {"base_url": "https://madouhd.cc", "label": "Madou HD"},
}


def category_info(category_id: str) -> Optional[Dict[str, str]]:
    """Returns metadata for a whitelisted madouer type id, else None."""
    t = (category_id or "").strip()
    info = MADOUER_TYPES.get(t)
    if not info:
        return None
    return {"cn": info["cn"], "en": info["en"], "group": info["group"], "content_type": "video"}


def is_enabled(category_id: str) -> bool:
    return (category_id or "").strip() in MADOUER_TYPES
