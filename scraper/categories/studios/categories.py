"""Category definitions for the studios category (multi-site).

Phase A site: madouer.cc (MacCMS). Phase B/C sites (dasj91, madouhd) register
here too so the pipeline/UI are stable, but their sources are added later.

Zone policy: the user opted to scrape every section the site exposes. Two
safety nets remain:
  - Type 20 (萝莉社) is permanently excluded: a section branded around 萝莉
    content is off-limits regardless of title filtering.
  - The ingest title blocklist (polish.py) still refuses non-consensual /
    real-victim titles (偷拍, 流出, 迷奸, 强奸, 未成年...) on every section.
"""

from __future__ import annotations

from typing import Dict, Optional

MADOUER_TYPES: Dict[str, Dict[str, str]] = {
    # Main nav sections
    "1": {"cn": "麻豆视频", "en": "Madou Videos", "group": "studios"},
    "2": {"cn": "日本视频", "en": "Japanese Videos", "group": "studios"},
    "3": {"cn": "欧美视频", "en": "Western Videos", "group": "studios"},
    "4": {"cn": "动漫视频", "en": "Anime Videos", "group": "studios"},
    "5": {"cn": "国产视频", "en": "Chinese Amateur", "group": "studios"},
    # Studio-brand sections (hidden beyond the homepage nav)
    "6": {"cn": "麻豆原创", "en": "Madou Original", "group": "studios"},
    "7": {"cn": "91制片厂", "en": "91 Studio", "group": "studios"},
    "8": {"cn": "天美传媒", "en": "Tianmei Media", "group": "studios"},
    "9": {"cn": "蜜桃影像", "en": "Peach Images", "group": "studios"},
    "10": {"cn": "星空传媒", "en": "Starlight Media", "group": "studios"},
    "11": {"cn": "皇家华人", "en": "Royal Chinese", "group": "studios"},
    "12": {"cn": "精东影业", "en": "Jingdong Studio", "group": "studios"},
    "13": {"cn": "乐播传媒", "en": "Lebo Media", "group": "studios"},
    "14": {"cn": "成人头条", "en": "Adult Headlines", "group": "studios"},
    "15": {"cn": "兔子先生", "en": "Mr. Rabbit", "group": "studios"},
    "16": {"cn": "杏吧原创", "en": "Xingba Original", "group": "studios"},
    "17": {"cn": "玩偶姐姐", "en": "HongKongDoll", "group": "studios"},
    "18": {"cn": "糖心Vlog", "en": "Tangxin Vlog", "group": "studios"},
    "22": {"cn": "华语原创", "en": "Chinese Original", "group": "studios"},
    "31": {"cn": "推特女神", "en": "Twitter Girls", "group": "studios"},
    "32": {"cn": "国产系列", "en": "Chinese Series", "group": "studios"},
}

DEFAULT_CATEGORIES = list(MADOUER_TYPES.keys())

# Sections permanently excluded, with reasons:
EXCLUDED_TYPES: Dict[str, str] = {
    "19": "site error template, no content",
    "21": "site error template, no content",
    "20": "萝莉社 — excluded on policy grounds regardless of title filtering",
    "23": "JAV — excluded per user decision",
    "24": "JAV — excluded per user decision",
    "25": "JAV — excluded per user decision",
    "26": "JAV — excluded per user decision",
    "27": "JAV — excluded per user decision",
    "28": "site error template, no content",
    "29": "site error template, no content",
    "30": "site error template, no content",
}

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
