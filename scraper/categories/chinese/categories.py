"""
Chinese Category — Source-Site Category Configuration for uxj.838238.xyz

SCOPE: Chinese-content zones only (37 of the site's 48 zones).
JAV (日韩伦理, 日本有码, 日本无码) and livestream (直播*) zones are
EXCLUDED and are never scraped.

Every zone is isolated as a subcategory via:
  - category_id   : the exact source-zone id (per-zone checkpoints in the pipeline)
  - category_group: coarse website-browse group — 'chinese' | 'western'
  - content_type  : 'video' (HLS playable) | 'torrent' (magnet entry)

Group mapping:
  chinese   — all Chinese amateur/studio/leak video + magnet zones
  western   — 欧美色情 magnet zone
"""

from typing import Dict, Any

# ─── Video categories (/list/{id}-{page}.html → /video/{id}.html) ───────────
# id: {cn, en, group}
VIDEO_CATEGORIES: Dict[str, Dict[str, str]] = {
    # ── group: chinese ──
    "20110496": {"cn": "国产精品", "en": "Chinese Premium", "group": "chinese"},
    "20030496": {"cn": "国产传媒", "en": "Chinese Studios", "group": "chinese"},
    "20040496": {"cn": "外围探花", "en": "Tanhua Escorts", "group": "chinese"},
    "20050496": {"cn": "大神作品", "en": "Master Works", "group": "chinese"},
    "20090496": {"cn": "网黄女神", "en": "Web Goddesses", "group": "chinese"},
    "20100496": {"cn": "良家人妻", "en": "Housewives", "group": "chinese"},
    "20060496": {"cn": "夫妻交换", "en": "Wife Swapping", "group": "chinese"},
    "20080496": {"cn": "绿帽淫妻", "en": "Cuckold", "group": "chinese"},
    "20120496": {"cn": "近亲乱伦", "en": "Incest", "group": "chinese"},
    "20010496": {"cn": "强奸迷奸", "en": "Rape", "group": "chinese"},
    "20020496": {"cn": "亚洲媚黑", "en": "Asian BBC", "group": "chinese"},
    "20140496": {"cn": "港台三级", "en": "HK-TW Classic", "group": "chinese"},
    "20150496": {"cn": "情侣泄密", "en": "Couple Leaks", "group": "chinese"},
    "20160496": {"cn": "浴室偷拍", "en": "Bathroom Spy", "group": "chinese"},
    "20170496": {"cn": "酒店偷拍", "en": "Hotel Spy", "group": "chinese"},
    "20180496": {"cn": "女性按摩", "en": "Massage", "group": "chinese"},
    "20190496": {"cn": "街拍抄底", "en": "Street Snap", "group": "chinese"},
    "20200496": {"cn": "家庭监控", "en": "Home Cam", "group": "chinese"},
    "20210496": {"cn": "激情艳舞", "en": "Erotic Dance", "group": "chinese"},
    "20220496": {"cn": "模特私拍", "en": "Model Shoots", "group": "chinese"},
    "20230496": {"cn": "裸贷裸条", "en": "Nude Loans", "group": "chinese"},
    "20240496": {"cn": "字母调教", "en": "BDSM", "group": "chinese"},
    "20250496": {"cn": "捆绑绳艺", "en": "Bondage", "group": "chinese"},
    "20260496": {"cn": "变态重口", "en": "Extreme", "group": "chinese"},
    "20270496": {"cn": "户外露出", "en": "Exhibition", "group": "chinese"},
    "20380496": {"cn": "网红流出", "en": "Influencer Leaks", "group": "chinese"},
    "20400496": {"cn": "ｘ福利姬", "en": "Fuliji", "group": "chinese"},
    "20390496": {"cn": "金主定制", "en": "Sugar Daddy", "group": "chinese"},
    "20350496": {"cn": "丝足诱惑", "en": "Stockings Feet", "group": "chinese"},
    "20370496": {"cn": "成人动漫", "en": "Hentai", "group": "chinese"},
    "20340496": {"cn": "TSCD变性", "en": "Trans", "group": "chinese"},
    "20360496": {"cn": "厕拍偷拍", "en": "Toilet Spy", "group": "chinese"},
}

# ─── Magnet/torrent categories (/list/{id}-{page}.html → /torrent/{id}.html) ─
MAGNET_CATEGORIES: Dict[str, Dict[str, str]] = {
    # ── group: chinese ──
    "19920497": {"cn": "国产专区", "en": "Chinese Magnet", "group": "chinese"},
    "19960497": {"cn": "传媒作品", "en": "Studio Magnet", "group": "chinese"},
    "19980497": {"cn": "网黄女神", "en": "Web Goddesses Magnet", "group": "chinese"},
    "19990497": {"cn": "绿帽淫妻", "en": "Cuckold Magnet", "group": "chinese"},
    # ── group: western ──
    "19950497": {"cn": "欧美色情", "en": "Western Magnet", "group": "western"},
}

ALL_ENABLED_CATEGORIES: Dict[str, Dict[str, Any]] = {
    **{k: {**v, "content_type": "video"} for k, v in VIDEO_CATEGORIES.items()},
    **{k: {**v, "content_type": "torrent"} for k, v in MAGNET_CATEGORIES.items()},
}

DEFAULT_CATEGORIES = list(ALL_ENABLED_CATEGORIES.keys())

CATEGORY_GROUPS = ("chinese", "western")


def category_info(category_id: str) -> Dict[str, Any]:
    """Returns config for a category id, or {} when the id is unknown."""
    return ALL_ENABLED_CATEGORIES.get(str(category_id).strip(), {})


def is_enabled(category_id: str) -> bool:
    return str(category_id).strip() in ALL_ENABLED_CATEGORIES


def categories_by_group(group: str) -> Dict[str, Dict[str, Any]]:
    """Returns all enabled categories belonging to one group."""
    return {
        cid: info for cid, info in ALL_ENABLED_CATEGORIES.items()
        if info.get("group") == group
    }
