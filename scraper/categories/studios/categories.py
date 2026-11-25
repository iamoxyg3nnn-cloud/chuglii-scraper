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
    "2": "日本视频 — every play page has an empty player (url='') — JAV placeholder pages with no stream; probing all 4,097 pages would be pure waste",
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
    "dasj91": {"base_url": "https://91dasj.com", "label": "91 Dasj"},
    "madouhd": {"base_url": "https://madouhd.cc", "label": "Madou HD"},
}

# 91dasj.com zone allowlist. Only staged/synthetic/commercial sections are
# crawled. Excluded as NCII risks (real-person leak/scandal content):
#   今日吃瓜 mrxl, 热门吃瓜 mldjh, 网红吃瓜 whmx, 网黄吃瓜 whzq, 天涯吃瓜 tycg
#   学生校园 xyml (student campus), 每日大赛 jchj (91-ecosystem contest uploads)
# The ingest title blocklist stays as a second gate on every allowed zone.
DASJ91_CATEGORIES: Dict[str, Dict[str, str]] = {
    "aifl": {"cn": "AI短剧", "en": "AI Dramas"},
    "rmbg": {"cn": "反差女神", "en": "Contrast Goddess"},
    "jxth": {"cn": "探花大神", "en": "Tanhua Masters"},
    "ldcz": {"cn": "绿帽磁场", "en": "Cuckold Stories"},
    "qwys": {"cn": "伦理猎奇", "en": "Taboo Drama"},
}

# Full crawl plan: (site_key, category_id) pairs the pipeline iterates.
# madouhd.cc category allowlist (cateN ids). Produced studio content; the
# real-person-risk sections are excluded:
#   cate7 网红黑料 (internet-celebrity leaks), cate9 强奸迷奸 (rape theme),
#   cate10 校园猛料 (campus), cate16 摄像偷拍 (hidden camera), cate25 人兽激情
# The ingest title blocklist stays as a second gate on every allowed zone.
MADOUHD_CATEGORIES: Dict[str, Dict[str, str]] = {
    "cate3": {"cn": "真实乱伦", "en": "Taboo Drama", "group": "studios"},
    "cate4": {"cn": "自慰喷水", "en": "Solo & Squirting", "group": "studios"},
    "cate5": {"cn": "巨乳肥臀", "en": "Busty & Curvy", "group": "studios"},
    "cate6": {"cn": "嫩穴白虎", "en": "Smooth & Tight", "group": "studios"},
    "cate8": {"cn": "扫街探店", "en": "Street Pickup", "group": "studios"},
    "cate11": {"cn": "制服诱惑", "en": "Uniform Temptation", "group": "studios"},
    "cate12": {"cn": "媚黑淫女", "en": "Interracial", "group": "studios"},
    "cate13": {"cn": "sm调教", "en": "BDSM", "group": "studios"},
    "cate14": {"cn": "美女主播", "en": "Cam Girls", "group": "studios"},
    "cate15": {"cn": "绿帽换妻", "en": "Cuckold & Swinging", "group": "studios"},
    "cate17": {"cn": "明星淫梦", "en": "Celebrity Dreams", "group": "studios"},
    "cate18": {"cn": "男同/女同", "en": "LGBT", "group": "studios"},
    "cate19": {"cn": "成人综艺", "en": "Adult Variety", "group": "studios"},
    "cate20": {"cn": "足交口交", "en": "Footjob & Oral", "group": "studios"},
    "cate21": {"cn": "勾引搭讪", "en": "Pickup & Seduction", "group": "studios"},
    "cate23": {"cn": "户外野战", "en": "Outdoor", "group": "studios"},
    "cate24": {"cn": "颅内高潮", "en": "Mindgasm", "group": "studios"},
    "cate26": {"cn": "淫语歌曲", "en": "Erotic Songs", "group": "studios"},
    "cate28": {"cn": "Cosplay", "en": "Cosplay", "group": "studios"},
    "cate29": {"cn": "VR影片", "en": "VR Films", "group": "studios"},
    "cate30": {"cn": "18禁三级", "en": "Softcore III", "group": "studios"},
    "cate31": {"cn": "伪娘人妖", "en": "Femboy & Ladyboy", "group": "studios"},
}

MADOUHD_EXCLUDED: Dict[str, str] = {
    "cate7": "网红黑料 — real-person leak/scandal content, excluded as NCII risk",
    "cate9": "强奸迷奸 — rape/drug theme, excluded on policy grounds",
    "cate10": "校园猛料 — campus content, excluded as NCII risk",
    "cate16": "摄像偷拍 — hidden-camera/voyeur content, excluded as NCII risk",
    "cate25": "人兽激情 — bestiality theme, excluded on policy grounds",
}

ALL_ZONES = (
    [("madouer", t) for t in MADOUER_TYPES]
    + [("dasj91", s) for s in DASJ91_CATEGORIES]
    + [("madouhd", c) for c in MADOUHD_CATEGORIES]
)

DASJ91_EXCLUDED: Dict[str, str] = {
    "mrxl": "吃瓜 zone — real-person leak/scandal content (NCII risk)",
    "mldjh": "吃瓜 zone — real-person leak/scandal content (NCII risk)",
    "whmx": "吃瓜 zone — real-person leak/scandal content (NCII risk)",
    "whzq": "吃瓜 zone — real-person leak/scandal content (NCII risk)",
    "tycg": "吃瓜 zone — real-person leak/scandal content (NCII risk)",
    "xyml": "student campus zone — NCII risk",
    "jchj": "91-ecosystem contest uploads — NCII risk",
}


def category_info(category_id: str) -> Optional[Dict[str, str]]:
    """Returns metadata for a whitelisted zone id (any site), else None."""
    t = (category_id or "").strip()
    info = MADOUER_TYPES.get(t) or DASJ91_CATEGORIES.get(t) or MADOUHD_CATEGORIES.get(t)
    if not info:
        return None
    return {"cn": info["cn"], "en": info["en"], "group": info.get("group", ""), "content_type": "video"}


def is_enabled(category_id: str) -> bool:
    t = (category_id or "").strip()
    return t in MADOUER_TYPES or t in DASJ91_CATEGORIES or t in MADOUHD_CATEGORIES


def is_site_enabled(site_key: str, category_id: str) -> bool:
    t = (category_id or "").strip()
    if site_key == "madouer":
        return t in MADOUER_TYPES
    if site_key == "dasj91":
        return t in DASJ91_CATEGORIES
    if site_key == "madouhd":
        return t in MADOUHD_CATEGORIES
    return False
