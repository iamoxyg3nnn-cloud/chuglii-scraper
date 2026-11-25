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
    "cate22": {"cn": "AV女优", "en": "JAV Actresses", "group": "studios"},
    "cate23": {"cn": "户外野战", "en": "Outdoor", "group": "studios"},
    "cate24": {"cn": "颅内高潮", "en": "Mindgasm", "group": "studios"},
    "cate26": {"cn": "淫语歌曲", "en": "Erotic Songs", "group": "studios"},
    "cate28": {"cn": "Cosplay", "en": "Cosplay", "group": "studios"},
    "cate29": {"cn": "VR影片", "en": "VR Films", "group": "studios"},
    "cate30": {"cn": "18禁三级", "en": "Softcore III", "group": "studios"},
    "cate31": {"cn": "伪娘人妖", "en": "Femboy & Ladyboy", "group": "studios"},
    "cate9": {"cn": "强奸迷奸", "en": "Forced Fantasy", "group": "studios"},
    "cate27": {"cn": "真实破处", "en": "Defloration", "group": "studios"},
    "cate71": {"cn": "重口-尿", "en": "Watersports", "group": "studios"},
    "cate72": {"cn": "重口-屎", "en": "Scat", "group": "studios"},
    "cate173": {"cn": "猎奇", "en": "Bizarre Extreme", "group": "studios"},
    "cate36": {"cn": "kittyxkum", "en": "kittyxkum", "group": "studios"},
    "cate37": {"cn": "桥本香菜", "en": "Hashimoto Kana", "group": "studios"},
    "cate38": {"cn": "玩偶姐姐", "en": "Doll Sister", "group": "studios"},
    "cate39": {"cn": "懒懒睡不醒", "en": "Lazy Sleepy", "group": "studios"},
    "cate40": {"cn": "Yui+Peachpie", "en": "Yui & Peachpie", "group": "studios"},
    "cate41": {"cn": "网红-辛尤里", "en": "Xin Yuli", "group": "studios"},
    "cate42": {"cn": "女神多乙", "en": "Duo Yi", "group": "studios"},
    "cate43": {"cn": "网红司雨", "en": "Si Yu", "group": "studios"},
    "cate44": {"cn": "台北娜娜", "en": "Taipei Nana", "group": "studios"},
    "cate45": {"cn": "唐伯虎", "en": "Tang Bohu", "group": "studios"},
    "cate46": {"cn": "Cola酱", "en": "Cola Jiang", "group": "studios"},
    "cate47": {"cn": "荡女刘玥", "en": "Liu Yue", "group": "studios"},
    "cate48": {"cn": "饼干姐姐", "en": "Cookie Sister", "group": "studios"},
    "cate49": {"cn": "Daisybaby", "en": "Daisybaby", "group": "studios"},
    "cate50": {"cn": "奶咪", "en": "Naimi", "group": "studios"},
    "cate51": {"cn": "ChineseBunny", "en": "ChineseBunny", "group": "studios"},
    "cate52": {"cn": "芋圆呀呀", "en": "Yuyu Yaya", "group": "studios"},
    "cate53": {"cn": "高清JVID", "en": "JVID HD", "group": "studios"},
    "cate55": {"cn": "精品AV", "en": "Premium AV", "group": "studios"},
    "cate56": {"cn": "无码AV", "en": "Uncensored AV", "group": "studios"},
    "cate57": {"cn": "无码流出", "en": "Uncensored Leaks", "group": "studios"},
    "cate58": {"cn": "中文字幕", "en": "Chinese Subtitles", "group": "studios"},
    "cate59": {"cn": "家庭乱伦", "en": "Family Taboo", "group": "studios"},
    "cate60": {"cn": "少女嫩妹", "en": "Young Girls", "group": "studios"},
    "cate61": {"cn": "职场制服", "en": "Office Uniform", "group": "studios"},
    "cate62": {"cn": "巨乳诱惑", "en": "Busty Temptation", "group": "studios"},
    "cate63": {"cn": "母乳孕妇", "en": "Lactating & Pregnant", "group": "studios"},
    "cate64": {"cn": "强制寝取NTR", "en": "NTR", "group": "studios"},
    "cate65": {"cn": "SM调教", "en": "SM Training", "group": "studios"},
    "cate66": {"cn": "水果Π-电影解说", "en": "Movie Commentary", "group": "studios"},
    "cate68": {"cn": "轻S-调教反差", "en": "Light SM", "group": "studios"},
    "cate69": {"cn": "拳交-肛交", "en": "Fisting & Anal", "group": "studios"},
    "cate70": {"cn": "同性-女调女", "en": "Lesbian", "group": "studios"},
    "cate73": {"cn": "SM-虐肛", "en": "SM Rough", "group": "studios"},
    "cate74": {"cn": "SM-虐阴", "en": "SM Torment", "group": "studios"},
    "cate75": {"cn": "SM-虐待", "en": "SM Abuse", "group": "studios"},
    "cate76": {"cn": "SM-捆绑", "en": "SM Bondage", "group": "studios"},
    "cate77": {"cn": "福利大全", "en": "Free-for-all", "group": "studios"},
    "cate79": {"cn": "国产精品", "en": "Chinese Premium", "group": "studios"},
    "cate80": {"cn": "麻豆出品", "en": "Madou Originals", "group": "studios"},
    "cate81": {"cn": "糖心Vlog", "en": "Tangxin Vlog", "group": "studios"},
    "cate82": {"cn": "茄子视频", "en": "Eggplant Video", "group": "studios"},
    "cate83": {"cn": "性视界", "en": "Sex Vision", "group": "studios"},
    "cate84": {"cn": "香蕉视频", "en": "Banana Video", "group": "studios"},
    "cate85": {"cn": "乌托邦", "en": "Utopia", "group": "studios"},
    "cate86": {"cn": "星空无限传媒", "en": "Starlight Media", "group": "studios"},
    "cate87": {"cn": "快播原创", "en": "Kuaibo Original", "group": "studios"},
    "cate88": {"cn": "皇家华人", "en": "Royal Chinese", "group": "studios"},
    "cate89": {"cn": "精东影业", "en": "Jingdong Pictures", "group": "studios"},
    "cate90": {"cn": "蜜桃影像", "en": "Peach Pictures", "group": "studios"},
    "cate91": {"cn": "果冻传媒", "en": "Jelly Media", "group": "studios"},
    "cate92": {"cn": "91制片厂", "en": "Studio 91", "group": "studios"},
    "cate93": {"cn": "天美传媒", "en": "Tianmei Media", "group": "studios"},
    "cate94": {"cn": "爱豆传媒", "en": "Aidou Media", "group": "studios"},
    "cate95": {"cn": "兔子先生", "en": "Mr. Rabbit", "group": "studios"},
    "cate97": {"cn": "扣扣传媒", "en": "Koukou Media", "group": "studios"},
    "cate98": {"cn": "杏吧原创", "en": "Xingba Original", "group": "studios"},
    "cate99": {"cn": "SA国际传媒", "en": "SA International", "group": "studios"},
    "cate101": {"cn": "兄妹相爱", "en": "Siblings", "group": "studios"},
    "cate102": {"cn": "偷腥嫂子", "en": "Cheating Sister-in-law", "group": "studios"},
    "cate103": {"cn": "偷欢小姨", "en": "Naughty Aunt", "group": "studios"},
    "cate104": {"cn": "师生乱伦", "en": "Teacher & Student", "group": "studios"},
    "cate105": {"cn": "全家乱P", "en": "Family Swap", "group": "studios"},
    "cate106": {"cn": "母子通奸", "en": "Mother & Son", "group": "studios"},
    "cate107": {"cn": "舅侄畸恋", "en": "Uncle & Niece", "group": "studios"},
    "cate108": {"cn": "姐弟情深", "en": "Sister & Brother", "group": "studios"},
    "cate109": {"cn": "父女忌恋", "en": "Father & Daughter", "group": "studios"},
    "cate110": {"cn": "儿媳诱惑", "en": "Daughter-in-law", "group": "studios"},
    "cate111": {"cn": "最爱岳母", "en": "Mother-in-law", "group": "studios"},
    "cate113": {"cn": "Pinkloving", "en": "Pinkloving", "group": "studios"},
    "cate114": {"cn": "Leah+Meow", "en": "Leah & Meow", "group": "studios"},
    "cate115": {"cn": "MollyRedWolf", "en": "MollyRedWolf", "group": "studios"},
    "cate116": {"cn": "Sia Siberia", "en": "Sia Siberia", "group": "studios"},
    "cate117": {"cn": "Zirael+Rem", "en": "Zirael & Rem", "group": "studios"},
    "cate118": {"cn": "Deluxe Bitch&#039;s", "en": "Deluxe Bitch", "group": "studios"},
    "cate119": {"cn": "Candy Love", "en": "Candy Love", "group": "studios"},
    "cate120": {"cn": "Purple Bitch", "en": "Purple Bitch", "group": "studios"},
    "cate121": {"cn": "Rosie rider&#039;s", "en": "Rosie Rider", "group": "studios"},
    "cate122": {"cn": "SweetieFox", "en": "SweetieFox", "group": "studios"},
    "cate123": {"cn": "FantasyBabe&#039;s", "en": "FantasyBabe", "group": "studios"},
    "cate124": {"cn": "MyAnny&#039;s", "en": "MyAnny", "group": "studios"},
    "cate125": {"cn": "Eva Elfie", "en": "Eva Elfie", "group": "studios"},
    "cate126": {"cn": "SkinLovers&#039;s", "en": "SkinLovers", "group": "studios"},
    "cate127": {"cn": "Melody Marks", "en": "Melody Marks", "group": "studios"},
    "cate128": {"cn": "Creamy Spot", "en": "Creamy Spot", "group": "studios"},
    "cate129": {"cn": "Ada Kham", "en": "Ada Kham", "group": "studios"},
    "cate131": {"cn": "精选推荐", "en": "Featured Picks", "group": "studios"},
    "cate132": {"cn": "少女嫩妹", "en": "Young Girls II", "group": "studios"},
    "cate133": {"cn": "法国剧情", "en": "French Drama", "group": "studios"},
    "cate134": {"cn": "巨乳肥臀", "en": "Busty & Curvy II", "group": "studios"},
    "cate135": {"cn": "啄木鸟", "en": "Woodpecker", "group": "studios"},
    "cate136": {"cn": "黑人巨屌", "en": "BBC", "group": "studios"},
    "cate137": {"cn": "全家乱伦", "en": "Family Taboo II", "group": "studios"},
    "cate138": {"cn": "中文字幕", "en": "Chinese Subtitles II", "group": "studios"},
    "cate139": {"cn": "成人电影", "en": "Adult Movies", "group": "studios"},
    "cate141": {"cn": "中文字幕", "en": "Chinese Subtitles III", "group": "studios"},
    "cate142": {"cn": "精品动漫", "en": "Premium Anime", "group": "studios"},
    "cate143": {"cn": "毁童年", "en": "Ruined Childhood", "group": "studios"},
    "cate144": {"cn": "最终幻想", "en": "Final Fantasy", "group": "studios"},
    "cate145": {"cn": "剧情中文", "en": "Chinese Plot", "group": "studios"},
    "cate146": {"cn": "完美世界", "en": "Perfect World", "group": "studios"},
    "cate147": {"cn": "近亲乱伦", "en": "Incest Anime", "group": "studios"},
    "cate148": {"cn": "电车痴汉", "en": "Train Molester", "group": "studios"},
    "cate149": {"cn": "3D同人", "en": "3D Doujin", "group": "studios"},
    "cate150": {"cn": "火影忍者", "en": "Naruto", "group": "studios"},
    "cate153": {"cn": "薇凹的性趣", "en": "Wei'ao", "group": "studios"},
    "cate154": {"cn": "王竹子教学", "en": "Wang Zhuzi", "group": "studios"},
    "cate155": {"cn": "Carrie雨千", "en": "Carrie Yuqian", "group": "studios"},
    "cate156": {"cn": "一三蜜桃说", "en": "Yisan Mitao", "group": "studios"},
    "cate157": {"cn": "1G.老湿", "en": "1G Laoshi", "group": "studios"},
    "cate158": {"cn": "许蓝方", "en": "Xu Lanfang", "group": "studios"},
    "cate159": {"cn": "小哥哥艾里", "en": "Ai Li", "group": "studios"},
    "cate160": {"cn": "misa 米砂", "en": "Misa", "group": "studios"},
    "cate161": {"cn": "中指通一下", "en": "Zhongzhi", "group": "studios"},
    "cate162": {"cn": "深夜保健室", "en": "Late-night Clinic", "group": "studios"},
    "cate164": {"cn": "原创", "en": "Original", "group": "studios"},
    "cate166": {"cn": "内射", "en": "Creampie", "group": "studios"},
    "cate167": {"cn": "乱伦", "en": "Incest", "group": "studios"},
    "cate168": {"cn": "母狗", "en": "Submissive", "group": "studios"},
    "cate170": {"cn": "PMV", "en": "PMV", "group": "studios"},
    "cate171": {"cn": "裸舞", "en": "Nude Dance", "group": "studios"},
    "cate172": {"cn": "口交", "en": "Blowjob", "group": "studios"},
    "cate174": {"cn": "混剪", "en": "Compilations", "group": "studios"},
}

MADOUHD_EXCLUDED: Dict[str, str] = {
    "cate7": "网红黑料 — real-person leak/scandal content, excluded as NCII risk",
    "cate10": "校园猛料 — campus content, excluded as NCII risk",
    "cate16": "摄像偷拍 — hidden-camera/voyeur content, excluded as NCII risk",
    "cate25": "人兽激情 — bestiality theme, excluded on policy grounds",
    "cate32": "抖音风 — douyin reuploads, excluded as NCII/reupload risk",
    "cate33": "裸贷曝光 — nude-debt leak exposés, excluded as NCII risk",
    "cate35": "全部 — sitewide superset listing, excluded to avoid duplicate rows",
    "cate96": "萝莉社 — hard site-wide exclusion",
    "cate165": "抖音 — douyin reupload tag, excluded as NCII/reupload risk",
    "cate169": "学生妹 — schoolgirl tag, excluded as minor-adjacent NCII risk",
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
