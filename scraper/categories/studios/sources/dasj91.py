"""
Source scraper for 91dasj.com (studios category, Phase B).

Site architecture (reverse-engineered 2026-11-25, see scratch/deep3/ANALYSIS_deep3.md):

  - Typecho blog (Mirages theme), Cloudflare-fronted but plain HTTP clients pass.
  - Listings: /category/{slug}/ — server-rendered <article itemscope> cards:
      cover via loadBannerDirect('https://pic.biacea.cn/...jpeg', ...)
      <time class="post-cover-date" datetime="...">
      <h2 class="post-card-title" itemprop="headline">{title}</h2>
      <a href="/archives/{id}/">
  - Pagination: /category/{slug}/{n}/ (NOT /page/n/).
  - Archive pages: one or more div.dplayer blocks, each with
      data-config="...escaped JSON..."  → video.url = m3u8 with ~1h auth_key
      data-video_id / data-video_title / data-video_tag_name (pipe-separated)
    Cover: <meta itemprop="image" content="https://pic.biacea.cn/...jpeg">

  STREAM POLICY: auth_key tokens expire in ~1 hour, so we NEVER store the
  stream URL. full_video_url is set to the sentinel "resolve"; playback goes
  through the Worker route /api/studios/resolve which fetches the archive at
  play time, extracts fresh m3u8 URLs, and rewrites them through the existing
  /api/player/stream pipeline (AES-128 keys on qw.mkimfs.cn get rewritten too).

  Zone policy: only staged/synthetic/commercial sections are whitelisted.
  All 吃瓜 (real-person leak/scandal) zones, 学生校园 and 每日大赛 are excluded
  as NCII risks; the ingest title blocklist stays as a second gate.
"""
import html as html_mod
import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from scrapling.fetchers import Fetcher  # type: ignore
except Exception:  # pragma: no cover - vendored fallback
    import sys as _sys
    from pathlib import Path as _Path
    _vendored = _Path(__file__).resolve().parents[3]  # .../scraper
    if str(_vendored) not in _sys.path:
        _sys.path.insert(0, str(_vendored))
    try:
        from scrapling.fetchers import Fetcher  # type: ignore
    except Exception:
        Fetcher = None

from scraper.categories.studios.categories import category_info, is_enabled

BASE_URL = "https://91dasj.com"
SOURCE_SITE = "dasj91"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# Sentinel stored in full_video_url: "play this via /api/studios/resolve".
RESOLVE_SENTINEL = "resolve"

_RE_ARTICLE = re.compile(r"<article itemscope[^>]*>(.*?)</article>", re.S)
_RE_CARD_LINK = re.compile(r'href="(/archives/(\d+)/)"')
_RE_CARD_TITLE = re.compile(r'post-card-title"[^>]*>([^<]+)<')
_RE_CARD_DATE = re.compile(r'<time[^>]*datetime="([^"]+)"')
_RE_CARD_COVER = re.compile(r"loadBannerDirect\('([^']+)'")
_RE_CONFIG = re.compile(r"data-config=(['\"])(.+?)\1", re.S)
_RE_VIDEO_ID = re.compile(r'data-video_id="([^"]+)"')
_RE_TAG_NAME = re.compile(r'data-video_tag_name="([^"]+)"')
_RE_META_COVER = re.compile(r'<meta itemprop="image" content="([^"]+)"')
_RE_META_DATE = re.compile(r'<meta itemprop="datePublished" content="([^"]+)"')
# Theme pagination bar exposes the exact total: 第 10 / 共 59 页
_RE_TOTAL_PAGES = re.compile(r'第\s*\d+\s*/\s*共\s*(\d+)\s*页')
_RE_EXTINF = re.compile(r'#EXTINF:([\d.]+)')


def _get_fetcher():
    if Fetcher is None:
        raise RuntimeError("scrapling Fetcher unavailable; cannot scrape dasj91")
    return Fetcher


def fetch_page(url: str, timeout: int = 30, retries: int = 3) -> Optional[Any]:
    last_err = None
    for attempt in range(retries):
        try:
            resp = _get_fetcher().get(url, stealthy_headers=True, timeout=timeout)
            status = getattr(resp, "status", 0)
            if status == 200:
                return resp
            if status in {403, 429, 500, 502, 503, 504} and attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
                continue
            last_err = f"HTTP {status}"
            break
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
    if last_err:
        print(f"    [dasj91] fetch failed {url[:90]}: {last_err}")
    return None


def _abs(url: Optional[str]) -> str:
    if not url:
        return ""
    u = str(url).strip()
    if not u:
        return ""
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("/"):
        u = urljoin_base(u)
    return u


def urljoin_base(path: str) -> str:
    return BASE_URL.rstrip("/") + path


# ─── Listing scrape ──────────────────────────────────────────────────────────

def scrape_listing(category_id: str, page_num: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Scrapes one listing page for a whitelisted dasj91 category slug.
    Page 1 uses /category/{slug}/; deeper pages /category/{slug}/{n}/.
    Returns (items, detected_max_pages).
    """
    slug = str(category_id).strip()
    if not is_enabled(slug):
        print(f"    [dasj91] category '{slug}' is not whitelisted. Skipping.")
        return [], None

    info = category_info(slug)
    page_num = max(1, int(page_num))
    if page_num == 1:
        url = f"{BASE_URL}/category/{slug}/"
    else:
        url = f"{BASE_URL}/category/{slug}/{page_num}/"
    resp = fetch_page(url)
    if not resp:
        return [], None

    page_html = getattr(resp, "html_content", "") or ""
    if len(page_html) < 5000 or "<article" not in page_html:
        return [], None

    items: List[Dict[str, Any]] = []
    seen_ids = set()
    for m in _RE_ARTICLE.finditer(page_html):
        block = m.group(1)
        link_m = _RE_CARD_LINK.search(block)
        if not link_m:
            continue
        path, post_id = link_m.group(1), link_m.group(2)
        if post_id in seen_ids:
            continue
        title_m = _RE_CARD_TITLE.search(block)
        title = re.sub(r"\s+", " ", html_mod.unescape(title_m.group(1))).strip() if title_m else ""
        if not title:
            # Fall back to the link text itself.
            tm = re.search(r'href="/archives/\d+/"[^>]*>([^<]+)<', block)
            title = re.sub(r"\s+", " ", html_mod.unescape(tm.group(1))).strip() if tm else ""
        if not title:
            continue
        seen_ids.add(post_id)

        cover = ""
        cover_m = _RE_CARD_COVER.search(block)
        if cover_m:
            cover = cover_m.group(1)
        published = None
        date_m = _RE_CARD_DATE.search(block)
        if date_m:
            try:
                published = datetime.strptime(date_m.group(1)[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc).isoformat()
            except Exception:
                published = None

        items.append({
            "source_site": SOURCE_SITE,
            "source_post_id": post_id,
            "content_type": "video",
            "title": title,
            "detail_url": urljoin_base(path),
            "thumbnail_url": cover or None,
            "clean_thumbnail_url": cover or None,
            "region": info.get("cn") or "",
            "region_en": info.get("en") or "",
            "published_at": published,
        })

    # Pagination: the theme's pagination bar exposes the exact total
    # (第 N / 共 M 页). Fall back to the highest linked page number.
    detected_max = None
    total_m = _RE_TOTAL_PAGES.search(page_html)
    if total_m:
        detected_max = int(total_m.group(1))
    else:
        pages = [int(p) for p in re.findall(r"/category/%s/(\d+)/" % re.escape(slug), page_html)]
        if pages:
            detected_max = max(pages)
    return items, detected_max


# ─── Detail enrichment (archive page) ────────────────────────────────────────

def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    """Fetches the archive page and extracts metadata + the resolve sentinel.

    Stream URLs are never stored (1h auth_key). Multi-part posts are handled
    at play time by the resolve route, which concatenates all parts.
    Sets `dead` when the archive page is unreachable.
    """
    merged = dict(card)
    resp = fetch_page(card.get("detail_url"), timeout=25, retries=2)
    if not resp:
        merged["dead"] = True
        return merged

    page_html = getattr(resp, "html_content", "") or ""
    configs = [m.group(2) for m in _RE_CONFIG.finditer(page_html)]
    if not configs:
        merged["dead"] = True
        return merged

    # Parse every part; keep the ones that carry a video url (fresh token).
    parts = 0
    first_part_url = ""
    for raw in configs:
        try:
            cfg = json.loads(html_mod.unescape(raw))
        except Exception:
            continue
        vurl = str((cfg.get("video") or {}).get("url") or "")
        if vurl.strip():
            parts += 1
            if not first_part_url:
                first_part_url = vurl
    if parts == 0:
        merged["dead"] = True
        return merged
    merged["parts"] = parts

    # Duration: fetch the first part's playlist and sum its EXTINF values
    # (one cheap request per new post; tokens are fresh here so it always 200s).
    try:
        import urllib.request as _ur
        req = _ur.Request(first_part_url, headers={"User-Agent": UA})
        playlist = _ur.urlopen(req, timeout=20).read().decode("utf-8", "replace")
        total_secs = sum(float(x) for x in _RE_EXTINF.findall(playlist))
        if total_secs > 0:
            merged["duration_seconds"] = int(round(total_secs))
            mins, secs = divmod(int(round(total_secs)), 60)
            merged["duration_formatted"] = f"{mins}:{secs:02d}" if mins < 60 else f"{mins // 60}:{mins % 60:02d}:{secs:02d}"
    except Exception as e:
        print(f"    [dasj91] duration probe failed for {card.get('source_post_id')}: {e}")

    # Sentinel instead of a stream URL: resolved fresh at play time.
    merged["full_video_url"] = RESOLVE_SENTINEL
    merged["video_mime_type"] = "application/x-mpegURL"

    # Tags: pipe-separated Chinese tags on the first dplayer block.
    tags: List[str] = []
    tag_m = _RE_TAG_NAME.search(page_html)
    if tag_m:
        for t in re.split(r"[|,]", tag_m.group(1)):
            t = t.strip()
            if t and t not in tags:
                tags.append(t)
    merged["tags"] = tags[:12]

    # Cover + date from meta tags (listing cover as fallback).
    cover_m = _RE_META_COVER.search(page_html)
    if cover_m:
        cover = _abs(cover_m.group(1))
        if cover:
            merged["thumbnail_url"] = cover
            merged["clean_thumbnail_url"] = cover
    date_m = _RE_META_DATE.search(page_html)
    if date_m:
        try:
            merged["published_at"] = datetime.strptime(date_m.group(1)[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            pass

    merged["resolution"] = None
    merged["quality_label"] = "HD"
    merged["quality_score"] = 720

    return merged
