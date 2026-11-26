"""
madouhd.cc source — Phase C.

Custom PHP site (stui-vodlist theme, same UI kit as madouer). Plain Fetcher
with stealthy headers works; no heavy anti-bot.

Listing  : /category/{cateN}/            (page 1)
           /category/{cateN}/{page}/     (deeper pages)
Cards    : li.stui-vodlist__item > a.stui-vodlist__thumb[href=/video/{id}/]
           img.lazy[data-original=thumb] + h4.stui-vodlist__title a[title]
Detail   : div#article-videos data-url="/video/{date}/{x}/{id}/{hash}.m3u8"
           data-cdnline="https://fgds54gg.sxtmo.com"
           full stream = cdnline + data-url  (direct HLS, no auth token —
           same store-the-stream-URL strategy as madouer)

Thumbs and streams share the fgds54gg.sxtmo.com CDN.
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from scraper.categories.studios.categories import category_info, is_site_enabled
from scraper.categories.studios.errors import ListingUnavailable
from scraper import net as _net

try:
    from scrapling.fetchers import Fetcher
except Exception:  # pragma: no cover
    Fetcher = None  # type: ignore

BASE_URL = "https://madouhd.cc"
SOURCE_SITE = "madouhd"

_RE_EXTINF = re.compile(r"#EXTINF:([\d.]+)")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
STREAM_HOST = "fgds54gg.sxtmo.com"

_RE_CARD = re.compile(
    r'<a class="stui-vodlist__thumb[^"]*"\s+href="(/video/(\d+)/)"[^>]*title="([^"]*)"[^>]*>'
    r'.*?<img[^>]*data-original="([^"]+)"',
    re.S,
)
# data-url path prefix varies per section (/video/, /laosiji/, ...) — match any.
_RE_PLAYER = re.compile(
    r'id="article-videos"[^>]*data-url="(/[^"]+\.m3u8)"[^>]*data-cdnline="([^"]+)"',
    re.S,
)
_RE_DATE = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
_RE_VIEWS = re.compile(r"([\d.]+)\s*([KM万]?)\s*观看")
_RE_TAG_LINKS = re.compile(r'href="/category/cate\d+/"[^>]*>([^<]{1,24})</a>')
_RE_PAGE_LINKS = re.compile(r"/category/%s/(\d+)/" % r"(?:cate\d+)")


def is_enabled(category_id: str) -> bool:
    return is_site_enabled("madouhd", category_id)


def fetch_page(url: str, timeout: int = 30, retries: int = 3) -> Optional[Any]:
    """Fetch via the shared Cloudflare-aware, cached HTTP layer.

    This site rate-limits hard (429 after ~7 rapid hits), so the shared layer
    gets a wider 429 backoff window than its default.
    """
    page = _net.fetch(url, timeout=timeout, retries=retries, max_backoff=90.0)
    fetch_page.last_status = page.status if page is not None else None
    if page is not None and page.status == 200 and not page.challenge:
        return page
    if page is not None and page.via != "dead-cache" and page.status not in (404, 410):
        reason = "Cloudflare challenge" if page.challenge else f"HTTP {page.status}"
        print(f"    [madouhd] fetch failed {url[:90]}: {reason}")
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
        u = urljoin(BASE_URL, u)
    return u


def scrape_listing(category_id: str, page_num: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    cate = str(category_id).strip()
    if not is_enabled(cate):
        print(f"    [madouhd] category '{cate}' is not whitelisted. Skipping.")
        return [], None

    info = category_info(cate)
    page_num = max(1, int(page_num))
    url = f"{BASE_URL}/category/{cate}/" if page_num == 1 else f"{BASE_URL}/category/{cate}/{page_num}/"
    resp = fetch_page(url)
    if not resp:
        # 404 past the true end of the listing; anything else is transient.
        if getattr(fetch_page, "last_status", None) == 404:
            return [], None
        raise ListingUnavailable(f"HTTP {getattr(fetch_page, 'last_status', 'error')} on {url}")

    html = getattr(resp, "html_content", "") or ""
    if len(html) < 5000 or "stui-vodlist" not in html:
        if "暂无内容" in html:
            return [], None  # genuine empty listing (site's own marker)
        raise ListingUnavailable(f"malformed listing page {url}")

    items: List[Dict[str, Any]] = []
    seen_ids = set()
    for m in _RE_CARD.finditer(html):
        play_path, vod_id, title_attr, thumb = m.groups()
        if vod_id in seen_ids:
            continue
        seen_ids.add(vod_id)
        title = re.sub(r"\s+", " ", title_attr).strip()
        if not title:
            continue
        items.append({
            "source_site": SOURCE_SITE,
            "source_post_id": vod_id,
            "content_type": "video",
            "title": title,
            "detail_url": _abs(play_path),
            "thumbnail_url": _abs(thumb),
            "clean_thumbnail_url": _abs(thumb),
            "region": info.get("cn") or "",
            "region_en": info.get("en") or "",
            "published_at": None,  # filled at enrich time
        })

    # Pagination: highest linked /category/{cate}/{n}/ page for THIS category
    pages = [int(p) for p in re.findall(r"/category/%s/(\d+)/" % re.escape(cate), html)]
    detected_max = max(pages) if pages else None
    return items, detected_max


def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    """Fetches the play page, extracts the direct HLS URL, tags and date.

    Sets `dead` when the play page is unreachable so the pipeline keeps the
    item unmarked for retry instead of storing junk.
    """
    merged = dict(card)
    resp = fetch_page(card.get("detail_url"), timeout=25, retries=2)
    if not resp:
        merged["dead"] = True
        return merged

    html = getattr(resp, "html_content", "") or ""
    m = _RE_PLAYER.search(html)
    if not m:
        merged["dead"] = True
        return merged

    stream = (m.group(2).rstrip("/") + m.group(1)).strip()
    merged["full_video_url"] = stream
    merged["video_mime_type"] = "application/x-mpegURL"

    # Duration: fetch the playlist and sum its EXTINF values (playlists are
    # AES-KEY-protected but EXTINF lines stay plaintext).
    try:
        import urllib.request as _ur
        req = _ur.Request(stream, headers={"User-Agent": UA, "Referer": BASE_URL + "/"})
        playlist = _ur.urlopen(req, timeout=20).read().decode("utf-8", "replace")
        total_secs = sum(float(x) for x in _RE_EXTINF.findall(playlist))
        if total_secs > 0:
            merged["duration_seconds"] = int(round(total_secs))
            mins, secs = divmod(int(round(total_secs)), 60)
            merged["duration_formatted"] = f"{mins}:{secs:02d}" if mins < 60 else f"{mins // 60}:{mins % 60:02d}:{secs:02d}"
    except Exception as e:
        print(f"    [madouhd] duration probe failed for {card.get('source_post_id')}: {e}")

    md = _RE_DATE.search(html)
    if md:
        try:
            merged["published_at"] = (
                datetime.strptime(md.group(1), "%Y-%m-%d %H:%M:%S")
                .replace(tzinfo=timezone.utc)
                .isoformat()
            )
        except ValueError:
            merged["published_at"] = None

    mv = _RE_VIEWS.search(html)
    if mv:
        try:
            num = float(mv.group(1))
            unit = mv.group(2)
            if unit == "K":
                num *= 1_000
            elif unit == "M":
                num *= 1_000_000
            elif unit == "万":
                num *= 10_000
            merged["views"] = int(num)
        except ValueError:
            pass

    tags = [t.strip() for t in _RE_TAG_LINKS.findall(html) if t.strip()]
    if tags:
        merged["tags"] = tags[:8]

    return merged
