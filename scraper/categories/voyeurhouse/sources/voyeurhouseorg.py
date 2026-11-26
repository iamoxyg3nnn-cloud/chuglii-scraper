"""Voyeur House source scraper (https://voyeur-house.org).

Isolated source adapter for Voyeur House. This module intentionally stores a
``source_site`` of ``voyeurhouseorg`` and uses stable post IDs from the detail
URL slug (``video_<id>.html``). No data from other categories is consulted.
"""

from __future__ import annotations

import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urljoin

try:
    from scrapling import Fetcher  # type: ignore
except Exception:  # pragma: no cover
    Fetcher = None

from scraper.proxy_pool import GLOBAL_PROXY_POOL
from scraper import net as _net

BASE_URL = "https://voyeur-house.org"
NEW_URL = f"{BASE_URL}/newvideos.html"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

SOURCE_SITE = "voyeurhouseorg"
LABEL = "Voyeur House"

DOOD_HOST_RE = re.compile(
    r"(?:^|\.)(?:dood|c?d000d|doodstream|dood\.to|dood\.ws|dood\.la|ds2play|bysezejataos)(?:\.|$)",
    re.I,
)
DEAD_MARKERS = (
    "video has been removed",
    "file not found",
    "not found",
    "no longer available",
    "unavailable",
    "deleted",
    "removed",
    "expired",
    "access denied",
    "owner of this video",
    "error 404",
)
MEDIA_MARKERS = ("<source", ".mp4", ".m3u8", "video%2fvh%2f", "file:", "sources :", "sources:", "fluidplayer")


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if hasattr(value, "clean"):
            value = value.clean()
    except Exception:
        pass
    return re.sub(r"\s+", " ", str(value)).strip()


def absolute(url: Optional[str], base: str = BASE_URL + "/") -> str:
    if not url:
        return ""
    value = str(url).strip()
    if value.startswith("//"):
        return "https:" + value
    if value.startswith("/"):
        return urljoin(base, value)
    return urljoin(base, value)


def extract_post_id(url: str) -> str:
    match = re.search(r"video_([a-z0-9]+)\.html?(?:[?#]|$)", url or "", re.I)
    return match.group(1).lower() if match else ""


def _proxy_for(url: str) -> Optional[str]:
    """Returns a proxy for Doodstream-family hosts (India geo-block workaround)."""
    host = ""
    try:
        from urllib.parse import urlparse

        host = (urlparse(url).hostname or "").lower()
    except Exception:
        host = (url or "").lower()
    if not DOOD_HOST_RE.search(host):
        return None
    proxy = (
        os.getenv("DOODSTREAM_PROXY_URL")
        or os.getenv("SCRAPER_PROXY_URL")
        or os.getenv("PROXY_URL")
        or os.getenv("RESIDENTIAL_PROXY")
    )
    if not proxy:
        proxy = GLOBAL_PROXY_POOL.get_proxy() if hasattr(GLOBAL_PROXY_POOL, "get_proxy") else None
    return proxy or None


def fetch_page(url: str, timeout: int = 25, retries: int = 3, *, referer: str = BASE_URL + "/", force_proxy: bool = False) -> Optional[Any]:
    """Fetch through the shared Cloudflare-aware cache.

    Proxy-free requests go through :mod:`scraper.net` (cache, politeness and
    Cloudflare solving). Proxy-bound requests keep the rotating-proxy path but
    still get challenge detection: a Cloudflare interstitial is never returned
    as if it were page content, and it falls back to the shared solver.
    """
    proxy = _proxy_for(url) if (force_proxy or DOOD_HOST_RE.search(url or "")) else None

    if not proxy:
        page = _net.fetch(url, timeout=timeout, retries=retries, referer=referer)
        if page is not None and page.status == 200 and not page.challenge:
            return page
        if page is not None and page.status not in (404, 410):
            reason = "Cloudflare challenge" if page.challenge else f"HTTP {page.status}"
            print(f"    [VH] fetch failed {url}: {reason}")
        return None

    last_error: Any = None
    for attempt in range(retries):
        try:
            kwargs: Dict[str, Any] = {
                "timeout": timeout,
                "headers": {
                    "User-Agent": UA,
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": referer,
                },
                "proxy": proxy,
                "proxies": {"http": proxy, "https": proxy},
            }
            print(f"    [VH] using proxy for {url}")
            resp = Fetcher.get(url, **kwargs) if Fetcher is not None else None
            if resp is None:
                return None
            status = int(getattr(resp, "status", 0) or 0)
            raw_body = bytes(getattr(resp, "body", b"") or b"")
            resp_headers = dict(getattr(resp, "headers", {}) or {})
            if _net.is_cf_challenge(status, raw_body, resp_headers):
                print(f"    [VH] Cloudflare challenge via proxy on {url}; retrying without proxy.")
                page = _net.fetch(url, timeout=timeout, retries=retries, referer=referer)
                if page is not None and page.status == 200 and not page.challenge:
                    return page
                return None
            if status == 200:
                return resp
            last_error = f"HTTP {status}"
            if status in {403, 429, 500, 502, 503, 504} and attempt < retries - 1:
                if hasattr(GLOBAL_PROXY_POOL, "report_failure"):
                    GLOBAL_PROXY_POOL.report_failure(proxy)
                time.sleep(1.5 * (attempt + 1))
                continue
            break
        except Exception as exc:  # pragma: no cover
            last_error = exc
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
    if last_error:
        print(f"    [VH] fetch failed {url}: {last_error}")
    return None


def _html(resp: Any) -> str:
    if resp is None:
        return ""
    return str(getattr(resp, "html_content", "") or getattr(resp, "text", "") or "")


def scrape_listing(page_num: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    url = NEW_URL if page_num <= 1 else f"{NEW_URL}?page={page_num}"
    resp = fetch_page(url)
    if not resp:
        return [], None
    html = _html(resp)

    items: List[Dict[str, Any]] = []
    seen = set()
    for block in re.findall(r"<li\b[^>]*>(.*?)</li>", html, re.I | re.S):
        if "video_" not in block or "-video_" not in block:
            continue
        link_match = re.search(r'href=["\']([^"\']*video_[a-z0-9]+\.html[^"\']*)["\']', block, re.I)
        if not link_match:
            continue
        detail_url = absolute(link_match.group(1))
        post_id = extract_post_id(detail_url)
        if not post_id or post_id in seen:
            continue

        title_match = re.search(r"<h3[^>]*>\s*<a[^>]+>(.*?)</a>", block, re.I | re.S)
        if not title_match:
            title_match = re.search(r"<h3[^>]*>(.*?)</h3>", block, re.I | re.S)
        title = clean_text(re.sub(r"<[^>]+>", "", title_match.group(1))) if title_match else ""
        if not title:
            a_title = re.search(r'<a[^>]+title=["\']([^"\']+)["\']', block, re.I)
            title = clean_text(a_title.group(1)) if a_title else ""

        thumb_match = re.search(r'data-echo=["\']([^"\']+)["\']', block, re.I) or re.search(r'<img[^>]+src=["\']([^"\']+)["\']', block, re.I)
        thumb = absolute(thumb_match.group(1)) if thumb_match else ""
        if thumb.endswith("melody-lzld.png") or "placeholder" in thumb.lower():
            thumb = ""

        duration_match = re.search(r"pm-label-duration[^>]*>([^<]+)", block, re.I)
        duration = clean_text(duration_match.group(1)) if duration_match else ""

        seen.add(post_id)
        items.append(
            {
                "source_site": SOURCE_SITE,
                "source_post_id": post_id,
                "title": title or f"Voyeur House {post_id}",
                "detail_url": detail_url,
                "thumbnail_url": thumb,
                "duration_formatted": duration,
            }
        )

    max_page = None
    page_hits = [int(p) for p in re.findall(r"page=(\d+)", html, re.I)]
    if page_hits:
        max_page = max(page_hits)
    return items, max_page


def _extract_iframe_src(html: str) -> str:
    match = re.search(r'<iframe\b[^>]*\bsrc=["\']([^"\']+)["\']', html, re.I | re.S)
    if not match:
        return ""
    src = clean_text(match.group(1))
    if src.startswith("//"):
        src = "https:" + src
    elif src.startswith("/"):
        src = urljoin(BASE_URL, src)
    return src


def _resolve_reallifecams_embed(embed_url: str) -> Tuple[Optional[str], Optional[str], bool]:
    """Resolves video.reallifecams.net watchvh.php embed to its direct MP4."""
    resp = fetch_page(embed_url, timeout=25, referer=BASE_URL + "/")
    html = _html(resp)
    if not html:
        return None, embed_url, True

    low = html.lower()
    if any(marker in low for marker in DEAD_MARKERS):
        return None, embed_url, True

    source_match = re.search(r'<source\b[^>]*\bsrc=["\']([^"\']+)["\']', html, re.I | re.S)
    if not source_match:
        source_match = re.search(r"video%2fvh%2f[^\"'\s<]+", html, re.I)
    if source_match:
        raw = unquote(clean_text(source_match.group(1)))
        if raw.startswith("//"):
            raw = "https:" + raw
        elif raw.startswith("/"):
            raw = urljoin("https://video.reallifecams.net", raw)
        elif not raw.startswith("http"):
            raw = urljoin("https://video.reallifecams.net/", raw)
        if raw.lower().endswith(".mp4") or "video/vh/" in raw.lower():
            return raw, embed_url, False
    return None, embed_url, True


def _resolve_embed(embed_url: str) -> Tuple[Optional[str], Optional[str], bool]:
    """Returns (direct stream or None, embed fallback URL, is_dead)."""
    if not embed_url:
        return None, "", True
    host = ""
    try:
        from urllib.parse import urlparse

        host = (urlparse(embed_url).hostname or "").lower()
    except Exception:
        host = embed_url.lower()

    if "video.reallifecams.net" in host or "watchvh.php" in embed_url.lower():
        return _resolve_reallifecams_embed(embed_url)

    # Doodstream-family and other lockers are kept as playable embed fallbacks.
    # Fetch them through a proxy where required, and quarantine obvious dead pages.
    resp = fetch_page(embed_url, timeout=25, referer=BASE_URL + "/")
    html = _html(resp)
    if resp is None:
        # A proxy is mandatory for Doodstream from India; if absent, do not mark
        # dead based on the blocked response. Keep as an embed fallback.
        if DOOD_HOST_RE.search(host):
            return None, embed_url, False
        return None, embed_url, True
    low = html.lower()
    fatal_dead = ("file not found", "video has been removed", "no longer available", "error 404", "not found")
    has_media = any(marker in low for marker in MEDIA_MARKERS)
    if any(marker in low for marker in fatal_dead) and not has_media:
        return None, embed_url, True
    return None, embed_url, False


def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    resp = fetch_page(card.get("detail_url", ""))
    html = _html(resp)
    if not html:
        # Transient fetch/bot-wall failure: keep it retryable instead of letting
        # the polisher classify a missing stream as a permanently dead post.
        card["status"] = "unresolved"
        card["dead_reason"] = "detail_fetch_failed"
        return card

    h1 = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.I | re.S)
    if h1:
        card["title"] = clean_text(re.sub(r"<[^>]+>", "", h1.group(1))) or card["title"]

    og_image = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)', html, re.I)
    if og_image:
        card["thumbnail_url"] = absolute(og_image.group(1))
        card["clean_thumbnail_url"] = card["thumbnail_url"]

    keywords = re.search(r'<meta[^>]+name=["\']keywords["\'][^>]+content=["\']([^"\']+)', html, re.I)
    tags = [clean_text(t).lower() for t in keywords.group(1).split(",")] if keywords else []
    tag_links = re.findall(r'<a[^>]+href=["\']([^"\']*/tags/[^"\']+)["\'][^>]*>(.*?)</a>', html, re.I)
    for _, label in tag_links:
        tag = clean_text(re.sub(r"<[^>]+>", "", label)).lower()
        if tag and tag not in tags:
            tags.append(tag)
    card["tags"] = [t for t in tags if t][:30]

    actors = [t for t in card["tags"] if len(t) > 2]
    card["actors"] = actors[:8]

    iframe = _extract_iframe_src(html)
    card["embed_url"] = iframe
    card["video_mime_type"] = "embed/iframe"
    card["status"] = "active"
    card["dead_reason"] = None

    direct, fallback, is_dead = _resolve_embed(iframe)
    if direct:
        card["full_video_url"] = direct
        card["video_mime_type"] = "video/mp4"
        card["status"] = "active"
    elif fallback:
        card["full_video_url"] = fallback
        card["video_mime_type"] = "embed/iframe"
        card["status"] = "dead" if is_dead else "active"
        if is_dead:
            card["dead_reason"] = "embed_dead"
    else:
        card["status"] = "dead"
        card["dead_reason"] = "no_embed"

    card["servers"] = []
    if direct:
        card["servers"].append({"name": "Voyeur House Direct", "server": "Voyeur House", "type": "direct_mp4", "embed_url": direct, "url": direct, "quality_score": 720, "quality_label": "720p HD"})
    if fallback and fallback != direct:
        host = ""
        try:
            from urllib.parse import urlparse

            host = urlparse(fallback).hostname or "Embed"
        except Exception:
            host = "Embed"
        card["servers"].append({"name": f"{host} Embed", "server": host, "type": "iframe", "embed_url": fallback, "url": fallback, "quality_score": 720, "quality_label": "720p HD"})

    return card
