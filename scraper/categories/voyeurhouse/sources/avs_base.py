"""Shared AVS-family catalog adapter for additional isolated Voyeur House websites.

This module contains no persistent state and never consults another category's
database. Each source adapter supplies its own base URL, listing template and
``source_site`` key; everything here is generic parsing / stream resolution.
"""

from __future__ import annotations

import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from scraper import net as _net

try:
    from curl_cffi import requests as creq  # type: ignore
except Exception:  # pragma: no cover
    creq = None

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover
    BeautifulSoup = None

try:
    from scraper.proxy_pool import GLOBAL_PROXY_POOL  # type: ignore
except Exception:  # pragma: no cover
    GLOBAL_PROXY_POOL = None

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Doodstream-family hosts are geo-blocked from India and always need a proxy.
DOOD_HOST_RE = re.compile(
    r"(?:^|\.)(?:dood|c?d000d|doodstream|ds2play|bysezejataos)(?:\.|$)",
    re.I,
)
# Other locker hosts use a proxy only when one is explicitly configured.
LOCKER_HOST_RE = re.compile(
    r"(?:^|\.)(?:clicknplay|playtwo|vtube)(?:\.|$)",
    re.I,
)

DEAD_MARKERS = (
    "can't create video code",
    "cant create video code",
    "file not found",
    "video has been removed",
    "no longer available",
    "video is no longer available",
    "not found",
    "error 404",
    "404 not found",
    "unavailable",
    "deleted",
    "removed",
    "expired",
    "access denied",
    "no available adapters",
    "failed to contact router",
    "domain is parked",
    "this domain is parked",
)

# Gounlimited's embed now hands traffic to ParkLogic/headline portals instead of
# a video file. Treat that whole chain as a dead upstream, never as a playable
# embed fallback.
PARKED_GATE_RE = re.compile(
    r"gounlimited\.to|parklogic|headlinelogic|click-v4|junimedclk",
    re.I,
)


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def absolute(url: Optional[str], base: str) -> str:
    raw = clean_text(url)
    if not raw:
        return ""
    if raw.startswith("//"):
        return "https:" + raw
    try:
        return urljoin(base, raw)
    except Exception:
        return raw


def host_of(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def proxy_for(url: str) -> Optional[str]:
    """Returns a proxy for locker/media hosts (India geo-block workaround).

    An explicitly configured proxy is used for every locker family. The shared
    auto-proxy pool is only a fallback for Doodstream-family hosts, which are
    unreachable from India; other lockers are fetched directly when no proxy is
    configured (their CDNs work from most egresses with relaxed TLS).
    """
    host = host_of(url) or url
    is_dood = bool(DOOD_HOST_RE.search(host))
    is_locker = bool(LOCKER_HOST_RE.search(host))
    if not is_dood and not is_locker:
        return None
    proxy = (
        os.getenv("DOODSTREAM_PROXY_URL")
        or os.getenv("SCRAPER_PROXY_URL")
        or os.getenv("PROXY_URL")
        or os.getenv("RESIDENTIAL_PROXY")
    )
    if proxy:
        return proxy
    if is_dood and GLOBAL_PROXY_POOL is not None and hasattr(GLOBAL_PROXY_POOL, "get_proxy"):
        try:
            return GLOBAL_PROXY_POOL.get_proxy() or None
        except Exception:
            return None
    return None


def fetch_html(
    url: str,
    timeout: int = 25,
    referer: str = "",
    retries: int = 3,
) -> Tuple[str, str, int]:
    """Fetch a page with Chrome impersonation.

    Returns ``(html, final_url, status)``. Retries with relaxed TLS verification
    on certificate errors (several locker CDNs present broken chains) and routes
    locker hosts through the configured proxy chain where available.
    """
    if not url or creq is None:
        return "", url, 0
    proxy = proxy_for(url)
    headers = {
        "User-Agent": UA,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    if referer:
        headers["Referer"] = referer
    last_error: Any = None
    for attempt in range(retries):
        for verify in (True, False):
            kwargs: Dict[str, Any] = {
                "headers": headers,
                "impersonate": "chrome124",
                "timeout": timeout,
                "verify": verify,
            }
            if proxy:
                kwargs["proxy"] = proxy
            try:
                resp = creq.get(url, **kwargs)
                html = resp.text or ""
                status = int(getattr(resp, "status_code", 0) or 0)
                final_url = str(getattr(resp, "url", url) or url)
                resp_headers = dict(getattr(resp, "headers", {}) or {})
                # Never hand a Cloudflare interstitial to a parser as content.
                if _net.is_cf_challenge(status, html.encode("utf-8", errors="ignore"), resp_headers):
                    print(f"    [AVS] Cloudflare challenge on {url}; escalating to shared solver.")
                    page = _net.fetch(url, timeout=timeout, retries=1, referer=referer or None)
                    if page is not None and page.status == 200 and not page.challenge:
                        return page.html_content, url, 200
                    return "", url, 403
                return html, final_url, status
            except Exception as exc:
                msg = str(exc).lower()
                if "certificate" in msg or "ssl" in msg or "verify" in msg:
                    last_error = exc
                    continue
                last_error = exc
                break
        if attempt < retries - 1:
            time.sleep(1.2 * (attempt + 1))
    if last_error and proxy and GLOBAL_PROXY_POOL is not None and hasattr(GLOBAL_PROXY_POOL, "report_failure"):
        try:
            GLOBAL_PROXY_POOL.report_failure(proxy)
        except Exception:
            pass
    return "", url, 0


# ---------------------------------------------------------------------------
# P.A.C.K.E.R. unpacker (clicknplay / vtube lockers)
# ---------------------------------------------------------------------------

def _eval_arg(source: str, start: int) -> str:
    i = start + 5
    depth = 1
    quote = None
    escaped = False
    while i < len(source):
        ch = source[i]
        if quote:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                quote = None
            i += 1
            continue
        if ch in "\"'`":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return source[start + 5:i]
        i += 1
    return ""


def _b36(value: int) -> str:
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    out = ""
    while value:
        value, rem = divmod(value, 36)
        out = digits[rem] + out
    return out or "0"


def unpack_eval(html: str) -> str:
    decoded: List[str] = []
    pos = 0
    while True:
        start = html.find("eval(function", pos)
        if start < 0:
            break
        arg = _eval_arg(html, start)
        pos = start + 5 + len(arg) + 1
        match = re.search(
            r"}\('((?:\\.|[^'\\])*)',(\d+),(\d+),'((?:\\.|[^'\\])*)'\.split\('\|'\)",
            arg,
            re.S,
        )
        if not match:
            continue
        packed = match.group(1).replace("\\'", "'").replace("\\\\", "\\")
        count = int(match.group(3))
        words = match.group(4).replace("\\'", "'").split("|")
        while count > 0:
            count -= 1
            if count < len(words) and words[count]:
                token = _b36(count)
                replacement = words[count]
                packed = re.sub(r"\b" + token + r"\b", lambda _m, r=replacement: r, packed)
        decoded.append(packed)
    return "\n".join(decoded)


def decode_at_hex_pages(html: str) -> str:
    """Decode AVS' ``str='@3C@69...'`` document.write iframe wrappers."""
    decoded: List[str] = []
    for match in re.finditer(r"str\s*=\s*['\"]([@0-9A-Fa-f]+)['\"]", html or ""):
        hex_part = match.group(1).replace("@", "")
        if not hex_part or len(hex_part) % 2:
            continue
        try:
            decoded.append(bytes.fromhex(hex_part).decode("utf-8", "ignore"))
        except Exception:
            continue
    return "\n".join(decoded)


def extract_direct_url(html: str, final_url: str) -> str:
    decoded = unpack_eval(html)
    candidates: List[str] = []
    candidates.extend(re.findall(r"file\s*:\s*[\"']([^\"']+)[\"']", decoded, re.I))
    candidates.extend(re.findall(r"<source\b[^>]*\bsrc=[\"']([^\"']+)[\"']", html, re.I | re.S))
    candidates.extend(re.findall(r"https?://[^\"'<>\s]+\.(?:mp4|m3u8)[^\"'<>\s]*", html, re.I))
    candidates.extend(re.findall(r"https?://[^\"'<>\s]+\.(?:mp4|m3u8)[^\"'<>\s]*", decoded, re.I))
    for raw in candidates:
        value = clean_text(raw).replace("\\/", "/")
        if ".mp4" in value.lower() or ".m3u8" in value.lower():
            return absolute(value, final_url)
    return ""


def resolve_stream(embed_url: str, referer: str, depth: int = 6) -> Tuple[str, str, bool]:
    """Follows locker iframe chains. Returns (direct_url, last_url, is_dead)."""
    url = embed_url
    for _ in range(depth):
        if not url:
            return "", "", True
        html, final_url, status = fetch_html(url, referer=referer)
        if not html or status == 0:
            is_locker = bool(LOCKER_HOST_RE.search(host_of(url)) or DOOD_HOST_RE.search(host_of(url)))
            return "", url, not is_locker
        decoded_html = decode_at_hex_pages(html)
        scan_html = f"{html}\n{decoded_html}"
        low = scan_html.lower()
        if PARKED_GATE_RE.search(f"{url} {final_url} {low}"):
            return "", final_url, True
        direct = extract_direct_url(scan_html, final_url)
        if direct:
            return direct, final_url, False
        if any(marker in low for marker in DEAD_MARKERS):
            return "", final_url, True
        soup = BeautifulSoup(scan_html, "lxml") if BeautifulSoup is not None else None
        iframes: List[str] = []
        if soup is not None:
            iframes = [f.get("src", "") for f in soup.select("iframe[src]")]
        else:
            iframes = re.findall(r"<iframe\b[^>]*\bsrc=[\"']([^\"']+)[\"']", scan_html, re.I | re.S)
        iframes = [
            f for f in iframes
            if f and "a-ads" not in f and "google" not in f and "doubleclick" not in f
        ]
        if not iframes:
            return "", final_url, False
        next_url = absolute(iframes[-1], final_url)
        if not next_url or next_url == url:
            return "", final_url, True
        url, referer = next_url, final_url
    return "", url, False


# ---------------------------------------------------------------------------
# Generic listing / detail parsing
# ---------------------------------------------------------------------------

def listing_url(template: str, page: int) -> str:
    if page <= 1:
        return template
    if "?" in template:
        return f"{template}&page={page}"
    return f"{template}?page={page}"


def scrape_listing(
    html: str,
    base_url: str,
    source_site: str,
    default_category: str,
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Parses an AVS-style listing page into isolated cards."""
    items: List[Dict[str, Any]] = []
    seen = set()
    soup = BeautifulSoup(html, "lxml") if BeautifulSoup is not None else None

    if soup is not None:
        anchors = soup.select('a[href^="/video/"]') + soup.select('a[href*="/video/"]')
        for anchor in anchors:
            href = anchor.get("href", "")
            match = re.search(r"/video/(\d+)/", href)
            if not match:
                continue
            post_id = match.group(1)
            if post_id in seen:
                continue
            card = anchor.parent or anchor
            title_el = card.select_one(".video-title, .content-title, h3, h4")
            title = clean_text(title_el.get_text(" ", strip=True)) if title_el else ""
            img = card.select_one(".thumb-overlay img, img")
            thumb = ""
            if img is not None:
                thumb = absolute(
                    img.get("src") or img.get("data-src") or img.get("data-original") or "",
                    base_url,
                )
                if not title:
                    title = clean_text(img.get("title") or img.get("alt") or "")
                if "placeholder" in thumb.lower() or thumb.endswith("melody-lzld.png"):
                    thumb = ""
            duration_el = card.select_one(".duration")
            duration = clean_text(duration_el.get_text(" ", strip=True)) if duration_el else ""
            views_el = card.select_one(".video-views, .views")
            views_raw = clean_text(views_el.get_text(" ", strip=True)) if views_el else ""
            age_el = card.select_one(".video-rating, .content-info time")
            age_raw = clean_text(age_el.get_text(" ", strip=True)) if age_el else ""
            seen.add(post_id)
            items.append({
                "source_site": source_site,
                "source_post_id": post_id,
                "title": title or f"{default_category} {post_id}",
                "detail_url": absolute(href, base_url),
                "thumbnail_url": thumb,
                "clean_thumbnail_url": thumb,
                "duration_formatted": duration,
                "views_raw": views_raw,
                "views_count": _parse_count(views_raw),
                "categories": [default_category],
            })
    else:  # pragma: no cover - fallback when bs4 unavailable
        for match in re.finditer(r'href=["\']([^"\']*?/video/(\d+)/[^"\']*)["\']', html, re.I):
            post_id = match.group(2)
            if post_id in seen:
                continue
            seen.add(post_id)
            items.append({
                "source_site": source_site,
                "source_post_id": post_id,
                "title": f"{default_category} {post_id}",
                "detail_url": absolute(match.group(1), base_url),
                "thumbnail_url": "",
                "clean_thumbnail_url": "",
                "duration_formatted": "",
                "views_raw": "",
                "views_count": 0,
                "categories": [default_category],
            })

    pages = [int(p) for p in re.findall(r"(?:\?|&)page=(\d+)", html, re.I)]
    return items, (max(pages) if pages else None)


def _parse_count(text: str) -> int:
    match = re.search(r"([\d,.]+)\s*(?:views|view)?", text or "", re.I)
    if not match:
        return 0
    try:
        return int(match.group(1).replace(",", "").replace(".", ""))
    except Exception:
        return 0


def enrich_detail(card: Dict[str, Any], base_url: str, default_category: str) -> Dict[str, Any]:
    detail_url = card.get("detail_url", "")
    html, final_url, status = fetch_html(detail_url, referer=base_url)
    card.setdefault("status", "active")
    card.setdefault("dead_reason", None)
    if status in (404, 410) or "/notfound" in final_url.lower():
        # A confirmed missing page is genuinely dead.
        card["status"] = "dead"
        card["dead_reason"] = "not_found"
        return card
    if not html or status == 0 or status >= 400:
        # Network/anti-bot/5xx failures are transient: never mark the post dead
        # or the ID is skipped forever after one challenged request.
        card["status"] = "unresolved"
        card["dead_reason"] = "detail_fetch_failed"
        return card

    soup = BeautifulSoup(html, "lxml") if BeautifulSoup is not None else None
    if soup is not None:
        og_title = soup.select_one('meta[property="og:title"]')
        if og_title and og_title.get("content"):
            card["title"] = clean_text(og_title["content"]) or card.get("title")
        og_image = soup.select_one('meta[property="og:image"]')
        if og_image and og_image.get("content"):
            thumb = absolute(og_image["content"], final_url)
            card["thumbnail_url"] = thumb
            card["clean_thumbnail_url"] = thumb
        description = soup.select_one('meta[name="description"], meta[property="og:description"]')
        card["description"] = clean_text(description.get("content", "")) if description else ""

        tags: List[str] = []
        for meta in soup.select('meta[property="video:tag"]'):
            tag = clean_text(meta.get("content", "")).lower()
            if tag and tag not in tags:
                tags.append(tag)
        for link in soup.select("a.tag"):
            tag = clean_text(link.get_text(" ", strip=True)).lower()
            if tag and tag not in tags:
                tags.append(tag)
        keywords = soup.select_one('meta[name="keywords"]')
        if keywords and keywords.get("content"):
            for tag in keywords["content"].split(","):
                tag = clean_text(tag).lower()
                if tag and tag not in tags:
                    tags.append(tag)
        card["tags"] = tags[:30]

        views_el = soup.select_one(".big-views span, .video-views")
        if views_el is not None:
            raw = clean_text(views_el.get_text(" ", strip=True))
            if raw:
                card["views_raw"] = raw
                card["views_count"] = _parse_count(raw)

        frames = [f.get("src", "") for f in soup.select(".video-embedded iframe[src], iframe[src]")]
    else:  # pragma: no cover
        tags = [clean_text(t).lower() for t in re.findall(r'<meta[^>]+property=["\']video:tag["\'][^>]+content=["\']([^"\']+)', html, re.I)]
        card["tags"] = tags[:30]
        card["description"] = ""
        frames = re.findall(r"<iframe\b[^>]*\bsrc=[\"']([^\"']+)[\"']", html, re.I | re.S)

    frames = [
        f for f in frames
        if f and "a-ads" not in f and "google" not in f and "doubleclick" not in f
    ]
    embed_url = absolute(frames[0], final_url) if frames else ""
    card["embed_url"] = embed_url
    card["categories"] = card.get("categories") or [default_category]

    if not embed_url:
        card["status"] = "dead"
        card["dead_reason"] = "no_embed"
        return card

    direct, last_url, is_dead = resolve_stream(embed_url, referer=final_url)
    servers: List[Dict[str, Any]] = []
    if direct:
        card["full_video_url"] = direct
        card["video_mime_type"] = "video/mp4"
        card["status"] = "active"
        card["dead_reason"] = None
        servers.append({
            "name": f"{default_category} Direct",
            "server": host_of(direct) or "Direct",
            "type": "direct_mp4",
            "url": direct,
            "embed_url": direct,
            "quality_score": 720,
            "quality_label": "720p HD",
        })
    else:
        card["full_video_url"] = last_url or embed_url
        card["video_mime_type"] = "embed/iframe"
        card["status"] = "dead" if is_dead else "active"
        card["dead_reason"] = "embed_dead" if is_dead else None
        servers.append({
            "name": f"{host_of(last_url or embed_url) or 'Embed'} Embed",
            "server": host_of(last_url or embed_url) or "Embed",
            "type": "iframe",
            "url": last_url or embed_url,
            "embed_url": embed_url,
            "quality_score": 720,
            "quality_label": "720p HD",
        })
    card["servers"] = servers
    return card
