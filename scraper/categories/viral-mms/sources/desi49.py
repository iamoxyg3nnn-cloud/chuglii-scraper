"""Source adapter for Desi49 (https://desi49.buzz)."""
import hashlib
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from curl_cffi import requests as cf_requests

from .base import (
    abs_url,
    clean_text,
    fetch_html,
    is_ad_iframe,
    parse_soup,
    verify_embed_alive,
    verify_stream_alive,
)

BASE_URL = "https://desi49.buzz"
SOURCE_NAME = "desi49"
_SESSION = cf_requests.Session()


def _slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    return path.split("/")[-1] if path else ""


def scrape_listing(page_num: int = 1) -> Tuple[List[Dict[str, Any]], Optional[int], bool]:
    url = f"{BASE_URL}/" if page_num <= 1 else f"{BASE_URL}/page/{page_num}/"
    html, status = fetch_html(url, session=_SESSION, referer=BASE_URL + "/")
    if status == 404:
        return [], page_num, True
    if not html:
        return [], None, False

    soup = parse_soup(html)
    items: List[Dict[str, Any]] = []
    seen_urls = set()

    for a in soup.select("a.thumb[href]"):
        detail_url = abs_url(BASE_URL, a.get("href"))
        if not detail_url or detail_url in seen_urls:
            continue
        if any(skip in detail_url for skip in ("/category/", "/tag/", "/page/")):
            continue
        seen_urls.add(detail_url)

        slug = _slug_from_url(detail_url)
        if not slug:
            continue

        img = a.select_one("img")
        title = clean_text(a.get("title")) or clean_text(img.get("alt") if img else "")
        if not title:
            title = slug.replace("-", " ").title()

        thumb = ""
        if img:
            thumb = abs_url(
                BASE_URL,
                img.get("data-src") or img.get("data-lazy-src") or img.get("src"),
            )

        dur_el = a.select_one(".duration, .time")
        # Fallback deterministic ID from slug if detail doesn't expose WP postid.
        fallback_id = hashlib.sha1(slug.encode("utf-8")).hexdigest()[:12]

        items.append({
            "source_site": SOURCE_NAME,
            "post_id": slug or fallback_id,
            "slug": slug,
            "title": title,
            "url": detail_url,
            "media": {
                "main_thumbnail": thumb,
                "thumbnail_url": thumb,
                "clean_thumbnail": thumb,
            },
            "metadata": {
                "duration": clean_text(dur_el.get_text(" ", strip=True) if dur_el else ""),
            },
            "taxonomy": {
                "categories": ["viral-mms"],
                "tags": [],
                "actors": [],
            },
        })

    max_page = page_num
    for pag_a in soup.select(".pagination a[href], .nav-links a[href], a.page-numbers[href]"):
        m = re.search(r"/page/(\d+)/?", pag_a.get("href", ""))
        if m:
            max_page = max(max_page, int(m.group(1)))

    return items, max_page, True


def enrich_detail(card: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
    detail_url = card.get("url") or ""
    if not detail_url:
        return None, "dead"

    html, status = fetch_html(detail_url, session=_SESSION, referer=BASE_URL + "/")
    if status == 404:
        return None, "dead"
    if not html:
        return None, "error"

    soup = parse_soup(html)

    # Keep slug as deterministic source_post_id so listing-level dedup matches DB.
    wp_post_id = ""
    if soup.body:
        body_cls = " ".join(soup.body.get("class") or [])
        m_pid = re.search(r"\bpostid-(\d+)\b", body_cls)
        if m_pid:
            wp_post_id = m_pid.group(1)

    h1 = soup.select_one("h1.entry-title, .entry-header h1, main h1, article h1")
    h1_txt = clean_text(h1.get_text(" ", strip=True) if h1 else "")
    if h1_txt and "desi49" in h1_txt.lower():
        h1_txt = ""
    title = h1_txt or card.get("title") or ""

    # Collect direct video candidates first, then non-ad iframe embeds.
    servers: List[Dict[str, Any]] = []
    seen_streams = set()

    raw_directs: List[str] = []
    for node in soup.select("video source[src], video[src]"):
        src = abs_url(BASE_URL, node.get("src"))
        if src:
            raw_directs.append(src)
    for m in re.finditer(r'https?://[^\s"\'<>\\]+\.(?:mp4|m3u8)(?:\?[^\s"\'<>\\]*)?', html, re.I):
        cand = m.group(0).strip()
        if "preview" not in cand.lower():
            raw_directs.append(cand)

    for cand in raw_directs:
        if cand in seen_streams:
            continue
        alive, final_url = verify_stream_alive(cand, referer=detail_url, session=_SESSION)
        if alive and final_url and final_url not in seen_streams:
            seen_streams.add(final_url)
            mime_type = "application/x-mpegURL" if ".m3u8" in final_url.lower() else "direct_mp4"
            servers.append({
                "name": "Desi49 Direct",
                "server": "Desi49 CDN",
                "embed_url": final_url,
                "type": mime_type,
                "quality_label": "HD",
            })

    # Extract non-ad iframe embeds and verify liveness.
    for ifr in soup.select("iframe"):
        raw_ifr = abs_url(BASE_URL, ifr.get("src") or ifr.get("data-src"))
        if not raw_ifr or is_ad_iframe(raw_ifr) or raw_ifr in seen_streams:
            continue
        if verify_embed_alive(raw_ifr, referer=detail_url, session=_SESSION):
            seen_streams.add(raw_ifr)
            host = urlparse(raw_ifr).hostname or "Mirror"
            servers.append({
                "name": f"Mirror ({host})",
                "server": host,
                "embed_url": raw_ifr,
                "type": "embed",
                "quality_label": "HD",
            })

    if not servers:
        return None, "dead"

    primary = servers[0]
    og_img = soup.select_one("meta[property='og:image']")
    thumb = clean_text(og_img.get("content")) if og_img else ""
    if not thumb:
        thumb = card["media"].get("main_thumbnail") or ""

    pub_meta = soup.select_one("meta[property='article:published_time']")
    pub_at = clean_text(pub_meta.get("content")) if pub_meta else None

    tags = [clean_text(a.get_text(" ", strip=True)).lower() for a in soup.select("a[rel='tag'], .tags a")]
    tags = [t for t in tags if t and t != "desi49"]

    enriched = dict(card)
    if wp_post_id:
        enriched["wp_post_id"] = wp_post_id
    enriched["title"] = title
    enriched["media"] = {
        "full_video_url": primary["embed_url"],
        "video_mime_type": "video/mp4" if primary["type"] == "direct_mp4" else (
            "application/x-mpegURL" if primary["type"] == "application/x-mpegURL" else "embed/iframe"
        ),
        "main_thumbnail": thumb,
        "thumbnail_url": thumb,
        "clean_thumbnail": thumb,
    }
    enriched["metadata"] = {
        **card.get("metadata", {}),
        "published_at": pub_at,
    }
    enriched["taxonomy"] = {
        "categories": ["viral-mms"],
        "tags": tags,
        "actors": [],
    }
    enriched["servers"] = servers
    return enriched, "ok"
