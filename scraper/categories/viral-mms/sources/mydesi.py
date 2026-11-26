"""Source adapter for MyDesi (https://mydesi.sbs)."""
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from curl_cffi import requests as cf_requests

from .base import (
    abs_url,
    clean_text,
    fetch_html,
    infer_quality_label,
    is_ad_iframe,
    parse_soup,
    pick_best_servers,
    verify_embed_alive,
    verify_stream_alive,
)

BASE_URL = "https://mydesi.sbs"
SOURCE_NAME = "mydesi"
_SESSION = cf_requests.Session()


def scrape_listing(page_num: int = 1) -> Tuple[List[Dict[str, Any]], Optional[int], bool]:
    url = f"{BASE_URL}/" if page_num <= 1 else f"{BASE_URL}/page/{page_num}/"
    html, status = fetch_html(url, session=_SESSION, referer=BASE_URL + "/")
    if status == 404:
        return [], page_num, True
    if not html:
        return [], None, False

    soup = parse_soup(html)
    items: List[Dict[str, Any]] = []
    seen_ids = set()

    for block in soup.select(".video-block"):
        a = block.select_one("a.thumb[href], a[href]")
        if not a:
            continue
        detail_url = abs_url(BASE_URL, a.get("href"))
        if not detail_url:
            continue

        post_id = clean_text(block.get("data-post-id"))
        if not post_id:
            post_id = urlparse(detail_url).path.strip("/").split("/")[-1]
        if not post_id or post_id in seen_ids:
            continue
        seen_ids.add(post_id)

        infos = block.select_one("a.infos")
        title_el = block.select_one(".title")
        img = block.select_one("img")
        title = (
            clean_text(title_el.get_text(" ", strip=True) if title_el else "")
            or clean_text(infos.get("title") if infos else "")
            or clean_text(img.get("alt") if img else "")
        )
        if not title:
            continue

        thumb = ""
        if img:
            thumb = abs_url(
                BASE_URL,
                img.get("data-src") or img.get("data-original") or img.get("src"),
            )

        dur_el = block.select_one(".duration")
        views_el = block.select_one(".views")

        items.append({
            "source_site": SOURCE_NAME,
            "post_id": post_id,
            "title": title,
            "url": detail_url,
            "media": {
                "main_thumbnail": thumb,
                "thumbnail_url": thumb,
                "clean_thumbnail": thumb,
            },
            "metadata": {
                "duration": clean_text(dur_el.get_text(" ", strip=True) if dur_el else ""),
                "views": clean_text(views_el.get_text(" ", strip=True) if views_el else ""),
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
    servers: List[Dict[str, Any]] = []
    seen_streams = set()

    raw_directs: List[Tuple[str, str]] = []
    for node in soup.select("video source[src], video source[data-src], video[src]"):
        src = abs_url(BASE_URL, node.get("src") or node.get("data-src"))
        if src:
            q_hint = clean_text(node.get("label") or node.get("title") or node.get("data-res"))
            raw_directs.append((src, q_hint))
    for m in re.finditer(r'https?://[^\s"\'<>\\]+\.(?:mp4|m3u8)(?:\?[^\s"\'<>\\]*)?', html, re.I):
        cand = m.group(0).strip()
        if "preview" not in cand.lower():
            raw_directs.append((cand, ""))

    for cand, q_hint in raw_directs:
        if cand in seen_streams:
            continue
        alive, final_url = verify_stream_alive(cand, referer=detail_url, session=_SESSION)
        if alive and final_url and final_url not in seen_streams:
            seen_streams.add(final_url)
            mime_type = "application/x-mpegURL" if ".m3u8" in final_url.lower() else "direct_mp4"
            qlabel = infer_quality_label(q_hint, final_url, default="HD")
            servers.append({
                "name": f"MyDesi Direct ({qlabel})" if qlabel != "HD" else "MyDesi Direct",
                "server": "MyDesi CDN",
                "embed_url": final_url,
                "type": mime_type,
                "quality_label": qlabel,
            })

    if not servers:
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

    servers = pick_best_servers(servers)
    primary = servers[0]
    og_img = soup.select_one("meta[property='og:image']")
    thumb = clean_text(og_img.get("content")) if og_img else ""
    if not thumb:
        thumb = card["media"].get("main_thumbnail") or ""

    pub_meta = soup.select_one("meta[property='article:published_time']")
    pub_at = clean_text(pub_meta.get("content")) if pub_meta else None

    tags = [clean_text(a.get_text(" ", strip=True)).lower() for a in soup.select("a[rel='tag'], .tags-list a")]
    tags = [t for t in tags if t]

    enriched = dict(card)
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
