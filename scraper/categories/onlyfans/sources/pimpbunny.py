"""
Source Scraper for PimpBunny (https://pimpbunny.net)
Extracts rich structured metadata from schema.org VideoObject and VideoJS players.
"""
import re
import json
from typing import List, Dict, Any, Optional
from scrapling import Fetcher

BASE_URL = "https://pimpbunny.net"

def parse_iso_duration(dur_str: Optional[str]) -> tuple[int, str]:
    """Parses ISO 8601 duration (e.g. 'PT9M23S' or '09:23') into (seconds, 'MM:SS')."""
    if not dur_str:
        return 600, "10:00"
    
    # Check PT#M#S pattern
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", dur_str)
    if m:
        h = int(m.group(1) or 0)
        m_val = int(m.group(2) or 0)
        s = int(m.group(3) or 0)
        total_sec = h * 3600 + m_val * 60 + s
        if h > 0:
            formatted = f"{h:02d}:{m_val:02d}:{s:02d}"
        else:
            formatted = f"{m_val:02d}:{s:02d}"
        return total_sec, formatted

    # Check MM:SS pattern
    m2 = re.match(r"(\d+):(\d+)(?::(\d+))?", dur_str.strip())
    if m2:
        parts = [int(p) for p in m2.groups() if p is not None]
        if len(parts) == 3:
            total_sec = parts[0] * 3600 + parts[1] * 60 + parts[2]
            formatted = f"{parts[0]:02d}:{parts[1]:02d}:{parts[2]:02d}"
        else:
            total_sec = parts[0] * 60 + parts[1]
            formatted = f"{parts[0]:02d}:{parts[1]:02d}"
        return total_sec, formatted

    return 600, "10:00"

def fetch_page(url: str, timeout: int = 25) -> Optional[Any]:
    try:
        resp = Fetcher.get(url, timeout=timeout)
        if resp.status == 200:
            return resp
    except Exception:
        pass
    return None

def scrape_listing(page_num: int) -> tuple[List[Dict[str, Any]], Optional[int]]:
    url = BASE_URL if page_num <= 1 else f"{BASE_URL}/page/{page_num}/"
    resp = fetch_page(url)
    if not resp:
        return [], None

    items = []
    seen_urls = set()

    for a in resp.css("a"):
        href = a.attrib.get("href", "")
        if not href or href in seen_urls:
            continue
        if href.endswith(("/categories/", "/tags/", "/actors/", "/login/", "/register/")) or "/page/" in href or "/actor/" in href or "/tag/" in href or "?" in href:
            continue

        # Video posts end in / and have title/slug
        if href.startswith(BASE_URL) and len(href.split("/")) >= 4:
            slug = href.rstrip("/").split("/")[-1]
            if len(slug) > 10 and not slug.startswith(("category", "tag", "actor", "author", "wp-")):
                seen_urls.add(href)
                img = a.css("img")
                thumb = img[0].attrib.get("data-src") or img[0].attrib.get("src") if img else ""
                title = a.attrib.get("title") or (a.text.clean() if a.text else "")
                if not title:
                    title = slug.replace("-", " ").title()

                items.append({
                    "source_site": "pimpbunny",
                    "source_post_id": slug,
                    "title": title,
                    "detail_url": href,
                    "thumbnail_url": thumb,
                    "models": [],
                    "published_at": None
                })

    # Estimate max page from pagination
    max_page = 154
    for pl in resp.css("a[href*='/page/']"):
        m = re.search(r"/page/(\d+)/", pl.attrib.get("href", ""))
        if m:
            p_val = int(m.group(1))
            if p_val > max_page:
                max_page = p_val

    return items, max_page

def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    url = card.get("detail_url")
    if not url:
        return card

    resp = fetch_page(url)
    if not resp:
        return card

    # 1. Parse structured JSON-LD VideoObject
    video_obj = None
    for s in resp.css("script"):
        if s.attrib.get("type") == "application/ld+json":
            try:
                data = json.loads(s.text)
                if isinstance(data, dict):
                    if data.get("@type") == "VideoObject":
                        video_obj = data
                        break
                    elif "@graph" in data:
                        for node in data["@graph"]:
                            if node.get("@type") == "VideoObject":
                                video_obj = node
                                break
            except Exception:
                pass

    real_title = card.get("title")
    thumbnail = card.get("thumbnail_url")
    published_at = card.get("published_at")
    duration_sec = 600
    duration_fmt = "10:00"
    content_url = None
    tags = []
    models = []

    if video_obj:
        real_title = video_obj.get("name") or real_title
        thumb_val = video_obj.get("thumbnailUrl")
        if isinstance(thumb_val, list) and thumb_val:
            thumbnail = thumb_val[0]
        elif isinstance(thumb_val, str):
            thumbnail = thumb_val

        published_at = video_obj.get("uploadDate") or published_at
        dur_raw = video_obj.get("duration")
        duration_sec, duration_fmt = parse_iso_duration(dur_raw)
        content_url = video_obj.get("contentUrl")

        # Extract model from description link
        desc = video_obj.get("description", "")
        m_actor = re.search(r"/actor/([^/\"]+)/", desc)
        if m_actor:
            models.append(m_actor.group(1).replace("-", " ").title())

    # Fallback to HTML extraction
    if not models:
        for a_el in resp.css("a[href*='/actor/']"):
            t = a_el.text.clean() if a_el.text else ""
            clean_m = t.replace("OnlyFans", "").strip()
            if clean_m and clean_m.lower() != "actors":
                models.append(clean_m)

    # Tags from tags/categories
    for t_el in resp.css("a[href*='/tag/'], a[href*='/category/']"):
        t = t_el.text.clean() if t_el.text else ""
        if t and t.lower() not in ["onlyfans leaks", "uncategorized", "video"]:
            tags.append(t)

    # Extract HTML5 Video tag
    video_tag = resp.css("video source")
    stream_url = video_tag[0].attrib.get("src") if video_tag else content_url

    servers = []
    if stream_url:
        servers.append({
            "name": "PimpBunny Stream",
            "embed_url": stream_url,
            "type": "direct_mp4",
            "headers": {"Referer": "https://pimpbunny.net/"}
        })
    servers.append({
        "name": "PimpBunny Web Player",
        "embed_url": url,
        "type": "iframe"
    })

    merged = dict(card)
    merged["title"] = real_title
    merged["thumbnail_url"] = thumbnail
    merged["clean_thumbnail_url"] = thumbnail
    merged["duration_seconds"] = duration_sec
    merged["duration_formatted"] = duration_fmt
    merged["full_video_url"] = stream_url or url
    merged["video_mime_type"] = "video/mp4" if stream_url else "embed/iframe"
    merged["servers"] = servers
    merged["models"] = list(set(models))
    merged["tags"] = list(set(tags))
    merged["published_at"] = published_at
    return merged
