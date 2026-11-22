"""
Source Scraper for SheeshFans (https://sheeshfans.com)
"""
import re
from typing import List, Dict, Any, Optional
from scrapling import Fetcher

BASE_URL = "https://sheeshfans.com"
CATEGORY_URL = f"{BASE_URL}/categories/onlyfans/"

def fetch_page(url: str, timeout: int = 25) -> Optional[Any]:
    try:
        resp = Fetcher.get(url, timeout=timeout)
        if resp.status == 200:
            return resp
    except Exception:
        pass
    return None

def scrape_listing(page_num: int) -> tuple[List[Dict[str, Any]], Optional[int]]:
    if page_num <= 1:
        url = CATEGORY_URL
    else:
        url = f"{CATEGORY_URL}?from={page_num:02d}"

    resp = fetch_page(url)
    if not resp:
        return [], None

    items = []
    seen_ids = set()

    for a in resp.css("a"):
        href = a.attrib.get("href", "")
        m = re.search(r"/video/(\d+)/([^/]+)/", href)
        if m:
            vid_id = m.group(1)
            if vid_id in seen_ids:
                continue
            seen_ids.add(vid_id)

            slug = m.group(2)
            title = a.attrib.get("title") or (a.text.clean() if a.text else "")
            if not title:
                title = slug.replace("-", " ").title()

            img = a.css("img")
            thumb = img[0].attrib.get("src") or img[0].attrib.get("data-src") if img else ""
            if thumb and thumb.startswith("//"):
                thumb = f"https:{thumb}"

            full_detail_url = href if href.startswith("http") else f"{BASE_URL}{href}"

            items.append({
                "source_site": "sheeshfans",
                "source_post_id": str(vid_id),
                "title": title,
                "detail_url": full_detail_url,
                "thumbnail_url": thumb,
                "models": [],
                "published_at": None
            })

    # Estimate max page from pagination
    max_page = 200
    for pl in resp.css("a"):
        href = pl.attrib.get("href", "")
        m = re.search(r"\?from=(\d+)", href)
        if m:
            p_val = int(m.group(1))
            if p_val > max_page:
                max_page = p_val

    return items, max_page

def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    url = card.get("detail_url")
    vid_id = card.get("source_post_id")
    if not url:
        return card

    resp = fetch_page(url)
    embed_url = f"https://sheeshfans.com/embed/{vid_id}"

    servers = [{
        "name": "SheeshFans Embed",
        "embed_url": embed_url
    }]

    if not resp:
        card_copy = dict(card)
        card_copy["servers"] = servers
        card_copy["full_video_url"] = embed_url
        card_copy["video_mime_type"] = "embed/iframe"
        return card_copy

    html = resp.html_content

    # Extract flashvars parameters
    models_match = re.search(r"video_models:\s*['\"]([^'\"]*)['\"]", html)
    models = [m.strip() for m in models_match.group(1).split(",") if m.strip()] if models_match else []

    title_match = re.search(r"video_title:\s*['\"]([^'\"]*)['\"]", html)
    real_title = title_match.group(1).strip() if title_match else card.get("title")

    tags_match = re.search(r"video_tags:\s*['\"]([^'\"]*)['\"]", html)
    tags = [t.strip() for t in tags_match.group(1).split(",") if t.strip()] if tags_match else []

    # Detect resolution quality badge from DOM
    quality_badge = "720p HD"
    q_span = resp.css(".quality, .hd, .badge-quality")
    if q_span:
        quality_badge = q_span[0].text.clean() if q_span[0].text else "1080p Full HD"

    servers = [{
        "name": f"SheeshFans Embed ({quality_badge})",
        "embed_url": embed_url,
        "quality_label": quality_badge
    }]

    # Check direct MP4 token link in flashvars (event_reporting2 or video_url)
    mp4_token_match = re.search(r"event_reporting2:\s*['\"](https?://[^'\"]+\.mp4[^'\"]*)['\"]", html)
    if mp4_token_match:
        servers.append({
            "name": f"Direct Stream ({quality_badge})",
            "embed_url": mp4_token_match.group(1),
            "type": "direct_mp4",
            "quality_label": quality_badge
        })

    merged = dict(card)
    merged["title"] = real_title or card.get("title")
    merged["servers"] = servers
    merged["full_video_url"] = embed_url
    merged["video_mime_type"] = "embed/iframe"
    merged["tags"] = tags
    merged["models"] = models
    return merged
