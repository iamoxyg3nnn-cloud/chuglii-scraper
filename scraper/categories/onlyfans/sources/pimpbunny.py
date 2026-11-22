"""
Source Scraper for PimpBunny (https://pimpbunny.com)
Kernel Video Sharing (KVS) platform for OnlyFans / adult content creator leaks.
"""
import re
import json
from typing import List, Dict, Any, Optional

BASE_URL = "https://pimpbunny.com"
CATEGORY_URL = f"{BASE_URL}/videos/"

def parse_iso_duration(dur_str: Optional[str]) -> tuple[int, str]:
    """Parses ISO 8601 duration (e.g. 'PT0H15M11S' or '15:11') into (seconds, 'MM:SS')."""
    if not dur_str:
        return 600, "10:00"
    
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

def fetch_page(url: str, timeout: int = 25) -> Optional[str]:
    """Fetches page content with realistic Chrome impersonation."""
    try:
        from curl_cffi import requests
        r = requests.get(
            url,
            headers={"Referer": "https://pimpbunny.com/"},
            impersonate="chrome124",
            timeout=timeout
        )
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None

def scrape_listing(page_num: int) -> tuple[List[Dict[str, Any]], Optional[int]]:
    """Scrapes video listings from https://pimpbunny.com/videos/"""
    if page_num <= 1:
        url = CATEGORY_URL
    else:
        url = f"{BASE_URL}/videos/{page_num}/"

    html = fetch_page(url)
    if not html:
        return [], None

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    items = []
    seen_urls = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href or href in seen_urls:
            continue
        if not ("/videos/" in href) or href == "/videos/" or href.startswith("/videos/2") or "/page=" in href:
            continue

        full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
        slug = href.rstrip("/").split("/")[-1]
        if len(slug) < 4 or slug.isdigit():
            continue

        img = a.find("img")
        thumb = ""
        if img:
            thumb = img.get("data-original") or img.get("data-src") or img.get("src") or ""
            if thumb.startswith("data:"):
                thumb = ""

        title = a.get("title") or (img.get("alt") if img else "") or a.text.strip()
        if not title:
            title = slug.replace("-", " ").title()

        seen_urls.add(href)
        items.append({
            "source_site": "pimpbunny",
            "source_post_id": slug,
            "title": title,
            "detail_url": full_url,
            "thumbnail_url": thumb,
            "models": [],
            "published_at": None
        })

    # Estimate max page from pagination
    max_page = 2000
    for pl in soup.find_all("a", href=True):
        m = re.search(r"/videos/(\d+)/", pl["href"])
        if m:
            p_val = int(m.group(1))
            if p_val > max_page:
                max_page = p_val

    return items, max_page

def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    """Fetches video detail and extracts KVS embed iframe and metadata."""
    url = card.get("detail_url")
    if not url:
        return card

    html = fetch_page(url)
    if not html:
        return card

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # 1. Parse structured JSON-LD VideoObject
    video_obj = None
    for sc in soup.find_all("script", type="application/ld+json"):
        try:
            d = json.loads(sc.text)
            if isinstance(d, dict) and d.get("@type") == "VideoObject":
                video_obj = d
                break
        except Exception:
            pass

    # 2. Extract numeric Video ID
    video_id = None
    m_id = re.search(r"videoId:\s*['\"](\d+)['\"]", html)
    if m_id:
        video_id = m_id.group(1)
    elif video_obj and video_obj.get("contentUrl"):
        m2 = re.search(r"/(\d+)/\d+_\w+\.mp4", video_obj["contentUrl"])
        if m2:
            video_id = m2.group(1)
    elif card.get("source_post_id") and card["source_post_id"].isdigit():
        video_id = card["source_post_id"]

    # 3. Models extraction (OnlyFans creators linked directly)
    models = []
    for a in soup.find_all("a", href=True):
        if "/onlyfans-creators/" in a["href"]:
            t = a.text.strip()
            if t and t not in models and t.lower() not in ["onlyfans creators", "view more"]:
                models.append(t)

    # 4. Tags extraction
    tags = []
    for a in soup.find_all("a", href=True):
        if "/tags/" in a["href"]:
            t = a.text.strip()
            if t and t != "Tags" and t not in tags:
                tags.append(t)

    real_title = (video_obj.get("name") if video_obj else None) or (soup.title.string.replace("| PimpBunny", "").strip() if soup.title else None) or card.get("title")

    # Duration
    duration_sec = 600
    duration_fmt = "10:00"
    if video_obj and video_obj.get("duration"):
        duration_sec, duration_fmt = parse_iso_duration(video_obj["duration"])

    # Thumbnail (crystal-clear 800x450 preview)
    thumbnail = (video_obj.get("thumbnailUrl") if video_obj else None) or card.get("thumbnail_url")
    if video_id and not thumbnail:
        folder = (int(video_id) // 1000) * 1000
        thumbnail = f"https://pimpbunny.com/contents/videos_screenshots/{folder}/{video_id}/preview.jpg"

    # 5. Exactly ONE verified KVS responsive embed player (Never multiple fake servers)
    servers = []
    embed_url = f"https://pimpbunny.com/embed/{video_id}" if video_id else None
    if embed_url:
        servers.append({
            "name": "PimpBunny (720p HD)",
            "embed_url": embed_url,
            "type": "iframe",
            "quality_label": "720p HD",
            "quality_score": 720,
            "headers": {"Referer": "https://pimpbunny.com/"}
        })

    merged = dict(card)
    merged["source_post_id"] = str(video_id or card.get("source_post_id"))
    merged["title"] = real_title
    merged["thumbnail_url"] = thumbnail
    merged["clean_thumbnail_url"] = thumbnail
    merged["duration_seconds"] = duration_sec
    merged["duration_formatted"] = duration_fmt
    merged["full_video_url"] = embed_url
    merged["video_mime_type"] = "embed/iframe"
    merged["servers"] = servers
    merged["models"] = list(set(models))
    merged["tags"] = list(set(tags))
    merged["published_at"] = video_obj.get("uploadDate") if video_obj else card.get("published_at")
    return merged
