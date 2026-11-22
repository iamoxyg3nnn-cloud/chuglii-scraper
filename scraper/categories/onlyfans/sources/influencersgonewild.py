"""
Source Scraper for InfluencersGoneWild (https://influencersgonewild.com)
"""
import re
from typing import List, Dict, Any, Optional
from scrapling import Fetcher

BASE_URL = "https://influencersgonewild.com"
CATEGORY_URL = f"{BASE_URL}/category/onlyfans/"

def fetch_page(url: str, timeout: int = 25) -> Optional[Any]:
    try:
        resp = Fetcher.get(url, timeout=timeout)
        if resp.status == 200:
            return resp
    except Exception:
        pass
    return None

def scrape_listing(page_num: int) -> tuple[List[Dict[str, Any]], Optional[int]]:
    url = CATEGORY_URL if page_num <= 1 else f"{BASE_URL}/category/onlyfans/page/{page_num}/"
    resp = fetch_page(url)
    if not resp:
        return [], None

    articles = resp.css("article")
    items = []

    for art in articles:
        classes = art.attrib.get("class", "").split()
        post_id = next((c[5:] for c in classes if c.startswith("post-") and c[5:].isdigit()), None)
        title_el = art.css(".entry-title a, h2 a, h3 a")
        title = title_el[0].text.clean() if title_el and title_el[0].text else art.attrib.get("aria-label", "")
        detail_url = title_el[0].attrib.get("href") if title_el else ""

        if not post_id or not detail_url:
            continue

        img_el = art.css("img")
        data_src = img_el[0].attrib.get("data-src") or img_el[0].attrib.get("src") if img_el else ""
        time_el = art.css("time")
        date_str = time_el[0].text.clean() if time_el and time_el[0].text else ""

        # Extract model from category links or title
        cat_links = art.css("a[href*='/category/']")
        models = []
        for cl in cat_links:
            href = cl.attrib.get("href", "")
            txt = cl.text.clean() if cl.text else ""
            if txt and txt.lower() not in ["onlyfans", "tiktok", "main categories", "celebrity"]:
                models.append(txt)

        items.append({
            "source_site": "influencersgonewild",
            "source_post_id": str(post_id),
            "title": title,
            "detail_url": detail_url,
            "thumbnail_url": data_src,
            "models": models,
            "published_at": date_str
        })

    # Estimate max page from pagination if available
    max_page = None
    for pl in resp.css("a.page-numbers, .pagination a"):
        m = re.search(r"/page/(\d+)/", pl.attrib.get("href", ""))
        if m:
            p_val = int(m.group(1))
            if max_page is None or p_val > max_page:
                max_page = p_val

    return items, max_page

def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    url = card.get("detail_url")
    if not url:
        return card

    resp = fetch_page(url)
    if not resp:
        return card

    # Extract HTML5 video direct MP4 source
    video_sources = resp.css("video source")
    mp4_url = None
    if video_sources:
        mp4_url = video_sources[0].attrib.get("src")
        if mp4_url and ".influencersgonewild.net" in mp4_url:
            mp4_url = re.sub(r"cdn\d+\.influencersgonewild\.net", "cdn04.influencersgonewild.net", mp4_url)

    servers = []
    if mp4_url:
        servers.append({
            "name": "Direct 1080p MP4 (CDN)",
            "embed_url": mp4_url,
            "type": "direct_mp4",
            "quality_label": "1080p Full HD",
            "headers": {"Referer": "https://influencersgonewild.com/"}
        })

    tags = [t.text.clean() for t in resp.css("a[href*='/tag/']") if t.text]
    
    # If no model yet, extract from detail category
    models = list(card.get("models", []))
    if not models:
        for cl in resp.css("a[href*='/category/']"):
            txt = cl.text.clean() if cl.text else ""
            if txt and txt.lower() not in ["onlyfans", "tiktok", "main categories", "celebrity"]:
                models.append(txt)

    merged = dict(card)
    merged["servers"] = servers
    merged["full_video_url"] = mp4_url
    merged["video_mime_type"] = "video/mp4" if mp4_url else "embed/iframe"
    merged["tags"] = tags
    merged["models"] = models
    return merged
