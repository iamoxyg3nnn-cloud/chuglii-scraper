"""
Source Scraper for InternetChicks (https://internetchicks.com)
"""
import re
from typing import List, Dict, Any, Optional
from scrapling import Fetcher

BASE_URL = "https://internetchicks.com"
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
        title_el = art.css(".entry-title a, h2 a")
        title = title_el[0].text.clean() if title_el and title_el[0].text else art.attrib.get("aria-label", "")
        detail_url = title_el[0].attrib.get("href") if title_el else ""

        if not post_id or not detail_url:
            continue

        models = [c.replace("actress-", "").replace("-", " ").title() for c in classes if c.startswith("actress-")]
        img_el = art.css("img")
        data_src = ""
        if img_el:
            candidate = img_el[0].attrib.get("data-src") or ""
            if not candidate or candidate.startswith("data:"):
                candidate = img_el[0].attrib.get("src") or ""
            if candidate.startswith("data:"):
                # Extract actual URL from data-srcset or srcset if available
                srcset = img_el[0].attrib.get("data-srcset") or img_el[0].attrib.get("srcset") or ""
                urls = re.findall(r"(https?://[^\s,]+)", srcset)
                candidate = urls[-1] if urls else ""
            data_src = candidate if not candidate.startswith("data:") else ""
        data_srcset = img_el[0].attrib.get("data-srcset") if img_el else ""
        time_el = art.css("time")
        date_str = time_el[0].text.clean() if time_el and time_el[0].text else ""

        items.append({
            "source_site": "internetchicks",
            "source_post_id": str(post_id),
            "title": title,
            "detail_url": detail_url,
            "thumbnail_url": data_src,
            "srcset": data_srcset,
            "models": models,
            "published_at": date_str
        })

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

    buttons = resp.css("button.button_choice_server")
    servers = []
    for b in buttons:
        onclick = b.attrib.get("onclick", "")
        m = re.search(r"playEmbed\(['\"]([^'\"]+)['\"]\)", onclick)
        if m:
            embed_url = m.group(1)
            name = "Unknown"
            quality = "720p HD"
            if "voe" in embed_url:
                name = "VOE (1080p HD)"
                quality = "1080p Full HD"
            elif "playmogo" in embed_url or "streamwish" in embed_url:
                name = "PlayMogo (720p HD)"
                quality = "720p HD"
            elif "streamtape" in embed_url:
                name = "Streamtape (720p)"
                quality = "720p"
            elif "bysezejataos" in embed_url or "dood" in embed_url:
                name = "Doodstream (480p SD)"
                quality = "480p SD"
            servers.append({"name": name, "embed_url": embed_url, "quality_label": quality})

    gallery_el = resp.css(".view-gallery-v2 a, .view-gallery-btn-v2, a[href*='imagetwist']")
    gallery_url = gallery_el[0].attrib.get("href") if gallery_el else None
    tags = [t.text.clean() for t in resp.css(".entry-tags a, a[rel='tag']") if t.text]

    merged = dict(card)
    merged["servers"] = servers
    merged["gallery_url"] = gallery_url
    merged["tags"] = tags
    return merged
