"""
Viral MMS Scraper Pipeline (Source: MMSVibeX)
Target: MMSVibeX (https://mmsvibex.life)
Parent Category: 'viral-mms'
Stores into Supabase isolated schema: 'scraper.videos'
Built with curl_cffi and BeautifulSoup for ultra-fast, zero-dependency cloud execution.
"""

import sys
import re
import time
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from curl_cffi import requests

# Ensure root paths are in sys.path
CURRENT_DIR = Path(__file__).resolve().parent
CATEGORIES_DIR = CURRENT_DIR.parent
SCRAPER_DIR = CATEGORIES_DIR.parent
PROJECT_ROOT = SCRAPER_DIR.parent

for p in [str(CURRENT_DIR), str(SCRAPER_DIR), str(PROJECT_ROOT)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from polish import polish_scraped_record
from scraper.db import upsert_scraped_videos_batch

BASE_URL = "https://mmsvibex.life"
SOURCE_NAME = "mmsvibex"
PARENT_CATEGORY = "viral-mms"

def derive_full_video_url(trailer_url: Optional[str], post_id: Optional[str] = None) -> Optional[str]:
    """Derives direct CDN processed MP4 stream URL from trailer preview or post_id."""
    if trailer_url:
        match = re.search(r"/trailers/(\d+)_trailer\.mp4", trailer_url)
        if match:
            return f"https://cdn.mmsvibex.fit/processed/{match.group(1)}_final.mp4"
    if post_id and str(post_id).isdigit():
        return f"https://cdn.mmsvibex.fit/processed/{post_id}_final.mp4"
    return None

def build_target_url(page_num: int = 1, filter_mode: str = "latest", category: Optional[str] = None) -> str:
    """Builds appropriate URL for global feed or specific sub-category."""
    cat = (category or "").strip().lower()
    if cat and cat != "all":
        if page_num == 1:
            return f"{BASE_URL}/category/{cat}/"
        return f"{BASE_URL}/category/{cat}/page/{page_num}/"
    else:
        if page_num == 1:
            return f"{BASE_URL}/?filter={filter_mode}"
        return f"{BASE_URL}/page/{page_num}/?filter={filter_mode}"

def detect_total_pages(category: Optional[str] = None, filter_mode: str = "latest") -> int:
    """Detects total pagination count from target URL."""
    try:
        url = build_target_url(1, filter_mode, category)
        res = requests.get(url, impersonate="chrome124", timeout=20)
        soup = BeautifulSoup(res.text, "html.parser")
        links = [a.get("href", "") for a in soup.select(".pagination a")]
        last_page = 1
        for l in links:
            if "/page/" in l:
                m = re.search(r"/page/(\d+)/", l)
                if m:
                    last_page = max(last_page, int(m.group(1)))
        return last_page
    except Exception as e:
        return 100

def scrape_listing_page(
    page_num: int = 1,
    filter_mode: str = "latest",
    category: Optional[str] = None,
    max_retries: int = 3
) -> List[Dict[str, Any]]:
    """Fetches a listing page and extracts video card data."""
    url = build_target_url(page_num, filter_mode, category)

    for attempt in range(1, max_retries + 1):
        try:
            res = requests.get(url, impersonate="chrome124", timeout=25)
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, "html.parser")
                articles = soup.select("article.thumb-block")
                items = []
                for art in articles:
                    post_id = art.get("data-post-id")
                    a_tag = art.select_one("a[href]")
                    detail_url = a_tag.get("href", "").strip() if a_tag else ""
                    title = ""
                    title_el = art.select_one(".title")
                    if title_el:
                        title = title_el.get_text(strip=True)
                    elif a_tag:
                        title = a_tag.get("title", "").strip()

                    trailer_url = art.get("data-trailer")
                    img = art.select_one("img.video-main-thumb")
                    main_thumb = art.get("data-main-thumb") or (img.get("src", "") if img else "")

                    dur_el = art.select_one(".duration")
                    duration = dur_el.get_text(strip=True) if dur_el else ""

                    views_el = art.select_one(".views")
                    views = views_el.get_text(strip=True) if views_el else ""

                    rat_el = art.select_one(".rating")
                    rating = rat_el.get_text(strip=True) if rat_el else ""

                    full_video_url = derive_full_video_url(trailer_url, post_id)

                    classes = art.get("class", [])
                    categories = [c.replace("category-", "") for c in classes if c.startswith("category-")]
                    if PARENT_CATEGORY not in categories:
                        categories.append(PARENT_CATEGORY)
                    if category and category.lower() != "all" and category not in categories:
                        categories.append(category.lower())

                    tags = [c.replace("tag-", "") for c in classes if c.startswith("tag-")]
                    actors = [c.replace("actors-", "") for c in classes if c.startswith("actors-")]

                    items.append({
                        "post_id": post_id,
                        "title": title,
                        "url": detail_url,
                        "media": {
                            "full_video_url": full_video_url,
                            "trailer_preview_url": trailer_url,
                            "main_thumbnail": main_thumb,
                        },
                        "metadata": {
                            "duration": duration,
                            "views": views,
                            "rating": rating,
                        },
                        "taxonomy": {
                            "categories": categories,
                            "tags": tags,
                            "actors": actors,
                        }
                    })
                return items
            elif res.status_code == 404:
                return []
            else:
                time.sleep(1.5)
        except Exception:
            time.sleep(1.5)
    return []

def run_scraper_pipeline(
    pages: int = 1,
    start_page: int = 1,
    filter_mode: str = "latest",
    category: Optional[str] = None,
    enrich_details: bool = False
) -> int:
    """Orchestrates extraction, polishing, and batch upsert into Supabase."""
    print("=" * 65)
    print(f"Viral MMS Ingestion | Mode: {filter_mode} | Pages: {start_page} to {start_page + pages - 1}")
    print("=" * 65)

    total_upserted = 0
    for p in range(start_page, start_page + pages):
        items = scrape_listing_page(p, filter_mode=filter_mode, category=category)
        if not items:
            break

        polished_batch = []
        for itm in items:
            polished = polish_scraped_record(itm)
            polished_batch.append(polished)

        res = upsert_scraped_videos_batch(polished_batch)
        count = len(polished_batch)
        total_upserted += count
        db_status = res.get('success', True) if isinstance(res, dict) else (bool(res) if res is not None else False)
        print(f"  [✓] Page {p}: Upserted {count} Viral MMS videos (DB status: {db_status})")
        time.sleep(0.5)

    return total_upserted
