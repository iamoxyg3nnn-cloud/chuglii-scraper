"""
Viral MMS Scraper Pipeline (Source: MMSVibeX)
Target: MMSVibeX (https://mmsvibex.life)
Parent Category: 'viral-mms'
Stores into Supabase isolated schema: 'scraper.videos'
"""

import sys
import re
import time
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor

# Ensure root paths are in sys.path
CURRENT_DIR = Path(__file__).resolve().parent
CATEGORIES_DIR = CURRENT_DIR.parent
SCRAPER_DIR = CATEGORIES_DIR.parent
PROJECT_ROOT = SCRAPER_DIR.parent

for p in [str(CURRENT_DIR), str(SCRAPER_DIR), str(PROJECT_ROOT)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from scrapling.fetchers import Fetcher
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
        # Target specific category (e.g. /category/pakistani/page/2/)
        if page_num == 1:
            return f"{BASE_URL}/category/{cat}/"
        return f"{BASE_URL}/category/{cat}/page/{page_num}/"
    else:
        # Global listing with filter
        if page_num == 1:
            return f"{BASE_URL}/?filter={filter_mode}"
        return f"{BASE_URL}/page/{page_num}/?filter={filter_mode}"

def detect_total_pages(category: Optional[str] = None, filter_mode: str = "latest") -> int:
    """Detects total pagination count from target URL."""
    try:
        url = build_target_url(1, filter_mode, category)
        page = Fetcher.get(url, timeout=20)
        links = page.css(".pagination a::attr(href)").getall()
        last_page = 1
        for l in links:
            if "/page/" in l:
                m = re.search(r"/page/(\d+)/", l)
                if m:
                    last_page = max(last_page, int(m.group(1)))
        return last_page
    except Exception as e:
        print(f"[!] Warning: Could not detect total pages ({e}). Defaulting to 100.")
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
            page = Fetcher.get(url, timeout=25)
            if page.status == 200:
                articles = page.css("article.thumb-block")
                items = []
                for art in articles:
                    post_id = art.attrib.get("data-post-id")
                    detail_url = art.css("a::attr(href)").get("").strip()
                    title = art.css(".title::text").get("").strip() or art.css("a::attr(title)").get("").strip()
                    trailer_url = art.attrib.get("data-trailer")
                    main_thumb = art.attrib.get("data-main-thumb") or art.css("img.video-main-thumb::attr(src)").get("")
                    duration = art.css(".duration::text").get("").strip()
                    views = art.css(".views::text").get("").strip()
                    rating = art.css(".rating::text").get("").strip()

                    full_video_url = derive_full_video_url(trailer_url, post_id)

                    classes = art.attrib.get("class", "").split()
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
            elif page.status == 404:
                print(f"[*] Page {page_num} returned 404 (end of pagination reached).")
                return []
            else:
                print(f"[!] Warning: Page {page_num} returned HTTP {page.status}. Retrying ({attempt}/{max_retries})...")
        except Exception as e:
            print(f"[!] Error fetching Page {page_num} (attempt {attempt}/{max_retries}): {e}")
            time.sleep(1.5)

    return []

def enrich_video_detail(item: Dict[str, Any]) -> Dict[str, Any]:
    """Deep enrichment: fetches detail page for exact durationHint and tags."""
    detail_url = item.get("url")
    if not detail_url:
        return item

    try:
        safe_url = quote(detail_url, safe=":/?&=#+%")
        detail_page = Fetcher.get(safe_url, timeout=20)
        if detail_page.status != 200:
            return item

        sources = detail_page.css("video source")
        if sources:
            item["media"]["full_video_url"] = sources[0].attrib.get("src")
            item["media"]["video_mime_type"] = sources[0].attrib.get("type", "video/mp4")

        for text in detail_page.css(".entry-content *::text").getall():
            if "durationHint" in text and "schemaVersion" in text:
                try:
                    import json
                    d = json.loads(text)
                    item["metadata"]["duration_seconds"] = d.get("durationHint")
                    break
                except Exception:
                    pass

        detailed_tags = [t.strip() for t in detail_page.css(".tags-list a::text").getall() if t.strip()]
        if detailed_tags:
            existing_tags = set(item.get("taxonomy", {}).get("tags", []))
            existing_tags.update(detailed_tags)
            item["taxonomy"]["tags"] = list(existing_tags)

        pub = detail_page.css("meta[property='article:published_time']::attr(content)").get("")
        if pub:
            item["metadata"]["published_at"] = pub

    except Exception:
        pass

    return item

def run_scraper_pipeline(
    pages: int = 1,
    filter_mode: str = "latest",
    category: Optional[str] = None,
    enrich_details: bool = False,
    workers: int = 4
):
    """Executes the full pipeline: Crawl -> Polish -> Supabase Batch Upsert."""
    cat_label = category.upper() if category and category != "all" else "ALL (GLOBAL FEED)"
    total_detected = detect_total_pages(category, filter_mode)
    total_target_pages = total_detected if (pages <= 0 or pages > total_detected) else pages

    print("=" * 72)
    print(f"STARTING VIRAL MMS CRAWLER [Parent Category: '{PARENT_CATEGORY}']")
    print(f"Target Filter/Category: {cat_label} | Mode: {filter_mode}")
    print(f"Pages to Crawl: {total_target_pages} (Detected Total: {total_detected})")
    print(f"Enrich Details: {'ENABLED' if enrich_details else 'FAST STREAM DERIVATION'}")
    print(f"Database Target: Supabase (Isolated schema: 'scraper.videos')")
    print("=" * 72)

    start_time = time.time()
    total_scraped = 0
    total_stored = 0
    empty_streak = 0

    for page_num in range(1, total_target_pages + 1):
        raw_items = scrape_listing_page(page_num, filter_mode, category)
        if not raw_items:
            empty_streak += 1
            if empty_streak >= 3:
                print(f"[*] 3 consecutive empty pages. Ending crawl at page {page_num}.")
                break
            continue
        empty_streak = 0

        total_scraped += len(raw_items)

        if enrich_details:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                enriched_items = list(executor.map(enrich_video_detail, raw_items))
        else:
            enriched_items = raw_items

        polished_batch = [
            polish_scraped_record(item, source_site=SOURCE_NAME)
            for item in enriched_items
        ]

        res = upsert_scraped_videos_batch(polished_batch)
        total_stored += len(polished_batch)

        pct = round((page_num / total_target_pages) * 100, 1)
        print(f"[{page_num}/{total_target_pages}] ({pct}%) +{len(polished_batch)} videos upserted (Total DB: {total_stored})")
        time.sleep(0.3)

    elapsed = round(time.time() - start_time, 2)
    print("=" * 72)
    print(f"CRAWL COMPLETE in {elapsed}s")
    print(f"Total Scraped: {total_scraped} | Successfully Saved/Updated in DB: {total_stored}")
    print("=" * 72)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Viral MMS Ingestion Pipeline")
    parser.add_argument("--pages", type=int, default=1, help="Number of pages to scrape (0 for all detected pages)")
    parser.add_argument("--filter", type=str, default="latest", help="Filter mode: latest, popular, most-viewed")
    parser.add_argument("--category", type=str, default=None, help="Target specific subcategory (e.g. pakistani, indian, all)")
    parser.add_argument("--enrich", action="store_true", help="Enable deep detail page fetch")
    parser.add_argument("--workers", type=int, default=4, help="Concurrency worker count for detail enrichment")

    args = parser.parse_args()
    run_scraper_pipeline(
        pages=args.pages,
        filter_mode=args.filter,
        category=args.category,
        enrich_details=args.enrich,
        workers=args.workers
    )
