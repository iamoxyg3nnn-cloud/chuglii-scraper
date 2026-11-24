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
import json
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple
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
KNOWN_IDS_FILE = CURRENT_DIR / "known_post_ids.json"
CHECKPOINT_FILE = CURRENT_DIR / "checkpoint.json"

DEFAULT_CHECKPOINT = {
    "current_page": 1,
    "max_pages": 221,
    "completed": False,
    "total_ingested": 0,
}


def load_checkpoint() -> Dict[str, Any]:
    """Loads Viral MMS checkpoint: Supabase first, atomic local cache second.

    A brand-new runner must never assume the backfill is finished, so the
    fallback defaults to an *incomplete* cursor.
    """
    from scraper.progress import load_checkpoint as _load
    return _load("viral-mms", CHECKPOINT_FILE, DEFAULT_CHECKPOINT)


def save_checkpoint(data: Dict[str, Any]) -> bool:
    """Saves Viral MMS checkpoint remotely and mirrors it locally."""
    from scraper.progress import save_checkpoint as _save
    data["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
    return _save("viral-mms", data, CHECKPOINT_FILE)


def load_known_ids() -> Set[str]:
    """Loads cached post IDs from Supabase + local JSON (Supabase wins)."""
    from scraper.progress import load_known_ids as _load
    ids = _load("viral-mms", KNOWN_IDS_FILE)
    save_known_ids(ids)
    return ids


def save_known_ids(known_ids: Set[str], new_ids: Optional[Set[str]] = None,
                   status: str = "stored") -> None:
    """Mirrors IDs locally and records only the newly-confirmed ones remotely."""
    from scraper.progress import mark_known_ids, save_known_ids as _save
    if new_ids:
        mark_known_ids(new_ids, default_site="viral-mms", status=status)
    _save(KNOWN_IDS_FILE, known_ids)

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

def detect_total_pages(category: Optional[str] = None, filter_mode: str = "latest") -> Optional[int]:
    """Detects total pagination count; returns None when detection fails."""
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
        print(f"[!] Could not detect Viral MMS page count: {e}")
        return None

def scrape_listing_page_with_status(
    page_num: int = 1,
    filter_mode: str = "latest",
    category: Optional[str] = None,
    max_retries: int = 3
) -> Tuple[List[Dict[str, Any]], bool]:
    """Fetches a listing page and returns ``(items, fetch_ok)``.

    ``fetch_ok=False`` means the page was blocked or timed out; callers must
    not advance a cursor.  A 404 is a confirmed end-of-catalog (``True``).
    """
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
                return items, True
            elif res.status_code == 404:
                return [], True
            else:
                time.sleep(1.5)
        except Exception:
            time.sleep(1.5)
    return [], False


def scrape_listing_page(
    page_num: int = 1,
    filter_mode: str = "latest",
    category: Optional[str] = None,
    max_retries: int = 3
) -> List[Dict[str, Any]]:
    """Backwards-compatible wrapper: returns items only."""
    return scrape_listing_page_with_status(page_num, filter_mode, category, max_retries)[0]


def run_scraper_pipeline(
    pages: int = 1,
    start_page: int = 1,
    filter_mode: str = "latest",
    category: Optional[str] = None,
    enrich_details: bool = False,
    known_ids: Optional[Set[str]] = None
) -> int:
    """Orchestrates extraction, deduplication, polishing, and batch upsert into Supabase."""
    print("=" * 65)
    print(f"Viral MMS Ingestion | Mode: {filter_mode} | Pages: {start_page} to {start_page + pages - 1}")
    print("=" * 65)

    if known_ids is None:
        known_ids = load_known_ids()

    total_upserted = 0
    for p in range(start_page, start_page + pages):
        items, fetch_ok = scrape_listing_page_with_status(p, filter_mode=filter_mode, category=category)
        if not fetch_ok:
            print(f"  [!] Page {p} could not be fetched (blocked/timeout). Stopping so the cursor is not advanced.")
            break
        if not items:
            break

        new_items = [itm for itm in items if str(itm.get("post_id")) not in known_ids]
        if not new_items:
            print(f"  Page {p}: Up to date (all {len(items)} releases already indexed)")
            continue

        polished_batch = [polish_scraped_record(itm) for itm in new_items]

        res = upsert_scraped_videos_batch(polished_batch)
        db_ok = res is not None and not (isinstance(res, dict) and res.get("success") is False)
        if db_ok:
            newly_confirmed = {str(itm.get("post_id")) for itm in new_items}
            known_ids.update(newly_confirmed)
            save_known_ids(known_ids, new_ids=newly_confirmed, status="stored")
            total_upserted += len(polished_batch)
            print(f"  [✓] Page {p}: Ingested {len(polished_batch)} new Viral MMS videos")
        else:
            print(f"  [!] Page {p}: upsert failed ({res}); keeping {len(new_items)} IDs unmarked for retry")
        time.sleep(0.5)

    return total_upserted
