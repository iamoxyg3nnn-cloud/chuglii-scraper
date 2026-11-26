"""
Buu Mal (Myanmar) Complete Scraper Pipeline
Target: Buu Mal (https://www.buumal.com)
Stores into Supabase isolated schema: 'scraper.myanmar_videos'
Features:
 - Multi-threaded listing and detail page enrichment (ThreadPoolExecutor)
 - Comprehensive tag and pagination backfill with persistent checkpoints
 - 24/7 continuous watcher daemon mode
 - Local post-ID deduplication cache for <1ms checks
 - Direct Cloudflare R2 MP4 stream detection
 - Automatic exponential backoff and safe retries
"""

import os
import sys
import re
import json
import time
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
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

from scraper.categories.myanmar.polish import polish_buumal_record
from scraper.db import (
    upsert_myanmar_videos_batch,
    get_myanmar_videos,
    wait_for_supabase,
    is_supabase_healthy
)

BASE_URL = "https://www.buumal.com"
CHROME_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

CHECKPOINT_FILE = CURRENT_DIR / "checkpoint.json"
KNOWN_IDS_FILE = CURRENT_DIR / "known_post_ids.json"

# Supported Buu Mal categories and tags
BUUMAL_TAGS = {
    "all": "All Videos",
    "myanmar": "Myanmar",
    "mamagyi": "VIP. Ma Ma Gyi",
    "mmporns": "MM Porns",
    "drkogyi": "VIP. Dr Ko Gyi",
    "samusar": "Samusar",
    "drhmonegyi": "Dr.Hmone Gyi",
    "mmhdhub": "MM HD Hub",
    "kosargyi": "Ko Sar Gyi",
}

# ─────────────────────────────────────────────────────────────────────────────
# Cache & Checkpoint Persistence
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_CHECKPOINT = {"current_page": 34, "max_pages": 3690, "total_ingested": 0}


def load_known_ids() -> Set[str]:
    """Loads cached post IDs from Supabase + local JSON (Supabase wins)."""
    from scraper.progress import load_known_ids as _load
    ids = _load("myanmar", KNOWN_IDS_FILE)
    save_known_ids(ids)
    return ids


def save_known_ids(known_ids: Set[str], new_ids: Optional[Set[str]] = None,
                   status: str = "stored") -> None:
    """Mirrors IDs locally and records only newly-confirmed IDs remotely."""
    from scraper.progress import mark_known_ids, save_known_ids as _save
    if new_ids:
        mark_known_ids(new_ids, default_site="myanmar", status=status)
    _save(KNOWN_IDS_FILE, known_ids)


def load_checkpoint() -> Dict[str, Any]:
    """Loads Myanmar checkpoint: Supabase first, atomic local cache second."""
    from scraper.progress import load_checkpoint as _load
    state = _load("myanmar", CHECKPOINT_FILE, DEFAULT_CHECKPOINT)
    if "current_page" not in state and "last_page" in state:
        state["current_page"] = state["last_page"]
    return state


def save_checkpoint(data: Dict[str, Any]) -> bool:
    """Saves Myanmar checkpoint remotely and mirrors it locally."""
    from scraper.progress import save_checkpoint as _save
    if "current_page" in data and "last_page" not in data:
        data["last_page"] = data["current_page"]
    return _save("myanmar", data, CHECKPOINT_FILE)

# ─────────────────────────────────────────────────────────────────────────────
# HTTP Request Utilities
# ─────────────────────────────────────────────────────────────────────────────

def make_request(url: str, retries: int = 4, timeout: int = 25) -> Optional[str]:
    """Fetches a URL using curl_cffi with Chrome TLS impersonation and exponential backoff."""
    for attempt in range(retries):
        try:
            res = requests.get(
                url,
                impersonate="chrome124",
                timeout=timeout,
                headers={
                    "User-Agent": CHROME_UA,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.buumal.com/"
                }
            )
            if res.status_code == 200:
                return res.text
            elif res.status_code in (404, 410):
                # Confirmed end-of-catalog / removed page, distinct from a
                # transient network failure (None) so the cursor can advance.
                return ""
            else:
                time.sleep(1.5 * (attempt + 1))
        except Exception as e:
            if attempt == retries - 1:
                print(f"[!] Request failed for {url}: {e}")
                return None
            time.sleep(2 * (attempt + 1))
    return None

# ─────────────────────────────────────────────────────────────────────────────
# Parsing Functions
# ─────────────────────────────────────────────────────────────────────────────

def build_listing_url(page: int = 1, tag: Optional[str] = "myanmar") -> str:
    """Builds appropriate URL for Buu Mal catalog listing."""
    tag_clean = (tag or "").strip().lower()
    if tag_clean and tag_clean != "all":
        if page <= 1:
            return f"{BASE_URL}?tag={tag_clean}"
        return f"{BASE_URL}?tag={tag_clean}&page={page}"
    else:
        if page <= 1:
            return BASE_URL
        return f"{BASE_URL}?page={page}"

def parse_listing_page(html: str) -> List[Dict[str, Any]]:
    """Extracts video card summaries from Buu Mal listing page HTML."""
    soup = BeautifulSoup(html, "html.parser")
    items = []
    
    # Columns contain video cards: <div class="column ..."><a href="https://www.buumal.com/video/ID">...</a></div>
    columns = soup.select(".column")
    for col in columns:
        link = col.find("a", href=True)
        if not link:
            continue
        href = link.get("href", "")
        if "/video/" not in href:
            continue
            
        post_id = href.rstrip("/").split("/")[-1]
        if not post_id or len(post_id) < 3:
            continue
            
        # Detail URL
        detail_url = urljoin(BASE_URL, href)
        
        # Thumbnail image
        img = col.find("img")
        thumbnail_url = img.get("src") if img else None
        
        # Title and relative time
        paragraphs = col.select(".card-content p, .content p")
        title = ""
        relative_time = ""
        if paragraphs:
            title = paragraphs[0].get_text(strip=True)
            if len(paragraphs) > 1:
                relative_time = paragraphs[1].get_text(strip=True)
        
        items.append({
            "source_site": "buumal",
            "source_post_id": post_id,
            "detail_url": detail_url,
            "thumbnail_url": thumbnail_url,
            "title": title,
            "relative_time": relative_time
        })
        
    return items

def enrich_video_detail(item: Dict[str, Any]) -> Dict[str, Any]:
    """Fetches video detail page and extracts player source URL and tags."""
    detail_url = item.get("detail_url")
    if not detail_url:
        return item

    html = make_request(detail_url, retries=3, timeout=15)
    if not html:
        return item

    soup = BeautifulSoup(html, "html.parser")
    
    # Full title from title tag or h1 if better
    if soup.title and soup.title.string:
        page_title = soup.title.string.strip()
        cleaned_page_title = page_title.replace("Buu Mal (ဘူးမယ်)", "").strip()
        if cleaned_page_title:
            item["title"] = cleaned_page_title

    # Extract direct video source from <video><source src="..."/></video>
    source_tag = soup.find("source")
    if source_tag and source_tag.get("src"):
        item["full_video_url"] = source_tag.get("src")
        item["stream_url"] = source_tag.get("src")
        item["video_mime_type"] = source_tag.get("type") or "video/mp4"

    # Extract tags
    tags = []
    for a in soup.find_all("a", href=True):
        if "?tag=" in a["href"]:
            t_txt = a.get_text(strip=True)
            if t_txt and t_txt.lower() not in [t.lower() for t in tags]:
                tags.append(t_txt)
                
    for tag_span in soup.select(".tag"):
        txt = tag_span.get_text(strip=True)
        if txt and txt.lower() not in ["tags", "tag"] and txt.lower() not in [t.lower() for t in tags]:
            tags.append(txt)

    if tags:
        item["tags"] = tags

    return item

# ─────────────────────────────────────────────────────────────────────────────
# Pipeline Execution Flow
# ─────────────────────────────────────────────────────────────────────────────

_CF_CHALLENGE_MARKERS = (
    "Just a moment...",
    "challenge-platform",
    "cf-chl",
    "Attention Required! | Cloudflare",
)


def _looks_like_cf_challenge(html: str) -> bool:
    """Returns True for a bot-wall interstitial served with HTTP 200."""
    lowered = (html or "").lower()
    # Real catalog pages can embed Cloudflare scripts; only short interstitial
    # bodies are challenges.
    return len(html or "") < 20000 and any(
        marker.lower() in lowered for marker in _CF_CHALLENGE_MARKERS
    )


def scrape_page_with_status(page_num: int, tag: str, known_ids: Set[str], workers: int = 5) -> Tuple[List[Dict[str, Any]], bool]:
    """Scrapes a page and returns ``(polished_records, fetch_ok)``.

    ``fetch_ok=False`` for request failures or a bot-wall interstitial; an
    empty or fully-known catalog page is a successful fetch and may advance
    the cursor.  ``make_request`` returns ``""`` only for a confirmed 404/410.
    """
    url = build_listing_url(page_num, tag)
    print(f"[*] Crawling [{tag}] page {page_num}: {url}")

    html = make_request(url)
    if html is None:
        print(f"[!] Empty response for page {page_num}")
        return [], False
    if html == "":
        print(f"[*] Page {page_num} returned 404/410; treating as end of catalog.")
        return [], True
    if _looks_like_cf_challenge(html):
        print(f"[!] Bot-wall interstitial on page {page_num}; not advancing the cursor.")
        return [], False

    cards = parse_listing_page(html)
    print(f"    Found {len(cards)} cards on page {page_num}")

    # Filter unseen IDs
    new_items = [c for c in cards if c["source_post_id"] not in known_ids]
    if not new_items:
        print(f"    All {len(cards)} items already in deduplication cache.")
        return [], True

    print(f"    Enriching {len(new_items)} new videos with {workers} workers...")
    enriched_items = []
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(enrich_video_detail, item): item for item in new_items}
        for future in as_completed(futures):
            try:
                res = future.result()
                if res and res.get("source_post_id") and (res.get("full_video_url") or res.get("stream_url")):
                    enriched_items.append(res)
            except Exception as e:
                print(f"[!] Error enriching item: {e}")

    # Polish records
    polished = []
    for raw in enriched_items:
        try:
            p = polish_buumal_record(raw)
            polished.append(p)
        except Exception as e:
            print(f"[!] Error polishing record {raw.get('source_post_id')}: {e}")

    return polished, True


def scrape_page(page_num: int, tag: str, known_ids: Set[str], workers: int = 5) -> List[Dict[str, Any]]:
    """Backwards-compatible wrapper: returns polished records only."""
    return scrape_page_with_status(page_num, tag, known_ids, workers)[0]


def run_pipeline(
    tag: str = "myanmar",
    start_page: Optional[int] = None,
    max_pages: int = 5,
    workers: int = 6,
    batch_size: int = 20,
    continuous: bool = False,
    all_tags: bool = False
):
    """Main pipeline loop with batch persistence and state saving."""
    tags_to_crawl = list(BUUMAL_TAGS.keys()) if all_tags else [tag]

    print("============================================================================")
    print(" CHUGLII - BUU MAL (MYANMAR) SCRAPER PIPELINE")
    print(f" Target Tags    : {', '.join(tags_to_crawl)}")
    print(f" Concurrency    : {workers} workers")
    print(f" Mode           : {'24/7 Daemon' if continuous else 'Batch Backfill'}")
    print(f" Storage Target : scraper.myanmar_videos (100% Segregated)")
    print("============================================================================")

    known_ids = load_known_ids()
    print(f"[+] Loaded {len(known_ids)} known post IDs from local cache.")

    checkpoint = load_checkpoint()
    total_upserted = 0

    for current_tag in tags_to_crawl:
        print(f"\n>>> Starting crawl for category/tag: [{current_tag}] ({BUUMAL_TAGS.get(current_tag, 'Custom')})")
        tag_start = start_page if start_page is not None and start_page > 0 else checkpoint.get(f"last_page_{current_tag}", checkpoint.get("last_page", 1))
        current_page = tag_start
        pages_processed = 0

        while True:
            try:
                records, fetch_ok = scrape_page_with_status(current_page, current_tag, known_ids, workers=workers)
                if not fetch_ok:
                    print(f"[!] Page {current_page} fetch failed; stopping tag [{current_tag}] without advancing the cursor.")
                    break

                new_from_page = 0
                if records:
                    print(f"[+] Upserting {len(records)} polished records into scraper.myanmar_videos...")
                    resp = upsert_myanmar_videos_batch(records)
                    print(f"    Upsert result: {resp}")
                    db_ok = resp is not None and not (isinstance(resp, dict) and resp.get("success") is False)
                    if not db_ok:
                        print(f"[!] Upsert failed on page {current_page}; keeping IDs unmarked and stopping this tag.")
                        break
                    newly_confirmed = {str(r["source_post_id"]) for r in records}
                    known_ids.update(newly_confirmed)
                    save_known_ids(known_ids, new_ids=newly_confirmed)
                    new_from_page = len(records)
                    total_upserted += new_from_page
                else:
                    print(f"[-] No new items to upsert on page {current_page}.")

                pages_processed += 1
                current_page += 1

                checkpoint[f"last_page_{current_tag}"] = current_page
                checkpoint["last_page"] = current_page
                checkpoint["last_tag"] = current_tag
                checkpoint["total_scraped"] = checkpoint.get("total_scraped", 0) + new_from_page
                checkpoint["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
                save_checkpoint(checkpoint)

                if not continuous and pages_processed >= max_pages:
                    print(f"[✓] Finished batch run of {max_pages} pages for tag [{current_tag}].")
                    break

                time.sleep(1.0)

            except KeyboardInterrupt:
                print("\n[!] User interrupted scraping. State saved.")
                return
            except Exception as e:
                print(f"[!] Unexpected error in crawler loop: {e}")
                time.sleep(5)

    print(f"\n============================================================================")
    print(f"Pipeline run complete. Total newly saved videos: {total_upserted}")
    print(f"Total known index size: {len(known_ids)} videos")
    print(f"============================================================================")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Buu Mal Myanmar Scraper Pipeline")
    parser.add_argument("--tag", default="myanmar", help="Tag to scrape (myanmar, mamagyi, mmporns, all, etc.)")
    parser.add_argument("--start-page", type=int, default=None, help="Starting page number (defaults to checkpoint)")
    parser.add_argument("--pages", type=int, default=5, help="Number of pages to scrape")
    parser.add_argument("--workers", type=int, default=6, help="Thread pool concurrency")
    parser.add_argument("--all-tags", action="store_true", help="Crawl across all site tags sequentially")
    parser.add_argument("--daemon", action="store_true", help="Run 24/7 continuous watcher daemon")
    args = parser.parse_args()

    run_pipeline(
        tag=args.tag,
        start_page=args.start_page,
        max_pages=args.pages,
        workers=args.workers,
        continuous=args.daemon,
        all_tags=args.all_tags
    )
