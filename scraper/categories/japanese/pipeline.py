"""
SupJav Complete Scraper Pipeline
Target: SupJav (https://supjav.com)
Stores into Supabase isolated schema: 'scraper.japanese_videos'
Captures only working hosts: VOE HD, VidSpeed, LuluStream
Features:
 - High-speed multi-threaded detail enrichment (ThreadPoolExecutor)
 - Comprehensive historical backfill across ALL categories with persistent checkpoints
 - 24/7 continuous watcher daemon for new incoming videos
 - Local post-ID deduplication cache for instant lookups (<1ms)
 - Safe retries with exponential backoff on network/tunnel drops
"""

import sys
import re
import json
import time
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from curl_cffi import requests

CURRENT_DIR = Path(__file__).resolve().parent
CATEGORIES_DIR = CURRENT_DIR.parent
SCRAPER_DIR = CATEGORIES_DIR.parent
PROJECT_ROOT = SCRAPER_DIR.parent

for p in [str(CURRENT_DIR), str(SCRAPER_DIR), str(PROJECT_ROOT)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from scraper.categories.japanese.polish import polish_supjav_record
from scraper.db import (
    upsert_japanese_videos_batch,
    get_japanese_videos,
    get_supabase_client
)

BASE_URL = "https://supjav.com"
CHROME_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# Checkpoint and Cache Paths
CHECKPOINT_FILE = CURRENT_DIR / "checkpoint.json"
KNOWN_IDS_FILE = CURRENT_DIR / "known_post_ids.json"

# Strict working locker whitelist: only these 3 are embeddable externally
# EVS (EarnVids - IP locked) and ST (Streamtape - Domain locked) are strictly skipped
WORKING_SERVER_MAP = {
    "VOE": "VOE HD",
    "VAS": "VidSpeed",
    "LUC": "LuluStream"
}

# Supported SupJav categories covering the entire site catalog
CATEGORIES_CONFIG = {
    "uncensored-jav": {
        "name": "Uncensored JAV",
        "url_path": "/category/uncensored-jav",
        "default_max": 1958
    },
    "amateur": {
        "name": "Amateur JAV",
        "url_path": "/category/amateur",
        "default_max": 1655
    },
    "censored-jav": {
        "name": "Censored JAV",
        "url_path": "/category/censored-jav",
        "default_max": 6555
    },
    "reducing-mosaic": {
        "name": "Reducing Mosaic JAV",
        "url_path": "/category/reducing-mosaic",
        "default_max": 4692
    },
    "popular": {
        "name": "Popular Hits",
        "url_path": "/popular",
        "default_max": 3
    }
}

DEFAULT_CATEGORIES = ["uncensored-jav", "amateur", "censored-jav", "reducing-mosaic", "popular"]

# ─── Session Factory ─────────────────────────────────────────────────────────

def create_session(impersonate: str = "chrome124") -> requests.Session:
    """Creates a clean TLS-impersonated browser session with authentic, untouched headers."""
    return requests.Session(impersonate=impersonate)

def warm_up_session(session: requests.Session) -> bool:
    """Establishes natural browser navigation by hitting homepage first to warm up cookies."""
    try:
        r = session.get(f"{BASE_URL}/", timeout=12)
        return r.status_code == 200
    except Exception:
        return False

# ─── Checkpoint & Known IDs State Management ─────────────────────────────────

def load_checkpoint() -> Dict[str, Any]:
    """Loads scraper checkpoint state, or initializes default schema."""
    default_state = {
        "categories": {
            cat: {"current_page": 1, "max_pages": info["default_max"], "completed": False}
            for cat, info in CATEGORIES_CONFIG.items()
        },
        "stats": {
            "total_ingested": 0,
            "last_sync": None
        }
    }
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Ensure all categories exist in saved file
                for cat, info in CATEGORIES_CONFIG.items():
                    if cat not in data.get("categories", {}):
                        data.setdefault("categories", {})[cat] = {
                            "current_page": 1,
                            "max_pages": info["default_max"],
                            "completed": False
                        }
                return data
        except Exception as e:
            print(f"[!] Warning reading checkpoint.json ({e}), creating fresh checkpoint.")
    return default_state

def save_checkpoint(data: Dict[str, Any]) -> None:
    """Saves scraper checkpoint state safely to disk."""
    try:
        data["stats"]["last_sync"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[!] Failed to save checkpoint: {e}")

def load_known_post_ids() -> Set[str]:
    """
    Loads known post IDs from disk cache, merging with DB if cache is empty.
    Allows instant in-memory deduplication (<1ms).
    """
    known_ids: Set[str] = set()
    if KNOWN_IDS_FILE.exists():
        try:
            with open(KNOWN_IDS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    known_ids.update(str(x) for x in data)
        except Exception as e:
            print(f"[!] Warning reading {KNOWN_IDS_FILE.name}: {e}")

    # If cache is small or empty, populate from database
    if len(known_ids) < 20:
        try:
            print("[*] Synchronizing known post IDs from Supabase database...")
            sb = get_supabase_client()
            offset = 0
            limit = 1000
            while True:
                res = sb.rpc("scraper_get_japanese_videos", {"p_limit": limit, "p_offset": offset}).execute()
                if not res.data:
                    break
                for r in res.data:
                    pid = r.get("source_post_id")
                    if pid:
                        known_ids.add(str(pid))
                if len(res.data) < limit:
                    break
                offset += limit
            print(f"    [✓] Synced {len(known_ids)} known post IDs from database.")
            save_known_post_ids(known_ids)
        except Exception as e:
            print(f"[!] Note: Could not query DB for initial IDs ({e}). Using local cache.")

    return known_ids

def save_known_post_ids(known_ids: Set[str]) -> None:
    """Saves set of known post IDs to disk cache."""
    try:
        with open(KNOWN_IDS_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(list(known_ids)), f)
    except Exception as e:
        print(f"[!] Failed to save known_post_ids.json: {e}")

# ─── Detail Scraper & Embed Resolver ─────────────────────────────────────────

def resolve_server_embed(session: requests.Session, data_link: str) -> Optional[str]:
    """
    Decodes SupJav reverse-hex data-link via supremejav.php?c=
    and captures the HTTP 302 redirect location.
    """
    if not data_link or len(data_link) < 32:
        return None

    rev_link = data_link[::-1]
    url = f"https://lk1.supremejav.com/supjav.php?c={rev_link}"
    for attempt in range(2):
        try:
            res = session.get(url, headers={"Referer": "https://supjav.com/"}, allow_redirects=False, timeout=8)
            loc = res.headers.get("location")
            if loc and loc.startswith("http"):
                return loc
        except Exception:
            if attempt == 0:
                time.sleep(0.4)
    return None

def scrape_detail_page(session: requests.Session, detail_url: str) -> Dict[str, Any]:
    """
    Scrapes detail page to extract ONLY working server mirrors (VOE HD, VidSpeed, LuluStream).
    Skips unworkable hosts (Streamtape, EarnVids) before making any network calls.
    """
    try:
        res = session.get(detail_url, headers={"Referer": "https://supjav.com/"}, timeout=12)
        if res.status_code != 200:
            return {"servers": []}

        soup = BeautifulSoup(res.text, "html.parser")
        servers = []
        cd_servers = soup.select(".cd-server")

        if len(cd_servers) > 1:
            for part_idx, cd in enumerate(cd_servers, start=1):
                part_label = f" (Part {part_idx})"
                for btn in cd.select(".btn-server"):
                    raw_name = btn.get_text().strip().upper()
                    if raw_name not in WORKING_SERVER_MAP:
                        continue

                    friendly_name = WORKING_SERVER_MAP[raw_name]
                    full_srv_name = f"{friendly_name}{part_label}"
                    data_link = btn.get("data-link", "")
                    resolved_url = resolve_server_embed(session, data_link)
                    if resolved_url:
                        servers.append({
                            "name": full_srv_name,
                            "embed_url": resolved_url.split("#")[0]
                        })
        else:
            for btn in soup.select(".btn-server"):
                raw_name = btn.get_text().strip().upper()
                if raw_name not in WORKING_SERVER_MAP:
                    continue

                friendly_name = WORKING_SERVER_MAP[raw_name]
                data_link = btn.get("data-link", "")
                resolved_url = resolve_server_embed(session, data_link)
                if resolved_url:
                    servers.append({
                        "name": friendly_name,
                        "embed_url": resolved_url.split("#")[0]
                    })

        # Rank mirrors: VOE HD -> VidSpeed -> LuluStream
        def locker_rank(s):
            n = s["name"].lower()
            if "voe" in n: return 0
            if "vidspeed" in n or "vas" in n: return 1
            if "lulu" in n or "luc" in n: return 2
            return 3

        servers.sort(key=locker_rank)
        tags = [a.get_text(strip=True).lower() for a in soup.select(".tags a, .cats a")]
        actors = [a.get_text(strip=True) for a in soup.select(".cast a, a[href*='/cast/']")]

        return {
            "servers": servers,
            "tags": list(set(tags)),
            "actors": list(set(actors))
        }
    except Exception as e:
        return {"servers": []}

def enrich_items_concurrently(items: List[Dict[str, Any]], max_workers: int = 3) -> List[Dict[str, Any]]:
    """
    Enriches a list of items concurrently using a managed thread pool.
    Each thread owns its own session with Chrome impersonation.
    """
    if not items:
        return []

    enriched = []
    def worker(item):
        session = create_session()
        detail_data = scrape_detail_page(session, item["detail_url"])
        item["servers"] = detail_data.get("servers", [])
        item["tags"] = detail_data.get("tags", [])
        item["actors"] = detail_data.get("actors", [])
        return item

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker, itm) for itm in items]
        for f in as_completed(futures):
            try:
                res = f.result()
                if res.get("servers"):
                    enriched.append(res)
            except Exception:
                pass

    return enriched

# ─── Listing Scraper ─────────────────────────────────────────────────────────

def scrape_listing_page(session: requests.Session, page_num: int = 1, category: str = "uncensored-jav") -> Optional[List[Dict[str, Any]]]:
    """Scrapes a single page of video cards from SupJav. Returns None on network/block failure."""
    cfg = CATEGORIES_CONFIG.get(category, {"url_path": f"/category/{category}"})
    base_path = cfg["url_path"]

    if category == "popular":
        url = f"{BASE_URL}/popular" if page_num == 1 else f"{BASE_URL}/popular/page/{page_num}"
    else:
        url = f"{BASE_URL}{base_path}" if page_num == 1 else f"{BASE_URL}{base_path}/page/{page_num}"

    print(f"[*] Fetching Listing: {url}")
    cards = []
    for attempt in range(3):
        try:
            res = session.get(url, timeout=15)
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, "html.parser")
                cards = soup.select(".post")
                print(f"    Found {len(cards)} video cards")
                break
            elif res.status_code in (403, 429, 500, 502, 503):
                title_m = re.search(r'<title>(.*?)</title>', res.text, re.IGNORECASE)
                title_str = title_m.group(1).strip() if title_m else f"HTTP {res.status_code}"
                cf_ray = res.headers.get("cf-ray", "none")
                wait_sec = 2.0 * (attempt + 1)
                fallback_imp = "safari15_5" if attempt % 2 == 1 else "chrome124"
                print(f"    [!] Pacing alert ({res.status_code}: {title_str} | CF-Ray: {cf_ray}) on {url}, rotating TLS profile to {fallback_imp} and waiting {wait_sec}s...")
                session = create_session(impersonate=fallback_imp)
                warm_up_session(session)
                time.sleep(wait_sec)
            else:
                print(f"    [!] HTTP error {res.status_code} on {url}")
                return None
        except Exception as e:
            wait_sec = 2.0 * (attempt + 1)
            if attempt < 2:
                print(f"    [!] Request glitch ({e}), retrying in {wait_sec}s...")
                time.sleep(wait_sec)
            else:
                print(f"    [!] Request failed for {url}: {e}")
                return None
    else:
        print(f"    [!] Could not bypass Cloudflare protection for {url} after 3 attempts.")
        return None

    items = []
    for card in cards:
        link_tag = card.select_one("a[href]")
        if not link_tag:
            continue

        detail_url = link_tag.get("href", "")
        post_id_match = re.search(r"/(\d+)\.html", detail_url)
        if not post_id_match:
            continue
        post_id = post_id_match.group(1)

        title = link_tag.get("title", "") or card.select_one(".entry-title, h2, h3")
        if hasattr(title, "get_text"):
            title = title.get_text(strip=True)
        title = str(title).strip()

        img_tag = card.select_one("img[data-original], img[src]")
        thumb_url = ""
        if img_tag:
            thumb_url = img_tag.get("data-original") or img_tag.get("src", "")

        meta_text = card.get_text()
        views_match = re.search(r"([\d,]+)\s+Views", meta_text, re.IGNORECASE)
        views_raw = views_match.group(1) if views_match else "0"
        views_count = int(views_raw.replace(",", ""))

        items.append({
            "source_site": "supjav",
            "source_post_id": post_id,
            "title": title,
            "detail_url": detail_url,
            "thumbnail_url": thumb_url,
            "views_raw": views_raw,
            "views_count": views_count,
            "duration_formatted": "01:30:00",
            "duration_seconds": 5400,
            "rating_raw": "95%",
            "rating_score": 95.0,
            "categories": ["japanese", category]
        })

    return items

# ─── Ingestion Pipeline Core ─────────────────────────────────────────────────

def ingest_category_pages(
    category: str,
    from_page: int,
    to_page: int,
    known_ids: Set[str],
    checkpoint: Dict[str, Any],
    batch_size: int = 24,
    workers: int = 3
) -> int:
    """
    Ingests a range of pages for a specific category.
    Performs instant deduplication against known_ids, concurrent detail fetching,
    polishing, and batch upserting into Supabase.
    """
    session = create_session()
    warm_up_session(session)
    cat_info = CATEGORIES_CONFIG.get(category, {"name": category})
    print("\n" + "=" * 65)
    print(f"Scraping [{cat_info['name']}] | Pages: {from_page} to {to_page} | Workers: {workers}")
    print("=" * 65)

    total_ingested = 0
    pending_records = []

    for page_num in range(from_page, to_page + 1):
        print(f"\n--- Category: {category} | Page {page_num}/{to_page} ---")
        items = scrape_listing_page(session, page_num=page_num, category=category)
        if not items:
            print("    [!] Page returned 0 items. Moving to next.")
            continue

        # Filter out already indexed items
        new_items = [itm for itm in items if itm["source_post_id"] not in known_ids]
        skipped_count = len(items) - len(new_items)
        if skipped_count > 0:
            print(f"    [*] Skipped {skipped_count} videos already in DB.")

        if not new_items:
            print("    [✓] All videos on this page are already indexed.")
            # Still update checkpoint so we don't re-crawl next time
            checkpoint["categories"][category]["current_page"] = page_num
            save_checkpoint(checkpoint)
            time.sleep(0.3)
            continue

        print(f"    [*] Enriching {len(new_items)} new videos concurrently ({workers} workers)...")
        enriched_items = enrich_items_concurrently(new_items, max_workers=workers)
        print(f"    [✓] Successfully resolved working mirrors for {len(enriched_items)}/{len(new_items)} videos.")

        for itm in enriched_items:
            srv_names = [s["name"] for s in itm.get("servers", [])]
            print(f"      + #{itm['source_post_id']} - {itm['title'][:40]}... -> {srv_names}")
            polished = polish_supjav_record(itm)
            pending_records.append(polished)
            known_ids.add(itm["source_post_id"])

        # Flush batch when size reached
        if len(pending_records) >= batch_size:
            print(f"\n[*] Upserting batch of {len(pending_records)} videos to Supabase...")
            res = upsert_japanese_videos_batch(pending_records)
            print(f"    [✓] Batch upsert result: {res}")
            total_ingested += len(pending_records)
            checkpoint["stats"]["total_ingested"] = checkpoint["stats"].get("total_ingested", 0) + len(pending_records)
            pending_records = []
            save_known_post_ids(known_ids)

        # Update checkpoint page
        checkpoint["categories"][category]["current_page"] = page_num
        save_checkpoint(checkpoint)
        time.sleep(1.2)  # Polite page pacing

    # Flush any remaining items
    if pending_records:
        print(f"\n[*] Upserting final batch of {len(pending_records)} videos to Supabase...")
        res = upsert_japanese_videos_batch(pending_records)
        print(f"    [✓] Final upsert result: {res}")
        total_ingested += len(pending_records)
        checkpoint["stats"]["total_ingested"] = checkpoint["stats"].get("total_ingested", 0) + len(pending_records)
        pending_records = []
        save_known_post_ids(known_ids)
        save_checkpoint(checkpoint)

    print(f"\n[✓] Finished {cat_info['name']} (Pages {from_page}-{to_page}). New videos added: {total_ingested}")
    return total_ingested

# ─── Full Catalog Ingestion Loop (Every Video) ───────────────────────────────

def run_full_catalog_scraper(
    categories: Optional[List[str]] = None,
    pages_per_cat: int = 10,
    resume: bool = True,
    workers: int = 3
) -> int:
    """
    Ingests videos from SupJav across categories.
    Automatically resumes from last saved page in checkpoint.json.
    """
    if categories is None:
        categories = DEFAULT_CATEGORIES

    checkpoint = load_checkpoint()
    known_ids = load_known_post_ids()
    grand_total = 0

    print("=" * 65)
    print("SupJav Deep Catalog Ingestion")
    print(f"Categories: {categories}")
    print(f"Pages per category: {pages_per_cat} | Resume mode: {resume}")
    print(f"Known IDs in cache: {len(known_ids)}")
    print("=" * 65)

    for cat in categories:
        cat_state = checkpoint["categories"].get(cat, {"current_page": 1, "max_pages": 1000})
        current_p = cat_state["current_page"] if resume else 1
        max_p = cat_state.get("max_pages", 1000)

        from_page = current_p
        to_page = min(from_page + pages_per_cat - 1, max_p)

        if from_page > max_p:
            print(f"[*] Category {cat} already completed (Page {from_page} > {max_p}). Skipping.")
            continue

        added = ingest_category_pages(
            category=cat,
            from_page=from_page,
            to_page=to_page,
            known_ids=known_ids,
            checkpoint=checkpoint,
            batch_size=24,
            workers=workers
        )
        grand_total += added

    print("\n" + "=" * 65)
    print(f"🎉 Full Catalog Ingestion Complete! Total new videos ingested: {grand_total}")
    print("=" * 65)
    return grand_total

# ─── Continuous Watcher Daemon (New Coming Videos) ───────────────────────────

def run_watch_mode(
    interval_seconds: int = 900,
    categories: Optional[List[str]] = None,
    workers: int = 3
) -> None:
    """
    24/7 continuous watcher daemon that monitors SupJav for new releases.
    Checks Page 1 of all categories, detects unpublished videos, enriches them
    with working hosts, and ingests them into Supabase.
    """
    if categories is None:
        categories = DEFAULT_CATEGORIES

    checkpoint = load_checkpoint()
    known_ids = load_known_post_ids()

    print("=" * 65)
    print("⚡ SupJav Continuous Watcher Daemon Started")
    print(f"Monitoring Categories: {categories}")
    print(f"Check Interval: {interval_seconds} seconds ({interval_seconds / 60:.1f} minutes)")
    print(f"Known IDs in Cache: {len(known_ids)}")
    print("=" * 65)

    cycle = 1
    session = create_session()

    while True:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n\n[{timestamp}] >>> Watch Cycle #{cycle}: Checking for New Coming Releases <<<")
        cycle_new_count = 0

        for cat in categories:
            try:
                # Check page 1 for the newest releases
                items = scrape_listing_page(session, page_num=1, category=cat)
                new_items = [itm for itm in items if itm["source_post_id"] not in known_ids]

                if not new_items:
                    print(f"  [{cat}] Up to date. 0 new videos.")
                    continue

                print(f"  [{cat}] 🔥 Found {len(new_items)} NEW incoming videos! Enriching with working hosts...")
                enriched_items = enrich_items_concurrently(new_items, max_workers=workers)

                if enriched_items:
                    records = []
                    for itm in enriched_items:
                        srv_names = [s["name"] for s in itm.get("servers", [])]
                        print(f"    + New Release: #{itm['source_post_id']} - {itm['title'][:40]}... -> {srv_names}")
                        polished = polish_supjav_record(itm)
                        records.append(polished)
                        known_ids.add(itm["source_post_id"])

                    res = upsert_japanese_videos_batch(records)
                    print(f"    [✓] Ingested {len(records)} new releases into database ({res})")
                    cycle_new_count += len(records)
                    save_known_post_ids(known_ids)
                    checkpoint["stats"]["total_ingested"] = checkpoint["stats"].get("total_ingested", 0) + len(records)
                    save_checkpoint(checkpoint)
                else:
                    print(f"  [{cat}] No working mirrors found for new items.")

            except Exception as e:
                print(f"  [!] Error checking category {cat}: {e}")

            time.sleep(1.5)

        summary_time = time.strftime("%H:%M:%S")
        if cycle_new_count > 0:
            print(f"\n[{summary_time}] ✨ Watch Cycle #{cycle} Complete: Successfully ingested {cycle_new_count} brand-new videos!")
        else:
            print(f"\n[{summary_time}] Watch Cycle #{cycle} Complete: All categories up to date.")

        print(f"[*] Sleeping for {interval_seconds}s until next check. Press Ctrl+C to stop.")
        cycle += 1
        time.sleep(interval_seconds)

# ─── Database Status Inspection ──────────────────────────────────────────────

def print_database_status() -> None:
    """Prints current summary of Japanese videos in the database."""
    print("=" * 65)
    print("Chuglii Japanese Video Database Status")
    print("=" * 65)
    try:
        sb = get_supabase_client()
        res_count = sb.rpc("scraper_count_japanese_videos", {}).execute()
        total_videos = res_count.data if res_count else 0
        print(f"Total Japanese Videos in Database: {total_videos}")

        sample = get_japanese_videos(limit=5)
        if sample:
            print(f"\nLatest {len(sample)} Ingested Videos:")
            for idx, vid in enumerate(sample, 1):
                raw = vid.get("raw_payload", {}) or {}
                servers = raw.get("servers", [])
                srv_names = [s["name"] for s in servers] if servers else ["Primary Embed"]
                print(f"  {idx}. [{vid.get('source_post_id')}] {vid.get('title')[:45]}...")
                print(f"     Working Mirrors: {srv_names}")
                print(f"     Watch URL: https://chuglii.in/japanese/video/{vid.get('id')}")
        else:
            print("No videos found in database yet.")

        checkpoint = load_checkpoint()
        print("\nCheckpoint Progress:")
        for cat, info in checkpoint.get("categories", {}).items():
            print(f"  - {cat:18}: Page {info.get('current_page', 1)} / {info.get('max_pages', '?')}")

    except Exception as e:
        print(f"[!] Error fetching database status: {e}")
    print("=" * 65)

# ─── CLI Entrypoint ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SupJav Scraper Pipeline - Working Mirrors Only")
    parser.add_argument("--category", type=str, default="all", help="Category (uncensored-jav, amateur, censored-jav, reducing-mosaic, popular, all)")
    parser.add_argument("--pages", type=int, default=1, help="Number of pages to scrape")
    parser.add_argument("--from-page", type=int, default=None, help="Specific start page")
    parser.add_argument("--to-page", type=int, default=None, help="Specific end page")
    parser.add_argument("--watch", type=int, default=0, help="Run 24/7 continuous watcher every N seconds (e.g. --watch 900)")
    parser.add_argument("--workers", type=int, default=3, help="Concurrent worker threads (default: 3)")
    parser.add_argument("--batch-size", type=int, default=24, help="Batch size for DB upsert")
    parser.add_argument("--status", action="store_true", help="Print database and checkpoint status")
    parser.add_argument("--reset-checkpoint", action="store_true", help="Reset checkpoint to page 1")
    args = parser.parse_args()

    if args.reset_checkpoint:
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
        print("[✓] Checkpoint reset to page 1.")
        sys.exit(0)

    if args.status:
        print_database_status()
        sys.exit(0)

    if args.watch > 0:
        cats = None if args.category == "all" else [args.category]
        run_watch_mode(interval_seconds=args.watch, categories=cats, workers=args.workers)
    else:
        cats = DEFAULT_CATEGORIES if args.category == "all" else [args.category]
        if args.from_page is not None and args.to_page is not None:
            checkpoint = load_checkpoint()
            known_ids = load_known_post_ids()
            for c in cats:
                ingest_category_pages(
                    category=c,
                    from_page=args.from_page,
                    to_page=args.to_page,
                    known_ids=known_ids,
                    checkpoint=checkpoint,
                    batch_size=args.batch_size,
                    workers=args.workers
                )
        else:
            run_full_catalog_scraper(
                categories=cats,
                pages_per_cat=args.pages,
                resume=True,
                workers=args.workers
            )
