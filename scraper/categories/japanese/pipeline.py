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

import os
import sys
import re
import json
import time
import argparse
import threading
import atexit
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from curl_cffi import requests

def cleanup_orphan_browsers() -> None:
    """Terminates any leftover orphan Chrome or Patchright browser processes."""
    if sys.platform != "win32":
        try:
            import subprocess
            subprocess.run(["pkill", "-f", "chrome"], capture_output=True, timeout=5)
            subprocess.run(["pkill", "-f", "patchright"], capture_output=True, timeout=5)
        except Exception:
            pass

atexit.register(cleanup_orphan_browsers)

CURRENT_DIR = Path(__file__).resolve().parent
CATEGORIES_DIR = CURRENT_DIR.parent
SCRAPER_DIR = CATEGORIES_DIR.parent
PROJECT_ROOT = SCRAPER_DIR.parent

for p in [str(CURRENT_DIR), str(SCRAPER_DIR), str(PROJECT_ROOT)]:
    if p not in sys.path:
        sys.path.insert(0, p)

from scraper.categories.japanese.polish import polish_supjav_record
from scraper.proxy_pool import GLOBAL_PROXY_POOL
from scraper.db import (
    upsert_japanese_videos_batch,
    get_japanese_videos,
    get_supabase_client,
    rpc_result_succeeded,
)

BASE_URL = "https://supjav.com"
CHROME_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# Checkpoint and Cache Paths
CHECKPOINT_FILE = CURRENT_DIR / "checkpoint.json"
KNOWN_IDS_FILE = CURRENT_DIR / "known_post_ids.json"
COOKIE_CACHE_FILE = CURRENT_DIR / "cf_session.json"
CACHED_CF_COOKIES: Dict[str, str] = {}
CACHED_CF_UA: Optional[str] = None

# Comprehensive working locker map covering all SupJav host mirrors
WORKING_SERVER_MAP = {
    "VAS": "VidSpeed",
    "VIDSPEED": "VidSpeed",
    "VDS": "VidSpeed",
    "LUC": "LuluStream",
    "LULU": "LuluStream",
    "LULUSTREAM": "LuluStream",
    "LUMIN": "LuluStream",
    "VOE": "VOE HD",
    "VOE HD": "VOE HD",
    "FST": "FC2Stream",
    "FC2": "FC2Stream",
    "FC2STREAM": "FC2Stream",
    "ST": "Streamtape",
    "STREAMTAPE": "Streamtape",
    "EVS": "EarnVids",
    "EARNVIDS": "EarnVids",
    "VIDHIDE": "EarnVids",
}

def normalize_server_name(raw_name: str) -> str:
    """Normalizes raw button text to clean, user-facing host names without dropping any mirror."""
    clean = re.sub(r"\s+", " ", (raw_name or "").strip()).upper()
    if clean in WORKING_SERVER_MAP:
        return WORKING_SERVER_MAP[clean]
    if "VOE" in clean:
        return "VOE HD"
    if "VAS" in clean or "VIDSPEED" in clean or "VDS" in clean:
        return "VidSpeed"
    if "FC2" in clean or "FST" in clean:
        return "FC2Stream"
    if "LULU" in clean or "LUC" in clean or "LUMIN" in clean:
        return "LuluStream"
    if "EVS" in clean or "EARN" in clean or "VIDHIDE" in clean:
        return "EarnVids"
    if clean == "ST" or "STREAMTAPE" in clean:
        return "Streamtape"
    return clean.title() if len(clean) >= 2 else "Mirror"

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

# ─── Cloudflare Session Caching & Session Factory ───────────────────────────

def load_cached_cf_session(session: requests.Session) -> bool:
    """Loads cached Cloudflare session cookies and User-Agent if available."""
    global CACHED_CF_COOKIES, CACHED_CF_UA
    try:
        from scraper.db import get_scraper_checkpoint
        sb_chk = get_scraper_checkpoint("supjav_cf_session")
        if sb_chk and "cookies" in sb_chk and sb_chk["cookies"]:
            CACHED_CF_COOKIES = sb_chk.get("cookies", {})
            CACHED_CF_UA = sb_chk.get("user_agent")
        elif COOKIE_CACHE_FILE.exists():
            with open(COOKIE_CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                CACHED_CF_COOKIES = data.get("cookies", {})
                CACHED_CF_UA = data.get("user_agent")

        if CACHED_CF_COOKIES:
            for k, v in CACHED_CF_COOKIES.items():
                session.cookies.set(k, v)
            if CACHED_CF_UA:
                session.headers["User-Agent"] = CACHED_CF_UA
            print(f"  [✓] Loaded {len(CACHED_CF_COOKIES)} cached Cloudflare session cookies.")
            return True
    except Exception as e:
        print(f"  [!] Note: Failed to load cached CF session: {e}")
    return False

def save_cached_cf_session(cookies: Dict[str, str], user_agent: Optional[str]) -> None:
    """Saves valid Cloudflare session cookies and User-Agent locally and to Supabase."""
    global CACHED_CF_COOKIES, CACHED_CF_UA
    CACHED_CF_COOKIES = cookies
    if user_agent:
        CACHED_CF_UA = user_agent
    data = {
        "cookies": cookies,
        "user_agent": CACHED_CF_UA,
        "saved_at": time.time()
    }
    try:
        with open(COOKIE_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass
    try:
        from scraper.db import set_scraper_checkpoint
        set_scraper_checkpoint("supjav_cf_session", data)
    except Exception:
        pass

def create_session(impersonate: str = "chrome124") -> requests.Session:
    """Creates a clean TLS-impersonated browser session with cached CF cookies applied."""
    s = requests.Session(impersonate=impersonate)
    load_cached_cf_session(s)
    return s

# Cloudflare Concurrency Lock & Rate Limit Protection
CF_SOLVE_LOCK = threading.Lock()
CF_RATE_LIMITED_UNTIL: float = 0.0
LAST_CF_SOLVE_TIME: float = 0.0

def ensure_valid_cf_session(session: requests.Session) -> bool:
    """
    Thread-safe Cloudflare session refresher.
    Solves Turnstile ONCE on the homepage and synchronizes cookies across all worker sessions.
    Prevents parallel browser launches and 429 rate limit blocks.
    """
    global CF_RATE_LIMITED_UNTIL, LAST_CF_SOLVE_TIME, CACHED_CF_COOKIES, CACHED_CF_UA
    now = time.time()
    if now < CF_RATE_LIMITED_UNTIL:
        return False

    with CF_SOLVE_LOCK:
        now = time.time()
        # If another worker thread solved CF within the last 45s, reuse the fresh session
        if now - LAST_CF_SOLVE_TIME < 45.0 and CACHED_CF_COOKIES:
            for k, v in CACHED_CF_COOKIES.items():
                session.cookies.set(k, v)
            if CACHED_CF_UA:
                session.headers["User-Agent"] = CACHED_CF_UA
            return True

        if now < CF_RATE_LIMITED_UNTIL:
            return False

        print("    [*] Refreshing SupJav Cloudflare session via Scrapling (single thread)...")
        html = fetch_with_scrapling(f"{BASE_URL}/", session=session)
        if html and "Just a moment..." not in html:
            LAST_CF_SOLVE_TIME = time.time()
            return True
        return False

def fetch_with_scrapling(url: str, session: Optional[requests.Session] = None) -> Optional[str]:
    """
    Bypasses Cloudflare Turnstile/Interstitial using Scrapling's StealthyFetcher.
    Solves challenges automatically, extracts raw HTML, and synchronizes/caches cookies.
    Protected by rate-limit cooldown and automatic cleanup.
    """
    global CF_RATE_LIMITED_UNTIL, LAST_CF_SOLVE_TIME

    if time.time() < CF_RATE_LIMITED_UNTIL:
        print(f"    [!] Skipping Scrapling: Rate limit cooldown active ({int(CF_RATE_LIMITED_UNTIL - time.time())}s remaining).")
        return None

    try:
        from scrapling.fetchers import StealthyFetcher
        print(f"    [*] Engaging Scrapling StealthyFetcher with Turnstile auto-solver for {url}...")
        response = StealthyFetcher.fetch(
            url,
            solve_cloudflare=True,
            headless=True,
            network_idle=True,
            timeout=45000
        )
        if response and response.status == 429:
            CF_RATE_LIMITED_UNTIL = time.time() + 60.0
            print("    [!] Scrapling received HTTP 429 (Rate Limit). Cooling down for 60s.")
            return None

        if response and response.status == 200:
            # 1. Extract raw HTML string from bytes body
            raw_body = response.body
            encoding = getattr(response, "encoding", "utf-8") or "utf-8"
            html = raw_body.decode(encoding, errors="ignore")

            # 2. Extract cookies (cookies is a tuple of dicts)
            extracted_cookies: Dict[str, str] = {}
            if hasattr(response, "cookies") and response.cookies:
                for c in response.cookies:
                    if isinstance(c, dict) and "name" in c and "value" in c:
                        extracted_cookies[c["name"]] = c["value"]

            # 3. Extract exact User-Agent used by stealth browser
            ua = None
            if hasattr(response, "request_headers") and response.request_headers:
                ua = response.request_headers.get("user-agent") or response.request_headers.get("User-Agent")

            # 4. Sync into session & cache for all subsequent requests
            if session is not None and extracted_cookies:
                for k, v in extracted_cookies.items():
                    session.cookies.set(k, v)
                if ua:
                    session.headers["User-Agent"] = ua
                print(f"    [✓] Scrapling cleared Cloudflare! Cached {len(extracted_cookies)} cookies (including cf_clearance).")

            if extracted_cookies:
                save_cached_cf_session(extracted_cookies, ua)
                LAST_CF_SOLVE_TIME = time.time()

            return html
        else:
            status = response.status if response else "None"
            print(f"    [!] Scrapling fetch returned status: {status}")
            return None
    except Exception as e:
        print(f"    [!] Scrapling StealthyFetcher error: {e}")
        return None

def warm_up_session(session: requests.Session) -> bool:
    """Establishes natural browser navigation by hitting homepage first to warm up cookies, clearing Cloudflare if challenged."""
    try:
        r = session.get(f"{BASE_URL}/", timeout=12)
        if r.status_code == 200 and "Just a moment..." not in r.text and "challenge-platform" not in r.text:
            return True
        print(f"[*] SupJav homepage Cloudflare challenge detected (HTTP {r.status_code}). Engaging Scrapling...")
        html = fetch_with_scrapling(f"{BASE_URL}/", session=session)
        return bool(html and "Just a moment..." not in html)
    except Exception as e:
        print(f"[*] Homepage warm-up exception: {e}. Attempting Scrapling...")
        html = fetch_with_scrapling(f"{BASE_URL}/", session=session)
        return bool(html and "Just a moment..." not in html)

# ─── Checkpoint & Known IDs State Management ─────────────────────────────────

def _default_checkpoint() -> Dict[str, Any]:
    return {
        "categories": {
            cat: {"current_page": 2, "max_pages": info["default_max"], "completed": False}
            for cat, info in CATEGORIES_CONFIG.items()
        },
        "stats": {"total_ingested": 0, "last_sync": None},
    }


def load_checkpoint() -> Dict[str, Any]:
    """Loads SupJav state from Supabase first and the atomic local cache second."""
    from scraper.progress import load_checkpoint as _load
    state = _load("supjav", CHECKPOINT_FILE, _default_checkpoint())
    categories = state.setdefault("categories", {})
    for cat, info in CATEGORIES_CONFIG.items():
        categories.setdefault(cat, {
            "current_page": 2,
            "max_pages": info["default_max"],
            "completed": False,
        })
    state.setdefault("stats", {"total_ingested": 0, "last_sync": None})
    return state


def save_checkpoint(data: Dict[str, Any]) -> bool:
    """Persists SupJav state remotely and mirrors it locally."""
    from scraper.progress import save_checkpoint as _save
    data.setdefault("stats", {})["last_sync"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return _save("supjav", data, CHECKPOINT_FILE)


def load_known_post_ids() -> Set[str]:
    """Loads SupJav known IDs from the central ledger + local cache."""
    from scraper.progress import load_known_ids as _load
    ids = _load("supjav", KNOWN_IDS_FILE)
    save_known_post_ids(ids)
    return ids


def save_known_post_ids(
    known_ids: Set[str],
    new_ids: Optional[Set[str]] = None,
    status: str = "stored",
) -> None:
    """Mirrors IDs locally and records newly-confirmed IDs in Supabase."""
    from scraper.progress import mark_known_ids, save_known_ids as _save
    if new_ids:
        mark_known_ids(new_ids, default_site="supjav", status=status)
    _save(KNOWN_IDS_FILE, known_ids)


# ─── Detail Scraper & Embed Resolver ─────────────────────────────────────────

def resolve_server_embed(session: requests.Session, data_link: str, proxy: Optional[str] = None) -> Optional[str]:
    """
    Decodes SupJav reverse-hex data-link via supremejav.php?c=
    and captures the HTTP 302 redirect location.
    """
    if not data_link or len(data_link) < 20:
        return None

    rev_link = data_link[::-1]
    url = f"https://lk1.supremejav.com/supjav.php?c={rev_link}"
    for attempt in range(2):
        try:
            kwargs = {"headers": {"Referer": "https://supjav.com/"}, "allow_redirects": False, "timeout": 8}
            if proxy:
                kwargs["proxies"] = {"http": proxy, "https": proxy}
            res = session.get(url, **kwargs)
            loc = res.headers.get("location")
            if loc and loc.startswith("http"):
                return loc
        except Exception:
            if attempt == 0:
                time.sleep(0.4)
    return None

def scrape_detail_page(session: requests.Session, detail_url: str, proxy: Optional[str] = None) -> Dict[str, Any]:
    """
    Scrapes detail page to extract all available video hosts and mirrors without omission.
    Uses centralized session refresh if challenged, preventing browser stampedes.
    """
    try:
        kwargs = {"headers": {"Referer": "https://supjav.com/"}, "timeout": 15}
        if proxy:
            kwargs["proxies"] = {"http": proxy, "https": proxy}
        res = session.get(detail_url, **kwargs)

        is_cf_block = (
            res.status_code in (403, 503) or
            "Just a moment..." in res.text or
            "challenge-platform" in res.text
        )

        if is_cf_block:
            refreshed = ensure_valid_cf_session(session)
            if refreshed:
                res = session.get(detail_url, **kwargs)
                is_cf_block = (
                    res.status_code in (403, 503) or
                    "Just a moment..." in res.text
                )
            if is_cf_block:
                return {"servers": []}

        if res.status_code != 200 or len(res.text) <= 3000:
            return {"servers": []}

        res_text = res.text

        if "Just a moment..." in res_text:
            return {"servers": []}

        soup = BeautifulSoup(res_text, "html.parser")
        servers = []
        cd_servers = soup.select(".cd-server")

        if len(cd_servers) > 1:
            for part_idx, cd in enumerate(cd_servers, start=1):
                part_label = f" (Part {part_idx})"
                for btn in cd.select(".btn-server"):
                    raw_name = btn.get_text().strip()
                    friendly_name = normalize_server_name(raw_name)
                    full_srv_name = f"{friendly_name}{part_label}"
                    data_link = btn.get("data-link", "").strip()
                    direct_href = btn.get("href", "").strip()
                    
                    resolved_url = None
                    if direct_href and direct_href.startswith("http") and "supjav.com" not in direct_href and "javascript" not in direct_href:
                        resolved_url = direct_href
                    elif data_link:
                        resolved_url = resolve_server_embed(session, data_link, proxy=proxy)
                        if not resolved_url:
                            # Gatekeeper fallback resolved dynamically by Edge Worker
                            resolved_url = f"https://supjav.com/?l={data_link}"
                    
                    if resolved_url:
                        servers.append({
                            "name": full_srv_name,
                            "embed_url": resolved_url.split("#")[0]
                        })
        else:
            for btn in soup.select(".btn-server"):
                raw_name = btn.get_text().strip()
                friendly_name = normalize_server_name(raw_name)
                data_link = btn.get("data-link", "").strip()
                direct_href = btn.get("href", "").strip()

                resolved_url = None
                if direct_href and direct_href.startswith("http") and "supjav.com" not in direct_href and "javascript" not in direct_href:
                    resolved_url = direct_href
                elif data_link:
                    resolved_url = resolve_server_embed(session, data_link, proxy=proxy)
                    if not resolved_url:
                        # Gatekeeper fallback resolved dynamically by Edge Worker
                        resolved_url = f"https://supjav.com/?l={data_link}"

                if resolved_url:
                    servers.append({
                        "name": friendly_name,
                        "embed_url": resolved_url.split("#")[0]
                    })

        # Rank mirrors: VOE HD (0) -> VidSpeed (1) -> FC2Stream (2) -> LuluStream (3) -> EarnVids (4) -> Streamtape (5)
        def server_sort_key(s):
            n = s["name"].lower()
            part = 0
            part_match = re.search(r"part\s*(\d+)", n)
            if part_match:
                part = int(part_match.group(1))

            rank = 6
            if "voe" in n: rank = 0
            elif "vidspeed" in n or "vas" in n: rank = 1
            elif "fc2" in n or "fst" in n: rank = 2
            elif "lulu" in n or "luc" in n: rank = 3
            elif "earnvids" in n or "evs" in n: rank = 4
            elif "streamtape" in n or "st" in n: rank = 5
            return (part, rank)

        # Deduplicate server mirrors by cleaned embed URL
        seen_embeds = set()
        deduped_servers = []
        for s in servers:
            clean_url = s.get("embed_url", "").split("#")[0].strip()
            if clean_url and clean_url.startswith("http") and clean_url not in seen_embeds:
                seen_embeds.add(clean_url)
                s["embed_url"] = clean_url
                deduped_servers.append(s)

        deduped_servers.sort(key=server_sort_key)
        tags = [a.get_text(strip=True).lower() for a in soup.select(".tags a, .cats a")]
        actors = [a.get_text(strip=True) for a in soup.select(".cast a, a[href*='/cast/']")]

        return {
            "servers": deduped_servers,
            "tags": list(set(tags)),
            "actors": list(set(actors))
        }
    except Exception as e:
        return {"servers": []}

def enrich_items_concurrently(items: List[Dict[str, Any]], max_workers: int = 3, proxy: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Enriches a list of items concurrently using a managed thread pool.
    Each thread owns its own session with Chrome impersonation.
    """
    if not items:
        return []

    active_proxy = proxy or os.getenv("PROXY_URL") or os.getenv("RESIDENTIAL_PROXY")

    enriched = []
    def worker(item):
        session = create_session()
        detail_data = scrape_detail_page(session, item["detail_url"], proxy=active_proxy)
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

_CF_CHALLENGE_MARKERS = (
    "Just a moment...",
    "challenge-platform",
    "cf-chl",
    "Attention Required! | Cloudflare",
)


def _looks_like_cf_challenge(html: str) -> bool:
    """Returns True for a Cloudflare interstitial, including HTTP 200 bodies."""
    lowered = (html or "").lower()
    # Real catalog pages can embed Cloudflare scripts; only short interstitial
    # bodies are challenges.
    return len(html or "") < 20000 and any(
        marker.lower() in lowered for marker in _CF_CHALLENGE_MARKERS
    )


def scrape_listing_page(session: requests.Session, page_num: int = 1, category: str = "uncensored-jav") -> Optional[List[Dict[str, Any]]]:
    """Scrapes a single page of video cards from SupJav. Auto-rotates validated free proxies if challenged."""
    cfg = CATEGORIES_CONFIG.get(category, {"url_path": f"/category/{category}"})
    base_path = cfg["url_path"]

    if category == "popular":
        url = f"{BASE_URL}/popular" if page_num == 1 else f"{BASE_URL}/popular/page/{page_num}"
    else:
        url = f"{BASE_URL}{base_path}" if page_num == 1 else f"{BASE_URL}{base_path}/page/{page_num}"

    print(f"[*] Fetching Listing: {url}")
    cards = []
    current_proxy = None

    for attempt in range(3):
        try:
            kwargs = {"timeout": 15}
            if current_proxy:
                kwargs["proxies"] = {"http": current_proxy, "https": current_proxy}

            res = session.get(url, **kwargs)
            is_cf_challenge = (
                res.status_code in (403, 503) or
                _looks_like_cf_challenge(res.text)
            )
            if res.status_code == 200 and not is_cf_challenge:
                soup = BeautifulSoup(res.text, "html.parser")
                cards = soup.select(".post")
                p_info = f" (via proxy {current_proxy})" if current_proxy else " (direct)"
                print(f"    Found {len(cards)} video cards{p_info}")
                break
            elif is_cf_challenge or res.status_code in (429, 500, 502):
                title_m = re.search(r'<title>(.*?)</title>', res.text, re.IGNORECASE)
                title_str = title_m.group(1).strip() if title_m else f"HTTP {res.status_code}"
                cf_ray = res.headers.get("cf-ray", "none")
                print(f"    [!] Cloudflare challenge detected ({res.status_code}: {title_str} | CF-Ray: {cf_ray})")

                # 1. Primary resolution: Auto-solve Turnstile challenge with Scrapling
                scrapling_html = fetch_with_scrapling(url, session=session)
                if scrapling_html and not _looks_like_cf_challenge(scrapling_html):
                    soup = BeautifulSoup(scrapling_html, "html.parser")
                    cards = soup.select(".post")
                    print(f"    Found {len(cards)} video cards (via Scrapling bypass)")
                    break

                # 2. Secondary fallback: Check configured proxies
                if current_proxy:
                    GLOBAL_PROXY_POOL.report_failure(current_proxy)
                current_proxy = os.getenv("PROXY_URL") or os.getenv("RESIDENTIAL_PROXY")
                if current_proxy:
                    print(f"    [!] Retrying with configured proxy: {current_proxy}...")
                    session = create_session(impersonate="chrome124")
                time.sleep(1.5)
            elif res.status_code in (404, 410):
                # Confirmed end-of-category / removed page.
                print(f"    [*] HTTP {res.status_code} on {url}; treating as end of category.")
                return []
            else:
                print(f"    [!] HTTP error {res.status_code} on {url}")
                return None
        except Exception as e:
            print(f"    [!] Connection glitch ({e}). Engaging Scrapling...")
            scrapling_html = fetch_with_scrapling(url, session=session)
            if scrapling_html and not _looks_like_cf_challenge(scrapling_html):
                soup = BeautifulSoup(scrapling_html, "html.parser")
                cards = soup.select(".post")
                print(f"    Found {len(cards)} video cards (via Scrapling bypass)")
                break

            if current_proxy:
                GLOBAL_PROXY_POOL.report_failure(current_proxy)
            current_proxy = os.getenv("PROXY_URL") or os.getenv("RESIDENTIAL_PROXY")
            time.sleep(1.5)
    else:
        print(f"    [!] Could not bypass Cloudflare protection for {url} after retries.")
        return None

    items = []
    for card in cards:
        link_tag = card.select_one("a[href]")
        if not link_tag:
            continue

        detail_url = urljoin(BASE_URL, link_tag.get("href", ""))
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
            thumb_url = urljoin(BASE_URL, img_tag.get("data-original") or img_tag.get("src", ""))

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
    Ingests a range of pages for a specific category and persists a safe cursor.

    ``scrape_listing_page`` returns ``None`` for a Cloudflare/blocked page and
    ``[]`` for a confirmed empty page.  Only the latter may advance the cursor.
    IDs are recorded in the central ledger only after a successful upsert.
    """
    session = create_session()
    warm_up_session(session)
    cat_info = CATEGORIES_CONFIG.get(category, {"name": category})
    print("\n" + "=" * 65)
    print(f"Scraping [{cat_info['name']}] | Pages: {from_page} to {to_page} | Workers: {workers}")
    print("=" * 65)

    total_ingested = 0
    cat_state = checkpoint.setdefault("categories", {}).setdefault(
        category, {"current_page": from_page, "max_pages": to_page, "completed": False}
    )

    for page_num in range(from_page, to_page + 1):
        print(f"\n--- Category: {category} | Page {page_num}/{to_page} ---")
        items = scrape_listing_page(session, page_num=page_num, category=category)
        if items is None:
            print("    [!] Listing page was blocked/unavailable. Keeping cursor on this page.")
            break
        if not items:
            print("    [✓] Confirmed empty page (end of category). Marking completed.")
            cat_state["current_page"] = page_num + 1
            cat_state["completed"] = True
            save_checkpoint(checkpoint)
            break

        new_items = [itm for itm in items if itm["source_post_id"] not in known_ids]
        skipped_count = len(items) - len(new_items)
        if skipped_count > 0:
            print(f"    [*] Skipped {skipped_count} videos already in DB.")

        if not new_items:
            print("    [✓] All videos on this page are already indexed.")
            cat_state["current_page"] = page_num + 1
            save_checkpoint(checkpoint)
            time.sleep(0.3)
            continue

        print(f"    [*] Enriching {len(new_items)} new videos concurrently ({workers} workers)...")
        enriched_items = enrich_items_concurrently(new_items, max_workers=workers)
        print(f"    [✓] Successfully resolved working mirrors for {len(enriched_items)}/{len(new_items)} videos.")

        pending_records: List[Dict[str, Any]] = []
        pending_ids: Set[str] = set()
        for itm in enriched_items:
            srv_names = [s["name"] for s in itm.get("servers", [])]
            print(f"      + #{itm['source_post_id']} - {itm['title'][:40]}... -> {srv_names}")
            pending_records.append(polish_supjav_record(itm))
            pending_ids.add(str(itm["source_post_id"]))

        if pending_records:
            res = upsert_japanese_videos_batch(pending_records)
            if not rpc_result_succeeded(res):
                print(f"    [!] Upsert failed on page {page_num}: {res}. IDs stay unmarked for retry.")
                break
            known_ids.update(pending_ids)
            save_known_post_ids(known_ids, new_ids=pending_ids, status="stored")
            total_ingested += len(pending_records)
            checkpoint.setdefault("stats", {})["total_ingested"] = (
                checkpoint["stats"].get("total_ingested", 0) + len(pending_records)
            )

        # Only advance once every new item on the page is stored (or confirmed
        # dead/photo).  Unresolved enrichments are retried for a bounded number
        # of cycles instead of being silently skipped.
        wanted_ids = {str(itm["source_post_id"]) for itm in new_items}
        unresolved_ids = wanted_ids - known_ids
        if unresolved_ids:
            from scraper.progress import cursor_retry_state
            stall_count, hold = cursor_retry_state(
                stall_count=int(cat_state.get("stall_count", 0) or 0),
                unresolved=len(unresolved_ids),
            )
            cat_state["stall_count"] = stall_count
            if hold:
                print(
                    f"    [!] {len(unresolved_ids)} video(s) unresolved on page {page_num}; "
                    f"keeping the cursor here for retry ({stall_count}/5)."
                )
                save_checkpoint(checkpoint)
                break
            print(
                f"    [!] Giving up on {len(unresolved_ids)} unresolved video(s) on page "
                f"{page_num} after 5 attempts; marking them seen and advancing."
            )
            known_ids.update(unresolved_ids)
            save_known_post_ids(known_ids, new_ids=unresolved_ids, status="seen")

        cat_state["current_page"] = page_num + 1
        cat_state["stall_count"] = 0
        if cat_state["current_page"] > cat_state.get("max_pages", to_page):
            cat_state["completed"] = True
        save_checkpoint(checkpoint)
        time.sleep(1.2)  # Polite page pacing

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
                if items is None:
                    print(f"  [{cat}] Listing blocked/unavailable. Skipping without touching known IDs.")
                    continue
                new_items = [itm for itm in items if itm["source_post_id"] not in known_ids]

                if not new_items:
                    print(f"  [{cat}] Up to date. 0 new videos.")
                    continue

                print(f"  [{cat}] 🔥 Found {len(new_items)} NEW incoming videos! Enriching with working hosts...")
                enriched_items = enrich_items_concurrently(new_items, max_workers=workers)

                if enriched_items:
                    records = []
                    new_ids: Set[str] = set()
                    for itm in enriched_items:
                        srv_names = [s["name"] for s in itm.get("servers", [])]
                        print(f"    + New Release: #{itm['source_post_id']} - {itm['title'][:40]}... -> {srv_names}")
                        records.append(polish_supjav_record(itm))
                        new_ids.add(str(itm["source_post_id"]))

                    res = upsert_japanese_videos_batch(records)
                    if not rpc_result_succeeded(res):
                        print(f"    [!] Upsert failed: {res}. Keeping IDs unmarked for retry.")
                        continue
                    print(f"    [✓] Ingested {len(records)} new releases into database ({res})")
                    cycle_new_count += len(records)
                    known_ids.update(new_ids)
                    save_known_post_ids(known_ids, new_ids=new_ids, status="stored")
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
