import os
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import httpx
from dotenv import load_dotenv
try:
    from supabase import create_client, Client
except ImportError:
    create_client = None
    Client = Any

# Find root .env file
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
# Prioritize service_role key for scraper writes
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_API_KEY") or os.getenv("SUPABASE_ANON_KEY", "")

def get_supabase_client() -> Client:
    """Returns an authenticated Supabase client."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def apply_scraper_schema() -> Dict[str, Any]:
    """
    Applies scraper/schema.sql via the project's /functions/v1/migrate endpoint,
    ensuring 100% separation from the public backend tables.
    """
    schema_file = Path(__file__).resolve().parent / "schema.sql"
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found at {schema_file}")
    
    sql_content = schema_file.read_text(encoding="utf-8")
    migrate_url = f"{SUPABASE_URL.rstrip('/')}/functions/v1/migrate"
    
    headers = {
        "apikey": SUPABASE_KEY,
        "authorization": f"Bearer {SUPABASE_KEY}",
        "content-type": "application/json; charset=utf-8"
    }
    
    payload = {
        "name": "scraper_schema.sql",
        "sql": sql_content,
        "force": True
    }
    
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(migrate_url, json=payload, headers=headers)
        if resp.status_code == 200:
            return resp.json()
        else:
            return {
                "ok": False,
                "status_code": resp.status_code,
                "error": resp.text
            }

def upsert_scraped_video(polished_record: Dict[str, Any]) -> Any:
    """
    Inserts or updates a single video in scraper.videos via RPC bridge.
    Guarantees no duplication via (source_site, source_post_id) unique constraint.
    """
    return upsert_scraped_videos_batch([polished_record])

def upsert_scraped_videos_batch(polished_records: List[Dict[str, Any]], retries: int = 4) -> Any:
    """
    Batch inserts or updates video records in the segregated scraper.videos table.
    Uses resilient REST RPC with automatic exponential backoff on Cloudflare Tunnel hiccups.
    """
    if not polished_records:
        return {"success": True, "count": 0}
    
    res = call_supabase_rpc("scraper_upsert_videos", {"items": polished_records}, retries=retries)
    if res is not None:
        return res
    
    # Fallback to supabase-py client if available
    try:
        supabase = get_supabase_client()
        response = supabase.rpc("scraper_upsert_videos", {"items": polished_records}).execute()
        return response.data
    except Exception as e:
        print(f"[!] Supabase Viral MMS upsert batch failed after {retries} attempts: {e}")
        return {"success": False, "error": str(e), "count": 0}

def get_scraped_videos(source_site: Optional[str] = None, tag: Optional[str] = None, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Retrieves stored videos from scraper.videos table using RPC bridge.
    """
    supabase = get_supabase_client()
    params = {
        "p_source_site": source_site,
        "p_tag": tag,
        "p_limit": limit,
        "p_offset": offset
    }
    response = supabase.rpc("scraper_get_videos", params).execute()
    return response.data

import time

# ─── Isolated Japanese Database Functions ──────────────────────────────────

def upsert_japanese_videos_batch(polished_records: List[Dict[str, Any]], retries: int = 4) -> Any:
    """
    Batch inserts or updates video records in the segregated scraper.japanese_videos table.
    Uses resilient REST RPC with automatic exponential backoff on Cloudflare Tunnel hiccups.
    """
    if not polished_records:
        return {"success": True, "count": 0}
    
    res = call_supabase_rpc("scraper_upsert_japanese_videos", {"items": polished_records}, retries=retries)
    if res is not None:
        return res

    # Fallback to supabase-py client if available
    for attempt in range(retries):
        try:
            supabase = get_supabase_client()
            response = supabase.rpc("scraper_upsert_japanese_videos", {"items": polished_records}).execute()
            return response.data
        except Exception as e:
            if attempt == retries - 1:
                print(f"[!] Supabase Japanese JAV upsert batch failed after {retries} attempts: {e}")
                return {"success": False, "error": str(e), "count": 0}
            wait_time = (attempt + 1) * 3
            print(f"    [!] Database connection glitch ({type(e).__name__}), retrying in {wait_time}s (Attempt {attempt+1}/{retries})...")
            time.sleep(wait_time)

def get_japanese_videos(source_site: Optional[str] = None, tag: Optional[str] = None, category: Optional[str] = None, search: Optional[str] = None, sort: str = "newest", limit: int = 24, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Retrieves stored videos from scraper.japanese_videos table using RPC bridge.
    """
    supabase = get_supabase_client()
    params = {
        "p_source_site": source_site,
        "p_tag": tag,
        "p_category": category,
        "p_search": search,
        "p_sort": sort,
        "p_limit": limit,
        "p_offset": offset
    }
    response = supabase.rpc("scraper_get_japanese_videos", params).execute()
    return response.data


# ─── Isolated Myanmar Database Functions ────────────────────────────────────

def upsert_myanmar_video(polished_record: Dict[str, Any]) -> Any:
    """Inserts or updates a single video in scraper.myanmar_videos."""
    return upsert_myanmar_videos_batch([polished_record])

def upsert_myanmar_videos_batch(polished_records: List[Dict[str, Any]], retries: int = 4) -> Any:
    """
    Batch inserts or updates video records in the segregated scraper.myanmar_videos table.
    Uses resilient REST RPC with automatic exponential backoff on Cloudflare Tunnel hiccups.
    """
    if not polished_records:
        return {"success": True, "count": 0}
    
    res = call_supabase_rpc("scraper_upsert_myanmar_videos", {"items": polished_records}, retries=retries)
    if res is not None:
        return res

    # Fallback to supabase-py client if available
    for attempt in range(retries):
        try:
            supabase = get_supabase_client()
            response = supabase.rpc("scraper_upsert_myanmar_videos", {"items": polished_records}).execute()
            return response.data
        except Exception as e:
            if attempt == retries - 1:
                print(f"[!] Supabase Myanmar upsert batch failed after {retries} attempts: {e}")
                return {"success": False, "error": str(e), "count": 0}
            wait_time = (attempt + 1) * 3
            print(f"    [!] Database connection glitch ({type(e).__name__}), retrying in {wait_time}s (Attempt {attempt+1}/{retries})...")
            time.sleep(wait_time)

def get_myanmar_videos(source_site: Optional[str] = None, tag: Optional[str] = None, category: Optional[str] = None, search: Optional[str] = None, sort: str = "newest", limit: int = 24, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Retrieves stored videos from scraper.myanmar_videos table using RPC bridge.
    """
    supabase = get_supabase_client()
    params = {
        "p_source_site": source_site,
        "p_tag": tag,
        "p_category": category,
        "p_search": search,
        "p_sort": sort,
        "p_limit": limit,
        "p_offset": offset
    }
    response = supabase.rpc("scraper_get_myanmar_videos", params).execute()
    return response.data


# ─── Isolated OnlyFans Database Functions ───────────────────────────────────

ONLYFANS_SOURCE_TABLE_RPC = {
    "internetchicks": "scraper_insert_onlyfans_internetchicks_videos",
    "influencersgonewild": "scraper_insert_onlyfans_influencersgonewild_videos",
    "sheeshfans": "scraper_insert_onlyfans_sheeshfans_videos",
    "pimpbunny": "scraper_insert_onlyfans_pimpbunny_videos",
}

def upsert_onlyfans_video(polished_record: Dict[str, Any]) -> Any:
    """Inserts or updates a single video strictly in scraper.onlyfans_videos."""
    return upsert_onlyfans_videos_batch([polished_record])

def upsert_onlyfans_videos_batch(polished_records: List[Dict[str, Any]], retries: int = 4) -> Any:
    """
    Batch inserts or updates video records strictly in scraper.onlyfans_videos.
    Zero cross-contamination: never touches or falls back to scraper.videos.
    """
    if not polished_records:
        return {"success": True, "count": 0}

    # Isolated RPC strictly targeting scraper.onlyfans_videos
    res = call_supabase_rpc("scraper_upsert_onlyfans_videos", {"items": polished_records}, retries=retries)
    if res is not None:
        return res

    # Fallback to direct supabase-py client strictly on scraper_upsert_onlyfans_videos
    for attempt in range(retries):
        try:
            supabase = get_supabase_client()
            response = supabase.rpc("scraper_upsert_onlyfans_videos", {"items": polished_records}).execute()
            return response.data
        except Exception as e:
            if attempt == retries - 1:
                print(f"[!] Supabase OnlyFans upsert batch failed after {retries} attempts: {e}")
                return {"success": False, "error": str(e), "count": 0}
            wait_time = (attempt + 1) * 3
            print(f"    [!] Database connection glitch ({type(e).__name__}), retrying in {wait_time}s (Attempt {attempt+1}/{retries})...")
            time.sleep(wait_time)


def _onlyfans_source_record(record: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {
        "source_site", "source_post_id", "title", "slug", "detail_url", "full_video_url",
        "video_mime_type", "trailer_preview_url", "thumbnail_url", "clean_thumbnail_url",
        "quality_label", "quality_score", "duration_seconds", "duration_formatted",
        "views_count", "views_raw", "rating_score", "rating_raw", "categories", "tags",
        "actors", "status", "published_at", "raw_payload",
    }
    out = {k: v for k, v in record.items() if k in allowed}
    out.setdefault("source_site", record.get("source_site"))
    out.setdefault("status", "active")
    out.setdefault("categories", ["onlyfans"])
    out.setdefault("tags", [])
    out.setdefault("actors", [])
    out.setdefault("raw_payload", {})
    return out


def sync_onlyfans_source_tables_batch(polished_records: List[Dict[str, Any]], retries: int = 3) -> Dict[str, Any]:
    """Best-effort sync into isolated per-source OnlyFans tables.

    Deduplicates by source_post_id per source table before syncing to prevent
    duplicate rows from repeated crawls/dirty legacy rows.
    """
    if not polished_records:
        return {"success": True, "synced": 0}

    grouped: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for rec in polished_records:
        site = str(rec.get("source_site") or "").strip().lower()
        rpc_name = ONLYFANS_SOURCE_TABLE_RPC.get(site)
        if not rpc_name:
            continue
        clean = _onlyfans_source_record(rec)
        pid = str(clean.get("source_post_id") or "").strip()
        if not pid:
            continue
        # Last write wins within batch; canonical/cleaned records should come last.
        grouped.setdefault(rpc_name, {})[pid] = clean

    synced = 0
    errors: List[str] = []
    for rpc_name, by_pid in grouped.items():
        items = list(by_pid.values())
        for i in range(0, len(items), 50):
            chunk = items[i:i + 50]
            res = call_supabase_rpc(rpc_name, {"items": chunk}, retries=retries)
            if res is None:
                try:
                    supabase = get_supabase_client()
                    res = supabase.rpc(rpc_name, {"items": chunk}).execute().data
                except Exception as e:
                    errors.append(f"{rpc_name}: {e}")
                    continue
            if isinstance(res, dict) and res.get("success") is False:
                errors.append(f"{rpc_name}: {res.get('error')}")
            else:
                synced += int(res.get("inserted") or res.get("count") or len(chunk)) if isinstance(res, dict) else len(chunk)
    return {"success": not errors, "synced": synced, "errors": errors, "deduped_input": len(polished_records)}

def get_onlyfans_videos(source_site: Optional[str] = "internetchicks", tag: Optional[str] = None, category: Optional[str] = "onlyfans", search: Optional[str] = None, sort: str = "newest", limit: int = 24, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Retrieves stored videos strictly from scraper.onlyfans_videos table using RPC bridge.
    """
    supabase = get_supabase_client()
    params = {
        "p_source_site": source_site,
        "p_tag": tag,
        "p_category": category,
        "p_search": search,
        "p_sort": sort,
        "p_limit": limit,
        "p_offset": offset
    }
    try:
        response = supabase.rpc("scraper_get_onlyfans_videos", params).execute()
        return response.data or []
    except Exception as e:
        print(f"[!] Failed to fetch onlyfans videos: {e}")
        return []




# ─── Supabase Centralized Checkpoints & Progress Tracking ────────────────────

def is_supabase_healthy(timeout: float = 8.0) -> bool:
    """Checks whether the Supabase REST endpoint is reachable and returning valid responses."""
    if not SUPABASE_URL:
        return False
    endpoint = f"{SUPABASE_URL.rstrip('/')}/rest/v1/"
    headers = {
        "apikey": SUPABASE_KEY or "",
        "authorization": f"Bearer {SUPABASE_KEY}" if SUPABASE_KEY else "",
        "accept": "application/json"
    }
    try:
        with httpx.Client(timeout=timeout) as client:
            res = client.get(endpoint, headers=headers)
            # HTTP 200, 204, or 401 (auth challenge) indicates server & tunnel are online
            # HTTP 502, 503, 504, 530 indicates Cloudflare Tunnel down / origin offline
            return res.status_code in (200, 204, 401)
    except Exception:
        return False

def wait_for_supabase(max_wait_seconds: int = 600, poll_interval: int = 15) -> bool:
    """
    Blocks and polls until Supabase becomes healthy and reachable.
    Designed to handle Cloudflare Tunnel (530 / 1033) restarts or runner boot delays gracefully.
    Returns True when Supabase is online, or False if max_wait_seconds expires.
    """
    if is_supabase_healthy():
        return True

    print(f"\n[⏳] Supabase is offline or starting up (HTTP 530 / Cloudflare Tunnel reconnecting)...", flush=True)
    print(f"    Entering polite standby. Waiting for Supabase to come online (Max wait: {max_wait_seconds // 60}m)...", flush=True)
    
    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        time.sleep(poll_interval)
        elapsed = int(time.time() - start_time)
        if is_supabase_healthy():
            print(f"    [✓] Supabase is ONLINE and healthy! (Standby duration: {elapsed}s). Resuming scraper operations.\n", flush=True)
            return True
        print(f"    [Heartbeat] Supabase still booting / connecting... ({elapsed}s elapsed / {max_wait_seconds}s max)", flush=True)

    print(f"    [!] Timeout waiting for Supabase after {max_wait_seconds}s. Proceeding with offline resilience.\n", flush=True)
    return False

def call_supabase_rpc(func_name: str, params: Optional[Dict[str, Any]] = None, retries: int = 3, timeout: float = 15.0) -> Any:
    """Executes a Supabase RPC function via direct REST call with automatic retry."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    endpoint = f"{SUPABASE_URL.rstrip('/')}/rest/v1/rpc/{func_name}"
    headers = {
        "apikey": SUPABASE_KEY,
        "authorization": f"Bearer {SUPABASE_KEY}",
        "content-type": "application/json; charset=utf-8"
    }
    for attempt in range(retries):
        try:
            with httpx.Client(timeout=timeout) as client:
                res = client.post(endpoint, json=params or {}, headers=headers)
                if res.status_code == 200:
                    return res.json()
                elif res.status_code in (502, 530, 504):
                    if attempt == 0:
                        # Wait up to 120s for tunnel recovery before next retry
                        wait_for_supabase(max_wait_seconds=120, poll_interval=10)
                    else:
                        time.sleep(2.0 * (attempt + 1))
        except Exception:
            time.sleep(1.0)
    return None

def get_scraper_checkpoint(key: str) -> Dict[str, Any]:
    """Retrieves current progress checkpoint directly from Supabase scraper.checkpoints."""
    data = call_supabase_rpc("scraper_get_checkpoint", {"p_key": key})
    if isinstance(data, dict):
        return data
    return {}

def set_scraper_checkpoint(key: str, state: Dict[str, Any]) -> bool:
    """Persists progress checkpoint atomically into Supabase scraper.checkpoints."""
    data = call_supabase_rpc("scraper_set_checkpoint", {"p_key": key, "p_state": state})
    return bool(data and data.get("success"))

def get_scraper_known_post_ids(site: str, limit: int = 25000) -> set:
    """Retrieves recently indexed post IDs directly from Supabase to prevent duplicate processing."""
    data = call_supabase_rpc("scraper_get_known_post_ids", {"p_site": site, "p_limit": limit})
    if isinstance(data, list):
        return set(str(x) for x in data)
    return set()



