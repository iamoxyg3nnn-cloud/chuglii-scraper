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


def rpc_result_succeeded(result: Any) -> bool:
    """True only when an RPC result represents a completed write.

    Some Supabase functions report application failures with HTTP 200 and a
    JSON body such as ``{"success": false, "error": "..."}``.  Every caller
    that advances a cursor or records a known ID must use this check.
    """
    return result is not None and not (isinstance(result, dict) and result.get("success") is False)


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
    if rpc_result_succeeded(res):
        return res

    # Fallback to supabase-py client if available (same retry budget as every
    # other isolated table, so a transient tunnel blip cannot lose a batch).
    for attempt in range(retries):
        try:
            supabase = get_supabase_client()
            response = supabase.rpc("scraper_upsert_videos", {"items": polished_records}).execute()
            if rpc_result_succeeded(response.data):
                return response.data
            return {"success": False, "error": "RPC reported failure", "count": 0}
        except Exception as e:
            if attempt == retries - 1:
                print(f"[!] Supabase Viral MMS upsert batch failed after {retries} attempts: {e}")
                return {"success": False, "error": str(e), "count": 0}
            wait_time = (attempt + 1) * 3
            print(f"    [!] Database connection glitch ({type(e).__name__}), retrying in {wait_time}s (Attempt {attempt+1}/{retries})...")
            time.sleep(wait_time)

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


VIRAL_MMS_SITE_RPCS = {
    "spicymms": "scraper_upsert_spicymms_videos",
    "desi49": "scraper_upsert_desi49_videos",
    "mydesi": "scraper_upsert_mydesi_videos",
    "indianbfclips": "scraper_upsert_indianbfclips_videos",
}


def upsert_viral_mms_site_videos_batch(
    source_site: str, polished_records: List[Dict[str, Any]], retries: int = 4
) -> Any:
    """Batch upserts records strictly into the isolated per-site Viral MMS table."""
    if not polished_records:
        return {"success": True, "count": 0}
    rpc_name = VIRAL_MMS_SITE_RPCS.get(str(source_site or "").strip().lower())
    if not rpc_name:
        return {"success": False, "error": f"unknown viral-mms site: {source_site}", "count": 0}

    res = call_supabase_rpc(rpc_name, {"items": polished_records}, retries=retries)
    if rpc_result_succeeded(res):
        return res

    for attempt in range(retries):
        try:
            supabase = get_supabase_client()
            response = supabase.rpc(rpc_name, {"items": polished_records}).execute()
            if rpc_result_succeeded(response.data):
                return response.data
            return {"success": False, "error": "RPC reported failure", "count": 0}
        except Exception as e:
            if attempt == retries - 1:
                print(f"[!] Supabase Viral MMS [{source_site}] upsert failed after {retries} attempts: {e}")
                return {"success": False, "error": str(e), "count": 0}
            wait_time = (attempt + 1) * 3
            time.sleep(wait_time)


def get_viral_mms_site_videos(
    source_site: str,
    tag: Optional[str] = None,
    category: Optional[str] = None,
    search: Optional[str] = None,
    sort: str = "newest",
    limit: int = 24,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """Retrieves videos strictly from the site's isolated Viral MMS table."""
    site_key = str(source_site or "").strip().lower()
    if site_key not in VIRAL_MMS_SITE_RPCS:
        return []
    supabase = get_supabase_client()
    response = supabase.rpc(
        f"scraper_get_{site_key}_videos",
        {
            "p_source_site": site_key,
            "p_tag": tag,
            "p_category": category,
            "p_search": search,
            "p_sort": sort,
            "p_limit": limit,
            "p_offset": offset,
        },
    ).execute()
    return response.data or []

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
    if rpc_result_succeeded(res):
        return res

    # Fallback to supabase-py client if available
    for attempt in range(retries):
        try:
            supabase = get_supabase_client()
            response = supabase.rpc("scraper_upsert_japanese_videos", {"items": polished_records}).execute()
            if rpc_result_succeeded(response.data):
                return response.data
            return {"success": False, "error": "RPC reported failure", "count": 0}
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
    if rpc_result_succeeded(res):
        return res

    # Fallback to supabase-py client if available
    for attempt in range(retries):
        try:
            supabase = get_supabase_client()
            response = supabase.rpc("scraper_upsert_myanmar_videos", {"items": polished_records}).execute()
            if rpc_result_succeeded(response.data):
                return response.data
            return {"success": False, "error": "RPC reported failure", "count": 0}
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
    "influencersgonewild": "scraper_insert_onlyfans_influencersgonewild_videos",
    "sheeshfans": "scraper_insert_onlyfans_sheeshfans_videos",
    "borntobefuck": "scraper_insert_onlyfans_borntobefuck_videos",
    "uncutx": "scraper_insert_onlyfans_uncutx_videos",
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
    if rpc_result_succeeded(res):
        return res

    # Fallback to direct supabase-py client strictly on scraper_upsert_onlyfans_videos
    for attempt in range(retries):
        try:
            supabase = get_supabase_client()
            response = supabase.rpc("scraper_upsert_onlyfans_videos", {"items": polished_records}).execute()
            if rpc_result_succeeded(response.data):
                return response.data
            return {"success": False, "error": "RPC reported failure", "count": 0}
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

def get_onlyfans_videos(source_site: Optional[str] = "influencersgonewild", tag: Optional[str] = None, category: Optional[str] = "onlyfans", search: Optional[str] = None, sort: str = "newest", limit: int = 24, offset: int = 0) -> List[Dict[str, Any]]:
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


# ─── Isolated Chinese Category Database Functions ───────────────────────────

def upsert_chinese_video(polished_record: Dict[str, Any]) -> Any:
    """Inserts or updates a single record strictly in scraper.chinese_videos."""
    return upsert_chinese_videos_batch([polished_record])

def upsert_chinese_videos_batch(polished_records: List[Dict[str, Any]], retries: int = 4) -> Any:
    """
    Batch inserts or updates records strictly in scraper.chinese_videos.
    Zero cross-contamination: never touches or falls back to scraper.videos.
    """
    if not polished_records:
        return {"success": True, "count": 0}

    # Isolated RPC strictly targeting scraper.chinese_videos
    res = call_supabase_rpc("scraper_upsert_chinese_videos", {"items": polished_records}, retries=retries)
    if rpc_result_succeeded(res):
        return res

    # Fallback to direct supabase-py client strictly on scraper_upsert_chinese_videos
    for attempt in range(retries):
        try:
            supabase = get_supabase_client()
            response = supabase.rpc("scraper_upsert_chinese_videos", {"items": polished_records}).execute()
            if rpc_result_succeeded(response.data):
                return response.data
            return {"success": False, "error": "RPC reported failure", "count": 0}
        except Exception as e:
            if attempt == retries - 1:
                print(f"[!] Supabase Chinese upsert batch failed after {retries} attempts: {e}")
                return {"success": False, "error": str(e), "count": 0}
            wait_time = (attempt + 1) * 3
            print(f"    [!] Database connection glitch ({type(e).__name__}), retrying in {wait_time}s (Attempt {attempt+1}/{retries})...")
            time.sleep(wait_time)


def upsert_fangsung_videos_batch(polished_records: List[Dict[str, Any]], retries: int = 4) -> Any:
    """
    Batch inserts or updates records strictly in scraper.fangsung_videos.
    Zero cross-contamination: never touches or falls back to scraper.videos.
    """
    if not polished_records:
        return {"success": True, "count": 0}

    # Isolated RPC strictly targeting scraper.fangsung_videos
    res = call_supabase_rpc("scraper_upsert_fangsung_videos", {"items": polished_records}, retries=retries)
    if rpc_result_succeeded(res):
        return res

    # Fallback to direct supabase-py client strictly on scraper_upsert_fangsung_videos
    for attempt in range(retries):
        try:
            supabase = get_supabase_client()
            response = supabase.rpc("scraper_upsert_fangsung_videos", {"items": polished_records}).execute()
            if rpc_result_succeeded(response.data):
                return response.data
            return {"success": False, "error": "RPC reported failure", "count": 0}
        except Exception as e:
            if attempt == retries - 1:
                print(f"[!] Supabase fangsung upsert batch failed after {retries} attempts: {e}")
                return {"success": False, "error": str(e), "count": 0}
            wait_time = (attempt + 1) * 3
            print(f"    [!] Database connection glitch ({type(e).__name__}), retrying in {wait_time}s (Attempt {attempt+1}/{retries})...")
            time.sleep(wait_time)


def get_fangsung_videos(source_site: Optional[str] = None, category: Optional[str] = None, tag: Optional[str] = None, search: Optional[str] = None, sort: str = "newest", limit: int = 24, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Retrieves stored records strictly from scraper.fangsung_videos using RPC bridge.
    """
    supabase = get_supabase_client()
    params = {
        "p_source_site": source_site,
        "p_category": category,
        "p_tag": tag,
        "p_search": search,
        "p_sort": sort,
        "p_limit": limit,
        "p_offset": offset,
    }
    try:
        res = supabase.rpc("scraper_get_fangsung_videos", params).execute()
        return res.data or []
    except Exception as e:
        print(f"[!] get_fangsung_videos failed: {e}")
        return []


def upsert_studios_videos_batch(polished_records: List[Dict[str, Any]], retries: int = 4) -> Any:
    """
    Batch inserts or updates records in the per-site isolated studios tables
    (scraper.madouer_videos / scraper.dasj91_videos). Rows are routed by their
    source_site; mixed batches are split per site. Zero cross-contamination:
    never touches or falls back to scraper.videos.
    """
    if not polished_records:
        return {"success": True, "count": 0}

    by_site: Dict[str, List[Dict[str, Any]]] = {}
    for rec in polished_records:
        by_site.setdefault(str(rec.get("source_site") or "madouer"), []).append(rec)

    results = []
    for site, rows in by_site.items():
        rpc_name = f"scraper_upsert_{site}_videos"
        # Isolated RPC strictly targeting scraper.{site}_videos
        res = call_supabase_rpc(rpc_name, {"items": rows}, retries=retries)
        if rpc_result_succeeded(res):
            results.append(res)
            continue
        # Fallback to direct supabase-py client strictly on the same RPC
        for attempt in range(retries):
            try:
                supabase = get_supabase_client()
                response = supabase.rpc(rpc_name, {"items": rows}).execute()
                if rpc_result_succeeded(response.data):
                    results.append(response.data)
                    break
                results.append({"success": False, "error": "RPC reported failure", "count": 0})
                break
            except Exception as e:
                if attempt == retries - 1:
                    print(f"[!] Supabase {site} upsert batch failed after {retries} attempts: {e}")
                    results.append({"success": False, "error": str(e), "count": 0})
                else:
                    wait_time = (attempt + 1) * 3
                    print(f"    [!] Database connection glitch ({type(e).__name__}), retrying in {wait_time}s (Attempt {attempt+1}/{retries})...")
                    time.sleep(wait_time)
    return results[-1] if results else {"success": True, "count": 0}


def get_studios_videos(source_site: str = "madouer", category: Optional[str] = None, tag: Optional[str] = None, search: Optional[str] = None, sort: str = "newest", limit: int = 24, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Retrieves stored records strictly from scraper.{source_site}_videos using
    its per-site RPC bridge (scraper_get_madouer_videos / scraper_get_dasj91_videos).
    """
    supabase = get_supabase_client()
    params = {
        "p_category": category,
        "p_tag": tag,
        "p_search": search,
        "p_sort": sort,
        "p_limit": limit,
        "p_offset": offset,
    }
    try:
        res = supabase.rpc(f"scraper_get_{source_site}_videos", params).execute()
        return res.data or []
    except Exception as e:
        print(f"[!] get_studios_videos({source_site}) failed: {e}")
        return []


def get_chinese_videos(source_site: Optional[str] = None, content_type: Optional[str] = None, category: Optional[str] = None, tag: Optional[str] = None, search: Optional[str] = None, sort: str = "newest", limit: int = 24, offset: int = 0, group: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Retrieves stored records strictly from scraper.chinese_videos using RPC bridge.
    content_type: 'video' (playable HLS), 'torrent' (magnet entries) or None/'all'.
    group: subcategory group filter — 'chinese' | 'jav' | 'livestream' | 'western' | 'all'.
    category accepts an English category name, the 'chinese' parent tag, or a raw
    source-site category id (e.g. '20110496').
    """
    supabase = get_supabase_client()
    params = {
        "p_source_site": source_site,
        "p_content_type": content_type,
        "p_category": category,
        "p_tag": tag,
        "p_search": search,
        "p_sort": sort,
        "p_limit": limit,
        "p_offset": offset,
        "p_group": group,
    }
    try:
        response = supabase.rpc("scraper_get_chinese_videos", params).execute()
        return response.data or []
    except Exception as e:
        print(f"[!] Failed to fetch chinese videos: {e}")
        return []


# ============================================================================
# Isolated Voyeur House Database Functions
# No cross-category dedupe, no fallback to scraper.videos or other tables.
# ============================================================================

def upsert_voyeurhouse_videos_batch(polished_records: List[Dict[str, Any]], retries: int = 4) -> Any:
    if not polished_records:
        return {"success": True, "count": 0}
    res = call_supabase_rpc("scraper_upsert_voyeurhouse_videos", {"items": polished_records}, retries=retries)
    if rpc_result_succeeded(res):
        return res
    for attempt in range(retries):
        try:
            supabase = get_supabase_client()
            response = supabase.rpc("scraper_upsert_voyeurhouse_videos", {"items": polished_records}).execute()
            if rpc_result_succeeded(response.data):
                return response.data
            return {"success": False, "error": "RPC reported failure", "count": 0}
        except Exception as e:
            if attempt == retries - 1:
                print(f"[!] Supabase Voyeur House upsert failed after {retries} attempts: {e}")
                return {"success": False, "error": str(e), "count": 0}
            time.sleep((attempt + 1) * 3)


def get_voyeurhouse_videos(source_site: Optional[str] = None, tag: Optional[str] = None, category: Optional[str] = None, search: Optional[str] = None, sort: str = "newest", limit: int = 24, offset: int = 0) -> List[Dict[str, Any]]:
    supabase = get_supabase_client()
    params = {
        "p_source_site": source_site,
        "p_tag": tag,
        "p_category": category,
        "p_search": search,
        "p_sort": sort,
        "p_limit": limit,
        "p_offset": offset,
    }
    try:
        response = supabase.rpc("scraper_get_voyeurhouse_videos", params).execute()
        return response.data or []
    except Exception as e:
        print(f"[!] Failed to fetch Voyeur House videos: {e}")
        return []


# ============================================================================
# Voyeur House — additional isolated websites (AVS family)
# Same category as voyeurhouseorg, but each website has its OWN table + RPC
# family. No cross-site dedupe, merge, fallback, or canonical cache.
# ============================================================================

VOYEURHOUSE_AVS_SITES = (
    "hotscopesvoyeurhouse",
    "reallifecamto",
    "hotscopesspygasm",
    "hotscopesreallifecam",
)


def _voyeurhouse_avs_rpc(site: str, kind: str) -> str:
    if site not in VOYEURHOUSE_AVS_SITES:
        raise ValueError(f"Unknown Voyeur House source site: {site}")
    return f"scraper_{kind}_{site}_videos"


def upsert_voyeurhouse_site_videos_batch(site: str, polished_records: List[Dict[str, Any]], retries: int = 4) -> Any:
    """Writes ONLY to the isolated table belonging to ``site``."""
    if not polished_records:
        return {"success": True, "count": 0}
    rpc = _voyeurhouse_avs_rpc(site, "upsert")
    res = call_supabase_rpc(rpc, {"items": polished_records}, retries=retries)
    if rpc_result_succeeded(res):
        return res
    for attempt in range(retries):
        try:
            supabase = get_supabase_client()
            response = supabase.rpc(rpc, {"items": polished_records}).execute()
            if rpc_result_succeeded(response.data):
                return response.data
            return {"success": False, "error": "RPC reported failure", "count": 0}
        except Exception as e:
            if attempt == retries - 1:
                print(f"[!] Supabase Voyeur House upsert failed for {site} after {retries} attempts: {e}")
                return {"success": False, "error": str(e), "count": 0}
            time.sleep((attempt + 1) * 3)


def get_voyeurhouse_site_videos(
    site: str,
    source_site: Optional[str] = None,
    tag: Optional[str] = None,
    category: Optional[str] = None,
    search: Optional[str] = None,
    sort: str = "newest",
    limit: int = 24,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    supabase = get_supabase_client()
    params = {
        "p_source_site": source_site or site,
        "p_tag": tag,
        "p_category": category,
        "p_search": search,
        "p_sort": sort,
        "p_limit": limit,
        "p_offset": offset,
    }
    try:
        response = supabase.rpc(_voyeurhouse_avs_rpc(site, "get"), params).execute()
        return response.data or []
    except Exception as e:
        print(f"[!] Failed to fetch Voyeur House videos for {site}: {e}")
        return []


def count_voyeurhouse_site_videos(
    site: str,
    source_site: Optional[str] = None,
    tag: Optional[str] = None,
    category: Optional[str] = None,
    search: Optional[str] = None,
) -> int:
    supabase = get_supabase_client()
    params = {
        "p_source_site": source_site or site,
        "p_tag": tag,
        "p_category": category,
        "p_search": search,
    }
    try:
        response = supabase.rpc(f"scraper_count_{site}_videos", params).execute()
        return int(response.data or 0)
    except Exception as e:
        print(f"[!] Failed to count Voyeur House videos for {site}: {e}")
        return 0


def get_voyeurhouse_site_video_by_id(site: str, video_id: str) -> Optional[Dict[str, Any]]:
    supabase = get_supabase_client()
    try:
        response = supabase.rpc(f"scraper_get_{site}_video_by_id", {"p_id": video_id}).execute()
        rows = response.data or []
        return rows[0] if rows else None
    except Exception as e:
        print(f"[!] Failed to fetch Voyeur House video by id for {site}: {e}")
        return None


# ─── Supabase Centralized Checkpoints & Progress Tracking ────────────────────

def is_supabase_healthy(timeout: float = 8.0) -> bool:
    """Checks the scraper's Supabase RPC data plane, not just the REST root.

    The REST root can answer while the database/RPC layer is down, so health is
    probed through the same checkpoint RPC the scraper relies on for cursors
    and dedupe state.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    endpoint = f"{SUPABASE_URL.rstrip('/')}/rest/v1/rpc/scraper_get_checkpoint"
    headers = {
        "apikey": SUPABASE_KEY,
        "authorization": f"Bearer {SUPABASE_KEY}",
        "content-type": "application/json; charset=utf-8",
        "accept": "application/json"
    }
    try:
        with httpx.Client(timeout=timeout) as client:
            res = client.post(endpoint, headers=headers, json={"p_key": "__supabase_health_probe__"})
        if not 200 <= res.status_code < 300:
            return False
        try:
            data = res.json()
        except ValueError:
            return False
        return isinstance(data, dict) and data.get("success") is not False
    except Exception:
        return False

# Cached data-plane health, so crawl loops can cheaply ask "can we persist
# right now?" without an 8s RPC probe on every page. Updated whenever a probe
# runs; ``max_age`` keeps the answer honest for at most that many seconds.
_HEALTH_CACHE: Dict[str, Any] = {"ts": 0.0, "ok": True}


def supabase_available(max_age: float = 60.0, timeout: float = 8.0) -> bool:
    """Cached :func:`is_supabase_healthy`.

    Crawl loops use this to pause instead of scraping pages whose rows can
    never be written. On 2026-11-26 the daemon kept hammering 91dasj for 98
    minutes while the data plane was down.
    """
    now = time.time()
    if now - float(_HEALTH_CACHE.get("ts") or 0.0) < max_age:
        return bool(_HEALTH_CACHE.get("ok"))
    ok = is_supabase_healthy(timeout=timeout)
    _HEALTH_CACHE["ts"] = now
    _HEALTH_CACHE["ok"] = ok
    return ok


def mark_supabase_unavailable() -> None:
    """Force the next :func:`supabase_available` call to re-probe."""
    _HEALTH_CACHE["ts"] = 0.0
    _HEALTH_CACHE["ok"] = False


def wait_for_supabase(max_wait_seconds: int = 600, poll_interval: int = 15) -> bool:
    """
    Blocks and polls until Supabase's scraper RPC is usable.
    Returns True only when Supabase is online, or False when the wait expires.
    Callers must honor False by pausing; they must never scrape offline.
    """
    if is_supabase_healthy():
        return True

    max_wait_seconds = max(0, int(max_wait_seconds))
    poll_interval = max(1, int(poll_interval))
    print("\n[⏳] Supabase is offline or starting up (Cloudflare Tunnel / REST RPC unavailable)...", flush=True)
    print(f"    Entering polite standby. Waiting for Supabase to come online (Max wait: {max_wait_seconds // 60}m)...", flush=True)

    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        remaining = max_wait_seconds - (time.time() - start_time)
        time.sleep(min(poll_interval, remaining))
        elapsed = int(time.time() - start_time)
        if is_supabase_healthy():
            print(f"    [✓] Supabase is ONLINE and healthy! (Standby duration: {elapsed}s). Resuming scraper operations.\n", flush=True)
            return True
        print(f"    [Heartbeat] Supabase still booting / connecting... ({elapsed}s elapsed / {max_wait_seconds}s max)", flush=True)

    print(f"    [!] Supabase is still unavailable after {max_wait_seconds}s. Scraping remains paused.\n", flush=True)
    return False

def call_supabase_rpc(func_name: str, params: Optional[Dict[str, Any]] = None, retries: int = 3, timeout: float = 15.0) -> Any:
    """Executes a Supabase RPC via REST with retry and explicit failure reporting.

    A ``None`` return value now always means "the write did not happen".
    Callers must never treat it as success.  JSON payloads carrying
    ``"success": false`` are also treated as failures (some SQL functions
    signal application errors with HTTP 200).
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        print(f"[!] Supabase RPC {func_name} skipped: missing URL or key")
        return None
    endpoint = f"{SUPABASE_URL.rstrip('/')}/rest/v1/rpc/{func_name}"
    headers = {
        "apikey": SUPABASE_KEY,
        "authorization": f"Bearer {SUPABASE_KEY}",
        "content-type": "application/json; charset=utf-8"
    }
    attempts = max(1, retries)
    last_error = "no response"
    for attempt in range(attempts):
        try:
            with httpx.Client(timeout=timeout) as client:
                res = client.post(endpoint, json=params or {}, headers=headers)

            if 200 <= res.status_code < 300:
                try:
                    data = res.json()
                except ValueError:
                    data = None
                if isinstance(data, dict) and data.get("success") is False:
                    last_error = f"RPC reported failure: {data}"
                else:
                    return data
            else:
                last_error = f"HTTP {res.status_code}: {res.text[:300]}"
                if res.status_code in (502, 503, 504, 530):
                    if attempt == 0:
                        # Wait up to 120s for tunnel recovery before next retry
                        wait_for_supabase(max_wait_seconds=120, poll_interval=10)
                        continue
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"

        if attempt < attempts - 1:
            time.sleep(min(2.0 * (attempt + 1), 10.0))

    print(f"[!] Supabase RPC {func_name} failed after {attempts} attempts: {last_error}")
    return None

def get_scraper_checkpoint(key: str) -> Dict[str, Any]:
    """Retrieves current progress checkpoint directly from Supabase scraper.checkpoints."""
    data = call_supabase_rpc("scraper_get_checkpoint", {"p_key": key})
    if isinstance(data, dict) and data.get("success") is not False:
        return data
    return {}

def set_scraper_checkpoint(key: str, state: Dict[str, Any]) -> bool:
    """Persists progress checkpoint atomically into Supabase scraper.checkpoints."""
    data = call_supabase_rpc("scraper_set_checkpoint", {"p_key": key, "p_state": state})
    return isinstance(data, dict) and data.get("success") is True

def get_scraper_known_post_ids(site: str, limit: int = 200000) -> set:
    """Retrieves recently indexed post IDs directly from Supabase to prevent duplicate processing."""
    data = call_supabase_rpc("scraper_get_known_post_ids", {"p_site": site, "p_limit": limit})
    if isinstance(data, list):
        return set(str(x) for x in data)
    return set()


def add_scraper_known_post_ids(site: str, post_ids: List[str], status: str = "seen") -> Dict[str, Any]:
    """Persists newly-confirmed post IDs in the central deduplication ledger.

    ``status='stored'`` means the video row exists; ``status='seen'`` means the
    ID was checked and intentionally skipped (dead link / photo set).  Both
    prevent re-crawling, but only 'stored' counts as ingested.
    """
    ids = [str(x).strip() for x in post_ids if str(x).strip()]
    if not ids:
        return {"success": True, "count": 0}
    data = call_supabase_rpc(
        "scraper_add_known_post_ids",
        {"p_site": site, "p_ids": ids, "p_status": status},
    )
    if isinstance(data, dict):
        return data
    return {"success": False, "error": "scraper_add_known_post_ids unavailable", "count": 0}



