import os
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import httpx
from dotenv import load_dotenv
from supabase import create_client, Client

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

def upsert_scraped_videos_batch(polished_records: List[Dict[str, Any]]) -> Any:
    """
    Batch inserts or updates video records in the segregated scraper.videos table.
    """
    if not polished_records:
        return {"success": True, "count": 0}
    
    supabase = get_supabase_client()
    response = supabase.rpc("scraper_upsert_videos", {"items": polished_records}).execute()
    return response.data

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
    Includes automatic retry with exponential backoff for Cloudflare Tunnel hiccups.
    """
    if not polished_records:
        return {"success": True, "count": 0}
    
    supabase = get_supabase_client()
    for attempt in range(retries):
        try:
            response = supabase.rpc("scraper_upsert_japanese_videos", {"items": polished_records}).execute()
            return response.data
        except Exception as e:
            if attempt == retries - 1:
                print(f"[!] Supabase upsert batch failed after {retries} attempts: {e}")
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

