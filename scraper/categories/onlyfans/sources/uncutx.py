"""Source scraper for uncut-x.com (OnlyFans category).

Site architecture (reverse-engineered 2026-11-26, see
``scraper/categories/onlyfans/PLAN_two_new_sources.md``):

  - React SPA. There is NO server-rendered HTML to parse; the site's entire
    catalogue is a public Supabase/PostgREST API whose anon JWT ships inside
    ``/assets/index-*.js``.
  - ``videos`` holds 4,445 rows today, but **3,080 of them point at Cloudflare
    R2 objects that no longer exist** (``404`` even via the site's own
    ``r2-signed-url`` edge function, which still happily signs them). Only the
    ``media.uncut-x.com`` rows play. Those dead rows are filtered out *in the
    query*, so a dead video never costs a request and can never be ingested.
  - ``photos`` (42,998 rows) live on the same dead bucket -> skipped entirely.
  - ``duration`` is NULL for 1,167 of the 1,365 live rows, so it is derived
    here with ffprobe at enrich time.

Every stream still passes :func:`scraper.categories.onlyfans.stream_check.verify_stream`
before the record is returned, so the pipeline's "no stream" branch is a second
line of defence rather than the only one.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from scraper import net as _net
from scraper.categories.onlyfans.stream_check import verify_stream

BASE_URL = "https://uncut-x.com"
SOURCE_SITE = "uncutx"

PROJECT_URL = "https://gmmxzxdkmqhhmugapsok.supabase.co"
API = PROJECT_URL + "/rest/v1"

#: The only media host that still serves bytes. Everything under
#: ``*.r2.cloudflarestorage.com`` is dead and is excluded server-side.
ALIVE_MEDIA_HOST = "media.uncut-x.com"
DEAD_MEDIA_MARKER = "r2.cloudflarestorage.com"

PAGE_SIZE = 1000
MAX_PAGES = 10

_SELECT = (
    "id,slug,title,title_en,video_url,thumbnail_url,preview_url,duration,"
    "orientation,model_name,model_names,categories,tags,views,likes,"
    "created_at,source"
)

_ANON_CACHE: Optional[str] = None
_DURATION_ENABLED = os.environ.get("UNCUTX_FFPROBE", "1") not in ("0", "false", "")


# ─── Anon key discovery ─────────────────────────────────────────────────────

def _extract_anon_from_bundle() -> Optional[str]:
    """Pull the public anon JWT out of the SPA bundle (it is shipped to clients)."""
    try:
        index = _net.fetch(BASE_URL + "/", retries=1)
        if not index or index.status != 200:
            return None
        assets = re.findall(r"/assets/[A-Za-z0-9._-]+\.js", index.html_content)
        for asset in assets[:4]:
            bundle = _net.fetch(BASE_URL + asset, retries=1)
            if not bundle or bundle.status != 200:
                continue
            m = re.search(
                r"(eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,})",
                bundle.html_content,
            )
            if m:
                return m.group(1)
    except Exception:
        pass
    return None


def get_anon_key() -> str:
    global _ANON_CACHE
    if _ANON_CACHE:
        return _ANON_CACHE
    key = (os.environ.get("UNCUTX_ANON_KEY") or "").strip()
    if not key:
        key = _extract_anon_from_bundle() or ""
    if not key:
        raise RuntimeError(
            "uncutx: no anon key. Set UNCUTX_ANON_KEY or make the bundle "
            "/assets/index-*.js reachable (the site's public key is embedded there)."
        )
    _ANON_CACHE = key
    return key


# ─── PostgREST helpers ──────────────────────────────────────────────────────

def _api_get(query: str, *, timeout: float = 30.0) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """GET ``/rest/v1/<query>``. Returns (rows, total_from_content_range)."""
    key = get_anon_key()
    url = f"{API}/{query}"
    page = _net.fetch(
        url,
        timeout=timeout,
        retries=2,
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Prefer": "count=exact",
            "Accept": "application/json",
        },
        cache_ttl=300,
    )
    if page is not None and page.status == 416:
        # PostgREST answers 416 when the requested range is past the end of the
        # result set. That is a clean "no more rows", not an error, and it must
        # not read as a blocked fetch.
        return [], 0
    if page is None or page.status not in (200, 206):
        status = page.status if page is not None else "no-response"
        print(f"    [uncutx] API error {status} for {query[:80]}")
        return [], None
    try:
        rows = json.loads(page.html_content or "[]")
    except ValueError:
        print("    [uncutx] API returned non-JSON")
        return [], None
    total = None
    crange = (page.headers or {}).get("content-range") or (page.headers or {}).get("Content-Range")
    if crange and "/" in crange:
        try:
            total = int(crange.split("/")[-1])
        except ValueError:
            total = None
    return rows if isinstance(rows, list) else [], total


def fetch_page(url: str, timeout: int = 30, retries: int = 3) -> Optional[Any]:
    """Kept for parity with the other sources; the API helpers are preferred."""
    return _net.fetch(url, timeout=timeout, retries=retries)


# ─── Listing ────────────────────────────────────────────────────────────────

def _is_alive_row(row: Dict[str, Any]) -> bool:
    """Cheap shape check: dead rows never leave this module."""
    url = str(row.get("video_url") or "")
    if not url:
        return False
    if DEAD_MEDIA_MARKER in url:
        return False
    if ALIVE_MEDIA_HOST not in url:
        return False
    # 592 dead rows also carry a UUID as their title; a real title is prose.
    title = str(row.get("title_en") or row.get("title") or "")
    if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", title.strip()):
        return False
    return True


def _iso(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except ValueError:
        return None


def scrape_listing(page_num: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """One PostgREST page of live videos, newest first.

    The ``video_url=like.*media.uncut-x.com*`` filter is what excludes the 3,080
    dead R2 rows; it runs server-side so they cost us nothing.
    """
    page_num = max(1, int(page_num))
    offset = (page_num - 1) * PAGE_SIZE
    query = (
        f"videos?select={_SELECT}"
        f"&video_url=like.*{ALIVE_MEDIA_HOST}*"
        f"&order=created_at.desc"
        f"&limit={PAGE_SIZE}&offset={offset}"
    )
    rows, total = _api_get(query)
    if not rows:
        return [], None

    items: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        if not _is_alive_row(row):
            continue
        vid = str(row.get("id") or "").strip()
        if not vid or vid in seen:
            continue
        seen.add(vid)

        models = row.get("model_names") or []
        if not models and row.get("model_name"):
            models = [row.get("model_name")]

        items.append(
            {
                "source_site": SOURCE_SITE,
                "source_post_id": vid,
                "title": str(row.get("title_en") or row.get("title") or "").strip(),
                "detail_url": f"{BASE_URL}/video/{row.get('slug') or vid}",
                "thumbnail_url": row.get("thumbnail_url") or "",
                "preview_url": row.get("preview_url") or "",
                "models": [str(m).strip() for m in models if str(m).strip()],
                "tags": [str(t).strip() for t in (row.get("tags") or []) if str(t).strip()],
                "views_count": int(row.get("views") or 0),
                "views_raw": str(row.get("views") or 0),
                "published_at": _iso(row.get("created_at")),
                "duration_seconds": int(row["duration"]) if row.get("duration") else None,
                "video_url": row.get("video_url"),
                "orientation": row.get("orientation"),
            }
        )

    detected_max = None
    if total:
        detected_max = max(1, min(MAX_PAGES, (int(total) + PAGE_SIZE - 1) // PAGE_SIZE))
    return items, detected_max


# ─── Duration via ffprobe ───────────────────────────────────────────────────

def _ffprobe_duration(url: str) -> Optional[int]:
    """Best-effort container duration in seconds. Returns None when unavailable."""
    if not _DURATION_ENABLED:
        return None
    exe = shutil.which("ffprobe")
    if not exe:
        return None
    try:
        proc = subprocess.run(
            [
                exe, "-v", "error",
                # Read only the container header. Without these ffprobe can pull
                # the whole remote file and stall for ~25s per video (measured
                # on the first backfill run, which took >40min on 1000 rows).
                "-probesize", "32768",
                "-analyzeduration", "500000",
                "-rw_timeout", "8000000",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                "-user_agent", "Mozilla/5.0", url,
            ],
            capture_output=True, text=True, timeout=12,
        )
        raw = (proc.stdout or "").strip().splitlines()
        if not raw:
            return None
        seconds = float(raw[0])
        return int(round(seconds)) if seconds > 0 else None
    except Exception:
        return None


def _quality_for_height(height: Optional[int]) -> Tuple[str, int]:
    """Map a measured pixel height to a quality label + score.

    uncut-x exposes exactly ONE ``video_url`` per row and no resolution field,
    so the old code simply claimed "1080p Full HD" for every video. Labelling a
    720p file as 1080p poisons the ranking the site uses to order mirrors and
    to show quality badges, so the label is now measured.
    """
    if not height:
        return "HD", 720
    if height >= 2160:
        return "4K UHD", 2160
    if height >= 1440:
        return "1440p QHD", 1440
    if height >= 1080:
        return "1080p Full HD", 1080
    if height >= 720:
        return "720p HD", 720
    if height >= 480:
        return "480p SD", 480
    return f"{height}p", height


def _probe_media(url: str) -> Tuple[Optional[int], Optional[int]]:
    """(duration_seconds, height) from ONE header-only ffprobe call.

    Both values come from the same invocation: probing twice per video would
    double the most expensive part of enrichment.
    """
    if not _DURATION_ENABLED:
        return None, None
    exe = shutil.which("ffprobe")
    if not exe:
        return None, None
    try:
        proc = subprocess.run(
            [
                exe, "-v", "error",
                "-probesize", "32768",
                "-analyzeduration", "500000",
                "-rw_timeout", "8000000",
                "-show_entries", "format=duration:stream=height",
                "-of", "json",
                "-user_agent", "Mozilla/5.0", url,
            ],
            capture_output=True, text=True, timeout=12,
        )
        data = json.loads(proc.stdout or "{}")
    except Exception:
        return None, None

    duration = None
    try:
        seconds = float((data.get("format") or {}).get("duration"))
        duration = int(round(seconds)) if seconds > 0 else None
    except (TypeError, ValueError):
        duration = None

    heights = [
        int(stream.get("height"))
        for stream in (data.get("streams") or [])
        if isinstance(stream.get("height"), (int, float)) and stream.get("height")
    ]
    return duration, (max(heights) if heights else None)


def _fmt_duration(seconds: Optional[int]) -> Optional[str]:
    if not seconds or seconds <= 0:
        return None
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


# ─── Detail ─────────────────────────────────────────────────────────────────

def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    """Verify the stream and fill the gaps the API does not have.

    Thread-safe: called from ``enrich_items_concurrently`` with 5 workers.
    Never raises -- an unplayable video returns without ``servers`` so the
    pipeline's "no valid stream" branch drops it.
    """
    out = dict(card)
    url = str(out.get("video_url") or "").strip()
    if not url:
        return out

    alive, status, detail = verify_stream(
        url,
        kind="mp4",
        source_site=SOURCE_SITE,
        source_post_id=out.get("source_post_id"),
    )
    if not alive:
        print(
            f"      [uncutx] dead stream for {out.get('source_post_id')} "
            f"(HTTP {status}: {detail})"
        )
        return out

    probed_duration, height = _probe_media(url)
    duration = out.get("duration_seconds") or probed_duration
    out["duration_seconds"] = duration
    out["duration_formatted"] = _fmt_duration(duration)
    out["full_video_url"] = url
    out["video_mime_type"] = "video/mp4"
    label, score = _quality_for_height(height)
    out["servers"] = [
        {
            "name": f"Uncut-X Direct {label}",
            "server": "Uncut-X",
            "embed_url": url,
            "type": "direct_mp4",
            "quality_label": label,
            "quality_score": score,
        }
    ]
    return out
