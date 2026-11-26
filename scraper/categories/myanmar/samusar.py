"""
Adapter and Normalization Module for Samusar (https://www.samusar.com)

Targets:
  - Listings: /latest-updates/ and /latest-updates/{page}/
  - Detail player: Kernel Video Sharing `kt_player(..., <var>)` config + JSON-LD
  - Quality selection: strictly ranks verified streams descending (1080p > 720p > 480p > 360p)
  - Dead-video filter: verifies Range stream bytes before allowing DB ingest
  - Hover previews: extracts up to 8 evenly-spaced `timeline_screens_url` thumbnails
"""
import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from curl_cffi import requests as cf_requests

try:
    from scrapling.fetchers import Fetcher as _ScraplingFetcher  # type: ignore
except Exception:
    try:
        from scrapling import Fetcher as _ScraplingFetcher  # type: ignore
    except Exception:
        _ScraplingFetcher = None

BASE_URL = "https://www.samusar.com"
SOURCE_SITE = "samusar"
CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_CF_MARKERS = (
    "just a moment...",
    "challenge-platform",
    "cf-chl",
    "attention required! | cloudflare",
)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _abs_url(raw: Optional[str]) -> str:
    if not raw:
        return ""
    u = str(raw).strip()
    if not u or u.startswith(("data:", "javascript:")):
        return ""
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return urljoin(BASE_URL, u)
    return u


def _looks_like_cf_challenge(html: str) -> bool:
    low = (html or "").lower()
    return len(html or "") < 25000 and any(m in low for m in _CF_MARKERS)


def fetch_html(
    url: str,
    *,
    session: Optional[cf_requests.Session] = None,
    referer: Optional[str] = None,
    timeout: int = 20,
    retries: int = 3,
) -> Tuple[Optional[str], int]:
    """Fetch HTML via curl_cffi (Chrome TLS), falling back to Scrapling."""
    headers = {
        "User-Agent": CHROME_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if referer:
        headers["Referer"] = referer

    client = session or cf_requests
    last_status = 0

    for attempt in range(retries):
        try:
            r = client.get(
                url,
                impersonate="chrome124",
                timeout=timeout,
                headers=headers,
                allow_redirects=True,
            )
            last_status = int(getattr(r, "status_code", 0) or 0)
            html = getattr(r, "text", "") or ""
            if last_status == 200 and not _looks_like_cf_challenge(html):
                return html, 200
            if last_status in (404, 410):
                return None, 404
        except Exception:
            pass
        if attempt < retries - 1:
            time.sleep(1.2 * (attempt + 1))

    if _ScraplingFetcher is not None:
        try:
            sr = _ScraplingFetcher.get(url, stealthy_headers=True, timeout=timeout)
            status = int(getattr(sr, "status", 0) or 0)
            html = getattr(sr, "text", None) or getattr(sr, "html_content", "") or ""
            if status == 200 and not _looks_like_cf_challenge(html):
                return html, 200
            if status in (404, 410):
                return None, 404
            if status:
                last_status = status
        except Exception:
            pass

    return None, last_status


def verify_stream_alive(
    stream_url: str,
    *,
    referer: str,
    session: Optional[cf_requests.Session] = None,
    timeout: int = 12,
) -> Tuple[bool, str]:
    """Verify a KVS stream URL actually serves video bytes (not 404/HTML/GIF traps)."""
    u = _clean_text(stream_url)
    if not u.startswith(("http://", "https://")):
        return False, ""

    client = session or cf_requests
    try:
        r = client.get(
            u,
            impersonate="chrome124",
            timeout=timeout,
            headers={
                "User-Agent": CHROME_UA,
                "Referer": referer,
                "Range": "bytes=0-1023",
            },
            allow_redirects=True,
        )
        status = int(getattr(r, "status_code", 0) or 0)
        final_url = str(getattr(r, "url", u) or u).strip()
        ct = (r.headers.get("content-type") or "").lower()

        if status not in (200, 206):
            return False, ""
        if ct.startswith("image/") or "text/html" in ct:
            return False, ""
        if ct and not (ct.startswith("video/") or "octet-stream" in ct or "binary" in ct):
            return False, ""

        raw_bytes = getattr(r, "content", b"")[:200].lstrip().lower()
        if raw_bytes.startswith((b"<!doctype", b"<html", b"gif87a", b"gif89a")):
            return False, ""
        return True, final_url
    except Exception:
        return False, ""


def _resolution_rank(label: str = "", url: str = "") -> int:
    blob = f"{label or ''} {url or ''}".lower()
    for pat, score in (
        (r"(?:4320p?|8k)", 4320),
        (r"(?:2160p?|4k|uhd)", 2160),
        (r"(?:1440p?|2k|qhd)", 1440),
        (r"(?:1080p?|fhd|full[\s_-]?hd)", 1080),
        (r"(?:720p?|\bhd\b)", 720),
        (r"(?:480p?|\bsd\b)", 480),
        (r"(?:360p?)", 360),
        (r"(?:240p?)", 240),
    ):
        if re.search(pat, blob):
            return score
    return 0


def _parse_iso_duration(iso_dur: str) -> Tuple[Optional[int], Optional[str]]:
    m = re.match(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", (iso_dur or "").strip(), re.I)
    if not m:
        return None, None
    h = int(m.group(1) or 0)
    mi = int(m.group(2) or 0)
    sec = int(m.group(3) or 0)
    total = h * 3600 + mi * 60 + sec
    if total <= 0:
        return None, None
    if h > 0:
        return total, f"{h:02d}:{mi:02d}:{sec:02d}"
    return total, f"{mi:02d}:{sec:02d}"


def _extract_kt_config(html: str) -> Dict[str, str]:
    """Extracts KVS kt_player config regardless of randomized hex variable name."""
    var_name = ""
    m_call = re.search(r"kt_player\([^)]*,\s*([a-zA-Z0-9_]+)\s*\)", html or "")
    if m_call:
        var_name = m_call.group(1)

    block = ""
    if var_name:
        m_var = re.search(rf"var\s+{re.escape(var_name)}\s*=\s*\{{(.*?)\}}\s*;", html or "", re.S)
        if m_var:
            block = m_var.group(1)
    if not block:
        m_fv = re.search(r"var\s+flashvars\s*=\s*\{(.*?)\}\s*;", html or "", re.S)
        if m_fv:
            block = m_fv.group(1)

    out: Dict[str, str] = {}
    for km in re.finditer(r"([a-zA-Z0-9_]+)\s*:\s*'([^']*)'", block):
        out[km.group(1)] = km.group(2)
    return out


def _extract_jsonld_video(html: str) -> Dict[str, Any]:
    for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html or "", re.S | re.I):
        raw = m.group(1).strip()
        try:
            data = json.loads(raw)
        except Exception:
            continue
        if isinstance(data, dict) and data.get("@type") == "VideoObject":
            return data
    return {}


def build_listing_url(page_num: int = 1) -> str:
    if page_num <= 1:
        return f"{BASE_URL}/latest-updates/"
    return f"{BASE_URL}/latest-updates/{page_num}/"


def scrape_listing(
    page_num: int = 1,
    *,
    session: Optional[cf_requests.Session] = None,
) -> Tuple[List[Dict[str, Any]], Optional[int], bool]:
    url = build_listing_url(page_num)
    html, status = fetch_html(url, session=session, referer=f"{BASE_URL}/")
    if status == 404:
        return [], page_num, True
    if not html:
        return [], None, False

    soup = BeautifulSoup(html, "html.parser")
    items: List[Dict[str, Any]] = []
    seen_ids = set()

    cards = soup.select("#list_videos_latest_videos_list_items .item, .list-videos .item, .thumbs .item")
    for card in cards:
        a = card.select_one("a[href]")
        if not a:
            continue
        detail_url = _abs_url(a.get("href"))
        if not detail_url or "/videos/" not in detail_url:
            continue
        m_id = re.search(r"/videos/(\d+)/", detail_url)
        if not m_id:
            continue
        post_id = m_id.group(1)
        if post_id in seen_ids:
            continue
        seen_ids.add(post_id)

        title_el = card.select_one(".title, strong")
        img = card.select_one("img")
        title = (
            _clean_text(a.get("title"))
            or _clean_text(title_el.get_text(" ", strip=True) if title_el else "")
            or _clean_text(img.get("alt") if img else "")
        )
        if not title:
            continue

        thumb = ""
        if img:
            thumb = _abs_url(img.get("data-original") or img.get("data-src") or img.get("src"))

        items.append({
            "source_site": SOURCE_SITE,
            "source_post_id": post_id,
            "title": title,
            "detail_url": detail_url,
            "thumbnail_url": thumb,
        })

    max_page = page_num
    for el in soup.select(".pagination a[data-parameters], .pagination a[href]"):
        dp = el.get("data-parameters", "")
        for m in re.findall(r"from:(\d+)", dp):
            max_page = max(max_page, int(m))
        href = el.get("href", "")
        for m in re.findall(r"/latest-updates/(\d+)/?", href):
            max_page = max(max_page, int(m))

    if items and max_page <= page_num:
        if soup.select_one(".pagination a.next, .pagination a[data-parameters*='from:']"):
            max_page = page_num + 1

    return items, max_page, True


def _build_preview_frames(fv: Dict[str, str], thumb: str) -> List[str]:
    tmpl = _clean_text(fv.get("timeline_screens_url"))
    cnt_raw = _clean_text(fv.get("timeline_screens_count"))
    iv_raw = _clean_text(fv.get("timeline_screens_interval"))
    frames: List[str] = []
    if tmpl and "{time}" in tmpl and cnt_raw.isdigit() and int(cnt_raw) > 1:
        cnt = int(cnt_raw)
        iv = int(iv_raw) if iv_raw.isdigit() and int(iv_raw) > 0 else 10
        target_slots = min(8, cnt)
        step = max(1, cnt // target_slots)
        for idx in range(1, cnt + 1, step):
            frames.append(tmpl.replace("{time}", str(idx * iv)))
            if len(frames) >= 8:
                break
    return frames


def enrich_and_polish_detail(
    card: Dict[str, Any],
    *,
    session: Optional[cf_requests.Session] = None,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """Fetch detail page, verify at least one live stream, and return polished record.

    Status values: ``"ok"``, ``"dead"``, ``"error"``.
    """
    detail_url = card.get("detail_url") or ""
    post_id = str(card.get("source_post_id") or "").strip()
    if not detail_url or not post_id:
        return None, "dead"

    local_sess = session or cf_requests.Session()
    html, status = fetch_html(detail_url, session=local_sess, referer=f"{BASE_URL}/")
    if status == 404:
        return None, "dead"
    if not html:
        return None, "error"

    soup = BeautifulSoup(html, "html.parser")
    fv = _extract_kt_config(html)
    ld = _extract_jsonld_video(html)

    h1 = soup.select_one("h1")
    raw_title = (
        _clean_text(fv.get("video_title"))
        or _clean_text(h1.get_text(" ", strip=True) if h1 else "")
        or card.get("title")
        or ""
    )
    if not raw_title:
        return None, "dead"

    # Collect and verify all KVS video qualities; rank highest resolution first.
    servers: List[Dict[str, Any]] = []
    seen_urls = set()
    for key in ("video_alt_url3", "video_alt_url2", "video_alt_url", "video_url"):
        raw_stream = _abs_url(fv.get(key))
        if not raw_stream or "/get_file/" not in raw_stream:
            continue
        alive, final_url = verify_stream_alive(raw_stream, referer=detail_url, session=local_sess)
        if not alive or not final_url or final_url in seen_urls:
            continue
        seen_urls.add(final_url)
        qlabel = _clean_text(fv.get(f"{key}_text")) or (
            "1080p HD" if fv.get(f"{key}_fhd") == "1" else ("720p HD" if fv.get(f"{key}_hd") == "1" else "480p")
        )
        servers.append({
            "name": f"Samusar ({qlabel})",
            "server": "Samusar CDN",
            "embed_url": final_url,
            "type": "direct_mp4",
            "quality_label": qlabel,
        })

    if not servers:
        return None, "dead"

    servers.sort(
        key=lambda s: _resolution_rank(s.get("quality_label", ""), s.get("embed_url", "")),
        reverse=True,
    )
    primary = servers[0]

    thumb = _abs_url(fv.get("preview_url")) or _abs_url(ld.get("thumbnailUrl")) or card.get("thumbnail_url") or ""
    preview_frames = _build_preview_frames(fv, thumb)

    dur_sec, dur_fmt = _parse_iso_duration(str(ld.get("duration") or ""))

    pub_raw = _clean_text(ld.get("uploadDate"))
    published_at: Optional[str] = None
    if pub_raw:
        try:
            published_at = datetime.fromisoformat(pub_raw.replace("Z", "+00:00")).replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            published_at = None

    raw_tags = [t.strip().lower() for t in (fv.get("video_tags") or "").split(",") if t.strip()]
    for a in soup.select(".tags-list a, .video-info a[href*='/tags/'], .item-tags a"):
        txt = _clean_text(a.get_text(" ", strip=True)).lower()
        if txt and txt not in raw_tags and len(txt) < 50:
            raw_tags.append(txt)
    if "myanmar" not in raw_tags:
        raw_tags.insert(0, "myanmar")

    cats = [c.strip().lower() for c in (fv.get("video_categories") or "").split(",") if c.strip()]
    if "myanmar" not in cats:
        cats.insert(0, "myanmar")

    models = [m.strip() for m in (fv.get("video_models") or "").split(",") if m.strip()]

    title_slug_base = re.sub(r"[^\w\s-]", "", raw_title, flags=re.UNICODE).strip().lower()
    title_slug_base = re.sub(r"[-\s]+", "-", title_slug_base) or "samusar-video"
    slug = f"{title_slug_base[:80]}-{post_id}"

    record: Dict[str, Any] = {
        "source_site": SOURCE_SITE,
        "source_post_id": post_id,
        "title": raw_title,
        "slug": slug,
        "detail_url": detail_url,
        "full_video_url": primary["embed_url"],
        "video_mime_type": "video/mp4",
        "trailer_preview_url": None,
        "thumbnail_url": thumb or None,
        "clean_thumbnail_url": thumb or None,
        "duration_seconds": dur_sec,
        "duration_formatted": dur_fmt,
        "views_count": 0,
        "views_raw": "0",
        "rating_score": 100.0 if fv.get("video_alt_url_hd") == "1" else 90.0,
        "rating_raw": "100%",
        "categories": sorted(list(dict.fromkeys(cats))),
        "tags": list(dict.fromkeys(raw_tags)),
        "actors": models,
        "published_at": published_at,
        "raw_payload": {
            "source": SOURCE_SITE,
            "detail_url": detail_url,
            "stream_url": primary["embed_url"],
            "relative_time": pub_raw or None,
            "original_title": raw_title,
            "media": {
                "preview_frames": preview_frames or None,
            },
            "servers": servers,
        },
    }
    return record, "ok"
