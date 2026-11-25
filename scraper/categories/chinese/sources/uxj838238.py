"""
Source Scraper for uxj.838238.xyz (Chinese category, 95da.com network)

Site architecture (reverse-engineered):
  - Every page is an obfuscated loader: reversed-base64 payload -> document.write.
  - The decoded page embeds `j_b64 = '<base64 JSON>'` with ALL scrape-relevant
    data, plus a loadScript('/html/js/<rotating-id>.js') template reference.
  - Anti-bot is TLS/JA3 fingerprint filtering: plain HTTP clients get soft-404
    or a 403 interstitial; curl_cffi chrome124 impersonation gets 200.

Page types:
  /list/{cat_id}-{page}.html   listing  -> payload {p, l:{a:[20 items]}, cn, cc, ci, cl}
  /video/{id}.html             video    -> payload {cdn[8], name, l.a[12], m3, co, cn, ci, cl}
  /torrent/{id}.html           torrent  -> payload {name, l.a[12], ts, td, tr, tc, tc2, tt, tm, cn, ci, cl}

Soft-404 pages return HTTP 200 with a tiny error template -> detected by payload
presence, never by status code alone.
"""
import re
import json
import base64
import time
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urljoin

try:
    from scrapling import Fetcher  # type: ignore
except Exception:  # pragma: no cover - vendored fallback
    import sys as _sys
    from pathlib import Path as _Path
    _vendored = _Path(__file__).resolve().parents[3]  # .../scraper
    if str(_vendored) not in _sys.path:
        _sys.path.insert(0, str(_vendored))
    try:
        from scrapling import Fetcher  # type: ignore
    except Exception:
        Fetcher = None

from scraper.categories.chinese.categories import (
    category_info,
    is_enabled,
)

BASE_URL = "https://uxj.838238.xyz"
SOURCE_SITE = "uxj838238"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

_IMPERSONATE = "chrome124"
_fetcher = None


def _get_fetcher():
    global _fetcher
    if _fetcher is None:
        if Fetcher is None:
            raise RuntimeError("scrapling Fetcher unavailable; cannot scrape uxj838238")
        _fetcher = Fetcher(impersonate=_IMPERSONATE)
    return _fetcher


def fetch_page(url: str, timeout: int = 30, retries: int = 3) -> Optional[Any]:
    """Fetches a page with chrome124 TLS impersonation and polite backoff."""
    last_err = None
    for attempt in range(retries):
        try:
            resp = _get_fetcher().get(url, timeout=timeout)
            status = getattr(resp, "status", 0)
            if status == 200:
                return resp
            if status in {403, 429, 500, 502, 503, 504} and attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
                continue
            last_err = f"HTTP {status}"
            break
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
    if last_err:
        print(f"    [uxj838238] fetch failed {url}: {last_err}")
    return None


# ─── Payload decoding ────────────────────────────────────────────────────────

_RE_DECODER_FN = re.compile(r"function (\w+)\(\w+\)\{var \w+\s*=\s*window\.atob")
_RE_J_B64 = re.compile(r"j_b64\s*=\s*'([^']+)'")


def decode_payload(html: str) -> Optional[Dict[str, Any]]:
    """
    Decodes the obfuscated loader into the page's j_b64 JSON payload.
    Returns None when the page has no payload (soft-404 / interstitial / error).
    """
    if not html or len(html) < 800:
        return None
    m_fn = _RE_DECODER_FN.search(html)
    if not m_fn:
        return None
    fn = m_fn.group(1)
    start = html.find(fn + "('")
    if start < 0:
        return None
    start += len(fn) + 2
    end = html.find("'.split", start)
    if end < 0:
        # Some loaders terminate with '); directly
        end = html.find("')", start)
        if end < 0:
            return None
    encoded = html[start:end]
    try:
        decoded_html = base64.b64decode(encoded[::-1]).decode("utf-8", errors="replace")
    except Exception:
        return None
    m_json = _RE_J_B64.search(decoded_html)
    if not m_json:
        return None
    try:
        return json.loads(base64.b64decode(m_json.group(1)).decode("utf-8"))
    except Exception:
        return None


def extract_template_url(html: str) -> str:
    """Extracts the rotating /html/js/<id>.js template reference (best effort)."""
    try:
        decoded = _decoded_inner_html(html)
        m = re.search(r"loadScript\('(/html/js/[^']+)'\", ", decoded)
        if m:
            return m.group(1)
    except Exception:
        pass
    return ""


def _decoded_inner_html(html: str) -> str:
    m_fn = _RE_DECODER_FN.search(html)
    if not m_fn:
        return ""
    fn = m_fn.group(1)
    start = html.find(fn + "('")
    if start < 0:
        return ""
    start += len(fn) + 2
    end = html.find("'.split", start)
    if end < 0:
        return ""
    try:
        return base64.b64decode(html[start:end][::-1]).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _abs(url: Optional[str]) -> str:
    if not url:
        return ""
    u = str(url).strip()
    if not u:
        return ""
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("/"):
        u = urljoin(BASE_URL, u)
    return u


# ─── Item id / content-type helpers ─────────────────────────────────────────

_RE_VIDEO_URL = re.compile(r"^/video/(\d+)\.html$")
_RE_TORRENT_URL = re.compile(r"^/torrent/(\d+)\.html$")


def parse_item_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    """Returns (source_post_id, content_type) for a /video/ or /torrent/ path."""
    u = (url or "").split("?")[0]
    m = _RE_VIDEO_URL.match(u)
    if m:
        return m.group(1), "video"
    m = _RE_TORRENT_URL.match(u)
    if m:
        return m.group(1), "torrent"
    return None, None


# ─── Listing scrape ──────────────────────────────────────────────────────────

def scrape_listing(category_id: str, page_num: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Scrapes one listing page: /list/{category_id}-{page_num}.html
    Returns (items, detected_max_pages). Items carry listing-level metadata;
    stream/magnet enrichment happens in enrich_detail().
    Category-level filtering is enforced here: non-whitelisted ids yield [].
    """
    category_id = str(category_id).strip()
    if not is_enabled(category_id):
        print(f"    [uxj838238] category {category_id} is unknown on this site. Skipping.")
        return [], None

    info = category_info(category_id)
    url = f"{BASE_URL}/list/{category_id}-{max(1, int(page_num))}.html"
    resp = fetch_page(url)
    if not resp:
        return [], None

    payload = decode_payload(getattr(resp, "html_content", "") or "")
    if not payload:
        # Soft-404 / anti-bot page: no payload present.
        return [], None

    listing = payload.get("l") or {}
    raw_items = listing.get("a") or []
    p = payload.get("p") or {}
    try:
        total_pages = int(p.get("t") or 0) or None
    except Exception:
        total_pages = None

    cat_cn = str(payload.get("cn") or info.get("cn") or "").strip()
    cat_en = info.get("en") or ""
    cat_id = str(payload.get("ci") or category_id)

    items: List[Dict[str, Any]] = []
    seen_ids = set()
    for raw in raw_items:
        item_url = str(raw.get("url") or "").strip()
        post_id, content_type = parse_item_url(item_url)
        if not post_id or content_type is None:
            continue
        if post_id in seen_ids:
            continue
        seen_ids.add(post_id)
        title = re.sub(r"\s+", " ", str(raw.get("title") or "")).strip()
        if not title:
            continue
        items.append({
            "source_site": SOURCE_SITE,
            "source_post_id": post_id,
            "content_type": content_type,
            "title": title,
            "detail_url": _abs(item_url),
            "thumbnail_url": _abs(raw.get("pic")),
            "category_id": cat_id,
            "category_name_cn": cat_cn,
            "category_name_en": cat_en,
            "category_group": info.get("group", "chinese"),
            "published_at": None,  # site exposes no dates
        })

    return items, total_pages


# ─── Detail enrichment ───────────────────────────────────────────────────────

_RE_BTIH = re.compile(r"urn:btih:([0-9a-fA-F]{40})")
_RE_IMG_SRC = re.compile(r"src=[\"']([^\"']+\.(?:jpg|jpeg|png|webp))[\"']", re.I)
_RE_SRCSET = re.compile(r"srcset=[\"']([^\"']+)[\"']", re.I)


def _extract_screenshots(html_block: str, limit: int = 24) -> List[str]:
    """Extracts absolute screenshot image URLs from torrent thumb HTML blocks."""
    out: List[str] = []
    seen = set()
    for m in _RE_IMG_SRC.finditer(html_block or ""):
        u = _abs(m.group(1))
        if u and u not in seen:
            seen.add(u)
            out.append(u)
        if len(out) >= limit:
            break
    return out


def _enrich_video(card: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    cdn_list = payload.get("cdn") or []
    cdn_endpoints = []
    for c in cdn_list:
        u = _abs(c.get("url"))
        if u:
            cdn_endpoints.append({"name": str(c.get("name") or ""), "url": u})

    hls_url = _abs(payload.get("m3"))
    cover = _abs(payload.get("co")) or card.get("thumbnail_url")

    merged = dict(card)
    merged["full_video_url"] = hls_url or None
    merged["video_mime_type"] = "application/x-mpegURL" if hls_url else None
    merged["thumbnail_url"] = cover
    merged["clean_thumbnail_url"] = cover
    merged["cdn_endpoints"] = cdn_endpoints
    # Quality: site serves a single HLS rendition; keep the standard 720p baseline.
    merged["quality_label"] = "720p HD"
    merged["quality_score"] = 720
    merged["raw_payload"] = {
        "source": SOURCE_SITE,
        "cdn_count": len(cdn_endpoints),
        "cdn_hosts": sorted({c["url"].split("/")[2] for c in cdn_endpoints if "://" in c}),
        "related": [
            {"url": _abs(r.get("url")), "title": str(r.get("title") or "")}
            for r in (payload.get("l") or {}).get("a") or []
        ][:12],
    }
    return merged


def _enrich_torrent(card: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    magnet_uri = str(payload.get("tm") or "").strip()
    m_hash = _RE_BTIH.search(magnet_uri)
    size_label = str(payload.get("ts") or "").strip() or None
    try:
        duration_min = int(payload.get("td") or 0) or None
    except Exception:
        duration_min = None
    resolution = str(payload.get("tr") or "").strip() or None
    torrent_file = _abs(payload.get("tt")) or None

    screenshots = _extract_screenshots(str(payload.get("tc") or ""))
    # Poster picture block (tc2) may hold higher-res variants; append unseen ones.
    for m in _RE_SRCSET.finditer(str(payload.get("tc2") or "")):
        for cand in re.split(r"[,\s]+", m.group(1)):
            u = _abs(cand)
            if u and u.lower().endswith((".jpg", ".jpeg", ".png", ".webp")) and u not in screenshots:
                screenshots.append(u)

    if duration_min:
        duration_formatted = f"{duration_min // 60}:{duration_min % 60:02d}" if duration_min >= 60 else f"0:{duration_min:02d}"
    else:
        duration_formatted = None

    merged = dict(card)
    merged["magnet_uri"] = magnet_uri or None
    merged["magnet_hash"] = m_hash.group(1).lower() if m_hash else None
    merged["torrent_file_url"] = torrent_file
    merged["size_label"] = size_label
    merged["resolution"] = resolution
    merged["duration_seconds"] = duration_min * 60 if duration_min else None
    merged["duration_formatted"] = duration_formatted
    merged["screenshots"] = screenshots[:24]
    merged["thumbnail_url"] = card.get("thumbnail_url")
    merged["clean_thumbnail_url"] = card.get("thumbnail_url")
    merged["raw_payload"] = {
        "source": SOURCE_SITE,
        "related": [
            {"url": _abs(r.get("url")), "title": str(r.get("title") or "")}
            for r in (payload.get("l") or {}).get("a") or []
        ][:12],
    }
    return merged


def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    """Fetches the detail page and merges stream/magnet metadata into the card."""
    url = card.get("detail_url")
    if not url:
        return card
    resp = fetch_page(url)
    if not resp:
        # Anti-bot / transient failure: return the card untouched so the item
        # stays unmarked in the known-ID ledger and is retried later.
        return card
    payload = decode_payload(getattr(resp, "html_content", "") or "")
    if not payload:
        # Definitive soft-404 (content removed / bad id): mark as dead.
        card = dict(card)
        card["dead"] = True
        return card

    content_type = card.get("content_type") or "video"
    try:
        if content_type == "torrent":
            return _enrich_torrent(card, payload)
        return _enrich_video(card, payload)
    except Exception as e:
        print(f"    [uxj838238] enrich error {url}: {e}")
        return card
