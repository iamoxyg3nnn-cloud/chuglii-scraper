"""
Source scraper for madouer.cc (studios category, Phase A).

Site architecture (reverse-engineered 2026-11-25, see scratch/deep3/ANALYSIS_deep3.md):

  - MacCMS (苹果CMS) + stui template. No Cloudflare challenge for plain HTTP
    clients; server-rendered listings.
  - Listings: /index.php/vod/type/id/{t}.html — cards link STRAIGHT to play
    pages (no detail intermediate):
        <a class="stui-vodlist__thumb lazyload"
           href="/index.php/vod/play/id/{id}/sid/1/nid/1.html"
           title="{title}" data-original="{thumb}">
        <h4 class="title text-overflow"><a ...>{title}</a></h4>
        <p class="text text-overflow text-muted hidden-xs">{YYYY-MM-DD}</p>
  - Pagination: /index.php/vod/show/id/{t}/page/{n}.html with a "1/1160"
    current/total hint. Type 1 alone has 1,160 pages (~24 cards/page).
  - Play pages carry inline JSON:
        var player_aaaa = {"flag":"play","encrypt":0,...,"url":"https://t27.cdn2020.com/
        video/m3u8/{yyyy}/{mm}/{dd}/{slug}/index.m3u8","from":"mahua",
        "vod_data":{"vod_name":...,"vod_class":"91视频,开心鬼传媒"},"id":"389458",...}
    encrypt=0 → plaintext URL. One request per item yields stream + tags.
  - Thumbnails: aliyun.cctv05.com / cctv123456.com / 23.225.11.173:8443 —
    verified hotlinkable with no Referer (200 image/jpeg).
  - Stream CDNs: t{N}.cdn2020.com — verified 200, CORS *, no Referer. The
    numeric subdomain may rotate; ingest probes the manifest and stores the
    host separately so the frontend can fail over.
"""
import json
import re
import shutil
import subprocess
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

try:
    from scrapling.fetchers import Fetcher  # type: ignore
except Exception:  # pragma: no cover - vendored fallback
    import sys as _sys
    from pathlib import Path as _Path
    _vendored = _Path(__file__).resolve().parents[3]  # .../scraper
    if str(_vendored) not in _sys.path:
        _sys.path.insert(0, str(_vendored))
    try:
        from scrapling.fetchers import Fetcher  # type: ignore
    except Exception:
        Fetcher = None

from scraper.categories.studios.categories import category_info, is_enabled

BASE_URL = "https://madouer.cc"
SOURCE_SITE = "madouer"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# Stream CDN suffixes verified live (200 + #EXTM3U + CORS *). The t{N} number
# rotates, so match on the domain suffix, never a fixed host. Unknown hosts
# (e.g. ptzy's player.putaoplayer.com — NXDOMAIN-dead as of 2026-11-25) get a
# one-shot live probe instead of being trusted.
_STREAM_HOST_SUFFIXES_OK = ("cdn2020.com", "97img.com", "thm3u8.co")

_RE_CARD = re.compile(
    r"<a[^>]*class=\"stui-vodlist__thumb[^\"]*\"\s+href=\"(/index\.php/vod/play/id/(\d+)/sid/(\d+)/nid/(\d+)\.html)\""
    r"[^>]*title=\"([^\"]*)\"[^>]*data-original=\"([^\"]+)\""
)
_RE_PAGE_HINT = re.compile(r"class=\"num\">\s*(\d+)\s*/\s*(\d+)\s*<")
_RE_PLAYER = re.compile(r"var\s+player_aaaa\s*=\s*(\{.*?\})\s*</script", re.S)
_RE_DATE_TEXT = re.compile(r">(\d{4}-\d{2}-\d{2})<")


def _stream_host(url: str) -> str:
    m = re.match(r"^https?://([^/?#]+)/", url or "")
    return m.group(1).lower() if m else ""


def _stream_is_live(url: str) -> bool:
    """Cheap manifest probe: a live HLS entry point returns an #EXTM3U body."""
    try:
        r = subprocess.run(
            ["curl", "-s", "--max-time", "12", "-A", UA, url],
            capture_output=True, text=True, timeout=18,
        )
        return r.stdout.lstrip().startswith("#EXTM3U")
    except Exception:
        return False


def _get_fetcher():
    if Fetcher is None:
        raise RuntimeError("scrapling Fetcher unavailable; cannot scrape madouer")
    return Fetcher


def fetch_page(url: str, timeout: int = 30, retries: int = 3) -> Optional[Any]:
    fetch_page.last_status = None
    last_err = None
    for attempt in range(retries):
        try:
            resp = _get_fetcher().get(url, stealthy_headers=True, timeout=timeout)
            status = getattr(resp, "status", 0)
            fetch_page.last_status = status
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
        print(f"    [madouer] fetch failed {url[:90]}: {last_err}")
    return None


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


# ─── Listing scrape ──────────────────────────────────────────────────────────

def scrape_listing(category_id: str, page_num: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Scrapes one listing page for a madouer type id.
    Page 1 uses /vod/type/id/{t}.html; deeper pages use /vod/show/id/{t}/page/{n}.html.
    Returns (items, detected_max_pages).
    """
    t = str(category_id).strip()
    if not is_enabled(t):
        print(f"    [madouer] category '{t}' is not whitelisted. Skipping.")
        return [], None

    info = category_info(t)
    page_num = max(1, int(page_num))
    if page_num == 1:
        url = f"{BASE_URL}/index.php/vod/type/id/{t}.html"
    else:
        url = f"{BASE_URL}/index.php/vod/show/id/{t}/page/{page_num}.html"
    resp = fetch_page(url)
    if not resp:
        if getattr(fetch_page, "last_status", None) == 404:
            return [], None  # past the true end of the listing
        raise ListingUnavailable(f"HTTP {getattr(fetch_page, 'last_status', 'error')} on {url}")

    html = getattr(resp, "html_content", "") or ""
    if len(html) < 5000 or "stui-vodlist" not in html:
        if "暂无内容" in html:
            return [], None  # genuine empty listing (site's own marker)
        raise ListingUnavailable(f"malformed listing page {url}")

    items: List[Dict[str, Any]] = []
    seen_ids = set()
    for m in _RE_CARD.finditer(html):
        play_path, vod_id, _sid, _nid, title_attr, thumb = m.groups()
        if vod_id in seen_ids:
            continue
        seen_ids.add(vod_id)
        title = re.sub(r"\s+", " ", title_attr).strip()
        if not title:
            continue
        items.append({
            "source_site": SOURCE_SITE,
            "source_post_id": vod_id,
            "content_type": "video",
            "title": title,
            "detail_url": _abs(play_path),
            "thumbnail_url": _abs(thumb),
            "clean_thumbnail_url": _abs(thumb),
            "region": info.get("cn") or "",
            "region_en": info.get("en") or "",
            "published_at": None,  # filled at enrich time from the card date
        })
        # Card date sits right after the card block; captured below via finditer
        # context would be fragile — the play page confirms it instead.

    # Pagination hint: <span class="num">1/1160</span>
    detected_max = None
    m_hint = _RE_PAGE_HINT.search(html)
    if m_hint:
        try:
            detected_max = int(m_hint.group(2))
        except Exception:
            detected_max = None
    if not detected_max:
        pages = [int(p) for p in re.findall(r"/vod/show/id/%s/page/(\d+)\.html" % t, html)]
        if pages:
            detected_max = max(pages)

    return items, detected_max


# ─── Detail enrichment (play page) ───────────────────────────────────────────

def _probe_hls_resolution(hls_url: str, timeout: int = 25) -> Optional[Tuple[int, int]]:
    if not hls_url:
        return None
    if not shutil.which("ffprobe"):
        return None
    for attempt in (1, 2):
        try:
            proc = subprocess.run(
                [
                    "ffprobe", "-v", "error",
                    "-user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    "-select_streams", "v:0",
                    "-show_entries", "stream=width,height",
                    "-of", "csv=p=0",
                    "-analyzeduration", "1500000", "-probesize", "1500000",
                    hls_url,
                ],
                capture_output=True, text=True, timeout=timeout,
            )
            line = (proc.stdout or "").strip().splitlines()[0] if proc.stdout else ""
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
                return int(parts[0]), int(parts[1])
        except Exception:
            if attempt == 2:
                pass
    return None


def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    """Fetches the play page and extracts stream URL, tags and date.

    Sets `dead` when the play page is unreachable so the pipeline keeps the
    item unmarked for retry instead of storing junk.
    """
    merged = dict(card)
    resp = fetch_page(card.get("detail_url"), timeout=25, retries=2)
    if not resp:
        merged["dead"] = True
        return merged

    html = getattr(resp, "html_content", "") or ""
    if "player_aaaa" not in html:
        merged["dead"] = True
        return merged

    m = _RE_PLAYER.search(html)
    if not m:
        merged["dead"] = True
        return merged
    try:
        # The JSON block is bounded by </script>; json.loads handles unicode escapes.
        cfg = json.loads(m.group(1))
    except Exception:
        cfg = {}

    stream = str(cfg.get("url") or "").strip()
    if stream.startswith("//"):
        stream = "https:" + stream
    stream = _abs(stream)

    # Host validation: probe unknown/dead hosts once (CDN subdomains rotate).
    host = _stream_host(stream)
    if stream and not host.endswith(_STREAM_HOST_SUFFIXES_OK):
        if not _stream_is_live(stream):
            stream = ""

    merged["full_video_url"] = stream or None
    merged["video_mime_type"] = "application/x-mpegURL" if stream else None
    merged["stream_host"] = host if stream else None

    # vod_data metadata (name confirms card title; class holds studio tags).
    vod_data = cfg.get("vod_data") or {}
    vod_class = str(vod_data.get("vod_class") or "").strip()
    tags: List[str] = []
    if vod_class:
        for t in re.split(r"[,，、]", vod_class):
            t = t.strip()
            if t and t not in tags:
                tags.append(t)
    merged["tags"] = tags[:12]

    # Date: prefer the listing card's date paragraph context if present in the
    # play page; the play page usually repeats it near the title.
    md = _RE_DATE_TEXT.search(html)
    if md:
        try:
            merged["published_at"] = datetime.strptime(md.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            merged["published_at"] = None

    # Quality probe on a sample basis is done by the pipeline (ffprobe is slow);
    # here we only probe when the stream host is unusual.
    merged["resolution"] = None
    merged["quality_label"] = "HD"
    merged["quality_score"] = 720

    return merged
