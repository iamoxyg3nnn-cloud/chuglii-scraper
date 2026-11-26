"""
Source scraper for www.fangsung.com (fangsung category).

Site architecture (reverse-engineered 2026-11-25, see scratch/fangsung/ANALYSIS_fangsung.md):

  - Cloudflare-fronted; headless/plain clients get a Cloudflare interstitial or
    a DEGRADED page (78KB, no player markup). scrapling Fetcher with
    stealthy_headers (google referer) receives the full page (157KB) that
    contains the player + stream URL in plain text.
  - Listings: home page + /a片/search/{term}?page=N — 20 cards/page, "共{N}"
    total hint. Cards: <a href=.../a片/{id}/{slug}> with <img data-src=thumb>.
    NOTE: /免费a片 pages are category DIRECTORIES, not video listings.
  - Detail pages: inline player script has
        var source = "https://bbcdn.xianshipian.com/{cat}/{date}/{id}/video.m3u8";
    (plain text, no tokens). Player = Plyr + hls.js 1.7.3 (MSE blob).
  - Thumbnails cdn52.apianmeng.com/kk55/{cat}/{date}/{id}.jpg map 1:1 to the
    stream path (swap host, append /video.m3u8) — derivation fallback.
  - CDN headers: the manifest 403s without Referer: https://www.fangsung.com/;
    segments are open. ffprobe must send the Referer header.
  - The 4 obfuscated inline scripts per page are a third-party ad/popup kit
    (zfgloadedpopup) — NOT the player; ignored entirely.
"""
import re
import time
import shutil
import subprocess
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import quote, urljoin

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

from scraper.categories.fangsung.categories import category_info, is_enabled
from scraper import net as _net

BASE_URL = "https://www.fangsung.com"
SOURCE_SITE = "fangsung"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

_THUMB_HOST = "cdn52.apianmeng.com"
_STREAM_HOST = "bbcdn.xianshipian.com"

_RE_DETAIL_LINK = re.compile(r"href=\"(https://www\.fangsung\.com/a%E7%89%87/(\d+)/[^\"]+)\"")
_RE_CARD_IMG = re.compile(r"<img[^>]*data-src=\"(https://cdn\d+\.apianmeng\.com/[^\"]+)\"[^>]*alt=\"([^\"]*)\"")
_RE_TOTAL = re.compile(r"共\s*(\d+)")
_RE_VAR_SOURCE = re.compile(r"var\s+source\s*=\s*\"([^\"]+\.m3u8)\"")
_RE_TAG_CHIP = re.compile(r"<a[^>]*href=\"https://www\.fangsung\.com/a%E7%89%87/search/([^\"]+)\"[^>]*class=\"btn btn-outline-light btn-sm[^\"]*\"[^>]*>([^<]+)</a>")
_RE_DATE_IN_PATH = re.compile(r"/(\d{4}-\d{2}-\d{2})/")

# Stream CDNs observed on live detail pages (verified 2026-11-25 by probing
# manifests from every region term): all of these return real #EXTM3U bodies.
# kk2.apianmeng.com is DEAD (serves a parked "New API" HTML app) — the site
# still publishes it on some (anime-path) detail pages. fangsung rotates CDN
# subdomains (cdnYYYYMMDD.apianmeng.com), so the check is prefix/suffix based,
# never a fixed host list.
_STREAM_HOST_SUFFIXES_OK = ("xianshipian.com", "aomcyou.site", "apianmeng.com", "atukjhesk.com")
_STREAM_HOST_DEAD = ("kk2.apianmeng.com",)


def _stream_host(url: str) -> str:
    m = re.match(r"^https?://([^/?#]+)/", url or "")
    return m.group(1).lower() if m else ""


def _stream_is_live(url: str) -> bool:
    """Cheap manifest probe: a live HLS entry point returns an #EXTM3U body.
    Dead/parked hosts return HTML regardless of status code."""
    try:
        import subprocess as _sp
        r = _sp.run(
            ["curl", "-s", "--max-time", "12", "-A", UA, url],
            capture_output=True, text=True, timeout=18,
        )
        return r.stdout.lstrip().startswith("#EXTM3U")
    except Exception:
        return False


def _get_fetcher():
    if Fetcher is None:
        raise RuntimeError("scrapling Fetcher unavailable; cannot scrape fangsung")
    return Fetcher


def fetch_page(url: str, timeout: int = 30, retries: int = 3) -> Optional[Any]:
    """Fetch via the shared Cloudflare-aware, cached HTTP layer.

    The Google referer is kept: fangsung serves a degraded page without it.
    """
    page = _net.fetch(
        url,
        timeout=timeout,
        retries=retries,
        referer="https://www.google.com/",
    )
    fetch_page.last_status = page.status if page is not None else None
    if page is not None and page.status == 200 and not page.challenge:
        return page
    if page is not None and page.via != "dead-cache" and page.status not in (404, 410):
        reason = "Cloudflare challenge" if page.challenge else f"HTTP {page.status}"
        print(f"    [fangsung] fetch failed {url[:90]}: {reason}")
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


def derive_stream_url(thumbnail_url: str) -> str:
    """Derives the stream URL from a thumbnail URL (1:1 path mapping).

    cdn52.apianmeng.com/kk55/{cat}/{date}/{id}.jpg
        -> bbcdn.xianshipian.com/{cat}/{date}/{id}/video.m3u8
    """
    m = re.match(rf"https://{_THUMB_HOST}/kk55/(.+)\.(?:jpg|jpeg|png|webp)$", (thumbnail_url or "").split("?")[0])
    if m:
        return f"https://{_STREAM_HOST}/{m.group(1)}/video.m3u8"
    return ""


# ─── Listing scrape ──────────────────────────────────────────────────────────

def scrape_listing(category_term: str, page_num: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Scrapes one listing page: /a片/search/{term}?page={n}.
    Returns (items, detected_max_pages). Items carry listing-level metadata;
    stream enrichment happens in enrich_detail().
    """
    term = str(category_term).strip()
    if not is_enabled(term):
        print(f"    [fangsung] category '{term}' is unknown on this site. Skipping.")
        return [], None

    info = category_info(term)
    page_num = max(1, int(page_num))
    url = f"{BASE_URL}/a%E7%89%87/search/{quote(term)}?page={page_num}"
    resp = fetch_page(url)
    if not resp:
        return [], None

    html = getattr(resp, "html_content", "") or ""
    if len(html) < 5000 or "card-img-top" not in html:
        return [], None

    # Pair each detail link with the img inside its card block.
    # Note: anchors carry target="_blank" (and sometimes other attrs) before href.
    items: List[Dict[str, Any]] = []
    seen_ids = set()
    for m in re.finditer(
        r"<a[^>]*href=\"(https://www\.fangsung\.com/a%E7%89%87/(\d+)/[^\"]+)\">\s*<div class=\"card[^\"]*\">\s*"
        r"<img[^>]*data-src=\"(https://cdn\d+\.apianmeng\.com/[^\"]+)\"[^>]*alt=\"([^\"]*)\"",
        html,
    ):
        detail_url, post_id, thumb, alt_title = m.group(1), m.group(2), m.group(3), m.group(4)
        if post_id in seen_ids:
            continue
        seen_ids.add(post_id)
        title = re.sub(r"\s+", " ", alt_title).strip()
        if not title:
            continue
        items.append({
            "source_site": SOURCE_SITE,
            "source_post_id": post_id,
            "content_type": "video",
            "title": title,
            "detail_url": _abs(detail_url),
            "thumbnail_url": _abs(thumb),
            "region": term,
            "region_en": info.get("en") or "",
            "published_at": None,  # filled from thumb path at enrich time
        })

    # Total hint: 共1552 -> ceil(total/20) pages for this term.
    total = None
    m_total = _RE_TOTAL.search(html)
    if m_total:
        try:
            total = int(m_total.group(1))
        except Exception:
            total = None
    detected_max = None
    if total:
        detected_max = (total + 19) // 20
    else:
        pages = [int(p) for p in re.findall(r"page=(\d+)", html)]
        if pages:
            detected_max = max(pages)

    return items, detected_max


# ─── Detail enrichment ───────────────────────────────────────────────────────

def _probe_hls_resolution(hls_url: str, timeout: int = 25) -> Optional[Tuple[int, int]]:
    """Probes the real rendition resolution of an HLS stream with ffprobe.

    bbcdn.xianshipian.com manifests 403 without the site Referer, so the probe
    sends it explicitly. The site serves exactly one rendition per video
    (static VOD, no master variants), so this captures the only quality.
    """
    if not hls_url:
        return None
    if not shutil.which("ffprobe"):
        print("    [fangsung] WARNING: ffprobe not found on PATH; quality probing disabled")
        return None
    for attempt in (1, 2):
        try:
            proc = subprocess.run(
                [
                    "ffprobe", "-v", "error",
                    "-user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    "-headers", "Referer: https://www.fangsung.com/\r\n",
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
        except Exception as e:
            if attempt == 2:
                print(f"    [fangsung] ffprobe failed twice for {hls_url[:80]}: {e}")
    return None


def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    """Fetches a detail page and extracts the stream URL, tags and date.

    Sets `dead` when the detail page is unreachable/anti-bot gated so the
    pipeline can keep the item unmarked for retry instead of storing junk.
    """
    merged = dict(card)
    resp = fetch_page(card.get("detail_url"), timeout=25, retries=2)
    if not resp:
        merged["dead"] = True
        return merged

    html = getattr(resp, "html_content", "") or ""
    # Degraded-page detection: full pages always contain the player container.
    if "player-container" not in html and "var source" not in html:
        merged["dead"] = True
        return merged

    m = _RE_VAR_SOURCE.search(html)
    stream = _abs(m.group(1)) if m else ""
    if not stream:
        # Fallback: derive from the thumbnail path (verified 1:1 mapping).
        stream = derive_stream_url(card.get("thumbnail_url") or "")

    # Dead-host / unknown-host validation. The site occasionally publishes
    # parked CDNs (kk2.apianmeng.com) that return HTML instead of HLS. If the
    # extracted host is not a known-good CDN, probe the manifest once; if it
    # is not a live HLS entry point, try the thumb-derived URL instead, and
    # only give up (empty stream -> row stays out of the UI) when neither
    # works.
    host = _stream_host(stream)
    if stream and (host in _STREAM_HOST_DEAD or not host.endswith(_STREAM_HOST_SUFFIXES_OK)):
        if not _stream_is_live(stream):
            derived = derive_stream_url(card.get("thumbnail_url") or "")
            if derived and derived != stream and _stream_is_live(derived):
                stream = derived
            else:
                stream = ""
    merged["full_video_url"] = stream or None
    merged["video_mime_type"] = "application/x-mpegURL" if stream else None
    merged["clean_thumbnail_url"] = card.get("thumbnail_url")
    # Real resolution via ffprobe (single-rendition site; fallback 'HD').
    probed = _probe_hls_resolution(stream) if stream else None
    if probed:
        w, h = probed
        merged["resolution"] = f"{w}x{h}"
        # "p" convention = the smaller dimension (portrait/landscape agnostic)
        merged["quality_label"] = f"{min(w, h)}p"
        merged["quality_score"] = min(w, h)
    else:
        merged["resolution"] = None
        merged["quality_label"] = "HD"
        merged["quality_score"] = 720

    # Tag chips -> tags[] (CJK terms as-is).
    tags: List[str] = []
    for _, label in _RE_TAG_CHIP.findall(html):
        t = label.strip()
        if t and t not in tags:
            tags.append(t)
    merged["tags"] = tags[:12]

    # Publish date: the site encodes it in the thumb path ({cat}/{date}/{id}).
    md = _RE_DATE_IN_PATH.search(card.get("thumbnail_url") or "")
    if md:
        try:
            from datetime import datetime, timezone
            merged["published_at"] = datetime.strptime(md.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            merged["published_at"] = None

    return merged
