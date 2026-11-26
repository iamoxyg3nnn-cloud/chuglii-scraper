"""
Shared HTTP, Scrapling fallback, metadata helpers, and strict dead-video
verification for Viral MMS site adapters.
"""
import re
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from curl_cffi import requests as cf_requests

try:
    from scrapling.fetchers import Fetcher as _ScraplingFetcher  # type: ignore
except Exception:
    try:
        from scrapling import Fetcher as _ScraplingFetcher  # type: ignore
    except Exception:
        _ScraplingFetcher = None

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_CF_MARKERS = (
    "just a moment...",
    "challenge-platform",
    "cf-chl",
    "attention required! | cloudflare",
)

# Hosts that are permanently dead, parked, or serve only 404/ad redirects.
DEAD_EMBED_HOSTS = (
    "sbfull.", "sbfast.", "sblongvu.", "sbchill.", "sbbrisk.", "sbthe.",
    "sbanh.", "sblanh.", "sbspeed.", "sblona.", "sbrapid.", "sbface.",
    "sbhight.", "sbrity.", "sbsonic.", "likessb.", "lvturbo.",
    "slwatch.", "sltube.", "slmaxed.", "streamlare.",
    "fembed.", "upvideo.", "short.ink", "mixdrop.vc", "mixdrop.ch",
    "parklogic.", "resultfindernow.",
)

# Third-party ad / popunder iframes that must never be treated as video embeds.
AD_IFRAME_SUBSTRINGS = (
    "xlviiirdr", "magsrv", "exoclick", "juicyads", "popads", "blazingserver",
    "showmeyouradsnow", "dscgirls", "videobaba", "doubleclick", "googlesyndication",
    "trafficjunky", "chaturbate", "stripchat", "bongacams", "livejasmin",
    "realsrv", "tsyndicate", "a-ads", "adsterra",
)

DEAD_BODY_MARKERS = (
    '"fileid":"nofile"',
    "video not found",
    "file was deleted",
    "file not found",
    "404 not found",
    "this video has been removed",
    "video has been deleted",
    "parklogic",
    "resultfindernow",
)


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def abs_url(base_url: str, raw: Optional[str]) -> str:
    if not raw:
        return ""
    u = str(raw).strip()
    if not u or u.startswith("data:") or u.startswith("javascript:"):
        return ""
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return urljoin(base_url, u)
    return u


def looks_like_cf_challenge(html: str) -> bool:
    low = (html or "").lower()
    return len(html or "") < 25000 and any(m in low for m in _CF_MARKERS)


def is_ad_iframe(url: str) -> bool:
    low = (url or "").lower()
    return any(ad in low for ad in AD_IFRAME_SUBSTRINGS)


def fetch_html(
    url: str,
    *,
    session: Optional[cf_requests.Session] = None,
    referer: Optional[str] = None,
    timeout: int = 20,
    retries: int = 3,
) -> Tuple[Optional[str], int]:
    """Fetch HTML using curl_cffi (Chrome TLS), with Scrapling fallback."""
    headers = {
        "User-Agent": UA,
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
            if last_status == 200 and not looks_like_cf_challenge(html):
                return html, 200
            if last_status == 404:
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
            if status == 200 and not looks_like_cf_challenge(html):
                return html, 200
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
    """Verify a direct video/playlist URL actually serves media bytes.

    Rejects 403/404/410, fake 1x1 tracking GIFs (KVS hotlink trap), and HTML
    error pages. Returns ``(is_alive, resolved_final_url)``.
    """
    u = clean_text(stream_url)
    if not u.startswith(("http://", "https://")):
        return False, ""

    low = u.lower()
    if any(dead in low for dead in DEAD_EMBED_HOSTS):
        return False, ""

    client = session or cf_requests
    headers = {
        "User-Agent": UA,
        "Referer": referer,
        "Range": "bytes=0-1023",
    }

    try:
        r = client.get(
            u,
            impersonate="chrome124",
            timeout=timeout,
            headers=headers,
            allow_redirects=True,
        )
        status = int(getattr(r, "status_code", 0) or 0)
        final_url = str(getattr(r, "url", u) or u).strip()
        ct = (r.headers.get("content-type") or "").lower()

        if status not in (200, 206):
            return False, ""
        if any(dead in final_url.lower() for dead in DEAD_EMBED_HOSTS):
            return False, ""

        is_hls = ".m3u8" in final_url.lower() or "mpegurl" in ct
        if is_hls:
            body = (getattr(r, "text", "") or "")[:500]
            if "#EXTM3U" not in body and "html" in ct:
                return False, ""
            return True, final_url

        # Reject fake GIF/image hotlink traps and HTML error pages.
        if ct.startswith("image/") or "text/html" in ct:
            return False, ""
        if ct and not (
            ct.startswith("video/")
            or "octet-stream" in ct
            or "binary" in ct
        ):
            return False, ""

        # If content-type header was omitted, sniff first bytes for HTML.
        raw_bytes = getattr(r, "content", b"")[:200].lstrip().lower()
        if raw_bytes.startswith((b"<!doctype", b"<html", b"gif87a", b"gif89a")):
            return False, ""

        return True, final_url
    except Exception:
        return False, ""


def verify_embed_alive(
    embed_url: str,
    *,
    referer: str,
    session: Optional[cf_requests.Session] = None,
    timeout: int = 12,
) -> bool:
    """Verify an iframe embed URL is alive and not deleted/parked."""
    u = clean_text(embed_url)
    if not u.startswith(("http://", "https://")):
        return False
    low = u.lower()
    if any(dead in low for dead in DEAD_EMBED_HOSTS) or is_ad_iframe(low):
        return False

    client = session or cf_requests
    try:
        r = client.get(
            u,
            impersonate="chrome124",
            timeout=timeout,
            headers={"User-Agent": UA, "Referer": referer},
            allow_redirects=True,
        )
        status = int(getattr(r, "status_code", 0) or 0)
        if status >= 400:
            return False
        final_low = str(getattr(r, "url", u) or u).lower()
        if any(dead in final_low for dead in DEAD_EMBED_HOSTS):
            return False
        body = (getattr(r, "text", "") or "")[:12000].lower()
        if any(marker in body for marker in DEAD_BODY_MARKERS):
            return False
        return True
    except Exception:
        return False


def parse_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html or "", "html.parser")
