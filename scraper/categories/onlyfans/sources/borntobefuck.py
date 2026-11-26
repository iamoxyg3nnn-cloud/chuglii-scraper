"""Source scraper for borntobefuck.com (OnlyFans category).

Site architecture (reverse-engineered 2026-11-26, see
``scraper/categories/onlyfans/PLAN_two_new_sources.md``):

  - Laravel + Cloudflare. Listing pages are server-rendered; plain HTTP works.
  - **The catalogue is much larger than the listing suggests.** Video ids run
    to ~46,681, but ``/videos?page=1..50`` is only a rolling ~1-month window
    (597 ids) and ``sort=oldest`` is a *separate* window (961 ids, ids ~2-1600,
    overlapping the first by a single id). The middle of the id space is
    reachable only through the 94 category pages or by id.
  - **Streams exist only for ids >= ~16,400.** ``/videos/{id}/player`` returns
    200 with no m3u8 below that, so the 2023-24 archive is structurally
    unplayable and is skipped for free (no probe needed).
  - The watch page has no ``<video>``; the player is mounted from a fragment
    endpoint that must be requested with ``Accept: text/html`` and
    ``X-Requested-With: XMLHttpRequest`` plus a Referer, otherwise Cloudflare
    blocks it.
  - Media is ``content.zavastream.com/{tenant}/{uuid}/index.m3u8``: unsigned,
    stable, 720p + 480p, and it serves bytes only when the request carries a
    ``Referer`` from the watch page. Segments are relative.

This module exposes three entry points the rest of the pipeline uses:

  ``scrape_listing(page_num)``        -> the recent /videos window
  ``scrape_category_page(slug, page)``-> the archive (P3 backfill)
  ``enrich_detail(card)``             -> resolve + verify the HLS stream

plus ``enrich_from_id(video_id)`` for the sharded id sweep (P6).
"""
from __future__ import annotations

import html as html_mod
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from scraper import net as _net
from scraper.categories.onlyfans.stream_check import verify_stream

BASE_URL = "https://borntobefuck.com"
SOURCE_SITE = "borntobefuck"

#: ``/videos`` and every category paginate to a hard cap of ~80 pages; 50 is the
#: observed limit for the default (newest) listing.
MAX_LISTING_PAGES = 50
MAX_CATEGORY_PAGES = 80

#: Below this id the player fragment never contains a stream (binary-searched
#: 2026-11-26: no stream at 16,346, stream at 16,459). Used to skip the dead
#: archive without spending a single request.
MIN_STREAMABLE_ID = 16_459

#: The player fragment is Cloudflare-guarded unless these are present.
PLAYER_HEADERS = {
    "Accept": "text/html,application/xhtml+xml",
    "X-Requested-With": "XMLHttpRequest",
}

_RE_TITLE_SUFFIX = re.compile(r"\s*(?:Porn\s*Video|Pornovideo|Pornvideo)\s*BornToBeFuck\s*$", re.I)
_RE_CARD_SPLIT = re.compile(r'class="video(?=[\s"])')
_RE_WATCH = re.compile(r"/watch/(\d+)")
# The thumbnail tag puts class AFTER src, so match the whole <img> and read its
# attributes rather than assuming an order.
_RE_THUMB_TAG = re.compile(r'<img[^>]*class="thumbnail"[^>]*>', re.S)
_RE_ATTR = re.compile(r'([\w-]+)="([^"]*)"')
_RE_H3_TITLE = re.compile(r'class="video-title"[^>]*>(.*?)</h3>', re.S)
_RE_DURATION = re.compile(r'data-duration="(\d+)"')
_RE_VIEWS = re.compile(r'data-views="(\d+)"')
_RE_PUBLISHED = re.compile(r'data-published="(\d+)"')
_RE_MODEL_HANDLE = re.compile(r'href="[^"]*?/@([A-Za-z0-9_-]+)"')
_RE_MODEL_NAME = re.compile(r'class="video-channel[^"]*"[^>]*>([^<]*)<')
_RE_COUNTRY = re.compile(r'/categories/([a-z-]+)')
_RE_STREAM_INF = re.compile(
    r"#EXT-X-STREAM-INF:([^\n]*)\n\s*([^\n#]+)", re.I
)


# ─── Fetch helpers ──────────────────────────────────────────────────────────

def fetch_page(
    url: str,
    timeout: int = 30,
    retries: int = 3,
    *,
    headers: Optional[Dict[str, str]] = None,
    referer: Optional[str] = None,
    solve_cf: bool = True,
) -> Optional[Any]:
    """Shared Cloudflare-aware, cached fetch. Returns None for non-200."""
    page = _net.fetch(
        url,
        timeout=timeout,
        retries=retries,
        headers=headers,
        referer=referer,
        accept_challenge_fallback=solve_cf,
    )
    fetch_page.last_status = page.status if page is not None else None
    if page is not None and page.status == 200 and not page.challenge:
        return page
    if page is not None and page.via != "dead-cache" and page.status not in (404, 410):
        reason = "Cloudflare challenge" if page.challenge else f"HTTP {page.status}"
        print(f"    [borntobefuck] fetch failed {url[:90]}: {reason}")
    return None


def _clean(text: Optional[str]) -> str:
    return re.sub(r"\s+", " ", html_mod.unescape(text or "")).strip()


def _iso_from_unix(value: Optional[str]) -> Optional[str]:
    try:
        return datetime.fromtimestamp(int(value), timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return None


# ─── Card parsing ───────────────────────────────────────────────────────────

def _parse_cards(html: str) -> List[Dict[str, Any]]:
    """Parse every ``div.video.ranked`` card on a listing/category page."""
    if not html:
        return []
    chunks = _RE_CARD_SPLIT.split(html)[1:]
    items: List[Dict[str, Any]] = []
    seen = set()

    for chunk in chunks:
        body = chunk[:4200]                       # one card is ~3.1 KB
        watch = _RE_WATCH.search(body)
        if not watch:
            continue
        video_id = watch.group(1)
        if video_id in seen:
            continue
        seen.add(video_id)

        title = _clean((_RE_H3_TITLE.search(body) or [None, ""])[1] if _RE_H3_TITLE.search(body) else "")
        thumb = preview = ""
        thumb_tag = _RE_THUMB_TAG.search(body)
        if thumb_tag:
            attrs = {k.lower(): v for k, v in _RE_ATTR.findall(thumb_tag.group(0))}
            thumb = attrs.get("src", "")
            preview = attrs.get("data-preview-url", "")
            if not title:
                title = _clean(attrs.get("title", ""))
        if not title:
            continue
        title = _RE_TITLE_SUFFIX.sub("", title).strip()
        if not title:
            continue
        duration = (_RE_DURATION.search(body).group(1) if _RE_DURATION.search(body) else None)
        views = (_RE_VIEWS.search(body).group(1) if _RE_VIEWS.search(body) else None)
        published = (_RE_PUBLISHED.search(body).group(1) if _RE_PUBLISHED.search(body) else None)
        handle = (_RE_MODEL_HANDLE.search(body).group(1) if _RE_MODEL_HANDLE.search(body) else "")
        model_name = _clean((_RE_MODEL_NAME.search(body).group(1) if _RE_MODEL_NAME.search(body) else ""))
        country = (_RE_COUNTRY.search(body).group(1) if _RE_COUNTRY.search(body) else "")

        items.append(
            {
                "source_site": SOURCE_SITE,
                "source_post_id": video_id,
                "title": title,
                "detail_url": f"{BASE_URL}/watch/{video_id}",
                "thumbnail_url": thumb if thumb.startswith("http") else (urljoin(BASE_URL, thumb) if thumb else ""),
                "preview_url": preview,
                "models": [model_name or handle.replace("-", " ").title()] if (model_name or handle) else [],
                "model_handle": handle,
                "categories_raw": [country] if country else [],
                "duration_seconds": int(duration) if duration else None,
                "views_count": int(views) if views else 0,
                "views_raw": views or "0",
                "published_at": _iso_from_unix(published),
            }
        )
    return items


# ─── Listing surfaces ───────────────────────────────────────────────────────

def scrape_listing(page_num: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """The recent ``/videos`` window (rolling ~1 month, ids ~45k-46.7k)."""
    page_num = max(1, int(page_num))
    url = f"{BASE_URL}/videos?page={page_num}"
    resp = fetch_page(url)
    if not resp:
        return [], None
    return _parse_cards(resp.html_content), MAX_LISTING_PAGES


def scrape_category_page(slug: str, page_num: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """One page of ``/categories/{slug}`` — the archive surface (P3 backfill)."""
    slug = str(slug or "").strip().strip("/")
    if not slug:
        return [], None
    page_num = max(1, int(page_num))
    url = f"{BASE_URL}/categories/{slug}"
    if page_num > 1:
        url += f"?page={page_num}"
    resp = fetch_page(url)
    if not resp:
        return [], None
    return _parse_cards(resp.html_content), MAX_CATEGORY_PAGES


def list_categories() -> List[str]:
    """All category slugs advertised on ``/categories`` (94 at time of writing)."""
    resp = fetch_page(f"{BASE_URL}/categories")
    if not resp:
        return []
    slugs = {m.group(1) for m in _RE_COUNTRY.finditer(resp.html_content)}
    slugs.discard("")
    return sorted(slugs)


# ─── Stream resolution ──────────────────────────────────────────────────────

def player_fragment(video_id: str) -> Optional[str]:
    """Fetch the player fragment; returns its HTML or None.

    The fragment is the only place the m3u8 appears. It answers 200 with no
    stream for the pre-2024 archive, which is our dead-video signal.
    """
    vid = str(video_id).strip()
    if not vid.isdigit():
        return None
    if int(vid) < MIN_STREAMABLE_ID:
        return None                                # known unplayable, no request
    url = f"{BASE_URL}/videos/{vid}/player?lang=en"
    # A non-2xx here almost always means "restricted video", not a Cloudflare
    # challenge, and the fragment is not navigable so a stealth browser cannot
    # help. Skipping escalation saves ~20s per restricted video.
    resp = fetch_page(
        url,
        retries=1,          # a 403/404 here is definitive, not transient
        headers=PLAYER_HEADERS,
        referer=f"{BASE_URL}/watch/{vid}",
        solve_cf=False,
    )
    return resp.html_content if resp else None


def parse_master(master_url: str) -> List[Dict[str, Any]]:
    """Turn a master m3u8 into ``servers[]`` entries ordered best-first."""
    page = _net.fetch(master_url, retries=1, referer=BASE_URL + "/")
    if page is None or page.status != 200:
        return []
    text = page.html_content
    if "#EXTM3U" not in text:
        return []

    servers: List[Dict[str, Any]] = []
    for attrs, uri in _RE_STREAM_INF.findall(text):
        res_match = re.search(r"RESOLUTION=(\d+)x(\d+)", attrs, re.I)
        height = int(res_match.group(2)) if res_match else 0
        label = {1080: "1080p Full HD", 720: "720p HD", 480: "480p SD"}.get(height, f"{height}p" if height else "auto")
        servers.append(
            {
                "name": f"BornToBeFuck {label}",
                "server": "BornToBeFuck",
                "embed_url": urljoin(master_url, uri.strip()),
                "master_url": master_url,
                "type": "hls",
                "quality_label": label,
                "quality_score": height or 720,
            }
        )
    if not servers:
        servers.append(
            {
                "name": "BornToBeFuck HLS",
                "server": "BornToBeFuck",
                "embed_url": master_url,
                "master_url": master_url,
                "type": "hls",
                "quality_label": "720p HD",
                "quality_score": 720,
            }
        )
    servers.sort(key=lambda s: s["quality_score"], reverse=True)
    return servers


def resolve_stream(video_id: str) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    """(master_url, servers) for a video id, or (None, []) when unplayable."""
    html = player_fragment(video_id)
    if not html:
        return None, []
    match = re.search(r"https?://[^\"'\s]+index\.m3u8[^\"'\s]*", html)
    if not match:
        return None, []                            # fragment without a stream = dead
    master = match.group(0)
    return master, parse_master(master)


# ─── Detail enrichment ──────────────────────────────────────────────────────

def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    """Verify the HLS stream and attach servers. Thread-safe; never raises."""
    out = dict(card)
    video_id = str(out.get("source_post_id") or "").strip()
    referer = f"{BASE_URL}/watch/{video_id}"

    master, servers = resolve_stream(video_id)
    if not master:
        # Definitive: no fragment (unplayable archive id) or a 403 on it means
        # this video is restricted/gone. Flag it so the pipeline records it as
        # dead instead of re-probing it on every future cycle.
        out["stream_dead"] = True
        return out

    alive, status, detail = verify_stream(
        master,
        kind="hls",
        referer=referer,
        source_site=SOURCE_SITE,
        source_post_id=video_id,
    )
    if not alive:
        print(f"      [borntobefuck] dead stream for {video_id} (HTTP {status}: {detail})")
        out["stream_dead"] = True
        return out

    out["servers"] = servers
    out["full_video_url"] = master
    out["video_mime_type"] = "application/x-mpegURL"
    out["stream_referer"] = referer               # playback/eval hint for the proxy
    if not out.get("duration_seconds"):
        # The card normally carries data-duration; the fragment does not.
        out["duration_seconds"] = None
    return out


def enrich_from_id(video_id: str) -> Optional[Dict[str, Any]]:
    """Build a minimal card straight from an id (P6 id sweep).

    Title/thumbnail come from the player fragment; full metadata would need the
    watch page, so callers should prefer ``scrape_listing``/``scrape_category_page``
    when the id is already known.
    """
    html = player_fragment(video_id)
    if not html:
        return None
    title_m = re.search(r"<title>([^<]{3,200})</title>", html, re.I)
    poster_m = re.search(r'poster="([^"]+)"', html)
    title = _clean(title_m.group(1)) if title_m else ""
    title = _RE_TITLE_SUFFIX.sub("", title).strip()
    if not title:
        return None
    return {
        "source_site": SOURCE_SITE,
        "source_post_id": str(video_id),
        "title": title,
        "detail_url": f"{BASE_URL}/watch/{video_id}",
        "thumbnail_url": poster_m.group(1) if poster_m else "",
        "models": [],
        "views_count": 0,
        "views_raw": "0",
        "published_at": None,
        "duration_seconds": None,
    }
