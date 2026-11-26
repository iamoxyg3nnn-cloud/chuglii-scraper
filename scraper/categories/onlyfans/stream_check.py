"""Layer 1 of the dead-video gate: never ingest a stream that does not play.

Why this exists
---------------
On uncut-x.com, 3,080 of 4,445 videos point at Cloudflare R2 objects that no
longer exist. The bucket answers `400 InvalidArgument/Authorization` to a plain
request and `404` once you sign the URL -- both of which a naive scraper records
as "a video". The result is a feed where two thirds of the cards are broken.

This module answers one question: *does this stream actually serve playable
media right now?* It is deliberately strict and cheap:

- HLS  : master -> ``#EXTM3U`` + at least one ``#EXT-X-STREAM-INF``, then the
         first variant -> ``#EXTINF``, then the first segment -> 2xx and a
         ``video/*`` content type. A 200 master with 404 segments (a classic
         half-dead playlist) fails at the segment step.
- MP4  : ``Range: bytes=0-1023`` -> 2xx/206 and ``video/*``.

``400``/``403`` are never treated as alive: that is exactly how the R2 bucket
lied. Results are memoised per process so one crawl never probes a URL twice,
and every verdict is written to ``scraper.stream_health`` (best effort) so the
nightly revalidation job has a starting point.
"""
from __future__ import annotations

import hashlib
import threading
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse

__all__ = ["verify_stream", "probe_mp4", "probe_hls", "infer_kind", "clear_memo"]

_PROBE_LOCK = threading.Lock()
_MEMO: Dict[str, Tuple[bool, int, str]] = {}

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


# ─── Transport ──────────────────────────────────────────────────────────────

def _get(url: str, *, headers: Optional[Dict[str, str]] = None, timeout: float = 15.0):
    """GET with browser TLS impersonation. Returns (status, headers, body)."""
    hdrs = {"User-Agent": _UA, "Accept": "*/*"}
    if headers:
        hdrs.update(headers)
    try:
        from curl_cffi import requests as creq  # type: ignore

        r = creq.get(url, headers=hdrs, timeout=timeout, impersonate="chrome124", allow_redirects=True)
        return int(r.status_code or 0), dict(r.headers or {}), bytes(r.content or b"")
    except Exception:
        pass
    try:
        import httpx

        with httpx.Client(timeout=timeout, follow_redirects=True) as c:
            r = c.get(url, headers=hdrs)
            return int(r.status_code or 0), dict(r.headers or {}), r.content or b""
    except Exception:
        return 0, {}, b""


def _is_video_ctype(value: str) -> bool:
    v = (value or "").lower()
    return v.startswith("video/") or "mpegurl" in v or "octet-stream" in v


# ─── Probes ─────────────────────────────────────────────────────────────────

def probe_hls(url: str, referer: Optional[str] = None, timeout: float = 15.0) -> Tuple[bool, int, str]:
    headers = {"Referer": referer} if referer else None
    status, resp_headers, body = _get(url, headers=headers, timeout=timeout)
    if status != 200:
        return False, status, f"master HTTP {status}"
    text = body.decode("utf-8", errors="ignore")
    if "#EXTM3U" not in text:
        return False, status, "master is not a playlist"

    # A media playlist (segments) is perfectly valid input -- it is what gets
    # stored when a source exposes per-rendition URLs. Distinguish it from a
    # master BEFORE reading URIs, otherwise its .ts lines look like variants
    # and we probe a segment as if it were a playlist.
    if "#EXT-X-STREAM-INF" not in text:
        return _probe_media_playlist(url, text, referer, status, timeout)

    variants = [
        ln.strip() for ln in text.splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    ]
    if not variants:
        return False, status, "master has no variant URI"

    variant_url = urljoin(url, variants[0])
    v_status, _, v_body = _get(variant_url, headers=headers, timeout=timeout)
    if v_status != 200:
        return False, v_status, f"variant HTTP {v_status}"
    v_text = v_body.decode("utf-8", errors="ignore")
    if "#EXTINF" not in v_text:
        return False, v_status, "variant has no segments"
    return _first_segment(variant_url, v_text, referer, v_status, timeout)


def _probe_media_playlist(url, text, referer, status, timeout):
    if "#EXTINF" not in text:
        return False, status, "media playlist has no segments"
    return _first_segment(url, text, referer, status, timeout)


def _first_segment(playlist_url: str, playlist_text: str, referer, status, timeout):
    seg = None
    for line in playlist_text.splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            seg = s
            break
    if not seg:
        return False, status, "no segment URI"
    seg_url = urljoin(playlist_url, seg)
    headers = {"Range": "bytes=0-2047"}
    if referer:
        headers["Referer"] = referer
    s_status, s_headers, s_body = _get(seg_url, headers=headers, timeout=timeout)
    if s_status not in (200, 206):
        return False, s_status, f"first segment HTTP {s_status}"
    if not s_body:
        return False, s_status, "first segment empty"
    ctype = s_headers.get("content-type") or s_headers.get("Content-Type") or ""
    if ctype and not _is_video_ctype(ctype):
        return False, s_status, f"segment content-type {ctype[:32]}"
    return True, status, "hls ok"


def probe_mp4(url: str, referer: Optional[str] = None, timeout: float = 15.0) -> Tuple[bool, int, str]:
    headers = {"Range": "bytes=0-1023"}
    if referer:
        headers["Referer"] = referer
    status, resp_headers, body = _get(url, headers=headers, timeout=timeout)
    if status not in (200, 206):
        return False, status, f"mp4 HTTP {status}"
    ctype = resp_headers.get("content-type") or resp_headers.get("Content-Type") or ""
    if ctype and not _is_video_ctype(ctype):
        return False, status, f"mp4 content-type {ctype[:32]}"
    if not body:
        return False, status, "mp4 empty body"
    return True, status, "mp4 ok"


def infer_kind(url: str, declared: Optional[str] = None) -> str:
    if declared in ("hls", "mp4"):
        return declared
    path = urlparse(url or "").path.lower()
    if path.endswith(".m3u8") or "m3u8" in path:
        return "hls"
    if path.endswith((".mp4", ".m4v", ".webm", ".mov")):
        return "mp4"
    return "other"


# ─── Public entry point ─────────────────────────────────────────────────────

def verify_stream(
    url: str,
    *,
    kind: Optional[str] = None,
    referer: Optional[str] = None,
    timeout: float = 15.0,
    source_site: Optional[str] = None,
    source_post_id: Optional[str] = None,
    record: bool = True,
) -> Tuple[bool, int, str]:
    """Return ``(alive, http_status, detail)`` for a stream URL.

    Memoised per process. Never raises.
    """
    if not url:
        return False, 0, "no url"

    key = url
    with _PROBE_LOCK:
        hit = _MEMO.get(key)
    if hit is not None:
        return hit

    resolved_kind = infer_kind(url, kind)
    try:
        if resolved_kind == "hls":
            alive, status, detail = probe_hls(url, referer, timeout)
        elif resolved_kind == "mp4":
            alive, status, detail = probe_mp4(url, referer, timeout)
        else:
            # Unknown container: try a ranged GET, then fall back to HLS parsing.
            alive, status, detail = probe_mp4(url, referer, timeout)
            if not alive:
                alive, status, detail = probe_hls(url, referer, timeout)
    except Exception as exc:                       # never let a probe break ingest
        alive, status, detail = False, 0, f"{type(exc).__name__}: {exc}"

    with _PROBE_LOCK:
        _MEMO[key] = (alive, status, detail)
    if record:
        _record(url, alive, status, detail, resolved_kind, source_site, source_post_id)
    return alive, status, detail


def clear_memo() -> None:
    with _PROBE_LOCK:
        _MEMO.clear()


# ─── Health ledger (best effort) ────────────────────────────────────────────

def _record(url, alive, status, detail, kind, source_site, source_post_id) -> None:
    try:
        from scraper.db import get_supabase_client

        supabase = get_supabase_client()
        if supabase is None:
            return
        supabase.table("stream_health").upsert(
            {
                "url_hash": hashlib.sha1(url.encode("utf-8", errors="ignore")).hexdigest(),
                "source_site": source_site or "unknown",
                "source_post_id": str(source_post_id) if source_post_id else None,
                "host": urlparse(url).netloc,
                "url_kind": kind,
                "http_status": int(status or 0),
                "is_alive": bool(alive),
                "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "detail": (detail or "")[:200],
            },
            on_conflict="url_hash",
        ).execute()
    except Exception:
        # Ledger writes must never fail an ingest; Layer 2 re-probes anyway.
        pass
