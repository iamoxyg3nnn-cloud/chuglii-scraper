"""Shared HTTP layer for every scraper source.

Why this module exists
----------------------
Each category used to carry its own near-identical ``fetch_page`` that only
retried on 403/429/5xx. That had two production consequences:

1. **No Cloudflare awareness.** A challenge is served either as ``403`` or as
   ``200`` with a short "Just a moment..." body. The second case looked like a
   successful page, so the parser reported "malformed listing page" and the
   zone was retried forever.
2. **No caching.** Every retry went back to the network. On 2026-11-26 run
   45742796072 re-fetched the same dead ``91dasj.com/category/aifl/20/`` URL
   **13,185 times** (26,370 requests, all 404) because nothing remembered the
   URL was dead.

This module provides one fetch path with:

- Cloudflare challenge detection (status, headers and body markers).
- Escalation to Scrapling's ``StealthyFetcher`` with Turnstile solving, guarded
  by a process-wide lock plus cooldown so 4 workers never launch 4 browsers.
- A per-host ``cf_clearance`` cookie/UA cache, persisted to disk *and* to the
  Supabase checkpoint table so a solved challenge is reused across sessions.
- A two-tier response cache: a positive TTL cache for real pages, and a
  **negative cache for dead URLs** (404/410) that answers instantly for hours.
- A per-host minimum request interval so we cannot hammer a single origin.

Callers get a :class:`Page`; ``None`` still means "no usable page".
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

__all__ = [
    "Page",
    "fetch",
    "is_cf_challenge",
    "cf_host_status",
    "clear_cache",
    "cache_stats",
]

_PKG_DIR = Path(__file__).resolve().parent
CACHE_ROOT = Path(os.environ.get("SCRAPER_CACHE_DIR") or (_PKG_DIR / ".cache"))
_HTTP_DIR = CACHE_ROOT / "http"
_CF_DIR = CACHE_ROOT / "cf"

# ─── Tunables (env-overridable so Actions can tighten/loosen without a patch) ─

DEFAULT_TTL = int(os.environ.get("SCRAPER_CACHE_TTL", "900"))          # 15 min
DEAD_TTL = int(os.environ.get("SCRAPER_DEAD_CACHE_TTL", "21600"))      # 6 hours
HOST_MIN_INTERVAL = float(os.environ.get("SCRAPER_HOST_MIN_INTERVAL", "0.75"))
# A 5-hour session crawling ~177 zones cannot keep every page on disk.
CACHE_MAX_BYTES = int(float(os.environ.get("SCRAPER_CACHE_MAX_MB", "256")) * 1024 * 1024)
MAX_CACHE_BODY = int(float(os.environ.get("SCRAPER_CACHE_MAX_BODY_MB", "1")) * 1024 * 1024)
CF_SESSION_REUSE_SECONDS = 45.0
CF_RATE_LIMIT_COOLDOWN = 60.0
CF_MAX_SOLVES_PER_HOST = int(os.environ.get("SCRAPER_CF_MAX_SOLVES", "6"))

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Short Cloudflare interstitials. Real content pages can embed Cloudflare
# scripts, so markers only count inside a small body (see is_cf_challenge).
_CF_BODY_MARKERS = (
    "just a moment...",
    "challenge-platform",
    "cf-chl",
    "cf_chl_opt",
    "attention required! | cloudflare",
    "checking your browser",
    "enable javascript and cookies to continue",
    "cf-browser-verification",
)

# ─── Privileged state ────────────────────────────────────────────────────────

_CACHE_LOCK = threading.Lock()
_CF_LOCK = threading.Lock()                      # one browser solve at a time
_HOST_LOCK = threading.Lock()
_HOST_LAST: Dict[str, float] = {}                # host -> last request ts
_CF_LAST_SOLVE: Dict[str, float] = {}            # host -> last successful solve
_CF_COOLDOWN_UNTIL: Dict[str, float] = {}        # host -> 429 cooldown deadline
_CF_SOLVES: Dict[str, int] = {}                  # host -> solves this process
_CF_SESSIONS: Dict[str, Dict[str, Any]] = {}     # host -> {cookies, user_agent}
_CF_SESSIONS_LOADED: set[str] = set()


@dataclass
class Page:
    """Minimal response object that satisfies every existing source.

    Sources read ``.status`` and ``.html_content`` (or ``.text``); both work.
    """

    url: str
    status: int
    body: bytes = b""
    encoding: str = "utf-8"
    headers: Dict[str, str] = field(default_factory=dict)
    from_cache: bool = False
    via: str = "fetcher"      # fetcher | stealthy | cache | dead-cache
    challenge: bool = False

    @property
    def html_content(self) -> str:
        try:
            return self.body.decode(self.encoding or "utf-8", errors="ignore")
        except (LookupError, AttributeError):
            return self.body.decode("utf-8", errors="ignore")

    @property
    def text(self) -> str:
        return self.html_content

    def __bool__(self) -> bool:  # so `if page:` means "usable"
        return self.status == 200

    def __repr__(self) -> str:
        src = " [cached]" if self.from_cache else ""
        cf = " [cloudflare]" if self.challenge else ""
        return f"<Page {self.status} {self.url[:70]}{src}{cf}>"


# ─── Cloudflare detection ────────────────────────────────────────────────────

def is_cf_challenge(status: int, body: bytes = b"", headers: Optional[Dict[str, str]] = None) -> bool:
    """True when a response is a Cloudflare interstitial rather than content.

    Handles all three shapes seen in production:
    - ``403``/``503`` with a challenge body,
    - ``200`` with a short "Just a moment..." body (the dangerous one: it used
      to be parsed as a real page),
    - any status carrying the ``cf-mitigated: challenge`` header.
    """
    hdrs = {str(k).lower(): str(v).lower() for k, v in (headers or {}).items()}
    if "challenge" in hdrs.get("cf-mitigated", ""):
        return True
    if "cloudflare" in hdrs.get("server", "").lower() and status in (403, 503):
        if not body:
            return True

    text = (body or b"")[:32768].decode("utf-8", errors="ignore").lower()
    if not text:
        return status in (403, 503)
    # Only short pages can be interstitials; a 500 KB catalogue page that
    # merely mentions cloudflare is real content.
    if len(text) > 20000:
        return False
    return any(marker in text for marker in _CF_BODY_MARKERS)


# ─── Disk cache ──────────────────────────────────────────────────────────────

def _url_key(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8", errors="ignore")).hexdigest()


def _entry_path(url: str) -> Path:
    return _HTTP_DIR / f"{_url_key(url)}.json"


def _cache_get(url: str, ttl: int, allow_dead: bool = True) -> Optional[Page]:
    path = _entry_path(url)
    try:
        stat = path.stat()
    except OSError:
        return None
    age = time.time() - stat.st_mtime
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            data = json.load(fh)
        status = int(data.get("status") or 0)
        entry_age = time.time() - float(data.get("ts") or stat.st_mtime)
        limit = DEAD_TTL if status in (404, 410) else ttl
        if entry_age > limit:
            return None
        if status in (404, 410):
            if not allow_dead:
                return None
            return Page(
                url=url,
                status=status,
                body=b"",
                headers=data.get("headers") or {},
                from_cache=True,
                via="dead-cache",
            )
        body = (data.get("body") or "").encode("utf-8", errors="ignore")
        return Page(
            url=url,
            status=status,
            body=body,
            encoding=data.get("encoding") or "utf-8",
            headers=data.get("headers") or {},
            from_cache=True,
            via="cache",
        )
    except (OSError, ValueError):
        return None


def _evict_if_needed() -> None:
    """Keep the on-disk cache bounded, oldest first (caller holds the lock)."""
    if CACHE_MAX_BYTES <= 0:
        return
    try:
        files = [(f.stat().st_mtime, f.stat().st_size, f) for f in _HTTP_DIR.glob("*.json")]
    except OSError:
        return
    total = sum(size for _, size, _ in files)
    if total <= CACHE_MAX_BYTES:
        return
    for _, size, f in sorted(files):          # oldest mtime first
        try:
            f.unlink()
        except OSError:
            continue
        total -= size
        if total <= CACHE_MAX_BYTES:
            break


def _cache_put(url: str, page: Page, body: bytes) -> None:
    """Persist a cacheable response. 404/410 are cached long-term (dead URL)."""
    if page.status not in (200, 404, 410):
        return                      # never cache transient 403/429/5xx
    if page.status == 200 and page.challenge:
        return                      # never cache a challenge as content
    if page.status == 200 and len(body) > MAX_CACHE_BODY:
        return                      # keep oversized pages out of the cache
    try:
        with _CACHE_LOCK:
            _HTTP_DIR.mkdir(parents=True, exist_ok=True)
            payload = {
                "url": url,
                "status": page.status,
                "ts": time.time(),
                "encoding": page.encoding,
                "headers": {
                    k: v for k, v in (page.headers or {}).items()
                    if str(k).lower() in ("content-type", "cf-mitigated", "server")
                },
                # Dead URLs need no body; real pages keep their HTML.
                "body": "" if page.status in (404, 410) else body.decode(page.encoding or "utf-8", errors="ignore"),
            }
            tmp = _entry_path(url).with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            os.replace(tmp, _entry_path(url))
            _evict_if_needed()
    except OSError:
        pass


def cache_stats() -> Dict[str, int]:
    """Cheap counters for log lines (attempted/all written/expired)."""
    try:
        files = list(_HTTP_DIR.glob("*.json"))
    except OSError:
        return {"entries": 0, "dead": 0, "live": 0}
    dead = live = 0
    for f in files:
        try:
            with open(f, "r", encoding="utf-8", errors="ignore") as fh:
                if int(json.load(fh).get("status") or 0) in (404, 410):
                    dead += 1
                else:
                    live += 1
        except (OSError, ValueError):
            continue
    return {"entries": len(files), "dead": dead, "live": live}


def clear_cache(host: Optional[str] = None) -> int:
    """Drop cached responses (all, or only one host's). Returns files removed."""
    removed = 0
    try:
        files = list(_HTTP_DIR.glob("*.json"))
    except OSError:
        return 0
    for f in files:
        try:
            if host:
                with open(f, "r", encoding="utf-8", errors="ignore") as fh:
                    if host not in (json.load(fh).get("url") or ""):
                        continue
            f.unlink()
            removed += 1
        except (OSError, ValueError):
            continue
    return removed


# ─── Per-host politeness ─────────────────────────────────────────────────────

def _throttle(host: str, min_interval: Optional[float] = None) -> None:
    """Block until this host may be hit again. Keeps us at <= ~1.3 req/s/host."""
    interval = HOST_MIN_INTERVAL if min_interval is None else max(0.0, min_interval)
    if interval <= 0:
        return
    with _HOST_LOCK:
        last = _HOST_LAST.get(host, 0.0)
        now = time.time()
        wait = (last + interval) - now
        if wait <= 0:
            _HOST_LAST[host] = now
            return
        _HOST_LAST[host] = last + interval
    time.sleep(wait + random.uniform(0.0, 0.25))


# ─── Cloudflare cookie/UA cache ──────────────────────────────────────────────

def _cf_path(host: str) -> Path:
    return _CF_DIR / f"{host.replace(':', '_')}.json"


def _load_cf_session(host: str) -> Dict[str, Any]:
    """Load a cached cf_clearance session for a host (disk, then Supabase)."""
    if host in _CF_SESSIONS:
        return _CF_SESSIONS[host]
    data: Dict[str, Any] = {}
    if host not in _CF_SESSIONS_LOADED:
        _CF_SESSIONS_LOADED.add(host)
        # Supabase first: it survives a runner restart, the disk does not.
        try:
            from scraper.db import get_scraper_checkpoint

            sb = get_scraper_checkpoint(f"cf_session_{host}")
            if isinstance(sb, dict) and sb.get("cookies"):
                data = {"cookies": sb.get("cookies") or {}, "user_agent": sb.get("user_agent")}
        except Exception:
            pass
        if not data:
            try:
                with open(_cf_path(host), "r", encoding="utf-8") as fh:
                    local = json.load(fh)
                data = {"cookies": local.get("cookies") or {}, "user_agent": local.get("user_agent")}
            except (OSError, ValueError):
                data = {}
    _CF_SESSIONS[host] = data
    return data


def _save_cf_session(host: str, cookies: Dict[str, str], user_agent: Optional[str]) -> None:
    if not cookies:
        return
    session = {"cookies": cookies, "user_agent": user_agent}
    _CF_SESSIONS[host] = session
    try:
        _CF_DIR.mkdir(parents=True, exist_ok=True)
        with open(_cf_path(host), "w", encoding="utf-8") as fh:
            json.dump({**session, "saved_at": time.time()}, fh, indent=2)
    except OSError:
        pass
    # Best-effort cross-session persistence; never fatal.
    try:
        from scraper.db import set_scraper_checkpoint

        set_scraper_checkpoint(f"cf_session_{host}", {**session, "saved_at": time.time()})
    except Exception:
        pass


def cf_host_status(host: Optional[str] = None) -> Dict[str, Any]:
    """Diagnostics for logs: solves, cooldown and cached cookie count."""
    def _one(h: str) -> Dict[str, Any]:
        return {
            "solves": _CF_SOLVES.get(h, 0),
            "cooldown_left": max(0.0, round(_CF_COOLDOWN_UNTIL.get(h, 0.0) - time.time(), 1)),
            "cookies": len(_load_cf_session(h).get("cookies") or {}),
        }

    if host:
        return _one(host)
    return {h: _one(h) for h in set(list(_CF_SOLVES) + list(_CF_SESSIONS))}


def _solve_cloudflare(url: str, host: str, timeout_ms: int = 45000) -> Optional[Page]:
    """Solve a Cloudflare challenge with Scrapling's stealth browser.

    Serialized behind ``_CF_LOCK`` with a per-host cooldown and solve budget so
    parallel workers cannot stampede browsers or get the origin to 429 us.
    """
    now = time.time()
    if now < _CF_COOLDOWN_UNTIL.get(host, 0.0):
        return None
    if _CF_SOLVES.get(host, 0) >= CF_MAX_SOLVES_PER_HOST:
        return None
    # Another worker solved this host moments ago: reuse its cookies instead.
    if now - _CF_LAST_SOLVE.get(host, 0.0) < CF_SESSION_REUSE_SECONDS:
        cached = _load_cf_session(host)
        if cached.get("cookies"):
            return None     # caller retries the plain fetch with fresh cookies

    with _CF_LOCK:
        now = time.time()
        if now < _CF_COOLDOWN_UNTIL.get(host, 0.0):
            return None
        if now - _CF_LAST_SOLVE.get(host, 0.0) < CF_SESSION_REUSE_SECONDS:
            return None
        try:
            from scrapling.fetchers import StealthyFetcher
        except Exception as exc:
            print(f"    [!] Cloudflare challenge on {host} but Scrapling is unavailable: {exc}")
            return None

        print(f"    [*] Cloudflare challenge on {host}; solving via Scrapling StealthyFetcher...")
        _CF_SOLVES[host] = _CF_SOLVES.get(host, 0) + 1
        try:
            resp = StealthyFetcher.fetch(
                url,
                solve_cloudflare=True,
                headless=True,
                network_idle=True,
                timeout=timeout_ms,
            )
        except Exception as exc:
            print(f"    [!] Scrapling solve failed for {host}: {exc}")
            return None

        status = getattr(resp, "status", 0) or 0
        if status == 429:
            _CF_COOLDOWN_UNTIL[host] = time.time() + CF_RATE_LIMIT_COOLDOWN
            print(f"    [!] {host} rate-limited during solve; cooling down {int(CF_RATE_LIMIT_COOLDOWN)}s.")
            return None

        body = bytes(getattr(resp, "body", b"") or b"")
        headers = dict(getattr(resp, "headers", {}) or {})
        if status != 200 or is_cf_challenge(status, body, headers):
            print(f"    [!] Cloudflare still blocking {host} after solve (HTTP {status}).")
            return None

        cookies: Dict[str, str] = {}
        raw_cookies = getattr(resp, "cookies", None) or ()
        if isinstance(raw_cookies, dict):
            cookies = {str(k): str(v) for k, v in raw_cookies.items()}
        else:
            for c in raw_cookies:
                if isinstance(c, dict) and "name" in c and "value" in c:
                    cookies[str(c["name"])] = str(c["value"])

        ua = None
        req_headers = getattr(resp, "request_headers", {}) or {}
        ua = req_headers.get("user-agent") or req_headers.get("User-Agent")

        _save_cf_session(host, cookies, ua)
        _CF_LAST_SOLVE[host] = time.time()
        print(f"    [✓] Cloudflare cleared for {host}; cached {len(cookies)} cookies (cf_clearance).")
        return Page(
            url=url,
            status=200,
            body=body,
            encoding=getattr(resp, "encoding", "utf-8") or "utf-8",
            headers=headers,
            via="stealthy",
        )


# ─── The fetch path ──────────────────────────────────────────────────────────

def _plain_get(
    url: str,
    timeout: float,
    headers: Optional[Dict[str, str]],
    referer: Optional[str],
) -> Optional[Page]:
    """One curl_cffi-backed request carrying any cached Cloudflare cookies."""
    try:
        from scrapling.fetchers import Fetcher
    except Exception:
        return None

    host = urlparse(url).netloc
    session = _load_cf_session(host)
    req_headers: Dict[str, str] = {"User-Agent": session.get("user_agent") or _UA}
    if referer:
        req_headers["Referer"] = referer
    if headers:
        req_headers.update(headers)

    kwargs: Dict[str, Any] = {
        "stealthy_headers": True,
        "timeout": timeout,
        "headers": req_headers,
    }
    if session.get("cookies"):
        kwargs["cookies"] = session["cookies"]
    try:
        resp = Fetcher.get(url, **kwargs)
    except Exception:
        return None

    body = bytes(getattr(resp, "body", b"") or b"")
    status = int(getattr(resp, "status", 0) or 0)
    resp_headers = dict(getattr(resp, "headers", {}) or {})
    page = Page(
        url=url,
        status=status,
        body=body,
        encoding=getattr(resp, "encoding", "utf-8") or "utf-8",
        headers=resp_headers,
        via="fetcher",
    )
    page.challenge = is_cf_challenge(status, body, resp_headers)
    return page


def fetch(
    url: str,
    *,
    timeout: float = 30.0,
    retries: int = 3,
    headers: Optional[Dict[str, str]] = None,
    referer: Optional[str] = None,
    allow_cache: bool = True,
    cache_ttl: Optional[int] = None,
    allow_dead_cache: bool = True,
    min_interval: Optional[float] = None,
    max_backoff: float = 8.0,
) -> Optional[Page]:
    """Fetch ``url`` with caching, Cloudflare handling and politeness.

    Returns a :class:`Page` for any HTTP answer that reached the origin
    (including 404/410, which callers may interpret as "end of listing"), or
    ``None`` when no answer could be obtained at all (network error,
    unresolvable Cloudflare block, or exhausted retries).

    A Cloudflare challenge is *never* returned as content: it comes back as
    ``Page(challenge=True)`` with a non-200 status, so parsers cannot mistake
    an interstitial for an empty listing.
    """
    if not url:
        return None

    ttl = DEFAULT_TTL if cache_ttl is None else max(0, int(cache_ttl))
    host = urlparse(url).netloc

    if allow_cache:
        cached = _cache_get(url, ttl, allow_dead=allow_dead_cache)
        if cached is not None:
            return cached

    attempts = max(1, int(retries))
    last_page: Optional[Page] = None

    for attempt in range(attempts):
        _throttle(host, min_interval)
        page = _plain_get(url, timeout, headers, referer)
        last_page = page

        if page is None:                              # transport failure
            if attempt < attempts - 1:
                time.sleep(min(1.5 * (attempt + 1), 6.0))
                continue
            return None

        # Dead URL: remember it so the next caller pays zero requests.
        if page.status in (404, 410):
            _cache_put(url, page, b"")
            return page

        if page.challenge or page.status in (403, 503):
            solved = _solve_cloudflare(url, host)
            if solved is not None:
                _cache_put(url, solved, solved.body)
                return solved
            # Cookies may have just been refreshed by another worker: retry.
            if attempt < attempts - 1:
                time.sleep(min(1.5 * (attempt + 1), 6.0))
                continue
            # Unresolved challenge: strip the interstitial body and force a
            # non-200 status so no parser can mistake it for an empty listing.
            return Page(
                url=url,
                status=403 if page.status == 200 else page.status,
                body=b"",
                encoding=page.encoding,
                headers=page.headers,
                challenge=True,
                via=page.via,
            )

        if page.status == 200:
            if allow_cache:
                _cache_put(url, page, page.body)
            return page

        if page.status in (429, 500, 502, 504) and attempt < attempts - 1:
            time.sleep(min(2.0 * (attempt + 1), max_backoff))
            continue

        return page

    return last_page
