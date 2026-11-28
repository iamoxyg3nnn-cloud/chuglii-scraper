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
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

__all__ = [
    "Page",
    "fetch",
    "is_cf_challenge",
    "is_origin_error_page",
    "cf_host_status",
    "clear_cache",
    "cache_stats",
    "is_block_status",
    "blocked_hosts",
    "forget_blocked_host",
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
CF_MAX_SOLVES_PER_HOST = int(os.environ.get("SCRAPER_CF_MAX_SOLVES", "4"))
# A host that fails this many solves in a row is not solvable from this runner:
# give up on the browser for it and let the proxy tier answer (or fail fast).
CF_MAX_FAILED_SOLVES = int(os.environ.get("SCRAPER_CF_MAX_FAILED_SOLVES", "2"))
CF_HOPELESS_COOLDOWN = float(os.environ.get("SCRAPER_CF_HOPELESS_COOLDOWN", "1800"))

# Proxy escalation. Origins routinely reject datacenter egress (GitHub Actions
# runners) with a non-standard status instead of a Cloudflare challenge —
# madouhd.cc answers 466 to every URL for a runner IP while a normal IP gets
# 200. Before this, ``fetch`` returned those answers untouched, so a whole
# category scraped zero rows forever. A blocked answer is now routed through a
# validated proxy (and through Scrapling's stealth browser for Cloudflare).
PROXY_FALLBACK = os.environ.get("SCRAPER_PROXY_FALLBACK", "1") not in ("0", "false", "False", "")
PROXY_TIMEOUT = float(os.environ.get("SCRAPER_PROXY_TIMEOUT", "20"))
# Free proxies are cheap and fail often: try several distinct ones per fetch
# instead of two, and never the same one twice.
PROXY_ATTEMPTS_PER_FETCH = int(os.environ.get("SCRAPER_PROXY_ATTEMPTS", "4"))

# Statuses a WAF/origin uses to reject the *client*, not to describe the URL.
# 404/410 stay out of this set: those mean "gone" and are cached as dead.
BLOCK_STATUSES = frozenset({401, 402, 403, 406, 407, 409, 418, 429, 451, 466, 468, 469, 511})

# Once a host has proved it blocks our direct egress, go proxy-first for a
# while instead of burning three doomed attempts per URL (madouhd has 40+
# zones and two passes each — 250+ wasted requests per session otherwise).
BLOCKED_HOST_TTL = float(os.environ.get("SCRAPER_BLOCKED_HOST_TTL", "1800"))
_BLOCKED_HOSTS: Dict[str, float] = {}
_BLOCKED_LOCK = threading.Lock()

# A host we cannot reach at all (direct times out AND the proxy fleet times out)
# must stop costing us: run 46114735791 still spent ~4 minutes per page on
# uxj.838238.xyz, because every one of 20 detail fetches re-paid a 30s direct
# timeout plus a 30s proxy timeout. After HOST_DOWN_STRIKES unreachable fetches
# the host is skipped entirely for HOST_DOWN_COOLDOWN seconds, so the feed moves
# on (items stay unmarked in the ledger and are retried later) instead of
# burning its whole slice on a dead route.
HOST_DOWN_STRIKES = int(os.environ.get("SCRAPER_HOST_DOWN_STRIKES", "3"))
HOST_DOWN_COOLDOWN = float(os.environ.get("SCRAPER_HOST_DOWN_COOLDOWN", "600"))
_HOST_DOWN_STRIKES: Dict[str, int] = {}
_HOST_DOWN_UNTIL: Dict[str, float] = {}
_HOST_DOWN_LOCK = threading.Lock()


def _host_is_down(host: str) -> bool:
    with _HOST_DOWN_LOCK:
        until = _HOST_DOWN_UNTIL.get(host, 0.0)
        if until and time.time() >= until:
            _HOST_DOWN_UNTIL.pop(host, None)
            _HOST_DOWN_STRIKES.pop(host, None)
            return False
        return bool(until)


def _note_host_unreachable(host: str) -> None:
    with _HOST_DOWN_LOCK:
        strikes = _HOST_DOWN_STRIKES.get(host, 0) + 1
        _HOST_DOWN_STRIKES[host] = strikes
        if strikes >= max(1, HOST_DOWN_STRIKES):
            _HOST_DOWN_UNTIL[host] = time.time() + HOST_DOWN_COOLDOWN
            _HOST_DOWN_STRIKES[host] = 0
            print(
                f"    [!] {host} unreachable through every route; skipping it for "
                f"{int(HOST_DOWN_COOLDOWN / 60)} min (items stay queued for retry)."
            )


def _note_host_reachable(host: str) -> None:
    with _HOST_DOWN_LOCK:
        _HOST_DOWN_STRIKES.pop(host, None)


def hosts_down() -> Dict[str, float]:
    """Diagnostics: host -> seconds left in its unreachable cooldown."""
    now = time.time()
    return {h: round(u - now, 1) for h, u in list(_HOST_DOWN_UNTIL.items()) if u > now}

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# ─── Origin-error pages ─────────────────────────────────────────────────────
# A dead *origin* (not us) still answers HTTP 200: borntobefuck.com returned
# `Internal server error.` with 22 bytes and a `status: 500 Internal Server
# Error` header for hours. Every parser downstream read that as "the listing is
# empty", so zones were pinned as COMPLETE and 5,328 live ids were recorded as
# dead (run 46093407022). An error page is never content and never absence.
_ORIGIN_ERROR_HEADERS = ("status", "x-origin-status", "cf-origin-status")
_ORIGIN_ERROR_MARKERS = (
    "internal server error",
    "500 internal server error",
    "502 bad gateway",
    "503 service temporarily unavailable",
    "503 service unavailable",
    "504 gateway time-out",
    "504 gateway timeout",
    "error establishing a database connection",
    "there has been a critical error on this website",
    "database error",
)
# A real page is bigger than this; an origin error page never is. Marker checks
# are gated on it so a normal article mentioning "database error" is safe.
_ORIGIN_ERROR_MAX_BODY = 4096

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
_CF_FAILED_SOLVES: Dict[str, int] = {}            # host -> consecutive failed solves
_CF_SESSIONS: Dict[str, Dict[str, Any]] = {}     # host -> {cookies, user_agent}
_CF_SESSIONS_LOADED: set[str] = set()


@dataclass
class Page:
    """Minimal response object that satisfies every existing source.

    Sources read ``.status`` and ``.html_content`` (or ``.text``); both work.
    A few sources (the OnlyFans aggregators) were written against Scrapling's
    selector API and call ``.css(selector)`` on the response, which this class
    did not provide: every one of their fetches died with
    ``'Page' object has no attribute 'css'``. ``css()`` here is a thin lxml
    wrapper returning nodes with the same ``attrib`` / ``text`` (``.clean()``)
    / nested ``css()`` surface.
    """

    url: str
    status: int
    body: bytes = b""
    encoding: str = "utf-8"
    headers: Dict[str, str] = field(default_factory=dict)
    from_cache: bool = False
    via: str = "fetcher"      # fetcher | stealthy | proxy | cache | dead-cache
    challenge: bool = False
    #: The origin (or its edge) answered with an error page instead of content.
    #: Never content, and never proof that a URL is gone.
    error: bool = False
    _root: Any = field(default=None, init=False, repr=False, compare=False)

    def _dom(self) -> Any:
        if self._root is None:
            from lxml import html as _html

            self._root = _html.fromstring(self.html_content)
        return self._root

    def css(self, selector: str) -> List[Any]:
        """Scrapling-compatible CSS selection over this page's HTML."""
        if not selector:
            return []
        try:
            return [_Node(el) for el in self._dom().cssselect(selector)]
        except Exception:
            return []

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
        return self.status == 200 and not self.error

    def __repr__(self) -> str:
        src = " [cached]" if self.from_cache else ""
        cf = " [cloudflare]" if self.challenge else ""
        err = " [origin-error]" if self.error else ""
        return f"<Page {self.status} {self.url[:70]}{src}{cf}{err}>"


# ─── Cloudflare detection ────────────────────────────────────────────────────

class _Text(str):
    """str that also answers Scrapling's ``.clean()`` (whitespace collapse)."""

    def clean(self) -> str:
        return " ".join(self.split())


class _Node:
    """Minimal selector node returned by :meth:`Page.css`."""

    __slots__ = ("_el",)

    def __init__(self, el: Any) -> None:
        self._el = el

    @property
    def attrib(self) -> Dict[str, str]:
        return dict(self._el.attrib or {})

    @property
    def text(self) -> _Text:
        return _Text(" ".join(part.strip() for part in self._el.itertext() if part.strip()))

    def get(self, key: str, default: Any = None) -> Any:
        return self._el.get(key, default)

    def css(self, selector: str) -> List["_Node"]:
        try:
            return [_Node(el) for el in self._el.cssselect(selector)]
        except Exception:
            return []

    def __repr__(self) -> str:
        return f"<_Node {self._el.tag} {dict(self._el.attrib or {})}>"

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


def _at_least(value: int, floor: int) -> int:
    """At least ``floor`` (a tunable of 0 would otherwise disable a breaker)."""
    try:
        return max(int(floor), int(value))
    except (TypeError, ValueError):
        return int(floor)


def is_origin_error_page(
    status: int, body: bytes = b"", headers: Optional[Dict[str, str]] = None
) -> bool:
    """True when the *origin* answered with an error page instead of content.

    This is the failure mode that silently corrupted the fleet: borntobefuck.com
    returned HTTP **200** carrying 22 bytes of ``Internal server error.`` for
    hours, so every parser saw "zero cards" and concluded the end of the
    catalogue (zones pinned COMPLETE, 5,328 live ids recorded dead).

    Detection is deliberately narrow so a healthy page is never misread:

    - the edge's echoed origin status (``status: 500 Internal Server Error`` on
      a 200 response, which Cloudflare sends),
    - a known error phrase **inside a body too small to be a real page**.
    """
    if status in (404, 410):
        return False                      # "gone" is a real answer, not an error
    hdrs = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
    for key in _ORIGIN_ERROR_HEADERS:
        echoed = hdrs.get(key, "").strip().lower()
        if not echoed:
            continue
        try:
            echoed_status = int(echoed.split()[0])
        except (ValueError, IndexError):
            continue
        if echoed_status >= 500:
            return True
    raw = body or b""
    if len(raw) > _ORIGIN_ERROR_MAX_BODY:
        return False
    text = raw.decode("utf-8", errors="ignore").lower()
    if not text.strip():
        return False
    return any(marker in text for marker in _ORIGIN_ERROR_MARKERS)


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
    if page.status == 200 and (page.challenge or page.error):
        return                      # never cache an interstitial or origin error
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


# ─── Block detection & per-host egress memory ───────────────────────────────

# Media URLs never benefit from a browser solve: a 403 on an HLS manifest or an
# mp4 segment is answered by the CDN (Referer/token), and "downloading" it via
# a headless browser is pointless. The fleet used to launch Chromium for these
# and log "Scrapling solve failed ... Executable doesn't exist" on every run.
_MEDIA_SUFFIXES = (
    ".m3u8", ".mpd", ".mp4", ".m4s", ".ts", ".webm", ".mkv",
    ".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif",
)


def _is_media_url(url: str) -> bool:
    path = (urlparse(url).path or "").lower()
    return path.endswith(_MEDIA_SUFFIXES)


def is_block_status(status: int) -> bool:
    """True for statuses that describe *us* being rejected, not the URL."""
    return int(status or 0) in BLOCK_STATUSES


def _mark_host_blocked(host: str) -> None:
    with _BLOCKED_LOCK:
        if host not in _BLOCKED_HOSTS:
            print(
                f"    [!] {host} rejects this runner's egress; routing its requests "
                f"through a proxy for the next {int(BLOCKED_HOST_TTL / 60)} min."
            )
        _BLOCKED_HOSTS[host] = time.time()


def _host_is_blocked(host: str) -> bool:
    ts = _BLOCKED_HOSTS.get(host)
    if not ts:
        return False
    if time.time() - ts > BLOCKED_HOST_TTL:
        _BLOCKED_HOSTS.pop(host, None)
        return False
    return True


def blocked_hosts() -> Dict[str, float]:
    """Diagnostics: host -> age in seconds of the last egress block."""
    now = time.time()
    return {h: round(now - ts, 1) for h, ts in list(_BLOCKED_HOSTS.items())}


def forget_blocked_host(host: str) -> None:
    _BLOCKED_HOSTS.pop(host, None)


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
    return _stealth_fetch(url, host, proxy=None, solve_cloudflare=True, timeout_ms=timeout_ms)


def _stealth_fetch(
    url: str,
    host: str,
    proxy: Optional[str] = None,
    solve_cloudflare: bool = True,
    timeout_ms: int = 45000,
) -> Optional[Page]:
    """Fetch ``url`` with Scrapling's StealthyFetcher, optionally via a proxy.

    A direct call solves the Cloudflare challenge; a proxied call is the last
    resort for origins that hard-block datacenter IPs (madouhd 466).
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
            print(f"    [!] Challenge on {host} but Scrapling is unavailable: {exc}")
            return None

        where = f" via proxy {proxy.split('@')[-1]}" if proxy else ""
        print(f"    [*] Blocked on {host}{where}; solving via Scrapling StealthyFetcher...")
        _CF_SOLVES[host] = _CF_SOLVES.get(host, 0) + 1
        kwargs: Dict[str, Any] = {
            "solve_cloudflare": bool(solve_cloudflare),
            "headless": True,
            "network_idle": True,
            "timeout": timeout_ms,
        }
        if proxy:
            kwargs["proxy"] = proxy
        try:
            resp = StealthyFetcher.fetch(url, **kwargs)
        except Exception as exc:
            print(f"    [!] Scrapling fetch failed for {host}: {exc}")
            return None

        status = getattr(resp, "status", 0) or 0
        if status == 429:
            _CF_COOLDOWN_UNTIL[host] = time.time() + CF_RATE_LIMIT_COOLDOWN
            print(f"    [!] {host} rate-limited during solve; cooling down {int(CF_RATE_LIMIT_COOLDOWN)}s.")
            return None

        body = bytes(getattr(resp, "body", b"") or b"")
        headers = dict(getattr(resp, "headers", {}) or {})
        if status != 200 or is_cf_challenge(status, body, headers):
            failed = _CF_FAILED_SOLVES.get(host, 0) + 1
            _CF_FAILED_SOLVES[host] = failed
            print(f"    [!] Still blocked on {host} after stealth fetch (HTTP {status}).")
            if failed >= _at_least(CF_MAX_FAILED_SOLVES, 2):
                # hotscopes.net answers 403 to this runner even through a solved
                # browser. Retrying it every cycle wasted ~7 minutes per host per
                # session; after two hopeless solves we stop launching browsers
                # for it and let the proxy tier (which fails fast) answer.
                _CF_COOLDOWN_UNTIL[host] = time.time() + CF_HOPELESS_COOLDOWN
                print(
                    f"    [!] {host} is not solvable from this runner; no more browser "
                    f"solves for {int(CF_HOPELESS_COOLDOWN / 60)} min."
                )
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
        _CF_FAILED_SOLVES[host] = 0
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
    proxy: Optional[str] = None,
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
        # EVERY request gets exactly one attempt. Scrapling's Fetcher retries
        # internally with a 30s timeout per attempt, so a direct request to an
        # origin that times out cost 3 x 30s *inside the fetcher* and then
        # net.fetch retried the whole thing twice more: the Chinese feed spent
        # hours on dead URLs while parsing nothing. Retry policy lives here.
        "retries": 1,
    }
    if session.get("cookies"):
        kwargs["cookies"] = session["cookies"]
    if proxy:
        kwargs["proxy"] = proxy
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
    if not page.challenge:
        page.error = is_origin_error_page(status, body, resp_headers)
    return page


def _proxy_pool():
    """The self-managing proxy fleet (lazy import: it pulls in curl_cffi)."""
    try:
        from scraper.proxy_pool import GLOBAL_PROXY_POOL

        return GLOBAL_PROXY_POOL
    except Exception:
        return None


def _proxy_get(
    url: str,
    timeout: float,
    headers: Optional[Dict[str, str]],
    referer: Optional[str],
    host: str,
) -> Optional[Page]:
    """One request through the rotating proxy fleet for a blocked host.

    Several different proxies are tried: free proxies die constantly, and a
    single dead one must not cost the caller an entire zone. Success/failure is fed
    back into the pool so the next request starts from a better place.
    """
    if not PROXY_FALLBACK:
        return None
    pool = _proxy_pool()
    if pool is None:
        return None

    # Ask the manager for coverage of this origin if it has none: free-proxy
    # harvesting is the only route available, and it takes ~90 s in the
    # background, so start it before the retries rather than after them.
    try:
        pool.ensure_for_host(url)
    except Exception:
        pass

    tried: set = set()
    for attempt in range(PROXY_ATTEMPTS_PER_FETCH):
        try:
            proxy = pool.get_proxy(target_url=url, rotate=attempt > 0, exclude=tried)
        except TypeError:
            proxy = pool.get_proxy(target_url=url, rotate=attempt > 0)
        except Exception as exc:
            print(f"    [!] Proxy pool unavailable for {host}: {exc}")
            return None
        if not proxy:
            print(f"    [!] No working proxy for {host} "
                  f"({len(tried)} tried); harvesting for it in the background.")
            return None
        tried.add(proxy)

        started = time.time()
        # PROXY_TIMEOUT, not max(timeout, PROXY_TIMEOUT): a blocked host must be
        # *detected* quickly. With 30 s per proxy attempt and two attempts, one
        # fetch cost a minute and the breaker took 4.5 minutes to trip (run
        # 46122179927 spent 8.5 of its 4-minute slice on this).
        page = _plain_get(url, PROXY_TIMEOUT, headers, referer, proxy=proxy)
        if page is not None and page.status == 200 and not page.challenge:
            page.via = "proxy"
            try:
                pool.report_success(proxy, url, int((time.time() - started) * 1000))
            except Exception:
                pass
            if _host_is_blocked(host):
                # Proxy traffic works for this host: keep it proxy-first instead
                # of re-failing directly every time the block flag expires.
                _BLOCKED_HOSTS[host] = time.time()
            _note_host_reachable(host)
            return page

        status = page.status if page is not None else "no response"
        print(f"    [-] Proxy {proxy.split('@')[-1]} failed for {host} ({status}); rotating.")
        try:
            pool.report_failure(proxy, url)
        except Exception:
            pass
        # A dead *proxy* is not evidence that the origin is unreachable:
        # counting these as unreachable tripped the host breaker from free-proxy
        # noise and skipped whole feeds ("unreachable through every route" while
        # two uxj proxies were still in the pool).
    print(f"    [-] All {len(tried)} proxies failed for {host}; asking for more.")
    try:
        pool.ensure_for_host(url)
    except Exception:
        pass
    return None


def _escalate(
    url: str,
    host: str,
    page: Optional[Page],
    timeout: float,
    headers: Optional[Dict[str, str]],
    referer: Optional[str],
    allow_cache: bool,
) -> Optional[Page]:
    """Last resort for a blocked answer: stealth browser, then proxy, then both.

    This is the path that rescues an origin blocking the runner's datacenter IP
    (madouhd 466) or serving a challenge the plain TLS fingerprint cannot pass
    (fangsung). Every step is best-effort and failures fall through.
    """
    # 1. Stealth browser directly (Scrapling is the Cloudflare solver).
    if _is_media_url(url):
        # Nothing a browser can do for a manifest/image; skip straight to proxy.
        solved = None
    else:
        solved = _solve_cloudflare(url, host)
    if solved is not None:
        if allow_cache:
            _cache_put(url, solved, solved.body)
        print(f"    [✓] {host} unblocked with Scrapling stealth browser.")
        return solved

    # 2. Plain request through a validated proxy.
    proxied = _proxy_get(url, timeout, headers, referer, host)
    if proxied is not None:
        if allow_cache:
            _cache_put(url, proxied, proxied.body)
        print(f"    [✓] {host} unblocked via proxy.")
        return proxied

    # 3. Stealth browser through a proxy (hard IP + challenge together).
    if PROXY_FALLBACK and not _is_media_url(url):
        pool = _proxy_pool()
        proxy = None
        if pool is not None:
            try:
                proxy = pool.get_proxy(target_url=url, rotate=True)
            except Exception:
                proxy = None
        if proxy:
            stealthed = _stealth_fetch(url, host, proxy=proxy)
            if stealthed is not None:
                try:
                    pool.report_success(proxy, url)
                except Exception:
                    pass
                if allow_cache:
                    _cache_put(url, stealthed, stealthed.body)
                print(f"    [✓] {host} unblocked via proxy + stealth browser.")
                return stealthed
            try:
                pool.report_failure(proxy, url)
            except Exception:
                pass

    _mark_host_blocked(host)
    return None


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
    accept_challenge_fallback: bool = True,
    min_interval: Optional[float] = None,
    max_backoff: float = 8.0,
) -> Optional[Page]:
    """Fetch ``url`` with caching, Cloudflare handling and politeness.

    Returns a :class:`Page` for any HTTP answer that reached the origin
    (including 404/410, which callers may interpret as "end of listing"), or
    ``None`` when no answer could be obtained at all (network error,
    unresolvable block, or exhausted retries).

    A block is *never* returned as content. Cloudflare interstitials, WAF
    statuses (403/429/451/466/...) and a host already known to reject this
    runner's egress are escalated: Scrapling stealth browser first, then a
    validated proxy, then both together. Only when every route fails does the
    answer come back as a non-200, empty ``Page`` so no parser mistakes it for
    an empty listing.
    """
    if not url:
        return None

    ttl = DEFAULT_TTL if cache_ttl is None else max(0, int(cache_ttl))
    host = urlparse(url).netloc

    if allow_cache:
        cached = _cache_get(url, ttl, allow_dead=allow_dead_cache)
        if cached is not None:
            return cached

    if _host_is_down(host):
        # Cool-down: no direct attempt. But a host that refused our egress is
        # exactly the case the free-proxy pool exists for, so still offer the
        # queue a proxy route before declaring the URL unobtainable.
        if accept_challenge_fallback:
            proxied = _proxy_get(url, timeout, headers, referer, host)
            if proxied is not None:
                if allow_cache:
                    _cache_put(url, proxied, proxied.body)
                return proxied
        return None

    attempts = max(1, int(retries))
    last_page: Optional[Page] = None

    # A host that already proved it blocks our egress: don't waste three
    # doomed direct attempts per URL, go through the proxy first.
    if accept_challenge_fallback and _host_is_blocked(host):
        proxied = _proxy_get(url, timeout, headers, referer, host)
        if proxied is not None:
            if allow_cache:
                _cache_put(url, proxied, proxied.body)
            return proxied

    for attempt in range(attempts):
        if _host_is_down(host):
            return None
        _throttle(host, min_interval)
        page = _plain_get(url, timeout, headers, referer)
        last_page = page

        if page is None:                              # transport failure
            _note_host_unreachable(host)
            if _host_is_down(host) or attempt == attempts - 1:
                # Every route out of this runner failed (SSL reset, timeout).
                # Try the proxy pool *before* concluding the URL is lost: the
                # breaker used to short-circuit here, so a host that fails
                # fast never reached the pool at all (uxj.838238.xyz).
                if accept_challenge_fallback:
                    proxied = _proxy_get(url, timeout, headers, referer, host)
                    if proxied is not None:
                        _mark_host_blocked(host)
                        _note_host_reachable(host)
                        if allow_cache:
                            _cache_put(url, proxied, proxied.body)
                        return proxied
                return None
            time.sleep(min(1.5 * (attempt + 1), 6.0))
            continue
            return None

        # Dead URL: remember it so the next caller pays zero requests.
        if page.status in (404, 410):
            _cache_put(url, page, b"")
            return page

        # An origin error page (HTTP 200 + "Internal server error.") is neither
        # content nor evidence that the URL is gone. Retry once, then hand back
        # an unusable page so no parser can read it as an empty catalogue.
        if page.error:
            if attempt < attempts - 1:
                time.sleep(min(1.5 * (attempt + 1), 6.0))
                continue
            return Page(
                url=url,
                status=page.status,
                body=b"",
                encoding=page.encoding,
                headers=page.headers,
                error=True,
                via=page.via,
            )

        if page.challenge or page.status in (403, 503):
            # A media 403 is a CDN rule (Referer/token/expiry), never a
            # Cloudflare interstitial: do not spend a browser launch on it.
            solved = (
                _solve_cloudflare(url, host)
                if accept_challenge_fallback and not _is_media_url(url)
                else None
            )
            if solved is not None:
                _cache_put(url, solved, solved.body)
                return solved
            # Cookies may have just been refreshed by another worker: retry.
            if attempt < attempts - 1:
                time.sleep(min(1.5 * (attempt + 1), 6.0))
                continue
            if accept_challenge_fallback:
                escalated = _escalate(url, host, page, timeout, headers, referer, allow_cache)
                if escalated is not None:
                    return escalated
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
            _note_host_reachable(host)
            if _host_is_blocked(host):
                forget_blocked_host(host)
            if allow_cache:
                _cache_put(url, page, page.body)
            return page
        if page.status in (429, 500, 502, 504) and attempt < attempts - 1:
            time.sleep(min(2.0 * (attempt + 1), max_backoff))
            continue

        # Any other WAF rejection (466/451/406/...): not content, not gone.
        if accept_challenge_fallback and is_block_status(page.status):
            escalated = _escalate(url, host, page, timeout, headers, referer, allow_cache)
            if escalated is not None:
                return escalated
            return Page(
                url=url,
                status=page.status,
                body=b"",
                encoding=page.encoding,
                headers=page.headers,
                challenge=True,
                via=page.via,
            )

        return page

    return last_page
