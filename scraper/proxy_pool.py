"""
Self-managing free-proxy fleet for the scrapers.

Zero manual effort: nothing here needs to be started, configured or fed by hand.
A daemon thread harvests public proxy lists, validates candidates against the
real origins that block GitHub's datacenter IPs, keeps a live pool, re-checks
it in the background, evicts dead entries and persists the survivors so the
next (ephemeral) Actions runner starts warm instead of from zero.

Why it exists
-------------
Several origins answer 403/451/466 to a runner IP while a normal IP gets 200
(madouhd.cc is the clearest example). ``scraper.net`` routes those requests
through this pool; before, the pool was only harvested on demand, per host,
twice per session, so a blocked site could not be scraped once the two
harvests were spent.

How it works
------------
1. ``ensure_started()`` (called from the daemon and from the first blocked
   fetch) starts one background thread, idempotently.
2. The thread: load persisted state -> top the pool up to ``MIN_POOL`` by
   harvesting + validating -> health-check stale entries -> repeat.
3. A proxy is only admitted when it returns real content for one of the
   configured targets (200, page-sized body, no Cloudflare/WAF interstitial).
   Which target it cleared is remembered per proxy ("host affinity").
4. ``get_proxy(url)`` is non-blocking: host-affine proxy first, then the
   least-recently-used healthy one, else ``None`` (the caller falls back to its
   direct/stealth path and tries again later).
5. ``report_success`` / ``report_failure`` keep scores; a proxy that fails
   twice is evicted immediately.

Everything is bounded: candidate caps, probe budget, TTLs and log noise are
tunable through the environment (see the constants below).
"""

from __future__ import annotations

import json
import os
import random
import re
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from ipaddress import ip_address
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from curl_cffi import requests

# ─── Configuration (env-overridable, sensible defaults) ──────────────────────

# Public lists that keep a machine-readable endpoint (plain text or JSON).
# Any additional URL may be appended via SCRAPER_PROXY_SOURCES.
PROXY_SOURCES: List[str] = [
    # plain text: ip:port per line
    "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&protocol=http&proxy_format=ipport&format=text&timeout=15000",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
    "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt",
    "https://raw.githubusercontent.com/yemixzy/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://openproxylist.xyz/http.txt",
    # JSON
    "https://proxylist.geonode.com/api/proxy-list?limit=200&page=1&sort_by=lastChecked&sort_type=desc&protocols=http,https",
]

# Origins that reject datacenter egress; a proxy that clears one of these is
# worth having. Override with SCRAPER_PROXY_TARGETS (comma separated URLs).
DEFAULT_TARGETS: List[str] = [
    "https://madouhd.cc/category/cate3/",
    "https://www.fangsung.com/",
    "https://uxj.838238.xyz/",
    "https://supjav.com/",
    "https://hotscopes.net/",
]

MIN_POOL = int(os.environ.get("SCRAPER_PROXY_MIN_POOL", "6"))
HARVEST_CANDIDATES = int(os.environ.get("SCRAPER_PROXY_CANDIDATES", "250"))
PROBE_WORKERS = int(os.environ.get("SCRAPER_PROXY_PROBE_WORKERS", "50"))
PROBE_TIMEOUT = float(os.environ.get("SCRAPER_PROXY_PROBE_TIMEOUT", "8"))
PROBE_BUDGET_SEC = float(os.environ.get("SCRAPER_PROXY_PROBE_BUDGET", "75"))
REFRESH_INTERVAL_SEC = float(os.environ.get("SCRAPER_PROXY_REFRESH_SEC", "600"))   # 10 min
HEALTH_INTERVAL_SEC = float(os.environ.get("SCRAPER_PROXY_HEALTH_SEC", "180"))     # 3 min
PROXY_TTL_SEC = float(os.environ.get("SCRAPER_PROXY_TTL_SEC", "1800"))             # 30 min
MAX_EVICT_SCORE = 2
ENABLED = os.environ.get("SCRAPER_PROXY_FALLBACK", "1") not in ("0", "false", "False", "")

STATE_KEY = "proxy_pool_v1"
STATE_PATH = Path(
    os.environ.get("SCRAPER_PROXY_STATE")
    or (Path(__file__).resolve().parent / ".cache" / "proxies" / "state.json")
)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_CF_MARKERS = (
    "just a moment...",
    "challenge-platform",
    "cf-chl",
    "attention required! | cloudflare",
    "checking your browser",
)

_RE_HOST = re.compile(r"^https?://([^/?#]+)/")


def _targets_from_env() -> List[str]:
    raw = os.environ.get("SCRAPER_PROXY_TARGETS", "").strip()
    if not raw:
        return list(DEFAULT_TARGETS)
    return [t.strip() for t in raw.split(",") if t.strip()]


def _sources_from_env() -> List[str]:
    raw = os.environ.get("SCRAPER_PROXY_SOURCES", "").strip()
    extra = [s.strip() for s in raw.split(",") if s.strip()]
    return PROXY_SOURCES + extra


def _normalize_proxy(value: str) -> Optional[str]:
    """'1.2.3.4:8080' / 'http=1.2.3.4:8080' / 'http://1.2.3.4:8080' -> URL."""
    v = (value or "").strip()
    if not v or v.startswith("#"):
        return None
    if "://" in v:
        scheme, rest = v.split("://", 1)
        if scheme.lower() not in ("http", "https"):
            return None            # socks/4/5 are not validated here
        v = rest
    if "=" in v:                   # proxyscrape "http=ip:port"
        v = v.split("=", 1)[1]
    v = v.split("/")[0].split("@")[-1]
    host, _, port = v.partition(":")
    if not host or not port.isdigit():
        return None
    try:
        ip = ip_address(host)
        if ip.is_private or ip.is_loopback or ip.is_multicast or ip.is_reserved:
            return None
    except ValueError:
        return None                # hostnames in free lists are unreliable
    return f"http://{host}:{int(port)}"


class AutoProxyPool:
    """Harvest, validate, rotate and remember free HTTP proxies."""

    def __init__(self, target_url: str = "", min_pool: int = MIN_POOL):
        self.target_url = target_url or DEFAULT_TARGETS[0]
        self.min_pool = min_pool

        # proxy -> {"hosts": {hostname: ts}, "ok": int, "fail": int,
        #           "last_ok": ts, "latency": ms}
        self._entries: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

        self._dirty = False
        self._last_persist = 0.0
        self._thread: Optional[threading.Thread] = None
        self._start_lock = threading.Lock()
        self._stop = threading.Event()
        self._refresh_now = threading.Event()

        self.last_refresh_at: float = 0.0
        self.stats: Dict[str, int] = {
            "harvested": 0,
            "probed": 0,
            "added": 0,
            "evicted": 0,
            "refreshes": 0,
        }

    # ── compatibility shims (older callers/tests) ──────────────────────────
    @property
    def working_proxies(self) -> List[str]:
        with self._lock:
            return [p for p, e in self._entries.items() if e.get("ok", 0) >= 1]

    @property
    def failed_proxies(self) -> Set[str]:
        with self._lock:
            return {p for p, e in self._entries.items() if e.get("fail", 0) >= MAX_EVICT_SCORE}

    # ── lifecycle ─────────────────────────────────────────────────────────

    def ensure_started(self, targets: Optional[Iterable[str]] = None) -> bool:
        """Start the background manager once. Safe to call from anywhere."""
        if not ENABLED:
            return False
        with self._start_lock:
            if self._thread is not None and self._thread.is_alive():
                return True
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                args=(list(targets or _targets_from_env()),),
                name="proxy-pool",
                daemon=True,
            )
            self._thread.start()
            return True

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        self._refresh_now.set()
        t = self._thread
        if t is not None:
            t.join(timeout=timeout)

    def _run(self, targets: List[str]) -> None:
        self._load_state()
        print(f"[proxy] manager started (pool={self.pool_size()}/{self.min_pool}, targets={len(targets)})")
        next_refresh = 0.0
        while not self._stop.is_set():
            try:
                now = time.time()
                if self.pool_size() < self.min_pool or now >= next_refresh or self._refresh_now.is_set():
                    self._refresh_now.clear()
                    self.refresh(targets)
                    next_refresh = time.time() + REFRESH_INTERVAL_SEC
                else:
                    self._health_check(targets)
                self._save_state()
            except Exception as exc:                      # never kill the thread
                print(f"[proxy] manager error: {type(exc).__name__}: {exc}")
            self._stop.wait(HEALTH_INTERVAL_SEC if self.pool_size() >= self.min_pool else 15)

    # ── pool state ────────────────────────────────────────────────────────

    def pool_size(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries.values() if e.get("ok", 0) >= 1)

    def _host_of(self, url: str) -> str:
        m = _RE_HOST.match(url or "")
        return (m.group(1) if m else "").lower()

    @staticmethod
    def _fresh(entry: Dict[str, Any], host: Optional[str] = None) -> bool:
        now = time.time()
        if host:
            ts = (entry.get("hosts") or {}).get(host, 0)
            return bool(ts) and now - ts <= PROXY_TTL_SEC
        return bool(entry.get("last_ok")) and now - entry["last_ok"] <= PROXY_TTL_SEC

    def _pick(self, host: str) -> Optional[str]:
        with self._lock:
            healthy = {p: e for p, e in self._entries.items() if e.get("ok", 0) >= 1}
        if not healthy:
            return None
        affine = [p for p, e in healthy.items() if host and self._fresh(e, host)]
        pool = affine or [p for p, e in healthy.items() if self._fresh(e)]
        if not pool:                                    # everything is stale
            pool = list(healthy)
        # Least recently validated first: spreads load and surfaces dead ones.
        pool.sort(key=lambda p: healthy[p].get("last_ok", 0))
        return pool[0]

    def get_proxy(self, target_url: Optional[str] = None, *, rotate: bool = False) -> Optional[str]:
        """A proxy for ``target_url`` or None. Never blocks on the network.

        An operator-supplied proxy (``SCRAPER_PROXY_URL``) always wins; the
        pool is only used when no such proxy is configured.
        """
        env_proxy = (
            os.getenv("SCRAPER_PROXY_URL")
            or os.getenv("PROXY_URL")
            or os.getenv("RESIDENTIAL_PROXY")
        )
        if env_proxy:
            return env_proxy
        if not ENABLED:
            return None
        self.ensure_started()
        host = self._host_of(target_url or self.target_url)
        chosen = self._pick(host)
        if chosen and rotate:
            with self._lock:
                choices = [p for p, e in self._entries.items() if e.get("ok", 0) >= 1 and self._fresh(e, host)]
            if choices:
                chosen = random.choice(choices)
        return chosen

    def report_success(self, proxy_url: str, target_url: Optional[str] = None, latency_ms: int = 0) -> None:
        if not proxy_url:
            return
        host = self._host_of(target_url or "")
        with self._lock:
            e = self._entries.setdefault(
                _normalize_proxy(proxy_url) or proxy_url,
                {"hosts": {}, "ok": 0, "fail": 0, "last_ok": 0.0, "latency": 0},
            )
            e["ok"] = int(e.get("ok", 0)) + 1
            e["last_ok"] = time.time()
            self._dirty = True
            if latency_ms:
                e["latency"] = int(latency_ms)
            if host:
                e.setdefault("hosts", {})[host] = time.time()

    def report_failure(self, proxy_url: str, target_url: Optional[str] = None) -> None:
        if not proxy_url:
            return
        key = _normalize_proxy(proxy_url) or proxy_url
        host = self._host_of(target_url or "")
        with self._lock:
            e = self._entries.get(key)
            if e is None:
                return
            e["fail"] = int(e.get("fail", 0)) + 1
            if host:
                (e.get("hosts") or {}).pop(host, None)
            if e["fail"] >= MAX_EVICT_SCORE:
                self.stats["evicted"] += 1
                self._dirty = True
                del self._entries[key]
                print(f"[proxy] evicted {key} after {e['fail']} failures (pool={self.pool_size()})")
                self._refresh_now.set()

    # ── harvest / validate ────────────────────────────────────────────────

    def harvest_candidates(self, max_candidates: int = HARVEST_CANDIDATES) -> List[str]:
        """Pull candidates from every configured source (tolerant parsing)."""
        found: Set[str] = set()
        for src in _sources_from_env():
            if len(found) >= max_candidates:
                break
            try:
                req = urllib.request.Request(src, headers={"User-Agent": "curl/7.88.1"})
                with urllib.request.urlopen(req, timeout=8) as resp:
                    raw = resp.read().decode("utf-8", errors="ignore")
            except Exception:
                continue
            before = len(found)
            if raw.lstrip().startswith("{") or raw.lstrip().startswith("["):
                found.update(self._parse_json(raw))
            else:
                for line in raw.splitlines():
                    p = _normalize_proxy(line)
                    if p:
                        found.add(p)
            if len(found) > before:
                print(f"[proxy] source ok: +{len(found) - before} (total {len(found)})")
        self.stats["harvested"] += len(found)
        return list(found)[:max_candidates]

    @staticmethod
    def _parse_json(raw: str) -> Set[str]:
        out: Set[str] = set()
        try:
            data = json.loads(raw)
        except ValueError:
            return out
        rows: Iterable[Any] = []
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            for key in ("data", "proxies", "results", "list", "proxy"):
                if isinstance(data.get(key), list):
                    rows = data[key]
                    break
        for item in rows:
            if isinstance(item, str):
                p = _normalize_proxy(item)
            elif isinstance(item, dict):
                ip = item.get("ip") or item.get("host") or item.get("proxy")
                port = item.get("port")
                p = _normalize_proxy(f"{ip}:{port}") if ip and port else None
                if p is None and isinstance(item.get("proxy"), str):
                    p = _normalize_proxy(item["proxy"])
            else:
                p = None
            if p:
                out.add(p)
        return out

    def _probe_proxy(self, proxy_url: str, target_url: Optional[str] = None) -> Optional[str]:
        """Return the proxy URL when it serves real content for the target.

        A WAF block (403/406/451/466), a Cloudflare interstitial, a tiny body or
        any transport error disqualifies it — an HTML error page from a proxy is
        exactly what used to be mistaken for a working one.
        """
        target = target_url or self.target_url
        started = time.time()
        try:
            r = requests.get(
                target,
                impersonate="chrome124",
                proxies={"http": proxy_url, "https": proxy_url},
                timeout=PROBE_TIMEOUT,
                allow_redirects=True,
            )
        except Exception:
            return None
        if r.status_code != 200 or len(r.text or "") < 3000:
            return None
        head = (r.text or "")[:4000].lower()
        if any(m in head for m in _CF_MARKERS):
            return None
        with self._lock:
            e = self._entries.setdefault(
                proxy_url, {"hosts": {}, "ok": 0, "fail": 0, "last_ok": 0.0, "latency": 0}
            )
            e["hosts"][self._host_of(target)] = time.time()
            e["last_ok"] = time.time()
            e["latency"] = int((time.time() - started) * 1000)
            e["ok"] = max(1, int(e.get("ok", 0)))
        return proxy_url

    def refresh(self, targets: Optional[Iterable[str]] = None, max_probes: int = 0, workers: int = 0) -> int:
        """Harvest + validate until the pool is topped up. Returns pool size."""
        target_list = [t for t in (targets or _targets_from_env()) if t]
        self.last_refresh_at = time.time()
        self.stats["refreshes"] += 1
        candidates = self.harvest_candidates(max_probes or HARVEST_CANDIDATES)
        if not candidates:
            print("[proxy] no candidates harvested this round")
            return self.pool_size()

        with self._lock:
            known = set(self._entries)
        # Re-validate stale entries too, so the pool heals itself.
        todo = [c for c in candidates if c not in known]
        random.shuffle(todo)
        need = max(0, self.min_pool - self.pool_size())
        budget = max(5, min(len(todo), (max_probes or HARVEST_CANDIDATES)))
        todo = todo[: max(budget, need * 20)]

        print(f"[proxy] validating {len(todo)} candidates against {len(target_list)} target(s)...")
        found = 0
        executor = ThreadPoolExecutor(max_workers=workers or PROBE_WORKERS)
        try:
            futures = {}
            for i, cand in enumerate(todo):
                # Spread candidates over targets so one host's coverage does not
                # starve the others; the first target is the most blocked one.
                target = target_list[i % len(target_list)]
                futures[executor.submit(self._probe_proxy, cand, target)] = (cand, target)
            deadline = time.time() + PROBE_BUDGET_SEC
            for fut in as_completed(futures, timeout=max(1.0, deadline - time.time())):
                self.stats["probed"] += 1
                ok = fut.result()
                if ok:
                    # Register here too: the probe already does it, but keeping
                    # the bookkeeping in refresh() means a stubbed/alternate
                    # prober cannot leave the pool empty after a successful
                    # validation round.
                    self.report_success(ok, futures[fut][1])
                    found += 1
                    self.stats["added"] += 1
                    if self.pool_size() >= self.min_pool:
                        break
        except Exception as exc:
            if type(exc).__name__ == "TimeoutError":
                print(f"[proxy] probe budget spent ({PROBE_BUDGET_SEC:.0f}s); keeping what we have")
            else:
                print(f"[proxy] refresh error: {type(exc).__name__}: {exc}")
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        size = self.pool_size()
        cover = self._coverage()
        print(
            f"[proxy] pool={size}/{self.min_pool} (+{found} new) | "
            f"probed={self.stats['probed']} | coverage={cover or '-'}"
        )
        return size

    # Kept for compatibility with older callers.
    def refresh_pool(self, max_probes: int = 150, workers: int = 35, target_url: Optional[str] = None) -> int:
        targets = [target_url] if target_url else None
        return self.refresh(targets=targets, max_probes=max_probes, workers=workers)

    def _health_check(self, targets: List[str]) -> None:
        """Re-probe stale entries so the pool never fills up with dead proxies."""
        with self._lock:
            stale = [
                (p, e) for p, e in self._entries.items()
                if not self._fresh(e) and e.get("ok", 0) >= 1
            ]
        if not stale:
            return
        for proxy, entry in stale[:6]:
            host = next(iter((entry.get("hosts") or {})), "")
            target = next((t for t in targets if self._host_of(t) == host), targets[0] if targets else self.target_url)
            ok = self._probe_proxy(proxy, target)
            if not ok:
                with self._lock:
                    self._entries.pop(proxy, None)
                self.stats["evicted"] += 1
                print(f"[proxy] stale proxy dropped: {proxy} (pool={self.pool_size()})")

    def _coverage(self) -> str:
        with self._lock:
            hosts: Dict[str, int] = {}
            for e in self._entries.values():
                for h in (e.get("hosts") or {}):
                    hosts[h] = hosts.get(h, 0) + 1
        return ", ".join(f"{h}:{n}" for h, n in sorted(hosts.items())[:4])

    # ── persistence (disk + Supabase, best effort) ────────────────────────

    def _save_state(self) -> None:
        with self._lock:
            payload = {
                "saved_at": time.time(),
                "stats": dict(self.stats),
                "proxies": {
                    p: {
                        "hosts": (e.get("hosts") or {}),
                        "ok": int(e.get("ok", 0)),
                        "fail": int(e.get("fail", 0)),
                        "last_ok": e.get("last_ok", 0),
                        "latency": e.get("latency", 0),
                    }
                    for p, e in self._entries.items()
                    if e.get("ok", 0) >= 1
                },
            }
        try:
            STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = STATE_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload), encoding="utf-8")
            os.replace(tmp, STATE_PATH)
        except OSError:
            pass
        # Mirror to Supabase at most every 5 minutes and only after a change:
        # the pool is otherwise re-saved on every 3-minute health cycle.
        now = time.time()
        if not self._dirty or now - self._last_persist < 300:
            return
        try:
            from scraper.db import set_scraper_checkpoint

            if payload["proxies"]:
                set_scraper_checkpoint(STATE_KEY, payload)
                self._dirty = False
                self._last_persist = now
        except Exception:
            pass

    def _load_state(self) -> None:
        payload: Dict[str, Any] = {}
        try:
            if STATE_PATH.exists():
                payload = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            payload = {}
        if not payload.get("proxies"):
            try:
                from scraper.db import get_scraper_checkpoint

                remote = get_scraper_checkpoint(STATE_KEY)
                if isinstance(remote, dict) and remote.get("proxies"):
                    payload = remote
            except Exception:
                pass
        now = time.time()
        restored = 0
        with self._lock:
            for p, e in (payload.get("proxies") or {}).items():
                normalised = _normalize_proxy(p)
                if not normalised:
                    continue
                last_ok = float(e.get("last_ok") or 0)
                if now - last_ok > PROXY_TTL_SEC:
                    continue                     # too old to trust
                self._entries[normalised] = {
                    "hosts": {str(h): float(ts) for h, ts in (e.get("hosts") or {}).items()},
                    "ok": int(e.get("ok") or 1),
                    "fail": 0,
                    "last_ok": last_ok,
                    "latency": int(e.get("latency") or 0),
                }
                restored += 1
        if restored:
            print(f"[proxy] restored {restored} proxy(ies) from a previous session")


# Global singleton used by scraper.net
GLOBAL_PROXY_POOL = AutoProxyPool()


def ensure_started() -> bool:
    """Start the background proxy manager (idempotent, never raises)."""
    try:
        return GLOBAL_PROXY_POOL.ensure_started()
    except Exception:
        return False


def status() -> Dict[str, Any]:
    """Small dict for cycle logs / troubleshooting."""
    pool = GLOBAL_PROXY_POOL
    return {
        "enabled": ENABLED,
        "pool": pool.pool_size(),
        "min_pool": pool.min_pool,
        "coverage": pool._coverage(),
        "stats": dict(pool.stats),
    }
