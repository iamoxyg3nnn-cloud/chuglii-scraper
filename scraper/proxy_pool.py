"""
Automated Live Free Proxy Harvester & Cloudflare Validator
Scrapes, validates, and rotates live public proxies on-the-fly to bypass datacenter IP restrictions.
"""

import os
import time
import json
import random
import urllib.request
import concurrent.futures
from typing import List, Optional, Set
from curl_cffi import requests

PROXY_SOURCES = [
    "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&protocol=http&proxy_format=ipport&format=text",
    "https://proxylist.geonode.com/api/proxy-list?limit=100&page=1&sort_by=lastChecked&sort_type=desc",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt"
]

TEST_TARGET_URL = "https://supjav.com/"

class AutoProxyPool:
    def __init__(self, target_url: str = TEST_TARGET_URL, min_working: int = 2):
        self.target_url = target_url
        self.min_working = min_working
        self.working_proxies: List[str] = []
        self.failed_proxies: Set[str] = set()
        self.last_refresh_at: float = 0.0
        self.refresh_cooldown: float = 120.0
        # A proxy that clears one origin may be useless (or blocked) for the
        # next one: validation results are remembered per target host.
        self._target_refreshed: dict = {}
        self._target_working: dict = {}

    def harvest_candidates(self, max_candidates: int = 300) -> List[str]:
        """Harvests fresh proxy candidates from multiple public APIs."""
        if len(self.failed_proxies) > 600:
            self.failed_proxies.clear()

        candidates = set()
        print("[*] Harvesting public proxy lists...")

        for src in PROXY_SOURCES:
            if len(candidates) >= max_candidates:
                break
            try:
                req = urllib.request.Request(src, headers={"User-Agent": "curl/7.88.1"})
                with urllib.request.urlopen(req, timeout=5) as resp:
                    raw = resp.read().decode("utf-8", errors="ignore")
                    if "geonode" in src:
                        data = json.loads(raw)
                        for item in data.get("data", []):
                            ip = item.get("ip")
                            port = item.get("port")
                            if ip and port:
                                candidates.add(f"http://{ip}:{port}")
                    else:
                        for line in raw.splitlines():
                            line = line.strip()
                            if line and ":" in line and not line.startswith("#"):
                                candidates.add(f"http://{line}")
                print(f"    Source fetched ({len(candidates)} total candidates gathered)")
            except Exception as e:
                pass

        return list(candidates)

    def _probe_proxy(self, proxy_url: str, target_url: Optional[str] = None) -> Optional[str]:
        """Tests a proxy directly against the target site being scraped.

        A proxy is only accepted when it returns real content for *that* host:
        200, a body large enough to be a page, and no Cloudflare/WAF
        interstitial or block status (madouhd.cc answers non-datacenter IPs
        with 200 and datacenter IPs with 466).
        """
        target = target_url or self.target_url
        try:
            r = requests.get(
                target,
                impersonate="chrome124",
                proxies={"http": proxy_url, "https": proxy_url},
                timeout=8
            )
            if r.status_code != 200 or len(r.text) < 3000:
                return None
            head = r.text[:4000].lower()
            if "just a moment..." in head or "cf-chl" in head or "challenge-platform" in head:
                return None
            return proxy_url
        except Exception:
            pass
        return None

    def refresh_pool(self, max_probes: int = 150, workers: int = 35, target_url: Optional[str] = None) -> int:
        """Probes proxy candidates concurrently and populates the verified working pool."""
        target = target_url or self.target_url
        self.last_refresh_at = time.time()
        self._target_refreshed[target] = self.last_refresh_at
        candidates = self.harvest_candidates(max_candidates=max_probes)
        if not candidates:
            print("[!] Could not harvest proxy candidates.")
            return len(self.working_proxies)

        print(f"[*] Validating {len(candidates)} candidate proxies against {target[:70]} ({workers} workers)...")
        found = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(self._probe_proxy, p, target): p for p in candidates if p not in self.failed_proxies}
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if res and res not in self.working_proxies:
                    self.working_proxies.append(res)
                    self._target_working.setdefault(target, []).append(res)
                    found += 1
                    print(f"    [✓] Found working proxy: {res} (Pool: {len(self.working_proxies)})")
                    if len(self.working_proxies) >= self.min_working:
                        # Cancel remaining to save time
                        for f in futures:
                            f.cancel()
                        break

        print(f"[*] Proxy refresh complete. Active verified pool size: {len(self.working_proxies)}")
        return len(self.working_proxies)

    def get_proxy(self, target_url: Optional[str] = None) -> Optional[str]:
        """Gets a working proxy for ``target_url`` (or the configured target).

        An explicit env proxy always wins. Otherwise the pool is refreshed
        against the host that is actually blocked, at most once per
        ``refresh_cooldown`` per host, so a session blocked on 40 zones of the
        same site harvests once instead of 40 times.
        """
        target = target_url or self.target_url
        env_proxy = os.getenv("SCRAPER_PROXY_URL") or os.getenv("PROXY_URL") or os.getenv("RESIDENTIAL_PROXY")
        if env_proxy:
            return env_proxy

        # Prefer proxies already proven against this exact target.
        local = [p for p in self._target_working.get(target, []) if p in self.working_proxies]
        if local:
            return random.choice(local)
        if self.working_proxies and not self._target_refreshed:
            return random.choice(self.working_proxies)

        now = time.time()
        last = max(self._target_refreshed.get(target, 0.0), self.last_refresh_at)
        if now - last >= self.refresh_cooldown:
            self.failed_proxies.clear()
            self.refresh_pool(max_probes=60, workers=20, target_url=target)
            local = [p for p in self._target_working.get(target, []) if p in self.working_proxies]
            if local:
                return random.choice(local)
        if self.working_proxies:
            return random.choice(self.working_proxies)
        return None

    def report_failure(self, proxy_url: str) -> None:
        """Marks a proxy as failed and removes it from the active pool."""
        if not proxy_url:
            return
        if proxy_url in self.failed_proxies:
            # Already evicted; repeated reports only spam the log.
            return
        self.failed_proxies.add(proxy_url)
        if proxy_url in self.working_proxies:
            self.working_proxies.remove(proxy_url)
        for target, proxies in self._target_working.items():
            if proxy_url in proxies:
                proxies.remove(proxy_url)
        print(f"    [-] Proxy failed and removed: {proxy_url} (Remaining: {len(self.working_proxies)})")


# Global singleton instance
GLOBAL_PROXY_POOL = AutoProxyPool()
