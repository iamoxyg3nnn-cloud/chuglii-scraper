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
    def __init__(self, target_url: str = TEST_TARGET_URL, min_working: int = 3):
        self.target_url = target_url
        self.min_working = min_working
        self.working_proxies: List[str] = []
        self.failed_proxies: Set[str] = set()

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

    def _probe_proxy(self, proxy_url: str) -> Optional[str]:
        """Tests a proxy directly against the Cloudflare-protected target site."""
        try:
            r = requests.get(
                self.target_url,
                impersonate="chrome124",
                proxies={"http": proxy_url, "https": proxy_url},
                timeout=6
            )
            # Must return 200, valid HTML, and not a Cloudflare challenge
            if r.status_code == 200 and len(r.text) > 8000 and "Just a moment..." not in r.text and "cf-chl" not in r.text:
                return proxy_url
        except Exception:
            pass
        return None

    def refresh_pool(self, max_probes: int = 150, workers: int = 35) -> int:
        """Probes proxy candidates concurrently and populates the verified working pool."""
        candidates = self.harvest_candidates(max_candidates=max_probes)
        if not candidates:
            print("[!] Could not harvest proxy candidates.")
            return len(self.working_proxies)

        print(f"[*] Validating {len(candidates)} candidate proxies against Cloudflare ({workers} workers)...")
        found = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(self._probe_proxy, p): p for p in candidates if p not in self.failed_proxies}
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if res and res not in self.working_proxies:
                    self.working_proxies.append(res)
                    found += 1
                    print(f"    [✓] Found working Cloudflare-bypassing proxy: {res} (Pool: {len(self.working_proxies)})")
                    if len(self.working_proxies) >= self.min_working:
                        # Cancel remaining to save time
                        for f in futures:
                            f.cancel()
                        break

        print(f"[*] Proxy refresh complete. Active verified pool size: {len(self.working_proxies)}")
        return len(self.working_proxies)

    def get_proxy(self) -> Optional[str]:
        """Gets a working proxy, prioritizing environment variable if provided."""
        env_proxy = os.getenv("PROXY_URL") or os.getenv("RESIDENTIAL_PROXY") or os.getenv("HTTPS_PROXY")
        if env_proxy:
            return env_proxy
        if self.working_proxies:
            return random.choice(self.working_proxies)
        # Limit candidate probes to prevent stalling if free proxies are exhausted
        if not self.working_proxies and len(self.failed_proxies) < 15:
            self.refresh_pool(max_probes=40, workers=20)
        if self.working_proxies:
            return random.choice(self.working_proxies)
        return None

    def report_failure(self, proxy_url: str) -> None:
        """Marks a proxy as failed and removes it from the active pool."""
        self.failed_proxies.add(proxy_url)
        if proxy_url in self.working_proxies:
            self.working_proxies.remove(proxy_url)
        print(f"    [-] Proxy failed and removed: {proxy_url} (Remaining: {len(self.working_proxies)})")


# Global singleton instance
GLOBAL_PROXY_POOL = AutoProxyPool()
