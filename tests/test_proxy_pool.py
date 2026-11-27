"""Tests for the proxy pool's per-target escalation behaviour.

Incident context: madouhd.cc answers HTTP 466 to GitHub runner IPs for every
URL. The fetch layer now routes those through a proxy, which is only useful if
a) candidates are validated against the host that is actually blocked and
b) a host that no public proxy can reach stops being re-harvested forever.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper.proxy_pool import AutoProxyPool  # noqa: E402


def test_env_proxy_wins(monkeypatch):
    monkeypatch.setenv("SCRAPER_PROXY_URL", "http://user:pass@residential:8080")
    pool = AutoProxyPool()
    assert pool.get_proxy("https://madouhd.cc/category/cate3/") == "http://user:pass@residential:8080"


def test_candidates_are_validated_against_the_blocked_host(monkeypatch):
    pool = AutoProxyPool()
    seen = {}

    def fake_refresh(max_probes=150, workers=35, target_url=None):
        seen["target"] = target_url
        pool.working_proxies = ["http://ok:1"]
        pool._target_working[target_url] = ["http://ok:1"]
        return 1

    monkeypatch.setattr(pool, "refresh_pool", fake_refresh)
    proxy = pool.get_proxy("https://madouhd.cc/category/cate3/")
    assert seen["target"] == "https://madouhd.cc/category/cate3/"
    assert proxy == "http://ok:1"


def test_refresh_is_capped_per_target(monkeypatch):
    """A host no proxy can reach must not trigger a harvest on every URL."""
    pool = AutoProxyPool()
    calls = []

    def fake_refresh(max_probes=150, workers=35, target_url=None):
        calls.append(target_url)
        pool._target_refreshed[target_url] = pool.last_refresh_at
        return 0

    monkeypatch.setattr(pool, "refresh_pool", fake_refresh)
    pool.refresh_cooldown = 0.0

    for _ in range(10):
        pool.get_proxy("https://blocked.test/x/")

    assert len(calls) == pool.max_attempts_per_target, f"harvested {len(calls)} times"


def test_probe_rejects_block_status_and_challenges(monkeypatch):
    """A 466 or a Cloudflare body from the proxy must not count as working."""
    pool = AutoProxyPool()

    class Resp:
        def __init__(self, status, text):
            self.status_code = status
            self.text = text

    monkeypatch.setattr(
        "scraper.proxy_pool.requests.get",
        lambda *a, **k: Resp(466, "denied"),
    )
    assert pool._probe_proxy("http://p:1", "https://madouhd.cc/category/cate3/") is None

    monkeypatch.setattr(
        "scraper.proxy_pool.requests.get",
        lambda *a, **k: Resp(200, "<html>Just a moment...</html>" + "x" * 5000),
    )
    assert pool._probe_proxy("http://p:1", "https://madouhd.cc/category/cate3/") is None

    monkeypatch.setattr(
        "scraper.proxy_pool.requests.get",
        lambda *a, **k: Resp(200, "<html>" + "y" * 9000 + "</html>"),
    )
    assert pool._probe_proxy("http://p:1", "https://madouhd.cc/category/cate3/") == "http://p:1"
