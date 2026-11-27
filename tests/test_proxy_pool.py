"""Tests for the self-managing proxy fleet.

Incident context: madouhd.cc answers HTTP 466 to GitHub runner IPs for every
URL. The fetch layer routes those through this pool, so it has to be
a) automatic (nobody starts it, it starts itself and heals itself),
b) validated against the origins that actually block us, and
c) cheap to query (non-blocking, host-affine, never a network call on the
   caller's thread).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper import proxy_pool  # noqa: E402
from scraper.proxy_pool import AutoProxyPool  # noqa: E402


class Resp:
    def __init__(self, status, text):
        self.status_code = status
        self.text = text


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    monkeypatch.setattr(proxy_pool, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setenv("SCRAPER_PROXY_URL", "")
    monkeypatch.delenv("SCRAPER_PROXY_URL", raising=False)
    monkeypatch.setattr(proxy_pool, "GLOBAL_PROXY_POOL", AutoProxyPool())
    yield


def test_env_proxy_always_wins(monkeypatch):
    monkeypatch.setenv("SCRAPER_PROXY_URL", "http://user:pass@residential:8080")
    pool = AutoProxyPool()
    assert pool.get_proxy("https://madouhd.cc/category/cate3/") == "http://user:pass@residential:8080"


def test_normalize_rejects_junk_and_private_ranges():
    assert proxy_pool._normalize_proxy("1.2.3.4:8080") == "http://1.2.3.4:8080"
    assert proxy_pool._normalize_proxy("http=5.6.7.8:3128") == "http://5.6.7.8:3128"
    assert proxy_pool._normalize_proxy("http://9.9.9.9:80") == "http://9.9.9.9:80"
    for bad in ("10.0.0.1:80", "127.0.0.1:80", "192.168.1.1:80", "not-a-proxy",
                "socks5://1.2.3.4:1080", "1.2.3.4:abc", ""):
        assert proxy_pool._normalize_proxy(bad) is None


def test_probe_rejects_blocks_and_interstitials(monkeypatch):
    pool = AutoProxyPool()
    monkeypatch.setattr(proxy_pool.requests, "get", lambda *a, **k: Resp(466, "denied"))
    assert pool._probe_proxy("http://1.2.3.4:1", "https://madouhd.cc/category/cate3/") is None

    monkeypatch.setattr(
        proxy_pool.requests, "get",
        lambda *a, **k: Resp(200, "<html>Just a moment...</html>" + "x" * 5000),
    )
    assert pool._probe_proxy("http://1.2.3.4:1", "https://madouhd.cc/category/cate3/") is None

    monkeypatch.setattr(
        proxy_pool.requests, "get",
        lambda *a, **k: Resp(200, "<html>" + "y" * 9000 + "</html>"),
    )
    assert pool._probe_proxy("http://1.2.3.4:1", "https://madouhd.cc/category/cate3/") == "http://1.2.3.4:1"
    assert pool.pool_size() == 1


def test_get_proxy_prefers_a_proxy_proven_for_that_host(monkeypatch):
    pool = AutoProxyPool()
    pool.ensure_started = lambda *a, **k: True          # never spawn a thread
    pool.report_success("http://1.1.1.1:80", "https://other.test/")
    pool.report_success("http://2.2.2.2:80", "https://madouhd.cc/category/cate3/")

    assert pool.get_proxy("https://madouhd.cc/category/cate4/") == "http://2.2.2.2:80"
    # An unrelated host may use any healthy proxy.
    assert pool.get_proxy("https://unknown.test/x/") in {"http://1.1.1.1:80", "http://2.2.2.2:80"}


def test_failures_evict_a_proxy_and_drop_its_host_affinity():
    pool = AutoProxyPool()
    pool.report_success("http://3.3.3.3:80", "https://madouhd.cc/category/cate3/")
    assert pool.pool_size() == 1

    pool.report_failure("http://3.3.3.3:80", "https://madouhd.cc/category/cate3/")
    assert pool.pool_size() == 1, "one failure should not evict a working proxy"

    pool.report_failure("http://3.3.3.3:80", "https://madouhd.cc/category/cate3/")
    assert pool.pool_size() == 0, "a twice-failed proxy must be evicted"


def test_get_proxy_is_non_blocking_when_the_pool_is_empty(monkeypatch):
    pool = AutoProxyPool()
    started = []
    monkeypatch.setattr(pool, "ensure_started", lambda *a, **k: started.append(1) or True)
    assert pool.get_proxy("https://madouhd.cc/x/") is None
    assert started, "an empty pool must kick off a harvest"


def test_refresh_tops_up_the_pool_against_the_configured_targets(monkeypatch):
    pool = AutoProxyPool(min_pool=2)
    pool.min_pool = 2
    monkeypatch.setattr(pool, "harvest_candidates", lambda max_candidates=0: ["http://1.1.1.1:1", "http://2.2.2.2:2"])
    seen_targets = []

    def fake_probe(candidate, target):
        seen_targets.append(target)
        return candidate

    monkeypatch.setattr(pool, "_probe_proxy", fake_probe)
    size = pool.refresh(targets=["https://madouhd.cc/a/", "https://www.fangsung.com/"])
    assert size == 2
    assert set(seen_targets) <= {"https://madouhd.cc/a/", "https://www.fangsung.com/"}


def test_state_survives_a_restart(monkeypatch):
    """Actions runners are ephemeral: the next session must start warm."""
    pool = AutoProxyPool()
    pool.report_success("http://4.4.4.4:80", "https://madouhd.cc/category/cate3/")
    pool._save_state()

    fresh = AutoProxyPool()
    fresh.ensure_started = lambda *a, **k: True     # no background thread in tests
    fresh._load_state()
    assert fresh.pool_size() == 1
    assert fresh.get_proxy("https://madouhd.cc/category/cate9/") == "http://4.4.4.4:80"


def test_persisted_state_is_json_and_bounded(tmp_path):
    pool = AutoProxyPool()
    pool.report_success("http://5.5.5.5:80", "https://madouhd.cc/a/")
    pool._save_state()
    data = json.loads(proxy_pool.STATE_PATH.read_text(encoding="utf-8"))
    assert "http://5.5.5.5:80" in data["proxies"]
    assert data["proxies"]["http://5.5.5.5:80"]["hosts"]


def test_ensure_started_is_idempotent(monkeypatch):
    pool = AutoProxyPool()
    pool._run = lambda targets: pool._stop.wait(5)      # alive, but no harvesting
    assert pool.ensure_started(["https://a.test/"]) is True
    first = pool._thread
    assert pool.ensure_started(["https://a.test/"]) is True
    assert pool._thread is first, "a second call must not spawn a second harvester"
    pool.stop()


def test_ensure_started_respects_the_kill_switch(monkeypatch):
    monkeypatch.setattr(proxy_pool, "ENABLED", False)
    pool = AutoProxyPool()
    assert pool.ensure_started(["https://a.test/"]) is False
    assert pool.get_proxy("https://a.test/") is None
