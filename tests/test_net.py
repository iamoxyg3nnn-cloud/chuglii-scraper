"""Tests for scraper/net.py — Cloudflare handling and the two-tier cache.

The negative-cache tests are the regression suite for run 45742796072, where
nothing remembered that a URL was dead and the same 404 was re-fetched 13,185
times.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper import net  # noqa: E402


@pytest.fixture(autouse=True)
def isolated_cache(tmp_path, monkeypatch):
    """Never touch the real cache dir, and reset per-host throttle state."""
    monkeypatch.setattr(net, "_HTTP_DIR", tmp_path / "http")
    monkeypatch.setattr(net, "_CF_DIR", tmp_path / "cf")
    monkeypatch.setattr(net, "HOST_MIN_INTERVAL", 0.0)
    net._HOST_LAST.clear()
    net._CF_SESSIONS.clear()
    net._CF_SESSIONS_LOADED.clear()
    net._CF_SOLVES.clear()
    net._CF_LAST_SOLVE.clear()
    net._CF_COOLDOWN_UNTIL.clear()
    yield


class FakeResp:
    def __init__(self, status, body=b"", headers=None, cookies=None):
        self.status = status
        self.body = body
        self.headers = headers or {}
        self.cookies = cookies or ()
        self.request_headers = {"user-agent": "TestAgent/1.0"}
        self.encoding = "utf-8"


def install_plain(monkeypatch, responses):
    """Stub the plain fetcher; returns the call log."""
    calls = []

    def fake_plain(url, timeout, headers, referer):
        calls.append(url)
        item = responses[min(len(calls) - 1, len(responses) - 1)]
        if item is None:
            return None
        return net.Page(
            url=url,
            status=item.status,
            body=item.body,
            headers=item.headers,
            via="fetcher",
            challenge=net.is_cf_challenge(item.status, item.body, item.headers),
        )

    monkeypatch.setattr(net, "_plain_get", fake_plain)
    return calls


def test_dead_url_is_fetched_once_then_cached(monkeypatch):
    """The incident: a 404 must never be re-requested on the next attempt."""
    calls = install_plain(monkeypatch, [FakeResp(404)])

    first = net.fetch("https://91dasj.com/category/aifl/20/")
    assert first is not None and first.status == 404
    assert first.via == "fetcher"

    for _ in range(50):                       # 50 more "retries"
        again = net.fetch("https://91dasj.com/category/aifl/20/")
        assert again.status == 404
        assert again.via == "dead-cache"
        assert again.from_cache is True

    assert len(calls) == 1, f"404 was re-fetched {len(calls)} times"


def test_dead_cache_can_be_bypassed(monkeypatch):
    calls = install_plain(monkeypatch, [FakeResp(404), FakeResp(200, b"<html>hi</html>")])
    net.fetch("https://x.test/p/2/")
    page = net.fetch("https://x.test/p/2/", allow_dead_cache=False)
    assert page.status == 200
    assert len(calls) == 2


def test_cloudflare_200_challenge_is_not_returned_as_content(monkeypatch):
    """A 200 'Just a moment...' body must never be parsed as a real page."""
    install_plain(
        monkeypatch,
        [FakeResp(200, b"<html><title>Just a moment...</title>challenge-platform</html>")],
    )
    monkeypatch.setattr(net, "_solve_cloudflare", lambda url, host: None)

    page = net.fetch("https://cf.test/list/", retries=1)
    assert page is not None
    assert page.challenge is True
    assert page.status != 200
    assert net.cache_stats()["entries"] == 0, "a challenge must never be cached as content"


def test_cloudflare_solve_result_is_used_and_cached(monkeypatch):
    install_plain(
        monkeypatch,
        [FakeResp(403, b"Attention Required! | Cloudflare")],
    )
    solved = net.Page(url="https://cf.test/list/", status=200, body=b"<html>real</html>", via="stealthy")
    monkeypatch.setattr(net, "_solve_cloudflare", lambda url, host: solved)

    page = net.fetch("https://cf.test/list/", retries=1)
    assert page.status == 200 and page.via == "stealthy" and page.body == b"<html>real</html>"


def test_positive_cache_serves_second_call_without_network(monkeypatch):
    calls = install_plain(monkeypatch, [FakeResp(200, b"<html>content</html>")])
    first = net.fetch("https://ok.test/a/", cache_ttl=600)
    second = net.fetch("https://ok.test/a/", cache_ttl=600)
    assert first.via == "fetcher"
    assert second.via == "cache"
    assert second.html_content == "<html>content</html>"
    assert len(calls) == 1


def test_zero_ttl_disables_positive_cache(monkeypatch):
    calls = install_plain(monkeypatch, [FakeResp(200, b"<html>x</html>")])
    net.fetch("https://ok.test/b/", cache_ttl=0)
    net.fetch("https://ok.test/b/", cache_ttl=0)
    assert len(calls) == 2


def test_transient_errors_are_retried_and_not_cached(monkeypatch):
    calls = install_plain(monkeypatch, [FakeResp(502), FakeResp(502), FakeResp(200, b"<html>ok</html>")])
    monkeypatch.setattr(net.time, "sleep", lambda s: None)
    page = net.fetch("https://flaky.test/x/", retries=3)
    assert page.status == 200
    assert len(calls) == 3
    assert net.cache_stats()["dead"] == 0


def test_transport_failure_returns_none(monkeypatch):
    install_plain(monkeypatch, [None])
    monkeypatch.setattr(net.time, "sleep", lambda s: None)
    assert net.fetch("https://down.test/", retries=2) is None


def test_is_cf_challenge_shapes():
    assert net.is_cf_challenge(403, b"") is True
    assert net.is_cf_challenge(503, b"") is True
    assert net.is_cf_challenge(200, b"<html>Just a moment...</html>") is True
    assert net.is_cf_challenge(200, b"Checking your browser before accessing") is True
    assert net.is_cf_challenge(200, b"", {"cf-mitigated": "challenge"}) is True
    # A real large page that merely embeds Cloudflare JS is content.
    assert net.is_cf_challenge(200, b"<html>" + b"a" * 30000 + b"challenge-platform</html>") is False
    assert net.is_cf_challenge(404, b"not found") is False
    assert net.is_cf_challenge(200, b"<html>real catalogue</html>") is False


def test_throttle_spaces_requests_per_host(monkeypatch):
    sleeps = []
    monkeypatch.setattr(net, "HOST_MIN_INTERVAL", 1.0)
    monkeypatch.setattr(net.time, "sleep", lambda s: sleeps.append(s))
    net._HOST_LAST["h.test"] = net.time.time()      # pretend we just requested
    net._throttle("h.test")
    assert sleeps and sleeps[0] > 0.5


def test_solve_budget_stops_browser_stampede(monkeypatch):
    """After the budget is spent a host must not keep launching browsers."""
    net._CF_SOLVES["cf.test"] = net.CF_MAX_SOLVES_PER_HOST
    assert net._solve_cloudflare("https://cf.test/", "cf.test") is None


def test_cache_size_cap_evicts_oldest_entries(monkeypatch):
    """A long session must not fill the runner disk."""
    monkeypatch.setattr(net, "CACHE_MAX_BYTES", 4000)
    install_plain(monkeypatch, [FakeResp(200, b"<html>" + b"x" * 1500 + b"</html>")])
    for i in range(12):
        net.fetch(f"https://big.test/page/{i}/", cache_ttl=600)
    stats = net.cache_stats()
    size = sum(f.stat().st_size for f in net._HTTP_DIR.glob("*.json"))
    assert size <= 4000 * 1.5, f"cache grew to {size} bytes"
    assert stats["entries"] < 12, "nothing was evicted"


def test_oversized_body_is_not_cached(monkeypatch):
    monkeypatch.setattr(net, "MAX_CACHE_BODY", 100)
    calls = install_plain(monkeypatch, [FakeResp(200, b"<html>" + b"y" * 500 + b"</html>")])
    first = net.fetch("https://huge.test/a/")
    second = net.fetch("https://huge.test/a/")
    assert first.status == 200 and second.via == "fetcher"
    assert len(calls) == 2, "an oversized page should not have been cached"


def test_cf_cooldown_blocks_further_solves(monkeypatch):
    net._CF_COOLDOWN_UNTIL["cf.test"] = net.time.time() + 60
    assert net._solve_cloudflare("https://cf.test/", "cf.test") is None
