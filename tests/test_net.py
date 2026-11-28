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
    monkeypatch.setattr(net, "PROXY_FALLBACK", False)   # no proxy harvesting in tests
    net._HOST_LAST.clear()
    net._CF_SESSIONS.clear()
    net._CF_SESSIONS_LOADED.clear()
    net._CF_SOLVES.clear()
    net._CF_LAST_SOLVE.clear()
    net._CF_COOLDOWN_UNTIL.clear()
    net._BLOCKED_HOSTS.clear()
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

    def fake_plain(url, timeout, headers, referer, proxy=None):
        calls.append((url, proxy))
        item = responses[min(len(calls) - 1, len(responses) - 1)]
        if item is None:
            return None
        challenge = net.is_cf_challenge(item.status, item.body, item.headers)
        return net.Page(
            url=url,
            status=item.status,
            body=item.body,
            headers=item.headers,
            via="fetcher",
            challenge=challenge,
            error=False if challenge else net.is_origin_error_page(
                item.status, item.body, item.headers
            ),
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


def test_is_block_status_shapes():
    for status in (403, 406, 429, 451, 466, 511):
        assert net.is_block_status(status) is True
    for status in (200, 301, 404, 410, 500):
        assert net.is_block_status(status) is False


def test_waf_status_escalates_to_proxy(monkeypatch):
    """madouhd.cc answers 466 to runner IPs; the proxy result must be used."""
    calls = install_plain(monkeypatch, [FakeResp(466, b"blocked")])
    monkeypatch.setattr(net, "PROXY_FALLBACK", True)
    monkeypatch.setattr(net, "_solve_cloudflare", lambda url, host: None)

    def fake_proxy_get(url, timeout, headers, referer, host):
        return net.Page(url=url, status=200, body=b"<html>catalogue</html>", via="proxy")

    monkeypatch.setattr(net, "_proxy_get", fake_proxy_get)
    page = net.fetch("https://madouhd.cc/category/cate3/", retries=1)
    assert page.status == 200 and page.via == "proxy"
    assert b"catalogue" in page.body
    assert len(calls) == 1


def test_waf_status_without_proxy_is_never_content(monkeypatch):
    install_plain(monkeypatch, [FakeResp(466, b"<html>still blocked</html>")])
    monkeypatch.setattr(net, "_solve_cloudflare", lambda url, host: None)
    page = net.fetch("https://madouhd.cc/category/cate3/", retries=1)
    assert page is not None
    assert page.status == 466
    assert page.challenge is True
    assert page.body == b"", "a WAF body must not reach a parser as content"
    assert net.cache_stats()["entries"] == 0


def test_blocked_host_goes_proxy_first_next_time(monkeypatch):
    """A host that blocked us must not burn direct attempts on every URL."""
    calls = install_plain(monkeypatch, [FakeResp(466), FakeResp(200, b"<html>ok</html>")])
    monkeypatch.setattr(net, "_solve_cloudflare", lambda url, host: None)
    net._BLOCKED_HOSTS["madouhd.cc"] = net.time.time()
    monkeypatch.setattr(net, "PROXY_FALLBACK", True)

    def fake_proxy_get(url, timeout, headers, referer, host):
        return net.Page(url=url, status=200, body=b"<html>via proxy</html>", via="proxy")

    monkeypatch.setattr(net, "_proxy_get", fake_proxy_get)
    page = net.fetch("https://madouhd.cc/category/cate4/", retries=2)
    assert page.status == 200 and page.via == "proxy"
    assert calls == [], "direct attempts were made against a known-blocking host"


def test_success_clears_blocked_host_memory(monkeypatch):
    install_plain(monkeypatch, [FakeResp(200, b"<html>ok</html>")])
    net._BLOCKED_HOSTS["was.blocked"] = net.time.time()
    net.fetch("https://was.blocked/a/", retries=1)
    assert "was.blocked" not in net._BLOCKED_HOSTS


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


def test_challenge_fallback_can_be_disabled(monkeypatch):
    """Some endpoints answer 403 for 'restricted', not Cloudflare: never launch
    a browser for those (it costs ~20s and cannot help)."""
    calls = install_plain(monkeypatch, [FakeResp(403, b"")])

    def must_not_run(url, host):
        raise AssertionError("solver launched despite accept_challenge_fallback=False")

    monkeypatch.setattr(net, "_solve_cloudflare", must_not_run)
    page = net.fetch("https://h.test/frag", retries=1, accept_challenge_fallback=False)
    assert page is not None and page.status == 403 and page.challenge is True
    assert len(calls) == 1


def test_cf_cooldown_blocks_further_solves(monkeypatch):
    net._CF_COOLDOWN_UNTIL["cf.test"] = net.time.time() + 60
    assert net._solve_cloudflare("https://cf.test/", "cf.test") is None


def test_page_css_matches_scrapling_selector_surface(monkeypatch):
    """The OnlyFans sources call resp.css(...) on a net.Page — that crashed with
    "'Page' object has no attribute 'css'" and disabled two sites completely."""
    html = (
        b'<html><body>'
        b'<article class="post"><h2 class="entry-title"><a href="/a/1"> First  post </a></h2>'
        b'<img data-src="t.jpg"></article>'
        b'<a class="page-numbers" href="/page/3/">3</a>'
        b'</body></html>'
    )
    install_plain(monkeypatch, [FakeResp(200, html)])
    page = net.fetch("https://igw.test/category/onlyfans/", retries=1)

    arts = page.css("article.post")
    assert len(arts) == 1
    assert arts[0].attrib.get("class") == "post"
    link = arts[0].css(".entry-title a")[0]
    assert link.attrib["href"] == "/a/1"
    assert link.text.clean() == "First post"
    assert arts[0].css("img")[0].attrib["data-src"] == "t.jpg"
    assert page.css("a.page-numbers")[0].attrib["href"] == "/page/3/"
    assert page.css("nothing-here") == []


def test_media_urls_never_launch_a_browser(monkeypatch):
    """An HLS manifest 403 is a CDN rule, not a Cloudflare interstitial.

    The fleet used to launch Chromium on content.zavastream.com/....m3u8 and log
    'Scrapling solve failed ... Executable doesn't exist' every run.
    """
    install_plain(monkeypatch, [FakeResp(403, b"")])

    def must_not_run(url, host):
        raise AssertionError(f"browser solve launched for media URL {url}")

    monkeypatch.setattr(net, "_solve_cloudflare", must_not_run)
    monkeypatch.setattr(net, "_is_media_url", lambda url: True)
    page = net.fetch("https://content.zavastream.com/x/index.m3u8", retries=1)
    assert page is not None and page.challenge is True


def test_is_media_url_shapes():
    assert net._is_media_url("https://cdn/x/index.m3u8")
    assert net._is_media_url("https://cdn/a/b/index.m3u8?token=1")
    assert net._is_media_url("https://cdn/img/cover.JPG")
    assert not net._is_media_url("https://site/category/cate3/")
    assert not net._is_media_url("https://site/page.html")


# ─── Origin-error pages (run 46093407022) ───────────────────────────────────
# borntobefuck.com answered HTTP 200 with 22 bytes of "Internal server error."
# for hours. Every parser read that as "zero cards", so zones were pinned
# COMPLETE and 5,328 live ids were recorded dead. An error page is never
# content, and never proof that a URL is gone.


def test_origin_error_header_is_detected():
    assert net.is_origin_error_page(
        200, b"Internal server error.", {"status": "500 Internal Server Error"}
    )


def test_origin_error_marker_needs_a_tiny_body():
    small = b"<html><body>Internal server error.</body></html>"
    huge = b"<html><body>" + b"x" * 9000 + b" internal server error</body></html>"
    assert net.is_origin_error_page(200, small, {})
    assert not net.is_origin_error_page(200, huge, {}), "a real page must never be misread"


def test_real_pages_are_not_error_pages():
    assert not net.is_origin_error_page(200, b"<html><title>Listing</title></html>", {})
    assert not net.is_origin_error_page(404, b"", {})
    assert not net.is_origin_error_page(200, b"", {})


def test_origin_error_page_is_never_content_or_cached(monkeypatch):
    install_plain(monkeypatch, [FakeResp(200, b"Internal server error.")])
    page = net.fetch("https://broken.test/list/", retries=1)
    assert page is not None
    assert page.error is True
    assert not page, "an error page must be falsy so `if not resp` catches it"
    assert page.body == b"", "the error body must never reach a parser"
    assert net.cache_stats()["entries"] == 0, "an origin error must never be cached"


def test_origin_error_is_retried_once(monkeypatch):
    calls = install_plain(
        monkeypatch,
        [FakeResp(200, b"Internal server error."), FakeResp(200, b"<html>real</html>")],
    )
    page = net.fetch("https://flaky.test/list/", retries=2)
    assert page.status == 200 and not page.error
    assert len(calls) == 2


def test_transport_failure_marked_blocked_when_proxy_succeeds(monkeypatch):
    """A timeout that a proxy fixes is an egress block, not a per-URL accident.

    Without this mark every following URL paid 3 x 30s of doomed direct
    timeouts first (ujx.838238.xyz managed ~1 URL per 90 seconds).
    """
    install_plain(monkeypatch, [None])
    good = net.Page(url="https://uxj.838238.xyz/list/1.html", status=200, body=b"<html>ok</html>")
    monkeypatch.setattr(net, "PROXY_FALLBACK", True)
    monkeypatch.setattr(net, "_proxy_get", lambda *a, **k: good)

    page = net.fetch("https://uxj.838238.xyz/list/1.html", retries=1)
    assert page.status == 200
    assert "uxj.838238.xyz" in net.blocked_hosts()


def test_unreachable_host_is_skipped_after_repeated_failures(monkeypatch):
    """A route that times out everywhere must stop costing the session.

    Run 46114735791 still spent ~4 minutes per page on uxj.838238.xyz: each of
    20 detail fetches re-paid a 30s direct timeout plus a 30s proxy timeout even
    though nothing ever answered.
    """
    calls = install_plain(monkeypatch, [None])
    monkeypatch.setattr(net, "PROXY_FALLBACK", True)
    monkeypatch.setattr(net, "_proxy_get", lambda *a, **k: None)
    monkeypatch.setattr(net, "HOST_DOWN_STRIKES", 2)

    url = "https://unreachable.test/list/1.html"
    assert net.fetch(url, retries=1) is None
    assert net.fetch(url, retries=1) is None
    assert net.fetch(url, retries=1) is None          # trips the breaker

    before = len(calls)
    assert net.fetch("https://unreachable.test/list/2.html", retries=1) is None
    assert len(calls) == before, "a down host must not be requested again"
    assert "unreachable.test" in net.hosts_down()


def test_a_working_host_clears_the_down_counter(monkeypatch):
    install_plain(monkeypatch, [None, FakeResp(200, b"<html>ok</html>")])
    monkeypatch.setattr(net, "PROXY_FALLBACK", False)
    net.fetch("https://recovers.test/a", retries=1)
    assert net.fetch("https://recovers.test/b", retries=1).status == 200
    assert "recovers.test" not in net.hosts_down()


def test_proxy_timeout_is_bounded_so_the_breaker_can_trip(monkeypatch):
    """A blocked host must be *detected* in seconds, not minutes.

    Run 46122179927 spent 8.5 of its 4-minute slice on this: each fetch paid
    30 s per proxy attempt, so the 3-strike breaker took 4.5 minutes to trip.
    """
    seen = {}

    def fake_plain(url, timeout, headers, referer, proxy=None):
        seen["timeout"] = timeout
        return None

    monkeypatch.setattr(net, "_plain_get", fake_plain)
    monkeypatch.setattr(net, "PROXY_FALLBACK", True)
    monkeypatch.setattr(net, "HOST_MIN_INTERVAL", 0.0)

    class Pool:
        def get_proxy(self, target_url=None, rotate=False):
            return "http://127.0.0.1:1"

        def report_failure(self, *a, **k):
            pass

        def report_success(self, *a, **k):
            pass

    monkeypatch.setattr(net, "_proxy_pool", lambda: Pool())
    monkeypatch.setattr(net, "PROXY_TIMEOUT", 7.0)

    net.fetch("https://blocked.test/x", timeout=30.0, retries=1)
    assert seen["timeout"] == 7.0, "proxy attempts must use PROXY_TIMEOUT, not the direct timeout"


def test_host_down_short_circuits_concurrent_workers(monkeypatch):
    """Once tripped, a worker must not start another doomed attempt."""
    calls = []

    def fake_plain(url, timeout, headers, referer, proxy=None):
        calls.append(url)
        return None

    monkeypatch.setattr(net, "_plain_get", fake_plain)
    monkeypatch.setattr(net, "PROXY_FALLBACK", False)
    monkeypatch.setattr(net, "HOST_MIN_INTERVAL", 0.0)
    monkeypatch.setattr(net, "HOST_DOWN_STRIKES", 2)

    for _ in range(4):
        net.fetch(f"https://gone.test/{len(calls)}", retries=1)
    before = len(calls)
    net.fetch("https://gone.test/late", retries=1)
    assert len(calls) == before


def test_blocked_host_tries_several_distinct_free_proxies(monkeypatch):
    """Free proxies die constantly, so one fetch must walk a few of them.

    The old ceiling of two attempts per fetch, combined with evicting a proxy
    after two failures it could not be blamed for, is what left the Chinese feed
    with no route at all.
    """
    monkeypatch.setattr(net, "_plain_get", lambda *a, **k: None)
    monkeypatch.setattr(net, "PROXY_FALLBACK", True)
    monkeypatch.setattr(net, "PROXY_TIMEOUT", 0.1)
    monkeypatch.setattr(net, "PROXY_ATTEMPTS_PER_FETCH", 4)
    monkeypatch.setattr(net, "HOST_MIN_INTERVAL", 0.0)

    offered = [f"http://10.9.9.{i}:80" for i in range(10)]
    asked = []

    class Pool:
        def get_proxy(self, target_url=None, rotate=False, exclude=None):
            asked.append(set(exclude or ()))
            left = [p for p in offered if p not in (exclude or set())]
            return left[0] if left else None

        def ensure_for_host(self, url):
            return False

        def report_failure(self, *a, **k):
            pass

        def report_success(self, *a, **k):
            pass

    monkeypatch.setattr(net, "_proxy_pool", lambda: Pool())
    assert net._proxy_get("https://uxj.838238.xyz/x", 5.0, None, None, "uxj.838238.xyz") is None
    assert len(asked) == 4
    # Every request excluded everything already tried, so no proxy is retried.
    assert asked[-1] == set(offered[:3])
