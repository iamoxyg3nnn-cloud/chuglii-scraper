"""Regression checks for durable scraper progress and dedupe state."""

import json

import pytest

from scraper import db, progress
from scraper.db import rpc_result_succeeded


def test_rpc_result_succeeded_rejects_application_failure():
    assert rpc_result_succeeded({"success": True, "count": 1}) is True
    assert rpc_result_succeeded({"success": False, "error": "boom"}) is False
    assert rpc_result_succeeded(None) is False


def test_load_checkpoint_remote_wins_and_updates_local_cache(tmp_path, monkeypatch):
    local = tmp_path / "checkpoint.json"
    local.write_text(json.dumps({"current_page": 1, "stale": True}), encoding="utf-8")
    monkeypatch.setattr(
        progress, "get_scraper_checkpoint", lambda key: {"current_page": 42, "stale": False}
    )

    state = progress.load_checkpoint("myanmar", local, {"current_page": 1})

    assert state["current_page"] == 42
    assert state["stale"] is False
    assert json.loads(local.read_text(encoding="utf-8"))["current_page"] == 42


def test_load_checkpoint_recovers_from_stale_local_cache_when_remote_fails(tmp_path, monkeypatch):
    local = tmp_path / "checkpoint.json"
    local.write_text(json.dumps({"current_page": 84, "max_pages": 3690}), encoding="utf-8")

    def fail(key):
        raise RuntimeError("supabase offline")

    monkeypatch.setattr(progress, "get_scraper_checkpoint", fail)

    state = progress.load_checkpoint("myanmar", local, {"current_page": 34})

    assert state["current_page"] == 84
    assert state["max_pages"] == 3690
    assert json.loads(local.read_text(encoding="utf-8"))["current_page"] == 84


def test_save_checkpoint_reports_remote_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(progress, "set_scraper_checkpoint", lambda key, state: False)
    local = tmp_path / "checkpoint.json"

    assert progress.save_checkpoint("viral-mms", {"current_page": 7}, local) is False
    assert json.loads(local.read_text(encoding="utf-8"))["current_page"] == 7


def test_load_known_ids_prefixes_per_site_ledger_ids(tmp_path, monkeypatch):
    local = tmp_path / "known_post_ids.json"
    local.write_text(json.dumps(["influencersgonewild:local-id"]), encoding="utf-8")
    monkeypatch.setattr(
        progress, "get_scraper_known_post_ids", lambda site, limit: {"remote-id"}
    )

    ids = progress.load_known_ids("influencersgonewild", local, prefix=True)

    assert "influencersgonewild:local-id" in ids
    assert "influencersgonewild:remote-id" in ids


def test_mark_known_ids_groups_by_site_and_honours_rpc_failure(monkeypatch):
    calls = []

    def fake_add(site, ids, status="stored"):
        calls.append((site, tuple(ids), status))
        return {"success": True, "count": len(ids)}

    monkeypatch.setattr(progress, "add_scraper_known_post_ids", fake_add)

    written = progress.mark_known_ids(
        {"influencersgonewild:abc", "sheeshfans:xyz", "bare-id"},
        default_site="myanmar",
        status="seen",
    )

    assert written == 3
    assert {site: (ids, status) for site, ids, status in calls} == {
        "influencersgonewild": (("abc",), "seen"),
        "myanmar": (("bare-id",), "seen"),
        "sheeshfans": (("xyz",), "seen"),
    }

    monkeypatch.setattr(
        progress,
        "add_scraper_known_post_ids",
        lambda site, ids, status="stored": {"success": False, "error": "write denied"},
    )
    assert progress.mark_known_ids({"influencersgonewild:retry-me"}) == 0


def test_finalize_status_keeps_failed_sites_isolated():
    state = {
        "sites": {
            "influencersgonewild": {"status": "completed"},
            "sheeshfans": {"status": "failed"},
            "voyeurhouse": {"status": "completed"},
        }
    }

    status = progress.finalize_status(state, ["myanmar", "sheeshfans"])

    assert status == "failed"
    assert state["failed_sites"] == ["myanmar", "sheeshfans"]
    assert state["sites"]["influencersgonewild"]["status"] == "completed"


@pytest.mark.parametrize("value", [{}, "", 0, ["ok"]])
def test_rpc_result_succeeded_accepts_other_results(value):
    assert rpc_result_succeeded(value) is True


def test_cursor_retry_holds_then_releases_unresolved_page():
    assert progress.cursor_retry_state(stall_count=0, unresolved=3) == (1, True)
    assert progress.cursor_retry_state(stall_count=3, unresolved=3) == (4, True)
    assert progress.cursor_retry_state(stall_count=4, unresolved=3) == (5, False)
    assert progress.cursor_retry_state(stall_count=7, unresolved=0) == (0, False)


def test_finalize_status_reports_partial_without_hiding_isolated_failures():
    state = {
        "sites": {
            "onlyfans": {"status": "partial"},
            "voyeurhouse": {"status": "completed"},
        }
    }

    assert progress.finalize_status(state, []) == "partial"
    assert state["degraded_sites"] == ["onlyfans"]
    assert state["failed_sites"] == []

    assert progress.finalize_status(state, ["myanmar"]) == "failed"
    assert state["failed_sites"] == ["myanmar"]
    assert state["degraded_sites"] == ["onlyfans"]


def _patch_cycle_dependencies(monkeypatch):
    import scraper.unified_scraper as unified

    monkeypatch.setattr(unified, "wait_for_supabase", lambda **kwargs: True)
    monkeypatch.setattr(unified, "wait_for_supabase_before_cycle", lambda *a, **k: True)
    monkeypatch.setattr(unified, "init_known_id_caches", lambda: None)
    monkeypatch.setattr(unified, "load_japanese_checkpoint", lambda: {})
    monkeypatch.setattr(unified, "human_jitter", lambda *args, **kwargs: None)
    saved = {}
    monkeypatch.setattr(
        progress, "save_progress",
        lambda key, state, local_file=None: saved.update(state) or True,
    )
    # Every feed runner must be stubbed: an unpatched one performs real network
    # scraping and hangs the suite (this is what pinned test_progress_state).
    for name in (
        "run_viral_mms_sync",
        "run_viral_mms_sites_sync",
        "run_japanese_jav_sync",
        "run_myanmar_sync",
        "run_onlyfans_sync",
        "run_voyeurhouse_sync",
        "run_chinese_sync",
        "run_fangsung_sync",
        "run_studios_sync",
    ):
        monkeypatch.setattr(unified, name, lambda **kwargs: 1)
    return unified, saved


def test_unified_cycle_discards_stale_feed_status(monkeypatch):
    unified, saved = _patch_cycle_dependencies(monkeypatch)
    unified.FEED_STATUS["supjav"] = "failed"

    result = unified.run_unified_sync_cycle(check_head=False)

    assert result["mms"] == 1
    assert saved["status"] == "completed"
    assert saved["failed_feeds"] == []
    assert saved["feeds"]["supjav"] == "completed"


def test_unified_cycle_reports_degraded_feeds(monkeypatch):
    unified, saved = _patch_cycle_dependencies(monkeypatch)

    def partial_onlyfans(**kwargs):
        unified.FEED_STATUS["onlyfans"] = "partial"
        return 0

    monkeypatch.setattr(unified, "run_onlyfans_sync", partial_onlyfans)

    unified.run_unified_sync_cycle(check_head=False)

    assert saved["status"] == "partial"
    assert saved["degraded_feeds"] == ["onlyfans"]
    assert saved["failed_feeds"] == []


def test_unified_cycle_does_no_live_network(monkeypatch):
    """Every feed runner must be stubbed; a leak would scrape and WRITE to prod.

    Regression: run_unified_sync_cycle also runs the Viral MMS isolated sub-sites
    (FEED 1b). When that runner was not stubbed the test suite silently ingested
    ~144 real videos into the production database on every run.
    """
    unified, _ = _patch_cycle_dependencies(monkeypatch)

    def _blocked(*args, **kwargs):
        raise AssertionError("test attempted real network I/O via an unstubbed runner")

    import curl_cffi.requests as creq

    monkeypatch.setattr(creq, "get", _blocked, raising=False)
    monkeypatch.setattr(creq, "post", _blocked, raising=False)
    monkeypatch.setattr(creq, "Session", _blocked, raising=False)

    import httpx

    class _BlockedClient:
        def __init__(self, *a, **k):
            raise AssertionError("test attempted real network I/O via httpx.Client")

    monkeypatch.setattr(httpx, "Client", _BlockedClient)
    monkeypatch.setattr(httpx, "get", _blocked, raising=False)

    result = unified.run_unified_sync_cycle(check_head=False)
    assert result["mms"] == 1


def test_supabase_health_requires_usable_rpc(monkeypatch):
    class FakeResponse:
        def __init__(self, status_code, payload):
            self.status_code = status_code
            self.payload = payload

        def json(self):
            return self.payload

    response = FakeResponse(200, {})

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def post(self, *args, **kwargs):
            return response

    monkeypatch.setattr(db.httpx, "Client", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr(db, "SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setattr(db, "SUPABASE_KEY", "test-key")

    assert db.is_supabase_healthy() is True

    response = FakeResponse(530, {})
    assert db.is_supabase_healthy() is False

    response = FakeResponse(200, {"success": False, "error": "database unavailable"})
    assert db.is_supabase_healthy() is False


def test_wait_for_supabase_reports_timeout_as_not_online(monkeypatch):
    times = iter([0, 0, 1, 2, 3])
    slept = []
    monkeypatch.setattr(db, "is_supabase_healthy", lambda: False)
    monkeypatch.setattr(db.time, "time", lambda: next(times, 3))
    monkeypatch.setattr(db.time, "sleep", lambda seconds: slept.append(seconds))

    assert db.wait_for_supabase(max_wait_seconds=2, poll_interval=1) is False
    assert slept == [1]


def test_wait_for_supabase_before_cycle_respects_session_deadline(monkeypatch):
    import scraper.unified_scraper as unified

    seen = {}
    monkeypatch.setattr(unified.time, "time", lambda: 100.0)
    monkeypatch.setattr(
        unified,
        "wait_for_supabase",
        lambda **kwargs: seen.update(kwargs) or True,
    )

    assert unified.wait_for_supabase_before_cycle(610.0) is True
    assert seen == {"max_wait_seconds": 510, "poll_interval": 15}

    monkeypatch.setattr(unified.time, "time", lambda: 700.0)
    assert unified.wait_for_supabase_before_cycle(610.0) is False
    assert seen["max_wait_seconds"] == 510


def test_unified_cycle_skips_all_feeds_while_supabase_is_down(monkeypatch):
    unified, saved = _patch_cycle_dependencies(monkeypatch)
    called = []
    # run_unified_sync_cycle gates on wait_for_supabase_before_cycle (not on
    # wait_for_supabase directly), so that is the symbol that must fail.
    monkeypatch.setattr(unified, "wait_for_supabase_before_cycle", lambda *a, **k: False)
    monkeypatch.setattr(unified, "init_known_id_caches", lambda: called.append("caches"))
    for name in (
        "run_viral_mms_sync",
        "run_japanese_jav_sync",
        "run_myanmar_sync",
        "run_onlyfans_sync",
        "run_voyeurhouse_sync",
    ):
        monkeypatch.setattr(unified, name, lambda **kwargs: called.append("feed") or 1)

    result = unified.run_unified_sync_cycle(check_head=False)

    assert result == {
        "mms": 0,
        "jav": 0,
        "myanmar": 0,
        "onlyfans": 0,
        "voyeurhouse": 0,
        "chinese": 0,
        "fangsung": 0,
        "studios": 0,
    }
    assert called == []
    assert saved == {}
