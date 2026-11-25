"""Regression checks for durable scraper progress and dedupe state."""

import json

import pytest

from scraper import progress
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
    local.write_text(json.dumps(["internetchicks:local-id"]), encoding="utf-8")
    monkeypatch.setattr(
        progress, "get_scraper_known_post_ids", lambda site, limit: {"remote-id"}
    )

    ids = progress.load_known_ids("internetchicks", local, prefix=True)

    assert "internetchicks:local-id" in ids
    assert "internetchicks:remote-id" in ids


def test_mark_known_ids_groups_by_site_and_honours_rpc_failure(monkeypatch):
    calls = []

    def fake_add(site, ids, status="stored"):
        calls.append((site, tuple(ids), status))
        return {"success": True, "count": len(ids)}

    monkeypatch.setattr(progress, "add_scraper_known_post_ids", fake_add)

    written = progress.mark_known_ids(
        {"internetchicks:abc", "sheeshfans:xyz", "bare-id"},
        default_site="myanmar",
        status="seen",
    )

    assert written == 3
    assert {site: (ids, status) for site, ids, status in calls} == {
        "internetchicks": (("abc",), "seen"),
        "myanmar": (("bare-id",), "seen"),
        "sheeshfans": (("xyz",), "seen"),
    }

    monkeypatch.setattr(
        progress,
        "add_scraper_known_post_ids",
        lambda site, ids, status="stored": {"success": False, "error": "write denied"},
    )
    assert progress.mark_known_ids({"internetchicks:retry-me"}) == 0


def test_finalize_status_keeps_failed_sites_isolated():
    state = {
        "sites": {
            "internetchicks": {"status": "completed"},
            "sheeshfans": {"status": "failed"},
            "voyeurhouse": {"status": "completed"},
        }
    }

    status = progress.finalize_status(state, ["myanmar", "sheeshfans"])

    assert status == "failed"
    assert state["failed_sites"] == ["myanmar", "sheeshfans"]
    assert state["sites"]["internetchicks"]["status"] == "completed"


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

    monkeypatch.setattr(unified, "wait_for_supabase", lambda **kwargs: None)
    monkeypatch.setattr(unified, "init_known_id_caches", lambda: None)
    monkeypatch.setattr(unified, "load_japanese_checkpoint", lambda: {})
    monkeypatch.setattr(unified, "human_jitter", lambda *args, **kwargs: None)
    saved = {}
    monkeypatch.setattr(
        progress, "save_progress",
        lambda key, state, local_file=None: saved.update(state) or True,
    )
    for name in (
        "run_viral_mms_sync",
        "run_japanese_jav_sync",
        "run_myanmar_sync",
        "run_onlyfans_sync",
        "run_voyeurhouse_sync",
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
