"""Google Translate support for the studios category.

Translates CJK titles to English via Google Translate's free public endpoint
(translate.googleapis.com gtx client — no API key required) and stores the
result in scraper.chinese_videos.title_en. Original titles are preserved.

Features:
- Persistent JSON cache (titles never re-translated across runs)
- Retry with exponential backoff, per-request timeout
- Graceful degradation: returns None on failure, caller falls back to the
  original title
"""

from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Optional

_TRANSLATE_URL = (
    "https://translate.googleapis.com/translate_a/single"
    "?client=gtx&sl=zh-CN&tl=en&dt=t&dj=1&q="
)
_CACHE_PATH = Path(__file__).resolve().parent / "translation_cache.json"
_CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\u3000-\u303f\uff01-\uff5e]")
_REQUEST_TIMEOUT = 12.0
_MAX_RETRIES = 3

_cache: Dict[str, Optional[str]] = {}
_cache_loaded = False
_cache_dirty = False

# A dead/blocked endpoint must not cost 3 requests x N titles: after this many
# consecutive transport failures we stop calling out for the rest of the
# process, and never record those failures as "this title cannot be
# translated". The old code wrote None into the persisted cache on *any*
# exception, so one transient outage permanently poisoned thousands of titles
# (5,492 rows with no title_en were sitting in chinese_videos because of it).
_FAIL_STREAK_LIMIT = 5
_fail_streak = 0
#: A *rate limit* is transient. This used to be a permanent boolean, so once the
#: free endpoint throttled us the remaining titles were skipped for the whole
#: session (run 46114735791: 697 translated, 767 given up on). The breaker now
#: cools down and retries instead of latching off.
_endpoint_down_until = 0.0
_COOLDOWN_BASE = float(__import__("os").environ.get("SCRAPER_TRANSLATE_COOLDOWN", "60"))
_COOLDOWN_MAX = 300.0
_MIN_INTERVAL = float(__import__("os").environ.get("SCRAPER_TRANSLATE_INTERVAL", "0.15"))
_last_request_at = 0.0


def _load_cache() -> None:
    global _cache, _cache_loaded
    if _cache_loaded:
        return
    try:
        if _CACHE_PATH.exists():
            _cache = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        _cache = {}
    _cache_loaded = True


def save_cache() -> None:
    """Persists the translation cache; call after batch translation runs."""
    global _cache_dirty
    if not _cache_dirty:
        return
    try:
        _CACHE_PATH.write_text(
            json.dumps(_cache, ensure_ascii=False, indent=0), encoding="utf-8"
        )
        _cache_dirty = False
    except Exception:
        pass


def has_cjk(text: str) -> bool:
    """True when the text contains CJK characters needing translation."""
    return bool(text and _CJK_RE.search(text))


def translate_to_en(text: str, retries: int = _MAX_RETRIES) -> Optional[str]:
    global _cache_dirty
    """
    Translates a title to English via Google Translate.
    Returns the English title, the original text when it needs no translation,
    or None when translation failed (caller should keep the original).
    """
    if not text or not str(text).strip():
        return None
    text = str(text).strip()
    if not has_cjk(text):
        return text

    _load_cache()
    if text in _cache:
        return _cache[text]

    global _fail_streak, _endpoint_down_until, _last_request_at
    now = time.time()
    if now < _endpoint_down_until:
        return None                       # cooling down, not giving up
    if _MIN_INTERVAL > 0:
        wait = (_last_request_at + _MIN_INTERVAL) - now
        if wait > 0:
            time.sleep(wait)

    url = _TRANSLATE_URL + urllib.parse.quote(text[:450])
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0 Safari/537.36"
                    )
                },
            )
            _last_request_at = time.time()
            with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as resp:
                payload = json.loads(resp.read().decode("utf-8", "replace"))
            _fail_streak = 0
            _endpoint_down_until = 0.0    # the endpoint is healthy again
            # dj=1 response: {"sentences": [{"trans": "...", ...}, ...], ...}
            sentences = payload.get("sentences") or []
            translated = "".join(
                s.get("trans", "") for s in sentences if isinstance(s, dict)
            ).strip()
            if translated:
                _cache[text] = translated
                _cache_dirty = True
                return translated
            # A *successful* answer with no translation is deterministic:
            # remember it so the same title is not re-sent every session.
            _cache[text] = None
            _cache_dirty = True
            return None
        except Exception:
            if attempt < retries - 1:
                time.sleep(0.8 * (attempt + 1))

    # Transport failure / rate limit: DO NOT cache. The title stays eligible for
    # the next run instead of being permanently marked untranslatable.
    _fail_streak += 1
    if _fail_streak >= _FAIL_STREAK_LIMIT:
        cool = min(_COOLDOWN_MAX, _COOLDOWN_BASE * (2 ** max(0, _fail_streak - _FAIL_STREAK_LIMIT)))
        _endpoint_down_until = time.time() + cool
        _fail_streak = 0
        print(
            f"    [!] Translation endpoint throttled; cooling down {int(cool)}s "
            "(titles stay queued, nothing is marked untranslatable)."
        )
    return None
