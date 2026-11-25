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
            with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as resp:
                payload = json.loads(resp.read().decode("utf-8", "replace"))
            # dj=1 response: {"sentences": [{"trans": "...", ...}, ...], ...}
            sentences = payload.get("sentences") or []
            translated = "".join(
                s.get("trans", "") for s in sentences if isinstance(s, dict)
            ).strip()
            if translated:
                _cache[text] = translated
                _cache_dirty = True
                return translated
        except Exception:
            if attempt < retries - 1:
                time.sleep(0.8 * (attempt + 1))
    _cache[text] = None  # negative cache: don't hammer a failing endpoint
    _cache_dirty = True
    return None
