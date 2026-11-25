"""Record polishing for the fangsung category.

Shapes raw source items into scraper.fangsung_videos rows. Videos must carry a
playable HLS m3u8; anything else is dropped upstream (this site has no
torrents). Titles are translated to English at ingest (translate.py); the
original CJK stays in `title`.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

try:
    from scraper.categories.chinese.translate import translate_to_en
except Exception:  # pragma: no cover
    import sys as _sys
    from pathlib import Path as _Path
    _p = _Path(__file__).resolve().parents[2] / "chinese"
    if str(_p) not in _sys.path:
        _sys.path.insert(0, str(_p))
    from translate import translate_to_en  # type: ignore


def _quality_from_resolution(resolution: Optional[str]) -> tuple:
    """Maps a resolution string to the standard quality label/score.

    Min-dimension "p" convention (portrait/landscape agnostic), matching the
    ffprobe-probed labels. Unknown/zero dims fall back to the generic 'HD'
    label, never a fake resolution or a bogus '0p'.
    """
    if not resolution:
        return "HD", 720
    text = str(resolution).strip().lower()
    m = re.search(r"(\d{3,4})\s*[x×*]\s*(\d{3,4})", text)
    if m:
        p = min(int(m.group(1)), int(m.group(2)))
        if p <= 0:
            return "HD", 720
    else:
        m2 = re.search(r"(\d{3,4})\s*p", text)
        if not m2:
            return "HD", 720
        p = int(m2.group(1))
    return f"{p}p", p


def polish_fangsung_record(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """Polishes a raw fangsung item into a schema-compliant record."""
    title = re.sub(r"\s+", " ", str(raw_item.get("title") or "")).strip()
    detail_url = str(raw_item.get("detail_url") or "").strip()
    if not title or not detail_url:
        raise ValueError("missing title or detail_url")

    # English title at ingest (site decision: UI is English-only).
    title_en = None
    try:
        en = translate_to_en(title)
        if en:
            title_en = en
    except Exception:
        title_en = None

    resolution = raw_item.get("resolution")
    quality_label, quality_score = _quality_from_resolution(resolution)

    record: Dict[str, Any] = {
        "source_site": "fangsung",
        "source_post_id": str(raw_item.get("source_post_id")),
        "content_type": "video",
        "title": title,
        "title_en": title_en,
        "slug": None,
        "detail_url": detail_url,
        "full_video_url": raw_item.get("full_video_url") or None,
        "video_mime_type": "application/x-mpegURL",
        "thumbnail_url": raw_item.get("thumbnail_url") or None,
        "clean_thumbnail_url": raw_item.get("thumbnail_url") or None,
        "quality_label": quality_label,
        "quality_score": quality_score,
        "duration_seconds": raw_item.get("duration_seconds"),
        "duration_formatted": raw_item.get("duration_formatted"),
        "region": raw_item.get("region") or None,
        "region_en": raw_item.get("region_en") or None,
        "resolution": resolution,
        "categories": ["fangsung"],
        "tags": raw_item.get("tags") or [],
        "status": "active",
        "published_at": raw_item.get("published_at"),
        "raw_payload": {},
    }
    return record
