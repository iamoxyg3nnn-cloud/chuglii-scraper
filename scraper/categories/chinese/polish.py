"""
Polishing and Data Normalization Module for Chinese Category Videos
Parent Category: 'chinese'
Records are persisted strictly into scraper.chinese_videos via its RPC bridge.
"""

import re
from typing import Dict, Any, List, Optional

from .translate import translate_to_en


def clean_chinese_title(raw_title: str) -> str:
    """Normalizes whitespace and strips site-brand noise from titles."""
    if not raw_title:
        return "Chinese Video"
    t = re.sub(r"\s+", " ", str(raw_title)).strip()
    # Some titles embed emoji placeholders / zero-width chars from the site.
    t = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", t)
    return t or "Chinese Video"


def generate_safe_slug(title: str, post_id: str, content_type: str = "video") -> str:
    """Generates a clean URL slug from title and post id (CJK-safe)."""
    cleaned = re.sub(r"[^\w\s-]", "", title, flags=re.UNICODE).strip().lower()
    cleaned = re.sub(r"[-\s]+", "-", cleaned)
    prefix = "chinese-torrent" if content_type == "torrent" else "chinese-video"
    if not cleaned:
        cleaned = f"{prefix}-{post_id}"
    return f"{cleaned[:80]}-{post_id}"


def parse_size_to_mb(size_label: Optional[str]) -> Optional[int]:
    """Parses '238 MB' / '1.2 GB' / '856 KB' into megabytes (best effort)."""
    if not size_label:
        return None
    m = re.search(r"([\d.]+)\s*(KB|MB|GB|TB)", str(size_label), re.I)
    if not m:
        return None
    try:
        val = float(m.group(1))
    except ValueError:
        return None
    unit = m.group(2).upper()
    if unit == "KB":
        return int(val / 1024)
    if unit == "MB":
        return int(val)
    if unit == "GB":
        return int(val * 1024)
    if unit == "TB":
        return int(val * 1024 * 1024)
    return None


def _quality_from_resolution(resolution: Optional[str]) -> tuple:
    """Maps a resolution string to the standard quality label/score.

    Accepts both '1280x720'-style dims and '1080p'-style labels (the site's
    torrent 'tr' field uses the latter). Uses the min-dimension "p"
    convention so portrait (592x1280) and landscape (1280x592) both read as
    '592p', matching the ffprobe-probed labels used for videos. Unknown
    formats fall back to the generic 'HD' label, never a fake resolution.
    """
    if not resolution:
        return "HD", 720
    text = str(resolution).strip().lower()
    m = re.search(r"(\d{3,4})\s*[x×*]\s*(\d{3,4})", text)
    if m:
        # "p" value = smaller dimension (portrait/landscape agnostic)
        p = min(int(m.group(1)), int(m.group(2)))
    else:
        m2 = re.search(r"(\d{3,4})\s*p", text)
        if not m2:
            return "HD", 720
        p = int(m2.group(1))
    return f"{p}p", p


def polish_chinese_record(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Polishes a raw scraped uxj838238 item into a schema-compliant record for
    scraper.chinese_videos. Videos carry an HLS m3u8 in full_video_url;
    torrents carry magnet/torrent metadata and no playable URL.
    Titles are translated to English (Google Translate) into title_en; the
    original CJK title is preserved in title for source fidelity.
    """
    post_id = str(raw_item.get("source_post_id") or "")
    content_type = str(raw_item.get("content_type") or "video").strip() or "video"
    cleaned_title = clean_chinese_title(raw_item.get("title") or "")
    slug = generate_safe_slug(cleaned_title, post_id, content_type)

    cat_en = str(raw_item.get("category_name_en") or "").strip()
    cat_cn = str(raw_item.get("category_name_cn") or "").strip()
    cat_id = str(raw_item.get("category_id") or "").strip()
    cat_group = str(raw_item.get("category_group") or "chinese").strip().lower()

    thumb = raw_item.get("thumbnail_url") or raw_item.get("clean_thumbnail_url")

    tags: List[str] = []
    seen_tags = set()
    for t in [cat_en, cat_cn, "chinese", cat_group, content_type]:
        clean_t = re.sub(r"\s+", " ", str(t or "").strip().lower())
        if not clean_t or len(clean_t) < 2 or len(clean_t) > 80 or clean_t in seen_tags:
            continue
        seen_tags.add(clean_t)
        tags.append(clean_t)

    if content_type == "torrent":
        quality_label, quality_score = _quality_from_resolution(raw_item.get("resolution"))
        duration_seconds = raw_item.get("duration_seconds")
        try:
            duration_seconds = int(duration_seconds) if duration_seconds else None
        except Exception:
            duration_seconds = None
        duration_formatted = raw_item.get("duration_formatted")
    else:
        # Probed real resolution from _enrich_video; 'HD' fallback (never a
        # fake '720p HD') when the probe could not run.
        quality_label = str(raw_item.get("quality_label") or "HD")
        quality_score = int(raw_item.get("quality_score") or 720)
        duration_seconds = None
        duration_formatted = None

    return {
        "source_site": raw_item.get("source_site") or "uxj838238",
        "source_post_id": post_id,
        "content_type": content_type,
        "title": cleaned_title[:255],
        "title_en": (translate_to_en(cleaned_title) or "")[:255] if cleaned_title else "",
        "slug": slug[:255],
        "detail_url": raw_item.get("detail_url") or "",
        "full_video_url": raw_item.get("full_video_url") or None,
        "video_mime_type": raw_item.get("video_mime_type") or (
            "application/x-mpegURL" if content_type == "video" else None
        ),
        "thumbnail_url": thumb,
        "clean_thumbnail_url": raw_item.get("clean_thumbnail_url") or thumb,
        "quality_label": quality_label,
        "quality_score": quality_score,
        "duration_seconds": duration_seconds,
        "duration_formatted": duration_formatted,
        "category_id": cat_id or None,
        "category_name_cn": cat_cn or None,
        "category_name_en": cat_en or None,
        "category_group": cat_group,
        "magnet_uri": raw_item.get("magnet_uri"),
        "magnet_hash": raw_item.get("magnet_hash"),
        "torrent_file_url": raw_item.get("torrent_file_url"),
        "size_label": raw_item.get("size_label"),
        "resolution": raw_item.get("resolution"),
        "cdn_endpoints": raw_item.get("cdn_endpoints") or [],
        "screenshots": raw_item.get("screenshots") or [],
        "categories": ["chinese"],
        "tags": tags,
        "status": "active",
        "published_at": raw_item.get("published_at"),  # site exposes no dates -> None
        "raw_payload": {
            "source": raw_item.get("source_site") or "uxj838238",
            "category": "chinese",
            "category_id": cat_id or None,
            "category_name_cn": cat_cn or None,
            "category_name_en": cat_en or None,
            "category_group": cat_group,
            "content_type": content_type,
            "size_mb": parse_size_to_mb(raw_item.get("size_label")) if content_type == "torrent" else None,
            "cdn_endpoints": raw_item.get("cdn_endpoints") or [],
            "screenshots": (raw_item.get("screenshots") or [])[:24],
            **(raw_item.get("raw_payload") or {}),
        },
    }
