"""Polishing/normalization for isolated Voyeur House records."""

import re
import urllib.parse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_CATEGORIES = ["voyeurhouse"]


def clean_title(value: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    text = re.sub(r"\s*-\s*Voyeur[\s-]*House.*$", "", text, flags=re.I)
    return text or "Voyeur House Video"


def parse_duration(value: Any) -> Tuple[Optional[int], str]:
    raw = str(value or "").strip()
    if not raw:
        return None, ""
    parts = [p for p in raw.split(":") if p.strip().isdigit()]
    if not parts:
        return None, raw
    nums = [int(p) for p in parts]
    seconds = 0
    for n in nums:
        seconds = seconds * 60 + n
    return seconds, raw


def parse_date(value: Any) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw[:19], fmt).replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            pass
    try:
        iso = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def normalize_url(value: Optional[str]) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parts = urllib.parse.urlsplit(raw)
        path = urllib.parse.quote(parts.path)
        return urllib.parse.urlunsplit((parts.scheme, parts.netloc, path, parts.query, ""))
    except Exception:
        return raw


def polish_voyeurhouse_record(record: Dict[str, Any]) -> Dict[str, Any]:
    source_post_id = str(record.get("source_post_id") or "").strip()
    title = clean_title(record.get("title"))
    thumb = normalize_url(record.get("clean_thumbnail_url") or record.get("thumbnail_url"))
    duration_seconds, duration_formatted = parse_duration(record.get("duration_formatted") or record.get("duration_seconds"))
    servers: List[Dict[str, Any]] = list(record.get("servers") or [])

    full_video_url = normalize_url(record.get("full_video_url"))
    if not full_video_url and servers:
        full_video_url = normalize_url(servers[0].get("url") or servers[0].get("embed_url"))

    status = str(record.get("status") or "active").strip().lower()
    if status not in {"active", "dead", "quarantined"}:
        status = "active"
    if not full_video_url:
        status = "dead"

    raw_payload: Dict[str, Any] = dict(record.get("raw_payload") or {})
    raw_payload.update(
        {
            "source_site": record.get("source_site"),
            "source": "voyeur-house.org",
            "embed_url": record.get("embed_url"),
            "dead_reason": record.get("dead_reason"),
            "servers": servers,
        }
    )

    return {
        "source_site": str(record.get("source_site") or "voyeurhouseorg").strip().lower(),
        "source_post_id": source_post_id,
        "title": title,
        "slug": record.get("slug") or re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:80],
        "detail_url": record.get("detail_url") or "",
        "full_video_url": full_video_url,
        "video_mime_type": record.get("video_mime_type") or ("video/mp4" if full_video_url.lower().endswith(".mp4") else "embed/iframe"),
        "trailer_preview_url": record.get("trailer_preview_url"),
        "thumbnail_url": normalize_url(record.get("thumbnail_url")) or thumb,
        "clean_thumbnail_url": thumb,
        "quality_label": record.get("quality_label") or "720p HD",
        "quality_score": int(record.get("quality_score") or 720),
        "duration_seconds": duration_seconds,
        "duration_formatted": duration_formatted,
        "views_count": int(record.get("views_count") or 0),
        "views_raw": record.get("views_raw") or "0",
        "rating_score": record.get("rating_score"),
        "rating_raw": record.get("rating_raw"),
        "categories": record.get("categories") or DEFAULT_CATEGORIES,
        "tags": record.get("tags") or [],
        "actors": record.get("actors") or [],
        "status": status,
        "published_at": parse_date(record.get("published_at")),
        "scraped_at": record.get("scraped_at"),
        "raw_payload": raw_payload,
    }
