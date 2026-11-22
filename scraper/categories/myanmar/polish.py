"""
Polishing and Data Normalization Module for Myanmar Videos
Source: Buu Mal (https://www.buumal.com)
Stores into Supabase isolated schema: 'scraper.myanmar_videos'
"""

import re
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

def generate_safe_slug(title: str, post_id: str) -> str:
    """Generates a clean URL slug from title and post id."""
    cleaned = re.sub(r"[^\w\s-]", "", title, flags=re.UNICODE).strip().lower()
    cleaned = re.sub(r"[-\s]+", "-", cleaned)
    if not cleaned:
        cleaned = f"myanmar-video-{post_id}"
    return f"{cleaned[:80]}-{post_id}"

def clean_thumbnail_url(raw_url: Optional[str]) -> Optional[str]:
    """Encodes non-ASCII and space characters in thumbnail URLs so browsers load them."""
    if not raw_url:
        return None
    try:
        p = urllib.parse.urlsplit(raw_url.strip())
        quoted_path = urllib.parse.quote(p.path)
        return urllib.parse.urlunsplit((p.scheme, p.netloc, quoted_path, p.query, p.fragment))
    except Exception:
        return raw_url

def parse_relative_time(rel_str: Optional[str]) -> Optional[str]:
    """Converts relative strings like '1 hour ago', '2 days ago', '1 year ago' into ISO timestamp."""
    if not rel_str:
        return None
    rel_str = rel_str.strip().lower()
    now = datetime.now(timezone.utc)
    
    m = re.search(r"(\d+)\s*(sec|second|min|minute|hour|day|week|month|year)s?\s*ago", rel_str)
    if not m:
        return now.isoformat()
    
    val = int(m.group(1))
    unit = m.group(2)
    
    if "sec" in unit:
        dt = now - timedelta(seconds=val)
    elif "min" in unit:
        dt = now - timedelta(minutes=val)
    elif "hour" in unit:
        dt = now - timedelta(hours=val)
    elif "day" in unit:
        dt = now - timedelta(days=val)
    elif "week" in unit:
        dt = now - timedelta(weeks=val)
    elif "month" in unit:
        dt = now - timedelta(days=val * 30)
    elif "year" in unit:
        dt = now - timedelta(days=val * 365)
    else:
        dt = now
    
    return dt.isoformat()

def clean_title(raw_title: str) -> str:
    """Strips site branding, redundant badges, and normalize Burmese title."""
    if not raw_title:
        return "Myanmar Private Video"
    t = raw_title.replace("Buu Mal (ဘူးမယ်)", "").replace("Buu Mal", "")
    t = re.sub(r"^\s*✅\s*", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t or "Myanmar Private Video"

def polish_buumal_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Polishes raw scraped video data from Buu Mal into schema format."""
    post_id = str(raw.get("source_post_id", "")).strip()
    raw_title = raw.get("title", "")
    title = clean_title(raw_title)
    slug = generate_safe_slug(title, post_id)
    
    thumb = clean_thumbnail_url(raw.get("thumbnail_url"))
    clean_thumb = thumb
    
    published_at = parse_relative_time(raw.get("relative_time"))
    
    # Process taxonomy
    raw_tags = raw.get("tags") or []
    tags = []
    for tag in raw_tags:
        cleaned_tag = str(tag).strip().lower()
        if cleaned_tag and cleaned_tag not in tags and len(cleaned_tag) < 50:
            tags.append(cleaned_tag)
    if "myanmar" not in tags:
        tags.insert(0, "myanmar")
        
    categories = ["myanmar"]
    
    stream_url = raw.get("stream_url") or raw.get("full_video_url")
    detail_url = raw.get("detail_url") or f"https://www.buumal.com/video/{post_id}"
    
    # Raw payload for server mirrors and metadata
    raw_payload = raw.get("raw_payload") or {}
    raw_payload.update({
        "source": "buumal",
        "relative_time": raw.get("relative_time"),
        "original_title": raw_title,
        "detail_url": detail_url
    })
    if stream_url:
        raw_payload["stream_url"] = stream_url

    return {
        "source_site": "buumal",
        "source_post_id": post_id,
        "title": title,
        "slug": slug,
        "detail_url": detail_url,
        "full_video_url": stream_url,
        "video_mime_type": "video/mp4",
        "trailer_preview_url": None,
        "thumbnail_url": thumb,
        "clean_thumbnail_url": clean_thumb,
        "duration_seconds": raw.get("duration_seconds"),
        "duration_formatted": raw.get("duration_formatted") or "HD",
        "views_count": int(raw.get("views_count") or 0),
        "views_raw": raw.get("views_raw"),
        "rating_score": float(raw.get("rating_score") or 100.0),
        "rating_raw": "100%",
        "categories": categories,
        "tags": tags,
        "actors": [],
        "published_at": published_at,
        "raw_payload": raw_payload
    }
