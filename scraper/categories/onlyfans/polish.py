"""
Polishing and Data Normalization Module for OnlyFans Videos
Source: Internet Chicks (https://internetchicks.com)
Parent Category: 'onlyfans'
Stores into Supabase isolated schema: 'scraper.onlyfans_videos' / 'scraper.videos'
"""

import re
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

def clean_onlyfans_title(raw_title: str) -> str:
    """Cleans site branding, 'Video Leaked', 'Leaked', and excess whitespace."""
    if not raw_title:
        return "OnlyFans Leaked Video"
    
    t = raw_title.replace("Internet Chicks", "").replace("InternetChicks.com", "")
    t = re.sub(r"\s*-\s*Internet Chicks.*$", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\bVideo Leaked\b", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\bPhotos Leaked\b", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\bOnlyfans Leaked\b", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+", " ", t).strip()
    return t or raw_title.strip()

def generate_safe_slug(title: str, post_id: str) -> str:
    """Generates a clean URL slug from title and post id."""
    cleaned = re.sub(r"[^\w\s-]", "", title, flags=re.UNICODE).strip().lower()
    cleaned = re.sub(r"[-\s]+", "-", cleaned)
    if not cleaned:
        cleaned = f"onlyfans-video-{post_id}"
    return f"{cleaned[:80]}-{post_id}"

def clean_thumbnail_url(raw_url: Optional[str], srcset: Optional[str] = None) -> Optional[str]:
    """
    Extracts the highest quality clean thumbnail URL.
    Prefers 680x450 or uncropped original over 340x225 placeholder.
    Strips query cache strings like ?x92823, and filters out 1x1 data: base64 placeholders.
    """
    if not raw_url and not srcset:
        return None

    target = raw_url or ""
    if target.startswith("data:"):
        target = ""

    if srcset:
        matches = re.findall(r"(https?://[^\s,]+)(?:\s+\d+[wx])?", srcset)
        if matches:
            target = matches[-1]  # Highest resolution candidate

    if not target or target.startswith("data:"):
        return None

    # Strip query parameters (e.g. ?x92823)
    target = target.split("?")[0].strip()

    # Encode safe URL
    try:
        p = urllib.parse.urlsplit(target)
        quoted_path = urllib.parse.quote(p.path)
        return urllib.parse.urlunsplit((p.scheme, p.netloc, quoted_path, "", ""))
    except Exception:
        return target

clean_image_url = clean_thumbnail_url


def parse_date_to_iso(date_str: Optional[str]) -> Optional[str]:
    """Parses date like 'November 22, 2026' into ISO 8601 UTC string."""
    if not date_str:
        return datetime.now(timezone.utc).isoformat()
    try:
        dt = datetime.strptime(date_str.strip(), "%B %d, %Y")
        return dt.replace(tzinfo=timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()

def prioritize_servers(servers: List[Dict[str, Any]]) -> tuple[Optional[str], List[Dict[str, Any]], str, int]:
    """
    Orders servers strictly by resolution and bitrate quality score.
    Highest quality stream is ALWAYS index 0 (primary full_video_url).
    Returns (primary_embed_url, sorted_servers, quality_label, quality_score).
    """
    if not servers:
        return None, [], "720p HD", 720

    def compute_quality(s: Dict[str, Any]) -> int:
        url = s.get("embed_url", "").lower()
        name = s.get("name", "").lower()
        mime = s.get("type", "").lower()
        combined = f"{name} {url}".lower()

        if any(k in combined for k in ["4k", "2160p", "uhd"]):
            base_res = 2160
        elif any(k in combined for k in ["2k", "1440p", "qhd"]):
            base_res = 1440
        elif any(k in combined for k in ["1080p", "1080", "fhd"]):
            base_res = 1080
        elif any(k in combined for k in ["720p", "720", "hd"]):
            base_res = 720
        elif any(k in combined for k in ["480p", "480", "sd"]):
            base_res = 480
        else:
            if "influencersgonewild.net" in url or "voe.sx" in url:
                base_res = 1080
            elif "sheeshfans.com" in url or "playmogo" in url or "streamtape" in url or "pimpbunny" in url:
                base_res = 720
            else:
                base_res = 480

        multiplier = 1.0
        if mime == "direct_mp4" or ".mp4" in url:
            multiplier = 1.25  # Direct uncompressed MP4
        elif "voe.sx" in url:
            multiplier = 1.15
        elif "playmogo" in url or "streamwish" in url:
            multiplier = 1.10
        elif "streamtape" in url:
            multiplier = 1.00
        elif "dood" in url or "bysezejataos" in url:
            multiplier = 0.85

        return int(base_res * multiplier)

    for s in servers:
        if "quality_score" not in s:
            s["quality_score"] = compute_quality(s)

    sorted_servers = sorted(servers, key=lambda x: x.get("quality_score", 0), reverse=True)
    best = sorted_servers[0]
    primary = best.get("embed_url")
    best_score = best.get("quality_score", 720)
    
    label = (
        "4K UHD" if best_score >= 2000 else
        "1080p Full HD" if best_score >= 1000 else
        "720p HD" if best_score >= 700 else "480p SD"
    )

    return primary, sorted_servers, label, best_score

def polish_onlyfans_record(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Polishes raw scraped InternetChicks OnlyFans item into a schema-compliant record.
    Matches Chuglii scraper standard:
      - source_site: 'internetchicks'
      - source_post_id: e.g. '65140'
      - categories: ['onlyfans']
      - video_mime_type: 'embed/iframe'
    """
    post_id = str(raw_item.get("post_id") or raw_item.get("source_post_id") or "")
    raw_title = raw_item.get("title") or "OnlyFans Video"
    cleaned_title = clean_onlyfans_title(raw_title)

    # Actor / Model
    models = raw_item.get("models") or raw_item.get("actors") or []
    if isinstance(models, str):
        models = [models]
    actors = [m.strip() for m in models if m and m.strip()]

    # If actor not in title, we can prepend actor for clarity: "Bralessforever - Sex At The Bar"
    display_title = cleaned_title
    if actors and actors[0].lower() not in cleaned_title.lower():
        display_title = f"{actors[0]} - {cleaned_title}"

    slug = generate_safe_slug(display_title, post_id)

    # Thumbnail
    raw_thumb = raw_item.get("thumbnail") or raw_item.get("thumbnail_url")
    raw_srcset = raw_item.get("srcset") or raw_item.get("data-srcset")
    clean_thumb = clean_image_url(raw_thumb, raw_srcset)
    valid_thumb = clean_thumb or (raw_thumb if raw_thumb and not str(raw_thumb).startswith("data:") else None)
    if not valid_thumb and post_id and post_id.isdigit():
        folder = (int(post_id) // 1000) * 1000
        valid_thumb = f"https://sheeshfans.com/contents/videos_screenshots/{folder}/{post_id}/preview.jpg"

    # Servers & Primary Embed (strictly prioritized by quality score)
    raw_servers = raw_item.get("servers") or []
    primary_embed, sorted_servers, quality_label, quality_score = prioritize_servers(raw_servers)

    # Published At
    date_str = raw_item.get("date") or raw_item.get("published_at")
    published_at = parse_date_to_iso(date_str)

    # Tags
    raw_tags = raw_item.get("tags") or []
    tags_set = set()
    for t in raw_tags:
        clean_t = t.strip().lower()
        if clean_t and len(clean_t) > 1 and clean_t not in ["internetchicks", "video", "videos"]:
            tags_set.add(clean_t)
    tags_set.add("onlyfans")
    tags_set.add("leaked")
    for a in actors:
        tags_set.add(a.lower())

    source_site = raw_item.get("source_site") or "internetchicks"
    return {
        "source_site": source_site,
        "source_post_id": post_id,
        "title": display_title[:255],
        "slug": slug[:255],
        "detail_url": raw_item.get("url") or raw_item.get("detail_url") or "",
        "full_video_url": primary_embed,
        "video_mime_type": raw_item.get("video_mime_type") or "embed/iframe",
        "trailer_preview_url": primary_embed,
        "thumbnail_url": valid_thumb,
        "clean_thumbnail_url": valid_thumb,
        "quality_label": quality_label,
        "quality_score": quality_score,
        "duration_seconds": raw_item.get("duration_seconds", 600),
        "duration_formatted": raw_item.get("duration_formatted", "10:00"),
        "views_count": int(raw_item.get("views_count", 0)),
        "views_raw": str(raw_item.get("views_raw", "0")),
        "rating_score": float(raw_item.get("rating_score", 95.0)),
        "rating_raw": str(raw_item.get("rating_raw", "95%")),
        "categories": ["onlyfans"],
        "tags": sorted(list(tags_set)),
        "actors": actors,
        "status": "active",
        "published_at": published_at,
        "raw_payload": {
            "source": source_site,
            "category": "onlyfans",
            "models": actors,
            "servers": sorted_servers,
            "mirrors": sorted_servers,
            "gallery_url": raw_item.get("gallery_url")
        }
    }
