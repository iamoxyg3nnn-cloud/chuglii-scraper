"""
Polishing and Data Normalization Module for OnlyFans Videos
Parent Category: 'onlyfans'
Records are persisted through the selected source's isolated table and route.
"""

import re
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

def clean_onlyfans_title(raw_title: str) -> str:
    """Cleans site branding, 'Video Leaked', 'Leaked', and excess whitespace."""
    if not raw_title:
        return "OnlyFans Leaked Video"
    
    t = re.sub(r"\bVideo Leaked\b", "", raw_title, flags=re.IGNORECASE)
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
    """Parses InfluencersGoneWild and SheeshFans dates into ISO 8601 UTC string."""
    if not date_str:
        return None
    raw = str(date_str).strip()
    if not raw:
        return None
    # ISO first
    try:
        iso = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    for fmt in ("%B %d, %Y, %H:%M", "%B %d, %Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            continue
    # Fallback: keep unknown instead of fabricating "now"
    return None

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
            elif "sheeshfans.com" in url or "playmogo" in url or "streamtape" in url:
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
    
    declared_label = str(best.get("quality_label") or "").strip()
    # Normalize bad URL labels; keep semantic labels like Auto.
    if declared_label.lower() in {"auto", "original / unspecified"}:
        label = declared_label
    elif re.match(r"^https?://", declared_label, re.I):
        m = re.search(r"(?<!\d)(2160|1440|1080|720|480|360)p?(?!\d)", declared_label, re.I)
        label = f"{m.group(1)}p" if m else "Original"
    elif re.search(r"\b(2160|1440|1080|720|480|360)p?\b", declared_label, re.I):
        m = re.search(r"\b(2160|1440|1080|720|480|360)p?\b", declared_label, re.I)
        label = f"{m.group(1)}p" if m else declared_label
    else:
        label = (
            "4K UHD" if best_score >= 2000 else
            "1440p QHD" if best_score >= 1350 else
            "1080p Full HD" if best_score >= 1000 else
            "720p HD" if best_score >= 700 else
            "480p SD" if best_score >= 400 else "Original"
        )

    return primary, sorted_servers, label, best_score

def format_duration(seconds: Optional[int]) -> Optional[str]:
    """Seconds -> ``M:SS`` / ``H:MM:SS``. None when unknown.

    Sources that only expose a raw second count (borntobefuck's listing cards)
    would otherwise fall through to the table default ``10:00``.
    """
    try:
        total = int(seconds or 0)
    except (TypeError, ValueError):
        return None
    if total <= 0:
        return None
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}" if hours else f"{minutes:02d}:{secs:02d}"


def polish_onlyfans_record(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Polishes raw scraped OnlyFans item into a schema-compliant record.
    Matches Chuglii scraper standard:
      - source_site: 'influencersgonewild' | 'sheeshfans'
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
    trailer_preview_url = raw_item.get("trailer_preview_url") or raw_item.get("preview_url")
    if not trailer_preview_url and sorted_servers:
        best_server = sorted_servers[0]
        if best_server.get("type") in {"direct_mp4", "direct_webm"}:
            # Fall back to the chosen direct rendition only when no dedicated
            # preview clip exists. The frontend loads it only after hover.
            trailer_preview_url = primary_embed

    # Published At
    date_str = raw_item.get("date") or raw_item.get("published_at")
    published_at = parse_date_to_iso(date_str)

    source_site = raw_item.get("source_site") or "influencersgonewild"

    # Tags - dedupe, preserve useful model/content tags.
    raw_tags = raw_item.get("tags") or []
    if isinstance(raw_tags, str):
        raw_tags = [raw_tags]
    seen_tags = set()
    tags = []
    for t in list(raw_tags) + ["onlyfans", "leaked"] + [a.lower() for a in actors]:
        clean_t = re.sub(r"\s+", " ", str(t or "").strip().lower())
        if not clean_t or len(clean_t) < 2 or len(clean_t) > 80:
            continue
        if clean_t in {"video", "videos", "photo", "photos"}:
            continue
        if clean_t in seen_tags:
            continue
        seen_tags.add(clean_t)
        tags.append(clean_t)

    _duration_seconds = int(raw_item.get("duration_seconds") or 0) or None
    return {
        "source_site": source_site,
        "source_post_id": post_id,
        "title": display_title[:255],
        "slug": slug[:255],
        "detail_url": raw_item.get("url") or raw_item.get("detail_url") or "",
        "full_video_url": primary_embed,
        "video_mime_type": raw_item.get("video_mime_type") or "embed/iframe",
        "trailer_preview_url": trailer_preview_url,
        "thumbnail_url": valid_thumb,
        "clean_thumbnail_url": valid_thumb,
        "quality_label": quality_label,
        "quality_score": quality_score,
        "duration_seconds": _duration_seconds,
        # Derive the display string when a source only knows the seconds. Without
        # this the table default '10:00' is stored, which is a lie for every
        # video that is not exactly ten minutes.
        "duration_formatted": (
            str(raw_item.get("duration_formatted") or "").strip()
            or format_duration(_duration_seconds)
        ),
        "views_count": int(raw_item.get("views_count", 0)),
        "views_raw": str(raw_item.get("views_raw", "0")),
        "rating_score": float(raw_item.get("rating_score", 95.0)),
        "rating_raw": str(raw_item.get("rating_raw", "95%")),
        "categories": ["onlyfans"],
        "tags": tags,
        "actors": actors,
        "status": "active",
        "published_at": published_at,
        "raw_payload": {
            "source": source_site,
            "category": "onlyfans",
            "models": actors,
            "servers": sorted_servers,
            "mirrors": sorted_servers,
            "preview_url": trailer_preview_url,
            "gallery_url": raw_item.get("gallery_url"),
            "screenshots": [
                u for u in dict.fromkeys([
                    str(x).strip() for x in (raw_item.get("screenshots") or raw_item.get("raw_payload", {}).get("screenshots") or [])
                    if isinstance(x, str) and str(x).strip() and not str(x).strip().startswith("data:")
                ])
            ][:40],
        }
    }
