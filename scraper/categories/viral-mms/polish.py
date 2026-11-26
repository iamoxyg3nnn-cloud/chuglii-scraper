import re
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse

def parse_views_count(views_raw: Optional[str]) -> int:
    """Converts strings like '1K', '2.5M', '350', '1,200' into integer counts."""
    if not views_raw:
        return 0
    cleaned = str(views_raw).strip().upper().replace(",", "").replace("VIEWS", "").strip()
    match = re.match(r"^([\d\.]+)\s*([KMGT]?)$", cleaned)
    if not match:
        digits = re.sub(r"[^\d]", "", cleaned)
        return int(digits) if digits else 0
    
    val, suffix = match.groups()
    try:
        num = float(val)
        multipliers = {"": 1, "K": 1_000, "M": 1_000_000, "G": 1_000_000_000}
        return int(num * multipliers.get(suffix, 1))
    except ValueError:
        return 0

def parse_rating_score(rating_raw: Optional[str]) -> Optional[float]:
    """Converts strings like '100%', '87.5%' into a numeric float between 0 and 100."""
    if not rating_raw:
        return None
    cleaned = str(rating_raw).strip().replace("%", "").strip()
    try:
        score = float(cleaned)
        return min(max(score, 0.0), 100.0)
    except ValueError:
        return None

def parse_duration_seconds(duration_str: Optional[str], duration_hint: Optional[int] = None) -> tuple[int, str]:
    """Returns (duration_seconds, formatted_duration)."""
    if duration_hint and duration_hint > 0:
        total_sec = int(duration_hint)
    elif duration_str:
        parts = [p.strip() for p in str(duration_str).split(":") if p.strip().isdigit()]
        if len(parts) == 2:
            total_sec = int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 3:
            total_sec = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        else:
            total_sec = 0
    else:
        total_sec = 0

    hours = total_sec // 3600
    minutes = (total_sec % 3600) // 60
    seconds = total_sec % 60
    if hours > 0:
        formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        formatted = f"{minutes:02d}:{seconds:02d}"

    return total_sec, formatted

def clean_title_text(title: str) -> str:
    """Normalizes whitespace and removes redundant branding."""
    if not title:
        return ""
    cleaned = re.sub(r"\s+", " ", title).strip()
    cleaned = re.sub(
        r"\s*[-|•]\s*(?:MMSVibeX|SpicyMMS|Spicy\s*MMS|Desi49|MyDesi|IndianBFClips).*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    ).strip()
    return cleaned

def clean_taxonomy_list(items: List[str]) -> List[str]:
    """Normalizes tag or category lists: lowercase, trimmed, deduplicated, sorted."""
    res = set()
    for item in items:
        if not item:
            continue
        cleaned = re.sub(r"\s+", " ", str(item)).strip().lower()
        cleaned = cleaned.strip("- \t\n")
        if cleaned and len(cleaned) > 1:
            res.add(cleaned)
    return sorted(list(res))

def extract_url_slug(url: str) -> str:
    """Extracts clean trailing path slug from URL."""
    if not url:
        return ""
    path = urlparse(url).path.strip("/")
    parts = path.split("/")
    return parts[-1] if parts else ""

def polish_scraped_record(raw: Dict[str, Any], source_site: str = "mmsvibex") -> Dict[str, Any]:
    """
    Transforms raw scraped video dictionary into a clean, normalized,
    production-ready record for Supabase (scraper.videos table) under the 'viral-mms' parent category.
    """
    raw_media = raw.get("media", {})
    raw_meta = raw.get("metadata", {})
    raw_tax = raw.get("taxonomy", {})

    title = clean_title_text(raw.get("title", ""))
    url = raw.get("url", "")
    slug = raw.get("slug") or extract_url_slug(url) or f"viral-mms-{raw.get('post_id', '')}"

    # Enrich multi-part titles from slug if title lacks part indicator
    part_match = re.search(r"-part-?([0-9]+)$", slug, flags=re.IGNORECASE) or re.search(r"-([0-9]{1,2})$", slug)
    if part_match and not re.search(r"part[ -]?[0-9]+", title, flags=re.IGNORECASE):
        part_num = part_match.group(1)
        if not re.search(rf"[ -]{part_num}$", title):
            title = f"{title} - Part {part_num}"

    dur_sec, dur_fmt = parse_duration_seconds(
        raw_meta.get("duration"),
        raw_meta.get("duration_seconds")
    )
    views_count = parse_views_count(raw_meta.get("views"))
    rating_score = parse_rating_score(raw_meta.get("rating"))

    published_at = raw_meta.get("published_at") or raw_meta.get("date_published")
    if published_at:
        try:
            dt = datetime.fromisoformat(str(published_at).replace("Z", "+00:00"))
            published_at = dt.isoformat()
        except Exception:
            published_at = datetime.utcnow().isoformat()
    else:
        published_at = datetime.utcnow().isoformat()

    # 1. Categories: Always include 'viral-mms' parent category
    categories = clean_taxonomy_list(raw_tax.get("categories", []))
    if "viral-mms" not in categories:
        categories.append("viral-mms")

    # 2. Extract and enrich sub-tags
    tags = clean_taxonomy_list(raw_tax.get("tags", []))
    t_low = f"{title.lower()} {' '.join(categories)} {' '.join(tags)}"

    # Auto-detect region/origin tags
    if any(k in t_low for k in ["pakistani", "lahore", "karachi", "urdu", "pashto", "rawalpindi"]):
        if "pakistani" not in tags:
            tags.append("pakistani")
    if any(k in t_low for k in ["indian", "desi", "bhabhi", "delhi", "mumbai", "punjabi", "tamil", "telugu", "mallu", "bengali"]):
        if "indian" not in tags:
            tags.append("indian")
    if any(k in t_low for k in ["bangladeshi", "dhaka"]):
        if "bangladeshi" not in tags:
            tags.append("bangladeshi")

    # Auto-detect content classification tags
    tag_keywords = {
        "desi": "desi-mms",
        "mms": "desi-mms",
        "bhabhi": "bhabhi",
        "viral": "viral-clips",
        "web series": "web-series",
        "ullu": "web-series",
        "amateur": "amateur",
        "village": "village",
        "dehati": "village",
        "teen": "teen",
        "college": "college",
        "hotel": "hotel",
        "couple": "couple",
        "leaked": "leaked",
        "scandal": "scandal"
    }
    for kw, tag_val in tag_keywords.items():
        if kw in t_low and tag_val not in tags:
            tags.append(tag_val)

    tags = sorted(list(set(tags)))
    categories.sort()

    actors = clean_taxonomy_list(raw_tax.get("actors", []))

    # Media stream & thumbnails
    full_video_url = raw_media.get("full_video_url")
    trailer_preview_url = raw_media.get("trailer_preview_url")
    main_thumbnail = raw_media.get("main_thumbnail") or raw_media.get("thumbnail_url")
    clean_thumbnail = raw_media.get("clean_thumbnail") or main_thumbnail

    return {
        "source_site": source_site,
        "source_post_id": str(raw.get("post_id", "")).strip(),
        "title": title,
        "slug": slug,
        "detail_url": url,
        "full_video_url": full_video_url,
        "video_mime_type": raw_media.get("video_mime_type", "video/mp4"),
        "trailer_preview_url": trailer_preview_url,
        "thumbnail_url": main_thumbnail,
        "clean_thumbnail_url": clean_thumbnail,
        "duration_seconds": dur_sec,
        "duration_formatted": dur_fmt,
        "views_count": views_count,
        "views_raw": str(raw_meta.get("views") or "0"),
        "rating_score": rating_score or 0.0,
        "rating_raw": str(raw_meta.get("rating") or "0%"),
        "categories": categories,
        "tags": tags,
        "actors": actors,
        "published_at": published_at,
        "raw_payload": raw
    }
