"""
Polish and normalization module for SupJav videos.
Extracts clean maker codes, cleans noise from titles, and formats payload.
"""

import re
from typing import Dict, Any, List

def clean_supjav_title(title: str) -> str:
    """Removes annoying tags, emojis, and marketing noise from SupJav titles."""
    if not title:
        return "Untitled Video"
    
    # Remove bracketed labels like [Uncensored], [Discontinued], [Flexible], etc.
    cleaned = re.sub(r"\[[^\]]*\]", "", title)
    
    # Remove emoji characters
    cleaned = re.sub(r"[\U00010000-\U0010ffff]", "", cleaned)
    cleaned = re.sub(r"[\u2000-\u3300]", "", cleaned)
    
    # Collapse excess whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or title.strip()

def extract_maker_code(title: str) -> str:
    """Extracts standard maker codes like FC2PPV-123456, SNOS-123, FNS-123."""
    match = re.search(r"\b([A-Z0-9]{3,8}[-_ ]\d{3,8})\b", title, re.IGNORECASE)
    if match:
        return match.group(1).upper().replace(" ", "-")
    return ""

def polish_supjav_record(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """Polishes raw scraped SupJav dictionary into a schema-compliant record."""
    raw_title = raw_item.get("title", "")
    maker_code = extract_maker_code(raw_title)
    cleaned_title = clean_supjav_title(raw_title)
    
    # Prefix maker code if not present in cleaned title
    if maker_code and maker_code not in cleaned_title.upper():
        cleaned_title = f"{maker_code} {cleaned_title}"

    # Slug generation
    slug_base = maker_code.lower() if maker_code else re.sub(r"[^a-z0-9]+", "-", cleaned_title.lower())
    slug = f"{slug_base[:60]}-{raw_item.get('source_post_id', 'vid')}"

    # Clean thumbnail (strip sizing parameter !320x216.jpg)
    thumb = raw_item.get("thumbnail_url", "")
    clean_thumb = thumb.split("!")[0] if thumb else ""

    # Merge tags
    tags = set([t.lower().strip() for t in raw_item.get("tags", []) if t and len(t) > 1])
    if maker_code:
        tags.add(maker_code.lower().split("-")[0])
    tags.add("japanese")
    tags.add("jav")

    # Categories (strictly isolated under 'japanese')
    raw_cats = [c.lower().strip() for c in raw_item.get("categories", []) if c and c != "viral-mms"]
    categories = list(set(["japanese"] + raw_cats))

    # Servers list: prioritize zero-restriction lockers (VOE, VidSpeed, LuluStream)
    servers = raw_item.get("servers", [])
    primary_embed = ""
    if servers:
        voe_server = next((s for s in servers if "voe" in s["name"].lower()), None)
        vidspeed_server = next((s for s in servers if "vidspeed" in s["name"].lower() or "vas" in s["name"].lower()), None)
        lulu_server = next((s for s in servers if "lulu" in s["name"].lower() or "luc" in s["name"].lower()), None)
        best = voe_server or vidspeed_server or lulu_server or servers[0]
        primary_embed = best["embed_url"]

    return {
        "source_site": "supjav",
        "source_post_id": str(raw_item.get("source_post_id", "")),
        "title": cleaned_title[:255],
        "slug": slug[:255],
        "detail_url": raw_item.get("detail_url", ""),
        "full_video_url": primary_embed or None,
        "video_mime_type": "embed/iframe",
        "trailer_preview_url": primary_embed or None,
        "thumbnail_url": thumb,
        "clean_thumbnail_url": clean_thumb or thumb,
        "duration_seconds": raw_item.get("duration_seconds", 3600),
        "duration_formatted": raw_item.get("duration_formatted", "01:00:00"),
        "views_count": int(raw_item.get("views_count", 0)),
        "views_raw": str(raw_item.get("views_raw", "0")),
        "rating_score": float(raw_item.get("rating_score", 95.0)),
        "rating_raw": str(raw_item.get("rating_raw", "95%")),
        "categories": categories,
        "tags": list(tags),
        "actors": raw_item.get("actors", []),
        "raw_payload": {
            "source": "supjav",
            "maker_code": maker_code,
            "embed_url": primary_embed,
            "servers": servers
        }
    }
