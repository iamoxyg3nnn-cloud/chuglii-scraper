"""
Source Scraper for PimpBunny (https://pimpbunny.com)
Kernel Video Sharing (KVS) platform for OnlyFans / adult content creator leaks.
"""
import re
import json
import html as html_lib
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlsplit

BASE_URL = "https://pimpbunny.com"
CATEGORY_URL = f"{BASE_URL}/videos/"
SHIELD_CHALLENGE_MARKERS = (
    "establishing a secure connection",
    "submitting... request id",
    "/.bunny-shield/assets/challenge-styles.css",
    'class="challenge-footer"',
)


def _pimpbunny_path(href: str) -> Optional[str]:
    """Return a same-site URL path, ignoring query strings and fragments."""
    parsed = urlsplit((href or "").strip())
    if parsed.scheme and parsed.scheme.lower() not in {"http", "https"}:
        return None
    if parsed.hostname and parsed.hostname.lower() not in {"pimpbunny.com", "www.pimpbunny.com"}:
        return None
    if parsed.netloc and not parsed.hostname:
        return None
    return parsed.path


def _find_video_object(value: Any) -> Optional[Dict[str, Any]]:
    """Find a VideoObject in a JSON-LD object, list, or @graph."""
    if isinstance(value, dict):
        schema_type = value.get("@type")
        if schema_type == "VideoObject" or (
            isinstance(schema_type, list) and "VideoObject" in schema_type
        ):
            return value
        for nested in value.values():
            match = _find_video_object(nested)
            if match:
                return match
    elif isinstance(value, list):
        for nested in value:
            match = _find_video_object(nested)
            if match:
                return match
    return None


def _normalize_kvs_url(value: str, page_url: str) -> str:
    value = (value or "").replace("\\/", "/").replace("\\u0026", "&").replace("\\x26", "&")
    value = html_lib.unescape(value.strip())
    if not value:
        return ""
    return urljoin(page_url, value)


def _extract_kvs_streams(page_html: str, page_url: str) -> List[Dict[str, Any]]:
    """Extract publicly embedded KVS video variants without executing page JS."""
    assignments: Dict[str, str] = {}
    pattern = re.compile(
        r"(?P<key>['\"]?(?:video_url|video_alt_url\d*)(?:_text)?['\"]?)\s*[:=]\s*"
        r"(?P<quote>['\"])(?P<value>.*?)(?P=quote)",
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(page_html):
        key = match.group("key").strip("'\"").lower()
        value = _normalize_kvs_url(match.group("value"), page_url)
        if value:
            assignments[key] = value

    # KVS flashvars can appear in unpacked/nonstandard forms; also scan for
    # direct signed get_file media URLs and infer quality from path.
    if not assignments:
        for m in re.finditer(
            r"https?://(?:[a-z0-9.-]+\.)?pimpbunny\.com/get_file/[^\s\"'<>]+?\.(?:mp4|m3u8|webm)(?:/[^\s\"'<>]*)?(?:\?[^\s\"'<>]*)?",
            page_html,
            re.IGNORECASE,
        ):
            url = _normalize_kvs_url(m.group(0), page_url)
            if "/get_file/" not in url:
                continue
            path = urlsplit(url).path.rstrip("/").lower()
            if re.search(r"_preview(?:[_./]|$)", path):
                continue
            q = 0
            qm = re.search(r"(?<!\d)(2160|1440|1080|720|480|360)p?(?!\d)", path)
            if qm:
                q = int(qm.group(1))
            key = f"video_alt_url{q or len(assignments)}"
            assignments.setdefault(key, url)
            if q:
                assignments.setdefault(f"{key}_text", f"{q}p")

    variants_by_url: Dict[str, Dict[str, Any]] = {}
    for key, value in assignments.items():
        if key.endswith("_text"):
            continue
        label = assignments.get(f"{key}_text", "").strip()
        stream_url = value
        parsed = urlsplit(stream_url)
        hostname = (parsed.hostname or "").lower()
        if parsed.scheme.lower() != "https" or not (
            hostname == "pimpbunny.com" or hostname.endswith(".pimpbunny.com")
        ):
            continue

        # KVS appends a trailing slash to signed media URLs (for example,
        # ``12345_1080p.mp4/``). Normalize it before checking the extension.
        path = parsed.path.rstrip("/").lower()
        if path.endswith(".mp4"):
            stream_type = "direct_mp4"
        elif path.endswith(".m3u8"):
            stream_type = "direct_hls"
        elif path.endswith(".webm"):
            stream_type = "direct_webm"
        else:
            continue

        quality_source = f"{label} {parsed.path}".lower()
        if re.search(r"(?:4k|uhd|2160p?)", quality_source):
            quality = 2160
        else:
            quality_match = re.search(r"(?<!\d)(1440|1080|720|480|360)p?(?!\d)", quality_source)
            quality = int(quality_match.group(1)) if quality_match else 0

        quality_label = label or (f"{quality}p" if quality else "Original / Unspecified")
        # Unknown renditions rank below explicitly labelled variants.
        quality_score = int(quality * (1.25 if stream_type == "direct_mp4" else 1.0)) if quality else 0
        variant = {
            "name": f"PimpBunny {quality_label}",
            "embed_url": stream_url,
            "type": stream_type,
            "quality_label": quality_label,
            "quality_score": quality_score,
            "headers": {"Referer": "https://pimpbunny.com/"},
        }
        existing = variants_by_url.get(stream_url)
        if not existing or variant["quality_score"] > existing["quality_score"]:
            variants_by_url[stream_url] = variant

    return sorted(variants_by_url.values(), key=lambda v: v.get("quality_score", 0), reverse=True)


def _extract_kvs_preview(page_html: str, page_url: str) -> Optional[str]:
    """Find a public KVS preview clip/animation URL, without executing scripts."""
    pattern = re.compile(
        r"(?P<key>['\"]?(?:video_preview_url|preview_video_url|trailer_preview_url|preview_url|video_preview)['\"]?)\s*[:=]\s*"
        r"(?P<quote>['\"])(?P<value>.*?)(?P=quote)",
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(page_html):
        value = match.group("value").replace("\\/", "/").replace("\\u0026", "&")
        value = html_lib.unescape(value.strip())
        if not value:
            continue
        preview_url = urljoin(page_url, value)
        parsed = urlsplit(preview_url)
        hostname = (parsed.hostname or "").lower()
        if parsed.scheme.lower() != "https" or not (
            hostname == "pimpbunny.com" or hostname.endswith(".pimpbunny.com")
        ):
            continue
        if re.search(r"\.(?:mp4|webm|webp|gif|mp4_preview)$", parsed.path, re.IGNORECASE):
            return preview_url
    return None


def _has_class_prefix(tag: Any, prefix: str) -> bool:
    return any(str(class_name).startswith(prefix) for class_name in tag.get("class", []))


def _scoped_link_labels(
    soup: Any,
    container_prefix: str,
    href_fragment: str,
    excluded_labels: tuple[str, ...] = (),
) -> List[str]:
    """Extract labels only from the main video's corresponding metadata list."""
    labels = []
    for container in soup.find_all(lambda tag: tag.name in {"ul", "div"} and _has_class_prefix(tag, container_prefix)):
        for anchor in container.find_all("a", href=True):
            if href_fragment not in anchor["href"]:
                continue
            label = anchor.get_text(" ", strip=True)
            if label and label.lower() not in excluded_labels:
                labels.append(label)
    return labels

def parse_iso_duration(dur_str: Optional[str]) -> tuple[int, str]:
    """Parses ISO 8601 duration (e.g. 'PT0H15M11S' or '15:11') into (seconds, 'MM:SS')."""
    if not dur_str:
        return 600, "10:00"
    
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", dur_str)
    if m:
        h = int(m.group(1) or 0)
        m_val = int(m.group(2) or 0)
        s = int(m.group(3) or 0)
        total_sec = h * 3600 + m_val * 60 + s
        if h > 0:
            formatted = f"{h:02d}:{m_val:02d}:{s:02d}"
        else:
            formatted = f"{m_val:02d}:{s:02d}"
        return total_sec, formatted

    m2 = re.match(r"(\d+):(\d+)(?::(\d+))?", dur_str.strip())
    if m2:
        parts = [int(p) for p in m2.groups() if p is not None]
        if len(parts) == 3:
            total_sec = parts[0] * 3600 + parts[1] * 60 + parts[2]
            formatted = f"{parts[0]:02d}:{parts[1]:02d}:{parts[2]:02d}"
        else:
            total_sec = parts[0] * 60 + parts[1]
            formatted = f"{parts[0]:02d}:{parts[1]:02d}"
        return total_sec, formatted

    return 600, "10:00"

def fetch_page(url: str, timeout: int = 25) -> Optional[str]:
    """Fetch a public page with Scrapling's standard Fetcher."""
    try:
        from scrapling import Fetcher

        response = Fetcher.get(url, timeout=timeout)
        if response.status != 200:
            return None
        html = response.html_content or ""
        lowered = html.lower()
        if any(marker in lowered for marker in SHIELD_CHALLENGE_MARKERS):
            return None
        return html or None
    except Exception:
        return None

def scrape_listing(page_num: int) -> tuple[List[Dict[str, Any]], Optional[int]]:
    """Scrapes video listings from https://pimpbunny.com/videos/"""
    if page_num <= 1:
        url = CATEGORY_URL
    else:
        url = f"{BASE_URL}/videos/{page_num}/"

    html = fetch_page(url)
    if not html:
        return [], None

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    items = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue

        # Match same-site video detail links: /videos/<slug>/
        path = _pimpbunny_path(href)
        m = re.fullmatch(r"/videos/([a-zA-Z0-9_-]+)/?", path or "")
        if not m:
            continue

        slug = m.group(1).lower()
        if slug.isdigit() or len(slug) < 5:
            continue

        # Real video cards on PimpBunny have an img tag inside
        img = a.find("img")
        if not img:
            continue

        full_url = f"{BASE_URL}/videos/{slug}/"
        thumb = img.get("data-original") or img.get("data-src") or img.get("src") or ""
        if thumb.startswith("data:"):
            thumb = ""

        title = a.get("title") or img.get("alt") or a.text.strip()
        if not title:
            title = slug.replace("-", " ").title()

        items.append({
            "source_site": "pimpbunny",
            "source_post_id": slug,
            "title": title,
            "detail_url": full_url,
            "thumbnail_url": thumb,
            "models": [],
            "published_at": None
        })

    # Estimate max page from pagination
    max_page = None
    for pl in soup.find_all("a", href=True):
        page_path = _pimpbunny_path(pl["href"])
        m_page = re.fullmatch(r"/videos/(\d+)/?", page_path or "")
        if m_page:
            p_val = int(m_page.group(1))
            if max_page is None or p_val > max_page:
                max_page = p_val

    return items, max_page

def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    """Fetches video detail and extracts KVS embed iframe and metadata."""
    url = card.get("detail_url")
    if not url:
        return card

    html = fetch_page(url)
    if not html:
        return card

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # 1. Parse structured JSON-LD VideoObject
    video_obj = None
    for sc in soup.find_all("script", type="application/ld+json"):
        try:
            video_obj = _find_video_object(json.loads(sc.string or sc.get_text()))
            if video_obj:
                break
        except Exception:
            pass

    # 2. Extract numeric Video ID
    video_id = None
    m_id = re.search(r"videoId:\s*['\"](\d+)['\"]", html)
    if m_id:
        video_id = m_id.group(1)
    elif video_obj and video_obj.get("contentUrl"):
        m2 = re.search(r"/(\d+)/\d+_\w+\.mp4", video_obj["contentUrl"])
        if m2:
            video_id = m2.group(1)
    elif video_obj and video_obj.get("thumbnailUrl"):
        m3 = re.search(r"/videos_screenshots/\d+/(\d+)/", video_obj["thumbnailUrl"])
        if m3:
            video_id = m3.group(1)

    # 3. Models extraction (OnlyFans creators linked directly)
    models = _scoped_link_labels(
        soup,
        container_prefix="pages-view-video-models__",
        href_fragment="/onlyfans-creators/",
        excluded_labels=("onlyfans creators", "view more"),
    )

    # 4. Tags from the main video's category list; exclude footer and related content.
    tags = _scoped_link_labels(
        soup,
        container_prefix="pages-view-video-categories__",
        href_fragment="/tags/",
        excluded_labels=("tags",),
    )

    real_title = (video_obj.get("name") if video_obj else None) or (soup.title.string.replace("| PimpBunny", "").strip() if soup.title else None) or card.get("title")

    # Duration
    duration_sec = 600
    duration_fmt = "10:00"
    if video_obj and video_obj.get("duration"):
        duration_sec, duration_fmt = parse_iso_duration(video_obj["duration"])

    # Thumbnail (crystal-clear 800x450 preview)
    thumbnail = (video_obj.get("thumbnailUrl") if video_obj else None) or card.get("thumbnail_url")
    if video_id and (not thumbnail or thumbnail.startswith("data:")):
        folder = (int(video_id) // 1000) * 1000
        thumbnail = f"https://pimpbunny.com/contents/videos_screenshots/{folder}/{video_id}/preview.jpg"

    # 5. Prefer openly embedded KVS media variants, ordered by declared resolution.
    servers = _extract_kvs_streams(html, url)
    preview_url = _extract_kvs_preview(html, url)
    if not servers and video_obj and video_obj.get("contentUrl"):
        # JSON-LD often repeats the public primary MP4 when the player config is absent.
        servers = _extract_kvs_streams(
            f"video_url: {json.dumps(video_obj['contentUrl'])}", url
        )

    servers.sort(key=lambda server: server.get("quality_score", 0), reverse=True)
    embed_url = servers[0]["embed_url"] if servers else None
    if not embed_url and video_id:
        embed_url = f"https://pimpbunny.com/embed/{video_id}"
        servers.append({
            "name": "PimpBunny Auto Player",
            "embed_url": embed_url,
            "type": "iframe",
            "quality_label": "Auto",
            "quality_score": 720,
            "headers": {"Referer": "https://pimpbunny.com/"}
        })

    best_type = servers[0].get("type") if servers else "iframe"
    if not preview_url and best_type in {"direct_mp4", "direct_webm"}:
        # When the source publishes no preview clip, use the selected direct
        # rendition only on hover; the frontend keeps it unloaded otherwise.
        preview_url = embed_url
    mime_types = {
        "direct_mp4": "video/mp4",
        "direct_hls": "application/vnd.apple.mpegurl",
        "direct_webm": "video/webm",
    }

    merged = dict(card)
    # Keep the listing slug as the stable source_post_id. The numeric KVS id
    # is only available after enrichment, so overwriting the id here meant the
    # known-ID cache (keyed by slug) never matched and every slug was skipped.
    if video_id:
        merged["video_id"] = str(video_id)
    merged["title"] = real_title
    merged["thumbnail_url"] = thumbnail
    merged["clean_thumbnail_url"] = thumbnail
    merged["duration_seconds"] = duration_sec
    merged["duration_formatted"] = duration_fmt
    merged["full_video_url"] = embed_url
    merged["video_mime_type"] = mime_types.get(best_type, "embed/iframe")
    merged["preview_url"] = preview_url
    merged["trailer_preview_url"] = preview_url
    merged["servers"] = servers
    merged["models"] = models
    merged["tags"] = tags
    merged["published_at"] = video_obj.get("uploadDate") if video_obj else card.get("published_at")
    return merged
