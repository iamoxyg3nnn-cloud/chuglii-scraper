"""
Source Scraper for SheeshFans (https://sheeshfans.com)

Improved with robust flashvars extraction, stable preview/screenshot metadata,
quality-aware direct stream handling, and conservative fallbacks.
"""
import re
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin

from scraper import net as _net

try:
    from scrapling import Fetcher  # type: ignore
except Exception:  # pragma: no cover - fallback for minimal envs
    Fetcher = None
    import requests

    from bs4 import BeautifulSoup

    class _Node:
        def __init__(self, el):
            self._el = el
            self.attrib = dict(el.attrs or {})
            class _T:
                def __init__(self, t):
                    self._t = t
                def clean(self):
                    return re.sub(r"\s+", " ", self._t or "").strip()
            self.text = _T(el.get_text(" ", strip=True))

        def css(self, selector: str):
            try:
                return [_Node(x) for x in self._el.select(selector)]
            except Exception:
                return []

    class _Resp:
        def __init__(self, status: int, html_content: str, url: str):
            self.status = status
            self.html_content = html_content
            self.text = html_content
            self.url = url
            self._soup = BeautifulSoup(html_content or "", "html.parser")

        def css(self, selector: str):
            try:
                return [_Node(x) for x in self._soup.select(selector)]
            except Exception:
                return []

    class _FetcherFallback:
        @staticmethod
        def get(url: str, timeout: int = 25, headers: Optional[Dict[str, str]] = None):
            r = requests.get(url, timeout=timeout, headers=headers or {}, allow_redirects=True)
            return _Resp(r.status_code, r.text, str(r.url))

    Fetcher = _FetcherFallback

BASE_URL = "https://sheeshfans.com"
CATEGORY_URL = f"{BASE_URL}/categories/onlyfans/"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
NAV_TAG_NOISE = {
    "onlyfans", "fans only", "only fans", "video", "videos", "photo", "photos",
    "leak", "leaked", "leaks",
}


def _clean(text: Any) -> str:
    if text is None:
        return ""
    try:
        if hasattr(text, "clean"):
            return str(text.clean()).strip()
    except Exception:
        pass
    return re.sub(r"\s+", " ", str(text)).strip()


def _abs(url: Optional[str]) -> str:
    if not url:
        return ""
    u = str(url).strip()
    if not u:
        return ""
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("/"):
        u = urljoin(BASE_URL, u)
    return u


def _quality_from_text(*values: Any) -> tuple[str, int]:
    blob = " ".join(str(v or "") for v in values).lower()
    if re.search(r"(?<!\d)(2160|4k|uhd)(?!\d)", blob):
        return "2160p 4K", 2160
    if re.search(r"(?<!\d)(1440|2k|qhd)(?!\d)", blob):
        return "1440p HD", 1440
    if re.search(r"(?<!\d)(1080|fhd)(?!\d)", blob):
        return "1080p Full HD", 1080
    if re.search(r"(?<!\d)720(?!\d)", blob):
        return "720p HD", 720
    if re.search(r"(?<!\d)480(?!\d)", blob):
        return "480p SD", 480
    return "720p HD", 720


def _is_probably_mp4(url: str) -> bool:
    return bool(re.search(r"\.mp4(?:[/?#]|$)", url or "", re.I))


def fetch_page(url: str, timeout: int = 25, retries: int = 3) -> Optional[Any]:
    """Fetch via the shared Cloudflare-aware, cached HTTP layer."""
    page = _net.fetch(url, timeout=timeout, retries=retries, referer=BASE_URL + "/")
    if page is not None and page.status == 200 and not page.challenge:
        return page
    if page is not None and page.via != "dead-cache" and page.status not in (404, 410):
        reason = "Cloudflare challenge" if page.challenge else f"HTTP {page.status}"
        print(f"    [SheeshFans] fetch failed {url}: {reason}")
    return None


def _extract_flashvars(html: str) -> Dict[str, str]:
    m = re.search(r"var\s+flashvars\s*=\s*\{(.*?)\};", html or "", re.S)
    if not m:
        return {}
    block = m.group(1)
    out: Dict[str, str] = {}
    for mm in re.finditer(r"([a-zA-Z0-9_]+)\s*:\s*'([^']*)'", block):
        out[mm.group(1)] = mm.group(2).strip()
    return out


def _extract_meta(resp: Any, key: str, attr: str = "property") -> str:
    for m in resp.css(f"meta[{attr}]"):
        if (m.attrib.get(attr) or "").lower() == key.lower():
            val = _clean(m.attrib.get("content"))
            if val:
                return val
    return ""


def scrape_listing(page_num: int) -> tuple[List[Dict[str, Any]], Optional[int]]:
    if page_num <= 1:
        url = CATEGORY_URL
    else:
        # Real pagination is path based (/categories/onlyfans/2/).
        url = f"{CATEGORY_URL}{page_num}/"

    resp = fetch_page(url)
    if not resp:
        return [], None

    items = []
    seen_ids = set()

    for a in resp.css("a[href*='/video/']"):
        href = a.attrib.get("href", "")
        m = re.search(r"/video/(\d+)/([^/]+)/", href)
        if not m:
            continue
        vid_id = m.group(1)
        if vid_id in seen_ids:
            continue
        seen_ids.add(vid_id)

        slug = m.group(2)
        title = _clean(a.attrib.get("title")) or _clean(getattr(a, "text", ""))
        if not title:
            title = slug.replace("-", " ").title()

        img = a.css("img")
        thumb = ""
        if img:
            attrs = img[0].attrib
            thumb = _abs(attrs.get("data-src") or attrs.get("data-original") or attrs.get("src") or "")
        detail_url = _abs(href)

        items.append({
            "source_site": "sheeshfans",
            "source_post_id": str(vid_id),
            "title": title,
            "detail_url": detail_url,
            "thumbnail_url": thumb,
            "models": [],
            "published_at": None,
        })

    max_page = page_num
    for pl in resp.css("a[href*='/categories/onlyfans/']"):
        href = pl.attrib.get("href", "")
        m = re.search(r"/categories/onlyfans/(\d+)/", href)
        if m:
            max_page = max(max_page, int(m.group(1)))
    for n in resp.css("a[rel='next'], link[rel='next']"):
        href = n.attrib.get("href", "")
        m = re.search(r"/categories/onlyfans/(\d+)/", href)
        if m:
            max_page = max(max_page, int(m.group(1)))

    return items, max_page


def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    url = card.get("detail_url")
    vid_id = str(card.get("source_post_id") or "").strip()
    if not url:
        return card

    resp = fetch_page(url)
    embed_url = f"https://sheeshfans.com/embed/{vid_id}" if vid_id else (url or "")

    if not resp:
        card_copy = dict(card)
        card_copy["servers"] = [{
            "name": "SheeshFans Embed",
            "server": "SheeshFans",
            "embed_url": embed_url,
            "type": "iframe",
            "quality_label": "720p HD",
            "quality_score": 720,
        }]
        card_copy["full_video_url"] = embed_url
        card_copy["video_mime_type"] = "embed/iframe"
        return card_copy

    html = getattr(resp, "html_content", "") or ""
    fv = _extract_flashvars(html)

    real_title = _clean(fv.get("video_title")) or _extract_meta(resp, "og:title") or _clean(card.get("title"))
    models = [m.strip() for m in (fv.get("video_models") or "").split(",") if m.strip()]
    if not models:
        for a in resp.css("a[href*='/models/']"):
            txt = _clean(getattr(a, "text", ""))
            if txt and txt.lower() != "models" and txt not in models:
                models.append(txt)

    tags = []
    seen_tags = set()
    raw_tags = [t.strip() for t in (fv.get("video_tags") or "").split(",") if t.strip()]
    for t in raw_tags:
        low = re.sub(r"\s+", " ", t.lower()).strip()
        if not low or len(low) < 2 or len(low) > 80 or low in NAV_TAG_NOISE or low in seen_tags:
            continue
        seen_tags.add(low)
        tags.append(low)
    if len(tags) < 3:
        for a in resp.css("a[href*='/tags/']"):
            txt = _clean(getattr(a, "text", "")).lower()
            if not txt or txt in seen_tags or txt in NAV_TAG_NOISE or len(txt) < 2:
                continue
            seen_tags.add(txt)
            tags.append(txt)

    # Quality: prefer explicit URL/DOM quality, avoid random DOM "1080p" recommendations.
    dom_quality = ""
    for sel in [".video-quality", ".quality", ".hd", ".badge-quality"]:
        el = resp.css(sel)
        if el:
            dom_quality = _clean(getattr(el[0], "text", ""))
            if dom_quality:
                break
    quality_label, quality_score = _quality_from_text(fv.get("video_url"), fv.get("video_url_text"), dom_quality, real_title)

    servers: List[Dict[str, Any]] = []
    seen_server_urls = set()

    def add_server(name: str, server: str, embed: str, stype: str, qlabel: str, qscore: int):
        u = _abs(embed)
        if not u or u in seen_server_urls:
            return
        seen_server_urls.add(u)
        servers.append({
            "name": name,
            "server": server,
            "embed_url": u,
            "type": stype,
            "quality_label": qlabel,
            "quality_score": qscore,
            "headers": {"Referer": BASE_URL + "/"},
        })

    # Direct MP4 candidates from flashvars. Avoid event_reporting2 unless clearly mp4 with video path.
    for key in ["video_url", "video_alt_url", "video_alt_url2", "video_alt_url3", "video_alt_url4"]:
        val = _abs(fv.get(key))
        if val and _is_probably_mp4(val) and "/get_file/" in val:
            ql, qs = _quality_from_text(val, quality_label)
            add_server(f"SheeshFans Direct {ql}", "SheeshFans CDN", val, "direct_mp4", ql, qs)

    er2 = _abs(fv.get("event_reporting2"))
    if er2 and _is_probably_mp4(er2) and "/get_file/" in er2:
        ql, qs = _quality_from_text(er2, quality_label)
        add_server(f"SheeshFans Direct {ql}", "SheeshFans CDN", er2, "direct_mp4", ql, qs)

    # Always include embed as fallback.
    add_server(f"SheeshFans Embed ({quality_label})", "SheeshFans", embed_url, "iframe", quality_label, max(480, quality_score - 120))

    # Thumbnail / preview
    preview_jpg = _abs(fv.get("preview_url"))
    preview_poster = _abs(fv.get("preview_url1")) or _abs(fv.get("preview_url2"))
    og_image = _abs(_extract_meta(resp, "og:image"))
    thumb = preview_jpg or og_image or _abs(card.get("thumbnail_url"))

    # Screenshots: keep stable static preview images only. Timeline templates can be unavailable/heavy.
    screenshots = []
    seen_shots = set()
    for key in ["preview_url1", "preview_url", "preview_url2"]:
        v = _abs(fv.get(key))
        if not v or v in seen_shots or _is_probably_mp4(v):
            continue
        # Keep image assets only.
        if not re.search(r"\.(?:jpe?g|png|webp)(?:[?#]|$)", v, re.I):
            continue
        seen_shots.add(v)
        screenshots.append(v)
    screenshots = screenshots[:12]

    # Duration: prefer og:video:duration (seconds); flashvars "duration" can be ad/pre-roll seconds.
    duration_seconds = 0
    for cand in [_extract_meta(resp, "og:video:duration"), _extract_meta(resp, "video:duration")]:
        if cand and str(cand).strip().isdigit():
            duration_seconds = int(str(cand).strip())
            break
    if duration_seconds <= 0:
        try:
            dur_raw = str(fv.get("duration") or "").strip()
            if dur_raw.isdigit() and int(dur_raw) > 30:
                duration_seconds = int(dur_raw)
        except Exception:
            duration_seconds = 0

    # Published date not reliably present; keep existing or og:video:release_date if present.
    published_at = card.get("published_at") or _extract_meta(resp, "og:video:release_date") or _extract_meta(resp, "article:published_time") or None

    # Choose primary URL: prefer direct mp4, else embed.
    direct_servers = [s for s in servers if s.get("type") == "direct_mp4"]
    direct_servers.sort(key=lambda s: int(s.get("quality_score") or 0), reverse=True)
    primary = direct_servers[0]["embed_url"] if direct_servers else embed_url
    mime = "video/mp4" if direct_servers else "embed/iframe"
    best_quality_label = direct_servers[0]["quality_label"] if direct_servers else quality_label
    best_quality_score = int(direct_servers[0]["quality_score"]) if direct_servers else quality_score

    merged = dict(card)
    merged["title"] = real_title or card.get("title")
    merged["thumbnail_url"] = thumb or card.get("thumbnail_url")
    merged["servers"] = sorted(servers, key=lambda s: (s.get("type") != "direct_mp4", -int(s.get("quality_score") or 0)))
    merged["full_video_url"] = primary
    merged["video_mime_type"] = mime
    merged["quality_label"] = best_quality_label
    merged["quality_score"] = best_quality_score
    merged["tags"] = tags
    merged["models"] = models
    merged["actors"] = models
    merged["published_at"] = published_at
    merged["duration_seconds"] = duration_seconds
    merged["duration_formatted"] = f"{duration_seconds // 60}:{duration_seconds % 60:02d}" if duration_seconds > 0 else card.get("duration_formatted")
    # Use stable poster image as preview; signed mp4 tokens expire quickly.
    merged["trailer_preview_url"] = preview_poster or preview_jpg or None
    merged["screenshots"] = screenshots
    merged["raw_payload"] = {
        **(card.get("raw_payload") or {}),
        "source": "sheeshfans",
        "flashvars_video_id": fv.get("video_id"),
        "preview_url": preview_poster or preview_jpg or None,
        "preview_image_url": preview_jpg or None,
        "og_image": og_image or None,
        "screenshots": screenshots,
        "timeline_screens_url": _abs(fv.get("timeline_screens_url")) or None,
    }
    return merged
