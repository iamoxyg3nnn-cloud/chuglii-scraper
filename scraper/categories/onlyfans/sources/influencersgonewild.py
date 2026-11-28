"""
Source Scraper for InfluencersGoneWild (https://influencersgonewild.com)
"""
import re
import time
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlsplit

from scraper import net as _net
from scraper.categories.onlyfans.media_probe import probe_media
from scraper.categories.onlyfans.polish import format_duration

try:
    from scrapling import Fetcher  # type: ignore
except Exception:
    Fetcher = None
    import requests
    from bs4 import BeautifulSoup

    class _Node:
        def __init__(self, el):
            self._el = el
            self.attrib = dict(el.attrs or {})
            class _T:
                def __init__(self,t): self._t=t
                def clean(self): return re.sub(r"\s+", " ", self._t or "").strip()
            self.text = _T(el.get_text(" ", strip=True))
        def css(self, selector):
            try: return [_Node(x) for x in self._el.select(selector)]
            except Exception: return []

    class _Resp:
        def __init__(self, status, html_content, url):
            self.status=status; self.html_content=html_content; self.text=html_content; self.url=url
            self._soup=BeautifulSoup(html_content or "", "html.parser")
        def css(self, selector):
            try: return [_Node(x) for x in self._soup.select(selector)]
            except Exception: return []

    class _FetcherFallback:
        @staticmethod
        def get(url, timeout=25, headers=None, follow_redirects=True):
            r=requests.get(url, timeout=timeout, headers=headers or {}, allow_redirects=follow_redirects)
            return _Resp(getattr(r,'status_code',0), r.text, str(getattr(r,'url',url)))
    Fetcher=_FetcherFallback


BASE_URL = "https://influencersgonewild.com"
CATEGORY_URL = f"{BASE_URL}/category/onlyfans/"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
GENERIC_TAGS = {
    "videos", "video", "photos", "photo", "cosplay", "full nude", "latina", "asian",
    "ebony", "onlyfans", "fansly", "patreon", "youtube", "tiktok", "twitch",
    "instagram", "celebrity", "leaked", "leaks", "nude", "nudes", "popular",
    "main categories", "popular categories", "nipple slips", "leaked nudes",
}
GENERIC_CATEGORIES = {
    "main categories", "popular categories", "nipple slips", "leaked nudes",
    "onlyfans", "fansly", "patreon", "youtube", "tiktok", "twitch", "instagram", "celebrity",
}


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if hasattr(value, "clean"):
            return str(value.clean()).strip()
    except Exception:
        pass
    return re.sub(r"\s+", " ", str(value)).strip()


def normalize_cdn_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return url
    u = str(url).strip()
    if u.startswith("//"):
        u = "https:" + u
    return re.sub(r"https?://cdn\d+\.influencersgonewild\.net", "https://cdn04.influencersgonewild.net", u, flags=re.I)


def fetch_page(url: str, timeout: int = 25, retries: int = 3) -> Optional[Any]:
    """Fetch via the shared Cloudflare-aware, cached HTTP layer."""
    page = _net.fetch(url, timeout=timeout, retries=retries, referer=BASE_URL + "/")
    if page is not None and page.status == 200 and not page.challenge and not page.error:
        return page
    if page is not None and page.via != "dead-cache" and page.status not in (404, 410):
        reason = "Cloudflare challenge" if page.challenge else f"HTTP {page.status}"
        print(f"    [IGW] fetch failed {url}: {reason}")
    return None


def _extract_max_page(resp: Any) -> Optional[int]:
    max_page = None
    for el in resp.css("a.page-numbers, .pagination a, a[href*='/page/'], link[rel='next'], a[rel='next']"):
        href = el.attrib.get("href", "") or ""
        m = re.search(r"/category/onlyfans/page/(\d+)/?", href)
        if not m:
            m = re.search(r"/page/(\d+)/?", href)
        if m:
            max_page = max(max_page or 0, int(m.group(1)))
    return max_page


def _extract_img(el: Any) -> str:
    if not el:
        return ""
    attrs = el.attrib
    for k in ("data-src", "data-lazy-src", "data-original", "src"):
        v = (attrs.get(k) or "").strip()
        if v and not v.startswith("data:"):
            return normalize_cdn_url(v) or ""
    srcset = attrs.get("data-srcset") or attrs.get("srcset") or ""
    urls = re.findall(r"(https?://[^\s,]+)", srcset)
    if urls:
        return normalize_cdn_url(urls[-1]) or ""
    return ""


def _extract_meta(resp: Any, key: str, attr: str = "property") -> Optional[str]:
    for m in resp.css(f"meta[{attr}]"):
        if (m.attrib.get(attr) or "").lower() == key.lower():
            val = (m.attrib.get("content") or "").strip()
            if val:
                return val
    return None


def scrape_listing(page_num: int) -> tuple[List[Dict[str, Any]], Optional[int]]:
    url = CATEGORY_URL if page_num <= 1 else f"{BASE_URL}/category/onlyfans/page/{page_num}/"
    resp = fetch_page(url)
    if not resp:
        return [], None

    articles = resp.css("article.post, article")
    items = []
    for art in articles:
        cls_raw = art.attrib.get("class", ""); classes = cls_raw.split() if isinstance(cls_raw, str) else list(cls_raw or [])
        post_id = next((c[5:] for c in classes if c.startswith("post-") and c[5:].isdigit()), None)
        title_el = art.css(".entry-title a, h2 a, h3 a, a[rel='bookmark']")
        if not title_el:
            continue
        title = _clean_text(getattr(title_el[0], "text", "")) or art.attrib.get("aria-label", "")
        detail_url = urljoin(BASE_URL, title_el[0].attrib.get("href") or "")
        if not post_id or not detail_url or "/category/" in urlsplit(detail_url).path.rstrip("/"):
            continue

        img = art.css("img")
        thumb = _extract_img(img[0]) if img else ""
        time_el = art.css("time")
        published_at = ""
        if time_el:
            published_at = time_el[0].attrib.get("datetime") or _clean_text(getattr(time_el[0], "text", ""))

        models: List[str] = []
        categories: List[str] = ["onlyfans"]
        for cl in art.css("a[href*='/category/']"):
            txt = _clean_text(getattr(cl, "text", ""))
            href = (cl.attrib.get("href") or "").lower()
            low = txt.lower()
            if not txt:
                continue
            if "/category/main-categories/" in href or low in GENERIC_CATEGORIES:
                if low and low not in categories and low not in {"main categories", "popular categories"}:
                    categories.append(low)
                continue
            if low not in {"popular", "nipple slips", "leaked nudes"} and txt not in models:
                models.append(txt)

        items.append({
            "source_site": "influencersgonewild",
            "source_post_id": str(post_id),
            "title": title,
            "detail_url": detail_url,
            "thumbnail_url": thumb,
            "models": models,
            "categories": list(dict.fromkeys(categories)),
            "published_at": published_at,
        })

    max_page = _extract_max_page(resp)
    # 404 pages on IGW may return page shell with 1 unrelated article; treat as end.
    if page_num > 1 and not items:
        max_page = page_num - 1
    return items, max_page


def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    url = card.get("detail_url")
    if not url:
        return card

    resp = fetch_page(url)
    if not resp:
        return card

    html = ""
    try:
        html = str(getattr(resp, "html_content", "") or getattr(resp, "text", "") or "")
    except Exception:
        html = ""

    # Direct MP4: prefer <source src>, then <video src>, then script/html scan.
    mp4_url = None
    for sel in ["video source[src]", "video source[data-src]", "source[type='video/mp4']", "video[src]", "video[data-src]"]:
        for el in resp.css(sel):
            src = el.attrib.get("src") or el.attrib.get("data-src")
            if src and ".mp4" in src.lower():
                mp4_url = normalize_cdn_url(src)
                break
        if mp4_url:
            break
    if not mp4_url:
        for pat in [
            r'https?://[^\s\"\'<>]+?\.mp4(?:\?[^\s\"\'<>]*)?',
            r'"file"\s*:\s*"(https?://[^"]+?\.mp4[^"]*)"',
            r'"src"\s*:\s*"(https?://[^"]+?\.mp4[^"]*)"',
        ]:
            m = re.search(pat, html, re.I)
            if m:
                mp4_url = normalize_cdn_url(m.group(1) if m.lastindex else m.group(0))
                break

    servers = []
    if mp4_url:
        quality = "1080p Full HD" if re.search(r"1080|fullhd|fhd", mp4_url, re.I) else "Original"
        qscore = 1080 if quality.startswith("1080") else 720
        servers.append({
            "name": f"Direct {quality} MP4 (CDN)",
            "server": "InfluencersGoneWild CDN",
            "embed_url": mp4_url,
            "type": "direct_mp4",
            "quality_label": quality,
            "quality_score": qscore,
            "headers": {"Referer": BASE_URL + "/"},
        })

    # Tags: keep specific tags, drop only global nav/footer noise. Keep model/content tags.
    tags = []
    seen_tags = set()
    nav_noise = {"videos", "video", "photos", "photo", "cosplay", "full nude", "latina", "asian", "ebony"}
    for t in resp.css("a[href*='/tag/']"):
        txt = _clean_text(getattr(t, "text", ""))
        low = re.sub(r"\s+", " ", txt.lower()).strip()
        if not low or len(low) < 2 or len(low) > 80:
            continue
        if low in nav_noise:
            continue
        # Avoid exact generic platform-only tag if better model tags exist later; keep for now.
        if low in seen_tags:
            continue
        seen_tags.add(low)
        tags.append(low)

    # Screenshots: only main post-content images; exclude related/thumb/nav noise.
    screenshots = []
    seen_img = set()
    content_selectors = [
        ".entry-content img",
        ".post-content img",
        ".g1-content-narrow .entry-content img",
        ".wp-block-image img",
        "article .entry-content figure img",
    ]
    content_imgs = []
    for sel in content_selectors:
        content_imgs.extend(resp.css(sel))
    if not content_imgs:
        content_imgs = resp.css("article img")
    for img in content_imgs:
        src = ""
        attrs = img.attrib or {}
        for k in ("data-src", "data-lazy-src", "data-original", "data-large-file", "src"):
            v = (attrs.get(k) or "").strip()
            if v and not v.startswith("data:"):
                src = normalize_cdn_url(v) or ""
                break
        if not src or src in seen_img:
            continue
        low_src = src.lower()
        if any(x in low_src for x in ["/logo", "avatar", "icon", "favicon", "emoji", "banner-ad", "gravatar", "/plugins/", "/themes/"]):
            continue
        # Skip tiny WP thumbnails/crops; keep likely full-size screenshots.
        if re.search(r"-\d{2,3}x\d{2,3}\.(?:jpe?g|png|webp)(?:\?|$)", low_src):
            continue
        if not any(x in low_src for x in ["/wp-content/uploads/", "cdn04.influencersgonewild.net", "cdn0"]):
            continue
        cls_raw = attrs.get("class") or ""; cls = " ".join(cls_raw.split() if isinstance(cls_raw,str) else list(cls_raw)).lower()
        if any(x in cls for x in ["avatar", "logo", "icon", "attachment-thumbnail", "wp-post-image"]):
            # wp-post-image can be featured; allow only if large upload and not tiny crop (already filtered)
            if "wp-post-image" in cls and re.search(r"-\d{2,3}x\d{2,3}\.", low_src):
                continue
        seen_img.add(src)
        screenshots.append(src)
    screenshots = screenshots[:24]

    # Models/categories from detail categories.
    models = list(dict.fromkeys([m for m in (card.get("models") or []) if m]))
    categories = list(dict.fromkeys([c for c in (card.get("categories") or ["onlyfans"]) if c]))
    for cl in resp.css("a[href*='/category/']"):
        txt = _clean_text(getattr(cl, "text", ""))
        href = (cl.attrib.get("href") or "").lower()
        low = txt.lower()
        if not txt:
            continue
        if "/category/main-categories/" in href or low in GENERIC_CATEGORIES:
            if low not in categories and low not in {"main categories", "popular categories"}:
                categories.append(low)
            continue
        if low not in {"popular", "nipple slips", "leaked nudes"} and txt not in models:
            models.append(txt)

    # Fallback: infer model from title prefix before dash.
    if not models:
        title = _clean_text(card.get("title"))
        if " - " in title:
            cand = title.split(" - ", 1)[0].strip()
            if 2 <= len(cand) <= 60 and cand.lower() not in GENERIC_TAGS:
                models = [cand]

    published_at = (
        _extract_meta(resp, "article:published_time")
        or _extract_meta(resp, "og:updated_time")
        or card.get("published_at")
    )
    og_image = normalize_cdn_url(_extract_meta(resp, "og:image") or _extract_meta(resp, "twitter:image") or card.get("thumbnail_url"))
    description = _extract_meta(resp, "description") or _extract_meta(resp, "og:description")

    # Duration: this site publishes none. There is no og:video:duration, no
    # JSON-LD duration and no flashvars value (verified against live pages), so
    # a value can only come from the media header itself. Without this the row
    # fell through to the table default 600 s -- every influencersgonewild video
    # on the site showed "10:00".
    duration_seconds, _height = probe_media(
        mp4_url or "", referer="https://influencersgonewild.com/",
    )

    merged = dict(card)
    merged["servers"] = servers
    merged["full_video_url"] = mp4_url
    if duration_seconds:
        merged["duration_seconds"] = duration_seconds
        merged["duration_formatted"] = format_duration(duration_seconds)
    merged["video_mime_type"] = "video/mp4" if mp4_url else "embed/iframe"
    merged["thumbnail_url"] = og_image or merged.get("thumbnail_url")
    merged["published_at"] = published_at
    merged["tags"] = tags
    merged["models"] = models
    merged["actors"] = models
    merged["categories"] = list(dict.fromkeys(categories or ["onlyfans"]))
    merged["screenshots"] = screenshots
    merged["raw_payload"] = {
        **(card.get("raw_payload") or {}),
        "source": "influencersgonewild",
        "og_image": og_image,
        "description": description,
        "detail_fetched": bool(resp),
        "screenshots": screenshots,
    }
    return merged


