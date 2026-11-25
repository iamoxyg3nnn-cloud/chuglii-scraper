"""
Source Scraper for InternetChicks (https://internetchicks.com)

Improved: robust fetch/retry, better metadata extraction, normalized server quality,
tag/model cleanup, screenshots/gallery extraction, and scrapling fallback.
"""
import re
import time
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin

try:
    from scrapling import Fetcher  # type: ignore
except Exception:  # pragma: no cover
    Fetcher = None
    import requests
    from bs4 import BeautifulSoup

    class _Node:
        def __init__(self, el):
            self._el = el
            self.attrib = dict(el.attrs or {})
            class _T:
                def __init__(self, t): self._t = t
                def clean(self): return re.sub(r"\s+", " ", self._t or "").strip()
            self.text = _T(el.get_text(" ", strip=True))
        def css(self, selector: str):
            try: return [_Node(x) for x in self._el.select(selector)]
            except Exception: return []

    class _Resp:
        def __init__(self, status: int, html_content: str, url: str):
            self.status = status
            self.html_content = html_content
            self.text = html_content
            self.url = url
            self._soup = BeautifulSoup(html_content or "", "html.parser")
        def css(self, selector: str):
            try: return [_Node(x) for x in self._soup.select(selector)]
            except Exception: return []

    class _FetcherFallback:
        @staticmethod
        def get(url: str, timeout: int = 25, headers: Optional[Dict[str, str]] = None):
            r = requests.get(url, timeout=timeout, headers=headers or {}, allow_redirects=True)
            return _Resp(r.status_code, r.text, str(r.url))
    Fetcher = _FetcherFallback

BASE_URL = "https://internetchicks.com"
CATEGORY_URL = f"{BASE_URL}/category/onlyfans/"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
TAG_NOISE = {"video", "videos", "photo", "photos", "leak", "leaked", "leaks", "onlyfans"}

# Hosts that are permanently shut down, parked, or return 404/410 across all embeds.
DEAD_HOST_SUBSTRINGS = (
    "sbfull.", "sbfast.", "sblongvu.", "sbchill.", "sbbrisk.", "sbthe.",
    "sbanh.", "sblanh.", "sbspeed.", "sblona.", "sbrapid.", "sbface.",
    "sbhight.", "sbrity.", "sbsonic.", "likessb.", "lvturbo.",
    "slwatch.", "sltube.", "slmaxed.", "streamlare.",
    "fembed.", "upvideo.", "short.ink", "mixdrop.vc",
)

# Cloudflare-protected hosts that return 403 challenge to non-browser scrapers
# even when the video is live in a real browser.
CF_CHALLENGED_HOSTS = (
    "dood", "d000d", "d0000d", "d0o0d", "dooood", "do0od", "dooodster",
    "ds2play", "ds2video", "dsvplay", "do7go", "d-s.io", "doply",
    "myvidplay", "vidply", "vide0", "vvide0", "playmogo",
)


def _is_embed_alive(embed_url: str) -> bool:
    """Filter out permanently dead hosts and verify checkable embeds (Streamtape/Vidoza/VOE/Filemoon)."""
    low = embed_url.lower()
    if any(dead in low for dead in DEAD_HOST_SUBSTRINGS):
        return False
    if any(cf in low for cf in CF_CHALLENGED_HOSTS):
        return True
    try:
        import requests as _req
        r = _req.get(
            embed_url,
            timeout=6,
            allow_redirects=True,
            headers={"User-Agent": UA, "Referer": BASE_URL + "/"},
        )
        if r.status_code in (404, 410):
            return False
        body = (r.text or "")[:8000]
        if "streamtape" in low and ('"fileid":"nofile"' in body or "Video not found!" in body):
            return False
        if ("vidoza" in low or "videzz" in low) and ("File was deleted" in body or "404 Not Found" in body):
            return False
        if "parklogic" in r.url.lower() or "parklogic" in body.lower():
            return False
    except Exception:
        # Network/ISP block (e.g. Streamtape in India) — do not falsely drop.
        return True
    return True


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
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("/"):
        u = urljoin(BASE_URL, u)
    return u


def _extract_meta(resp: Any, key: str, attr: str = "property") -> str:
    for m in resp.css(f"meta[{attr}]"):
        if (m.attrib.get(attr) or "").lower() == key.lower():
            val = _clean(m.attrib.get("content"))
            if val:
                return val
    return ""


def fetch_page(url: str, timeout: int = 25, retries: int = 3) -> Optional[Any]:
    last_err = None
    for attempt in range(retries):
        try:
            resp = Fetcher.get(url, timeout=timeout, headers={
                "User-Agent": UA,
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": BASE_URL + "/",
            })
            status = getattr(resp, "status", 0)
            if status == 200:
                return resp
            if status in {403, 429, 500, 502, 503, 504} and attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
                continue
            last_err = f"HTTP {status}"
            break
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
    if last_err:
        print(f"    [IC] fetch failed {url}: {last_err}")
    return None


def _server_from_embed(embed_url: str) -> Dict[str, Any]:
    u = embed_url.lower()
    if "voe.sx" in u or "voe." in u:
        return {"name": "VOE (1080p HD)", "server": "VOE", "quality_label": "1080p Full HD", "quality_score": 1080, "type": "iframe"}
    if "playmogo" in u or "streamwish" in u:
        return {"name": "PlayMogo (720p HD)", "server": "PlayMogo", "quality_label": "720p HD", "quality_score": 720, "type": "iframe"}
    if "streamtape" in u:
        return {"name": "Streamtape (720p)", "server": "Streamtape", "quality_label": "720p HD", "quality_score": 720, "type": "iframe"}
    if "dood" in u or "bysezejataos" in u:
        return {"name": "Doodstream (480p SD)", "server": "Doodstream", "quality_label": "480p SD", "quality_score": 480, "type": "iframe"}
    host = re.sub(r"^www\.", "", re.findall(r"https?://([^/]+)", embed_url)[0]) if re.findall(r"https?://([^/]+)", embed_url) else "Embed"
    return {"name": f"{host} Embed", "server": host, "quality_label": "720p HD", "quality_score": 720, "type": "iframe"}


def scrape_listing(page_num: int) -> tuple[List[Dict[str, Any]], Optional[int]]:
    url = CATEGORY_URL if page_num <= 1 else f"{BASE_URL}/category/onlyfans/page/{page_num}/"
    resp = fetch_page(url)
    if not resp:
        return [], None

    items = []
    for art in resp.css("article"):
        cls_raw = art.attrib.get("class", "")
        classes = cls_raw.split() if isinstance(cls_raw, str) else list(cls_raw or [])
        post_id = next((c[5:] for c in classes if c.startswith("post-") and c[5:].isdigit()), None)
        title_el = art.css(".entry-title a, h2 a, h3 a, a[rel='bookmark']")
        if not title_el:
            continue
        title = _clean(getattr(title_el[0], "text", "")) or _clean(art.attrib.get("aria-label"))
        detail_url = _abs(title_el[0].attrib.get("href"))
        if not post_id or not detail_url:
            continue

        models = []
        for c in classes:
            if c.startswith("actress-"):
                m = c.replace("actress-", "").replace("-", " ").strip().title()
                if m and m not in models:
                    models.append(m)

        img_el = art.css("img")
        thumb = ""
        srcset = ""
        if img_el:
            attrs = img_el[0].attrib
            srcset = attrs.get("data-srcset") or attrs.get("srcset") or ""
            for k in ("data-src", "data-lazy-src", "data-original", "src"):
                v = (attrs.get(k) or "").strip()
                if v and not v.startswith("data:"):
                    thumb = _abs(v)
                    break
            if not thumb and srcset:
                urls = re.findall(r"(https?://[^\s,]+)", srcset)
                if urls:
                    thumb = _abs(urls[-1])

        time_el = art.css("time")
        published_at = ""
        if time_el:
            published_at = _clean(time_el[0].attrib.get("datetime")) or _clean(getattr(time_el[0], "text", ""))

        items.append({
            "source_site": "internetchicks",
            "source_post_id": str(post_id),
            "title": title,
            "detail_url": detail_url,
            "thumbnail_url": thumb,
            "srcset": srcset,
            "models": models,
            "published_at": published_at,
        })

    max_page = page_num
    for pl in resp.css("a.page-numbers, .pagination a, a[href*='/page/'], a[rel='next'], link[rel='next']"):
        href = pl.attrib.get("href", "")
        m = re.search(r"/category/onlyfans/page/(\d+)/", href) or re.search(r"/page/(\d+)/", href)
        if m:
            max_page = max(max_page, int(m.group(1)))
    return items, max_page


def enrich_detail(card: Dict[str, Any]) -> Dict[str, Any]:
    url = card.get("detail_url")
    if not url:
        return card

    resp = fetch_page(url)
    if not resp:
        return card

    html = getattr(resp, "html_content", "") or ""
    servers = []
    seen = set()
    for b in resp.css("button.button_choice_server, button[onclick*='playEmbed']"):
        onclick = b.attrib.get("onclick", "") or ""
        m = re.search(r"playEmbed\(['\"]([^'\"]+)['\"]\)", onclick)
        if not m:
            continue
        embed_url = _abs(m.group(1))
        if not embed_url or embed_url in seen:
            continue
        seen.add(embed_url)
        if not _is_embed_alive(embed_url):
            continue
        meta = _server_from_embed(embed_url)
        servers.append({
            **meta,
            "embed_url": embed_url,
            "headers": {"Referer": BASE_URL + "/"},
        })
    # fallback: find playEmbed in raw html
    if not servers:
        for m in re.finditer(r"playEmbed\(['\"]([^'\"]+)['\"]\)", html):
            embed_url = _abs(m.group(1))
            if embed_url and embed_url not in seen:
                seen.add(embed_url)
                if not _is_embed_alive(embed_url):
                    continue
                meta = _server_from_embed(embed_url)
                servers.append({**meta, "embed_url": embed_url, "headers": {"Referer": BASE_URL + "/"}})

    tags = []
    seen_tags = set()
    for t in resp.css(".entry-tags a, a[rel='tag'], a[href*='/tag/']"):
        txt = _clean(getattr(t, "text", "")).lower()
        href = (t.attrib.get("href") or "").lower()
        if not txt or txt in seen_tags or len(txt) < 2 or len(txt) > 80:
            continue
        if "/actress/" in href:
            continue
        if txt in TAG_NOISE:
            continue
        seen_tags.add(txt)
        tags.append(txt)

    models = list(dict.fromkeys(card.get("models") or []))
    for a in resp.css("a[href*='/actress/'], a[href*='/tag/']"):
        href = (a.attrib.get("href") or "").lower()
        txt = _clean(getattr(a, "text", ""))
        if not txt:
            continue
        if "/actress/" in href and txt not in models and txt.lower() != "models":
            models.append(txt)

    gallery_url = None
    for a in resp.css("a[href*='imagetwist'], .view-gallery-v2 a, .view-gallery-btn-v2, a[href*='/gallery/']"):
        href = _abs(a.attrib.get("href"))
        if href:
            gallery_url = href
            break

    screenshots = []
    seen_img = set()
    for img in resp.css(".entry-content img, article .entry-content img, .post-content img, figure img"):
        src = ""
        attrs = img.attrib
        for k in ("data-src", "data-lazy-src", "data-original", "src"):
            v = (attrs.get(k) or "").strip()
            if v and not v.startswith("data:"):
                src = _abs(v)
                break
        if not src or src in seen_img:
            continue
        low = src.lower()
        if any(x in low for x in ["/logo", "avatar", "favicon", "icon", "emoji", "gravatar", "/plugins/", "/themes/"]):
            continue
        if not re.search(r"\.(?:jpe?g|png|webp)(?:[?#]|$)", low):
            continue
        if re.search(r"-\d{2,3}x\d{2,3}\.(?:jpe?g|png|webp)(?:[?#]|$)", low):
            continue
        seen_img.add(src)
        screenshots.append(src)
    screenshots = screenshots[:24]

    og_image = _abs(_extract_meta(resp, "og:image") or _extract_meta(resp, "twitter:image"))
    published_at = _extract_meta(resp, "article:published_time") or card.get("published_at")
    description = _extract_meta(resp, "description") or _extract_meta(resp, "og:description")

    merged = dict(card)
    merged["thumbnail_url"] = og_image or merged.get("thumbnail_url")
    merged["published_at"] = published_at
    merged["servers"] = servers
    merged["full_video_url"] = servers[0]["embed_url"] if servers else merged.get("full_video_url")
    merged["video_mime_type"] = "embed/iframe" if servers else merged.get("video_mime_type", "embed/iframe")
    merged["gallery_url"] = gallery_url
    merged["tags"] = tags
    merged["models"] = models
    merged["actors"] = models
    merged["screenshots"] = screenshots
    merged["raw_payload"] = {
        **(card.get("raw_payload") or {}),
        "source": "internetchicks",
        "og_image": og_image,
        "description": description,
        "screenshots": screenshots,
        "gallery_url": gallery_url,
    }
    return merged
