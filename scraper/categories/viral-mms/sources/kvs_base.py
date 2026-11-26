"""
Shared Kernel Video Sharing (KVS) adapter for SpicyMMS and IndianBFClips.

Both sites use KVS `/latest-updates/{page}/` listings and `flashvars` player
blocks whose `video_url` endpoint redirects (when called with the detail-page
session cookie and Referer) to a permanent direct CDN `.mp4` stream.
"""
import re
from typing import Any, Dict, List, Optional, Tuple
from curl_cffi import requests as cf_requests

from .base import (
    abs_url,
    clean_text,
    fetch_html,
    infer_quality_label,
    parse_soup,
    pick_best_servers,
    verify_embed_alive,
    verify_stream_alive,
)


def _extract_flashvars(html: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    m = re.search(r"var\s+flashvars\s*=\s*\{(.*?)\}\s*;", html or "", re.S)
    if not m:
        return out
    for km in re.finditer(r"([a-zA-Z0-9_]+)\s*:\s*'([^']*)'", m.group(1)):
        out[km.group(1)] = km.group(2)
    return out


class KvsViralMmsAdapter:
    def __init__(
        self,
        *,
        source_site: str,
        base_url: str,
        label: str,
    ) -> None:
        self.source_site = source_site
        self.base_url = base_url.rstrip("/")
        self.label = label
        self._session = cf_requests.Session()

    def build_listing_url(self, page_num: int) -> str:
        if page_num <= 1:
            return f"{self.base_url}/latest-updates/"
        return f"{self.base_url}/latest-updates/{page_num}/"

    def scrape_listing(self, page_num: int = 1) -> Tuple[List[Dict[str, Any]], Optional[int], bool]:
        url = self.build_listing_url(page_num)
        html, status = fetch_html(url, session=self._session, referer=self.base_url + "/")
        if status == 404:
            return [], page_num, True
        if not html:
            return [], None, False

        soup = parse_soup(html)
        items: List[Dict[str, Any]] = []
        seen_ids = set()

        cards = soup.select("#list_videos_latest_videos_list_items .item, .list-videos .item")
        if not cards:
            cards = soup.select(".thumbs .item, .item.thumb")

        for card in cards:
            a = card.select_one("a[href]")
            if not a:
                continue
            detail_url = abs_url(self.base_url, a.get("href"))
            if not detail_url or "/tags/" in detail_url or "/categories/" in detail_url:
                continue

            post_id = ""
            rt = a.get("data-rt", "")
            m_rt = re.search(r":(\d+):\d+:$", rt)
            if m_rt:
                post_id = m_rt.group(1)
            if not post_id:
                m_vid = re.search(r"/video/(\d+)/", detail_url)
                if m_vid:
                    post_id = m_vid.group(1)
            if not post_id or post_id in seen_ids:
                continue
            seen_ids.add(post_id)

            img = card.select_one("img")
            title = clean_text(a.get("title")) or clean_text(img.get("alt") if img else "")
            if not title:
                t_el = card.select_one(".title, strong")
                title = clean_text(t_el.get_text(" ", strip=True) if t_el else "")
            if not title:
                continue

            thumb = ""
            preview_count = 0
            if img:
                thumb = abs_url(
                    self.base_url,
                    img.get("data-original") or img.get("data-src") or img.get("src"),
                )
                cnt_raw = clean_text(img.get("data-cnt"))
                if cnt_raw.isdigit():
                    preview_count = int(cnt_raw)

            dur_el = card.select_one(".duration, .thumb-duration, .item-duration, .time")
            views_el = card.select_one(".views, .item-views")
            rating_el = card.select_one(".rating, .item-rating")

            media_obj: Dict[str, Any] = {
                "main_thumbnail": thumb,
                "thumbnail_url": thumb,
                "clean_thumbnail": thumb,
            }
            if preview_count > 1:
                media_obj["preview_count"] = preview_count

            items.append({
                "source_site": self.source_site,
                "post_id": post_id,
                "title": title,
                "url": detail_url,
                "media": media_obj,
                "metadata": {
                    "duration": clean_text(dur_el.get_text(" ", strip=True) if dur_el else ""),
                    "views": clean_text(views_el.get_text(" ", strip=True) if views_el else ""),
                    "rating": clean_text(rating_el.get_text(" ", strip=True) if rating_el else ""),
                },
                "taxonomy": {
                    "categories": ["viral-mms"],
                    "tags": [],
                    "actors": [],
                },
            })

        max_page = page_num
        for el in soup.select(".pagination a[data-parameters], .pagination a[href]"):
            dp = el.get("data-parameters", "")
            m_from = re.search(r"from:(\d+)", dp)
            if m_from:
                max_page = max(max_page, int(m_from.group(1)))
            href = el.get("href", "")
            m_p = re.search(r"/latest-updates/(\d+)/", href)
            if m_p:
                max_page = max(max_page, int(m_p.group(1)))
        # KVS often shows ajax pagination window; keep advancing when a full page arrives.
        if items and max_page <= page_num:
            next_btn = soup.select_one(".pagination a.next, .pagination a[data-parameters*='from:']")
            if next_btn:
                max_page = page_num + 1

        return items, max_page, True

    def enrich_detail(self, card: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        """Enrich and strictly verify the video stream is alive.

        Returns ``(enriched_card, status)`` where status is:
          - ``"ok"``: verified playable stream/embed
          - ``"dead"``: page or media is 404/deleted/unplayable (do not insert!)
          - ``"error"``: transient network/block failure (keep unmarked for retry)
        """
        detail_url = card.get("url") or ""
        post_id = str(card.get("post_id") or "")
        if not detail_url:
            return None, "dead"

        html, status = fetch_html(detail_url, session=self._session, referer=self.base_url + "/")
        if status == 404:
            return None, "dead"
        if not html:
            return None, "error"

        soup = parse_soup(html)
        fv = _extract_flashvars(html)

        title = clean_text(fv.get("video_title")) or card.get("title") or ""
        if not title:
            return None, "dead"

        # Resolve permanent CDN MP4 candidates from flashvars.
        servers: List[Dict[str, Any]] = []
        seen_urls = set()

        candidate_keys = ["video_url", "video_alt_url", "video_alt_url2", "video_alt_url3"]
        for key in candidate_keys:
            raw_stream = abs_url(self.base_url, fv.get(key))
            if not raw_stream or "/get_file/" not in raw_stream:
                continue
            alive, final_url = verify_stream_alive(
                raw_stream, referer=detail_url, session=self._session
            )
            if not alive or not final_url or final_url in seen_urls:
                continue
            seen_urls.add(final_url)
            q_hint = clean_text(fv.get(f"{key}_text"))
            if not q_hint:
                if key == "video_url" and fv.get("video_url_fhd") == "1":
                    q_hint = "1080p HD"
                elif key == "video_url" and fv.get("video_url_hd") == "1":
                    q_hint = "720p HD"
            qlabel = infer_quality_label(q_hint, final_url, default="HD")
            servers.append({
                "name": f"{self.label} ({qlabel})",
                "server": self.label,
                "embed_url": final_url,
                "type": "direct_mp4",
                "quality_label": qlabel,
            })

        # Verified embed fallback if direct MP4 resolution failed.
        if not servers and post_id:
            embed_url = f"{self.base_url}/embed/{post_id}"
            if verify_embed_alive(embed_url, referer=detail_url, session=self._session):
                servers.append({
                    "name": f"{self.label} Embed",
                    "server": self.label,
                    "embed_url": embed_url,
                    "type": "embed",
                    "quality_label": "HD",
                })

        if not servers:
            return None, "dead"

        servers = pick_best_servers(servers)
        primary = servers[0]
        list_thumb = card.get("media", {}).get("main_thumbnail") or ""
        thumb = (
            abs_url(self.base_url, fv.get("preview_url"))
            or list_thumb
            or ""
        )
        preview_frames: List[str] = []
        preview_count = int(card.get("media", {}).get("preview_count") or 0)
        if preview_count > 1 and list_thumb and re.search(r"/1\.(jpe?g|webp)(\?.*)?$", list_thumb, re.I):
            preview_frames = [
                re.sub(r"/1\.(jpe?g|webp)(\?.*)?$", rf"/{i}.\1\2", list_thumb, flags=re.I)
                for i in range(1, min(preview_count, 9) + 1)
            ]

        trailer = ""
        if post_id and post_id.isdigit():
            folder = (int(post_id) // 1000) * 1000
            cand_trailer = f"{self.base_url}/contents/videos_screenshots/{folder}/{post_id}/preview.mp4"
            t_alive, t_final = verify_stream_alive(cand_trailer, referer=detail_url, session=self._session, timeout=6)
            if t_alive:
                trailer = t_final

        dur_meta = soup.select_one("meta[property='og:video:duration'], meta[property='video:duration']")
        dur_hint = None
        if dur_meta and (dur_meta.get("content") or "").strip().isdigit():
            dur_hint = int(dur_meta["content"].strip())

        pub_meta = soup.select_one("meta[property='article:published_time']")
        pub_at = clean_text(pub_meta.get("content")) if pub_meta else ""

        tags = [t.strip().lower() for t in (fv.get("video_tags") or "").split(",") if t.strip()]
        cats = [c.strip().lower() for c in (fv.get("video_categories") or "").split(",") if c.strip()]
        models = [m.strip() for m in (fv.get("video_models") or "").split(",") if m.strip()]

        enriched = dict(card)
        enriched["title"] = title
        enriched["media"] = {
            "full_video_url": primary["embed_url"],
            "video_mime_type": "video/mp4" if primary["type"] == "direct_mp4" else "embed/iframe",
            "trailer_preview_url": trailer or None,
            "preview_frames": preview_frames or None,
            "main_thumbnail": thumb,
            "thumbnail_url": thumb,
            "clean_thumbnail": thumb,
        }
        enriched["metadata"] = {
            **card.get("metadata", {}),
            "duration_seconds": dur_hint,
            "published_at": pub_at or None,
        }
        enriched["taxonomy"] = {
            "categories": ["viral-mms", *cats],
            "tags": tags,
            "actors": models,
        }
        enriched["servers"] = servers
        return enriched, "ok"
