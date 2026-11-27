#!/usr/bin/env python3
"""Chuglii Live Worker — metadata-only companion to the scraper fleet.

Runs on GitHub Actions alongside the scraper daemon. Keeps the
`live_streams` table in Supabase fresh so the Cloudflare Pages edge worker
can serve the Watch Live feature without any origin server:

  - scrapes the 91吃瓜网 live tab catalogue (all pages)
  - upserts rows + deactivates vanished ones
  - translates Chinese titles to English (free Google endpoint)
  - probes real playback resolution (downloads ONE segment, ffprobe)
  - records everything in Supabase

No media is stored anywhere: playback streams straight from the upstream
CDN through the edge worker's media proxy.

Usage:
  python -m scraper.live_worker --session-minutes 55
  python -m scraper.live_worker --once
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from Crypto.Cipher import AES

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

UPSTREAM_BASE = "https://h4gaz3.jpzydpzm.cc"
MIRROR_BASES = [UPSTREAM_BASE, "https://box.jpzydpzm.cc"]
LIST_PATHS = [f"/category/sstp/live/{n}/" for n in range(1, 5)] + ["/category/sstp/"]
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
IMAGE_KEY = b"f5d965df75336270"
IMAGE_IV = b"97b60394abc2fbe1"
QUALITY_PROBE_PER_CYCLE = 6  # one segment each — cheap but real


def log(msg: str) -> None:
    print(f"[live-worker {datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def http_get(url: str, timeout: int = 20, binary: bool = False):
    """Fetch with browser TLS impersonation (curl_cffi) — the upstream blocks
    plain datacenter requests. Falls back to plain requests."""
    last = None
    candidates = [url]
    try:
        origin = re.match(r"https?://[^/]+", url).group(0)
        candidates += [url.replace(origin, m) for m in MIRROR_BASES if m != origin]
    except Exception:
        pass
    for candidate in candidates:
        # 1st choice: curl_cffi with Chrome impersonation (beats bot checks)
        try:
            from curl_cffi import requests as cffi
            r = cffi.get(candidate, headers={"User-Agent": UA, "Referer": "https://www.google.com/"}, timeout=timeout, impersonate="chrome")
            if r.status_code == 200:
                return r.content if binary else r.text
            last = f"upstream {r.status_code}"
        except Exception as e:
            last = str(e)
        # fallback: plain requests
        try:
            r = requests.get(candidate, headers={"User-Agent": UA, "Referer": "https://www.google.com/"}, timeout=timeout)
            if r.status_code == 200:
                return r.content if binary else r.text
            last = f"upstream {r.status_code}"
        except Exception as e:
            last = str(e)
    raise RuntimeError(f"GET failed: {last}")


# ─── Catalogue ───────────────────────────────────────────────────────────────

def scrape_catalogue() -> list[dict]:
    seen: dict[str, dict] = {}
    for path in LIST_PATHS:
        try:
            html = http_get(UPSTREAM_BASE + path)
        except Exception as e:
            log(f"list {path} failed: {e}")
            continue
        for m in re.finditer(r'<a class="realtime-card" href="([^"]+)" aria-label="([^"]*)">(.*?)</a>', html, re.S):
            href, title, inner = m.group(1), m.group(2).strip(), m.group(3)
            idm = re.search(r"/video/(\d+)/", href)
            if not idm:
                continue
            vid = idm.group(1)
            if vid in seen:
                continue
            tm = re.search(r'data-[a-z]+="(https?://[^"]+\.(?:jpe?g|png|webp))"', inner, re.I)
            seen[vid] = {"id": vid, "title": title, "thumbnail_url": tm.group(1) if tm else None}
    return list(seen.values())


def translate_title(title: str) -> str | None:
    try:
        r = requests.get(
            "https://translate.googleapis.com/translate_a/single",
            params={"client": "gtx", "sl": "zh-CN", "tl": "en", "dt": "t", "q": title[:400]},
            headers={"User-Agent": UA},
            timeout=10,
        )
        data = r.json()
        out = "".join(p[0] for p in data[0] if p and p[0])
        return out.strip() or None
    except Exception:
        return None


# ─── Supabase ────────────────────────────────────────────────────────────────

def sb():
    url = os.environ["SUPABASE_URL"].rstrip("/")
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    s = requests.Session()
    s.headers.update({"apikey": key, "Authorization": f"Bearer {key}"})
    return url, s


def sync_catalogue(streams: list[dict]) -> None:
    base, s = sb()
    now = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "id": int(v["id"]),
            "title": v["title"],
            "thumbnail_url": v["thumbnail_url"],
            "active": True,
            "last_seen_at": now,
        }
        for v in streams
    ]
    r = s.post(f"{base}/rest/v1/live_streams", json=rows, params={"on_conflict": "id"}, headers={"Prefer": "resolution=merge-duplicates"})
    if r.status_code >= 300:
        log(f"catalogue upsert failed: {r.status_code} {r.text[:200]}")
        return
    ids = ",".join(v["id"] for v in streams)
    s.patch(f"{base}/rest/v1/live_streams", json={"active": False}, params={"active": "eq.true", "id": f"not.in.({ids})"})
    log(f"catalogue synced: {len(rows)} active")


def translate_missing() -> None:
    base, s = sb()
    r = s.get(f"{base}/rest/v1/live_streams", params={"select": "id,title", "title_en": "is.null", "active": "eq.true", "limit": "10"})
    rows = r.json() or []
    for row in rows:
        en = translate_title(row["title"])
        if en:
            s.patch(f"{base}/rest/v1/live_streams", json={"title_en": en}, params={"id": f"eq.{row['id']}"})
    if rows:
        log(f"translated {len([x for x in rows if x])} titles")


# ─── Quality probe ───────────────────────────────────────────────────────────

def probe_quality_and_frame(vid: str) -> tuple[str | None, bytes | None]:
    """Download the segment at the CURRENT BROADCAST POSITION, ffprobe the
    resolution, and extract a JPEG frame — one fetch, two artifacts."""
    tmp = None
    try:
        html = http_get(f"{UPSTREAM_BASE}/video/{vid}/")
        m = re.search(r"data-config='([^']+)'", html)
        if not m:
            return None, None
        m3u8 = (json.loads(m.group(1)).get("video") or {}).get("url") or ""
        if not m3u8.startswith("http"):
            return None, None
        playlist = http_get(m3u8)
        lines = playlist.split("\n")
        base = m3u8.rsplit("/", 1)[0]
        segs = [l.strip() for l in lines if l.strip() and not l.strip().startswith("#")]
        durs = [float(x.group(1)) for x in (re.match(r"#EXTINF:([\d.]+)", l) for l in lines) if x]
        if not segs or not durs:
            return None, None
        keyline = next((l for l in lines if l.startswith("#EXT-X-KEY")), None)
        if not keyline:
            return None, None
        key_uri = re.search(r'URI="([^"]+)"', keyline).group(1)
        key_url = key_uri if key_uri.startswith("http") else f"{base}/{key_uri}"
        iv_hex = re.search(r"IV=0[xX]([0-9a-fA-F]+)", keyline)
        key = http_get(key_url, binary=True)
        media_seq = int(re.search(r"MEDIA-SEQUENCE=(\d+)", playlist).group(1)) if "MEDIA-SEQUENCE=" in playlist else 0

        # broadcast position: same formula the player uses (join 10s behind)
        duration = sum(durs)
        pos = max(0.0, (time.time() % duration) - 10)
        # find the segment covering pos
        acc = 0.0
        idx = 0
        for i, d in enumerate(durs):
            if acc + d > pos:
                idx = i
                break
            acc += d
            idx = i
        seg_url = segs[idx] if segs[idx].startswith("http") else f"{base}/{segs[idx]}"
        enc = http_get(seg_url, timeout=30, binary=True)
        iv = bytes.fromhex(iv_hex.group(1)) if iv_hex else (media_seq + idx).to_bytes(16, "big")
        dec = AES.new(key, AES.MODE_CBC, iv).decrypt(enc)
        with tempfile.NamedTemporaryFile(suffix=".ts", delete=False) as f:
            f.write(dec)
            tmp = f.name
        height = None
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v", "-show_entries", "stream=height", "-of", "json", tmp],
            capture_output=True, text=True,
        )
        try:
            height = json.loads(r.stdout or "{}").get("streams", [{}])[0].get("height")
        except Exception:
            pass
        frame = None
        fr = subprocess.run(
            ["ffmpeg", "-v", "error", "-y", "-i", tmp, "-frames:v", "1", "-vf", "scale=640:-2", "-q:v", "4", "-f", "image2", "-c:v", "mjpeg", "pipe:1"],
            capture_output=True,
        )
        if fr.returncode == 0 and len(fr.stdout) > 3000:
            frame = fr.stdout
        label = f"{height}p" if height else None
        return label, frame
    except Exception as e:
        log(f"  probe {vid}: {e}")
        return None, None
    finally:
        if tmp:
            Path(tmp).unlink(missing_ok=True)


def probe_missing_qualities(limit: int) -> None:
    base, s = sb()
    r = s.get(
        f"{base}/rest/v1/live_streams",
        params={"select": "id", "quality_label": "is.null", "active": "eq.true", "order": "last_seen_at.desc", "limit": str(limit)},
    )
    rows = r.json() or []
    done = 0
    for row in rows:
        label, frame = probe_quality_and_frame(str(row["id"]))
        patch = {}
        if label:
            patch["quality_label"] = label
        if frame:
            import base64
            patch["frame_jpeg"] = base64.b64encode(frame).decode()
            patch["has_frame"] = True
            patch["frame_captured_at"] = datetime.now(timezone.utc).isoformat()
        if patch:
            s.patch(f"{base}/rest/v1/live_streams", json=patch, params={"id": f"eq.{row['id']}"})
            done += 1
    if rows:
        log(f"quality+frame probes: {done}/{len(rows)} succeeded")


def refresh_frames(limit: int) -> None:
    """Re-capture frames for videos that already have one, oldest first —
    so thumbnails keep rotating with the broadcast."""
    base, s = sb()
    r = s.get(
        f"{base}/rest/v1/live_streams",
        params={"select": "id", "has_frame": "eq.true", "active": "eq.true", "order": "frame_captured_at.asc", "limit": str(limit)},
    )
    rows = r.json() or []
    done = 0
    for row in rows:
        label, frame = probe_quality_and_frame(str(row["id"]))
        if frame:
            import base64
            patch = {
                "frame_jpeg": base64.b64encode(frame).decode(),
                "frame_captured_at": datetime.now(timezone.utc).isoformat(),
            }
            if label:
                patch["quality_label"] = label
            s.patch(f"{base}/rest/v1/live_streams", json=patch, params={"id": f"eq.{row['id']}"})
            done += 1
    if rows:
        log(f"frame refresh: {done}/{len(rows)} updated")


# ─── Main loop ───────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--session-minutes", type=int, default=55)
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()
    deadline = time.time() + args.session_minutes * 60 if not args.once else float("inf")

    while time.time() < deadline:
        try:
            streams = scrape_catalogue()
            log(f"catalogue: {len(streams)} streams")
            if streams:
                sync_catalogue(streams)
                translate_missing()
                probe_missing_qualities(QUALITY_PROBE_PER_CYCLE)
                refresh_frames(QUALITY_PROBE_PER_CYCLE)
        except Exception as e:
            log(f"cycle failed: {e}")
        if args.once:
            break
        time.sleep(120)
    log("session over")


if __name__ == "__main__":
    main()
