#!/usr/bin/env python3
"""Chuglii Live Worker — runs alongside the scraper fleet on GitHub Actions.

Scrapes the 91吃瓜网 "live" tab (VOD loops presented as live), remuxes every
video into a clean, silence-filled, loudness-normalized fMP4 HLS with ffmpeg,
uploads it to Cloudflare R2, and records everything in Supabase
(`live_streams`). The Cloudflare Pages edge worker then serves the catalogue,
playback URLs and live thumbnails straight from that table + bucket — no
origin server needed.

The upstream source is broken in two ways this pipeline fixes:
  1. ~25% of fragments carry NO audio track → the player stalls at the hole.
     We re-lay the audio on a continuous timeline with digital silence in the
     gaps (verified gap-free output).
  2. Mic audio is recorded ~50 dB too quiet (peak ≈ -58 dBFS). We measure the
     assembled track's peak and apply clean gain to land it at ≈ -4 dBFS.

Usage:
  python -m scraper.live_worker --session-minutes 55
  python -m scraper.live_worker --once
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import struct
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import requests
from Crypto.Cipher import AES

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

UPSTREAM_BASE = "https://h4gaz3.jpzydpzm.cc"
MIRROR_BASES = [UPSTREAM_BASE, "https://box.jpzydpzm.cc"]
LIST_PATHS = [f"/sstp/live/{n}/" for n in range(1, 5)] + ["/category/sstp/"]
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
IMAGE_KEY = b"f5d965df75336270"
IMAGE_IV = b"97b60394abc2fbe1"
DOWNLOAD_CONCURRENCY = 10
PCM_RATE = 44100
PCM_CHANNELS = 2
TARGET_PEAK_DB = -4.0
MAX_GAIN_DB = 60.0
FRAMES_PER_VIDEO = 12


def log(msg: str) -> None:
    print(f"[live-worker {datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def http_get(url: str, timeout: int = 20, binary: bool = False):
    last = None
    candidates = [url]
    try:
        origin = re.match(r"https?://[^/]+", url).group(0)
        candidates += [url.replace(origin, m) for m in MIRROR_BASES if m != origin]
    except Exception:
        pass
    for candidate in candidates:
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
    # English titles for rows that don't have one yet
    r2 = s.get(f"{base}/rest/v1/live_streams", params={"select": "id,title,title_en", "title_en": "is.null", "active": "eq.true", "limit": "10"})
    for row in r2.json() or []:
        en = translate_title(row["title"])
        if en:
            s.patch(f"{base}/rest/v1/live_streams", json={"title_en": en}, params={"id": f"eq.{row['id']}"})


def pending_videos(limit: int) -> list[dict]:
    base, s = sb()
    r = s.get(
        f"{base}/rest/v1/live_streams",
        params={"select": "id,title,title_en,thumbnail_url,storage_path,duration_secs", "active": "eq.true", "storage_path": "is.null", "order": "last_seen_at.desc", "limit": str(limit)},
    )
    return r.json() or []


def mark_done(vid: str, patch: dict) -> None:
    base, s = sb()
    s.patch(f"{base}/rest/v1/live_streams", json=patch, params={"id": f"eq.{vid}"})


# ─── Remux (port of the api-server pipeline) ────────────────────────────────

def resolve_m3u8(vid: str) -> str:
    html = http_get(f"{UPSTREAM_BASE}/video/{vid}/")
    m = re.search(r"data-config='([^']+)'", html)
    if not m:
        raise RuntimeError("no player config")
    cfg = json.loads(m.group(1))
    url = (cfg.get("video") or {}).get("url") or ""
    if not url.startswith("http"):
        raise RuntimeError("no m3u8 in player config")
    return url


def download_and_assemble(m3u8: str, out_ts: Path) -> float:
    playlist = http_get(m3u8)
    lines = playlist.split("\n")
    base = m3u8.rsplit("/", 1)[0]
    segs = [l.strip() for l in lines if l.strip() and not l.strip().startswith("#")]
    if not segs:
        raise RuntimeError("empty playlist")
    keyline = next((l for l in lines if l.startswith("#EXT-X-KEY")), None)
    if not keyline:
        raise RuntimeError("playlist not AES encrypted")
    key_uri = re.search(r'URI="([^"]+)"', keyline).group(1)
    key_url = key_uri if key_uri.startswith("http") else f"{base}/{key_uri}"
    iv_hex = re.search(r"IV=0[xX]([0-9a-fA-F]+)", keyline)
    key = http_get(key_url, binary=True)
    media_seq = int(re.search(r"MEDIA-SEQUENCE=(\d+)", playlist).group(1)) if "MEDIA-SEQUENCE=" in playlist else 0

    buffers: list = [None] * len(segs)
    failed = 0

    def fetch(i: int):
        nonlocal failed
        uri = segs[i]
        url = uri if uri.startswith("http") else f"{base}/{uri}"
        try:
            enc = http_get(url, timeout=30, binary=True)
            iv = bytes.fromhex(iv_hex.group(1)) if iv_hex else (media_seq + i).to_bytes(16, "big")
            dec = AES.new(key, AES.MODE_CBC, iv).decrypt(enc)
            buffers[i] = dec
        except Exception:
            failed += 1
            buffers[i] = b""

    with ThreadPoolExecutor(DOWNLOAD_CONCURRENCY) as ex:
        list(ex.map(fetch, range(len(segs))))
    if failed > len(segs) * 0.1:
        raise RuntimeError(f"too many segment failures ({failed}/{len(segs)})")
    with open(out_ts, "wb") as f:
        for b in buffers:
            f.write(b)
    total = sum(dur for dur in (float(x.group(1)) for x in (re.match(r"#EXTINF:([\d.]+)", l) for l in lines) if x))
    return total


def ffprobe_json(*args: str) -> dict:
    r = subprocess.run(["ffprobe", "-v", "error", *args], capture_output=True, text=True)
    return json.loads(r.stdout or "{}")


def assemble_filled_audio(pcm_in: Path, pcm_out: Path, packets: list[tuple[float, float]], duration: float, t0: float) -> None:
    bps = PCM_RATE * PCM_CHANNELS * 2
    total = int(round(duration * bps))
    pos = 0
    idx = 0
    silence = b"\x00" * 65536
    with open(pcm_in, "rb") as fin, open(pcm_out, "wb") as fout:
        for pts, dur in packets:
            target = max(0, round((pts - t0) * bps))
            if target > pos:
                n = target - pos
                while n > 0:
                    c = min(n, len(silence))
                    fout.write(silence[:c])
                    pos += c
                    n -= c
            n = max(0, round(dur * bps))
            if n > 0:
                chunk = fin.read(n)
                if chunk:
                    fout.write(chunk)
                    pos += len(chunk)
                    idx += len(chunk)
        if total > pos:
            n = total - pos
            while n > 0:
                c = min(n, len(silence))
                fout.write(silence[:c])
                pos += c
                n -= c


def measure_peak(pcm: Path) -> float:
    r = subprocess.run(
        ["ffmpeg", "-v", "info", "-f", "s16le", "-ar", str(PCM_RATE), "-ac", str(PCM_CHANNELS), "-i", str(pcm), "-af", "astats=metadata=1", "-f", "null", "-"],
        capture_output=True, text=True,
    )
    m = re.search(r"Peak level dB: (-?[\d.]+)", r.stderr)
    return float(m.group(1)) if m else float("nan")


def remux_video(vid: str, workdir: Path) -> dict:
    """Returns {duration, segments} and writes index.m3u8 + init.mp4 + seg*.m4s into workdir."""
    t_start = time.time()
    m3u8 = resolve_m3u8(vid)
    full_ts = workdir / "full.ts"
    declared = download_and_assemble(m3u8, full_ts)
    log(f"  {vid}: assembled {full_ts.stat().st_size / 1048576:.0f}MB in {time.time() - t_start:.0f}s")

    # audio packets (pts + duration) and video start
    r = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "a", "-show_entries", "packet=pts_time,duration_time", "-of", "csv=p=0", str(full_ts)], capture_output=True, text=True)
    packets = []
    for line in r.stdout.splitlines():
        parts = line.strip().split(",")
        if len(parts) >= 2:
            try:
                packets.append((float(parts[0]), float(parts[1])))
            except ValueError:
                pass
    rv = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v", "-show_entries", "packet=pts_time", "-of", "csv=p=0", "-read_intervals", "%+#1", str(full_ts)], capture_output=True, text=True)
    try:
        t0 = float(rv.stdout.strip().split(",")[0])
    except (ValueError, IndexError):
        t0 = packets[0][0] if packets else 0.0
    vdur = ffprobe_json("-show_entries", "format=duration", "-of", "json", str(full_ts)).get("format", {}).get("duration")
    duration = float(vdur) if vdur else (declared or 3600.0)

    pcm_in = workdir / "audio.pcm"
    subprocess.run(["ffmpeg", "-v", "error", "-y", "-i", str(full_ts), "-map", "0:a", "-f", "s16le", "-ar", str(PCM_RATE), "-ac", str(PCM_CHANNELS), str(pcm_in)], check=True)
    pcm_filled = workdir / "filled.pcm"
    assemble_filled_audio(pcm_in, pcm_filled, packets, duration + 1, t0)
    pcm_in.unlink(missing_ok=True)

    peak = measure_peak(pcm_filled)
    gain = min(MAX_GAIN_DB, max(0.0, TARGET_PEAK_DB - peak)) if peak == peak and peak < -10 else 0.0
    filters = f"volume={gain:.1f}dB,highpass=f=100" if gain > 0.5 else None
    log(f"  {vid}: peak {peak:.1f} dBFS -> gain {gain:.1f} dB")

    cmd = ["ffmpeg", "-v", "error", "-y", "-i", "full.ts", "-f", "s16le", "-ar", str(PCM_RATE), "-ac", str(PCM_CHANNELS), "-i", "filled.pcm",
           "-map", "0:v", "-map", "1:a", "-c:v", "copy", "-c:a", "aac", "-b:a", "128k"]
    if filters:
        cmd += ["-af", filters]
    cmd += ["-avoid_negative_ts", "make_zero", "-f", "hls", "-hls_time", "6", "-hls_segment_type", "fmp4",
            "-hls_playlist_type", "vod", "-hls_list_size", "0", "-hls_segment_filename", "seg%05d.m4s", "index.m3u8"]
    subprocess.run(cmd, cwd=workdir, check=True)
    pcm_filled.unlink(missing_ok=True)
    full_ts.unlink(missing_ok=True)
    if not (workdir / "index.m3u8").exists():
        raise RuntimeError("ffmpeg produced no playlist")
    return {"duration": int(duration)}


def extract_frames(workdir: Path, duration: int) -> int:
    """cover.jpg + frame_000.jpg..frame_0NN.jpg at even intervals."""
    count = 0
    positions = [duration * f for f in (0.3,)] + [duration * (i + 0.5) / FRAMES_PER_VIDEO for i in range(FRAMES_PER_VIDEO)]
    for i, pos in enumerate(positions):
        name = "cover.jpg" if i == 0 else f"frame_{i - 1:03d}.jpg"
        r = subprocess.run(
            ["ffmpeg", "-v", "error", "-y", "-ss", f"{pos:.1f}", "-i", "index.m3u8", "-frames:v", "1", "-vf", "scale=640:-2", "-q:v", "4", str(workdir / name)],
            cwd=workdir, capture_output=True,
        )
        if (workdir / name).exists() and (workdir / name).stat().st_size > 3000:
            count += 1
    return count


# ─── R2 upload ───────────────────────────────────────────────────────────────

def r2_upload(workdir: Path, vid: str) -> str | None:
    account = os.environ.get("CF_ACCOUNT_ID")
    token = os.environ.get("CLOUDFLARE_API_TOKEN")
    bucket = os.environ.get("R2_BUCKET", "live-remux")
    if not account or not token:
        log("R2 upload skipped: CF_ACCOUNT_ID / CLOUDFLARE_API_TOKEN not set")
        return None
    base = f"https://api.cloudflare.com/client/v4/accounts/{account}/r2/buckets/{bucket}/objects"
    keys = sorted(p.name for p in workdir.iterdir() if p.is_file() and p.name != "full.ts")
    ok = 0
    for name in keys:
        data = (workdir / name).read_bytes()
        r = requests.put(f"{base}/{vid}%2F{name}", data=data, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/octet-stream"}, timeout=60)
        if r.status_code in (200, 201):
            ok += 1
        else:
            log(f"  upload {name}: {r.status_code} {r.text[:120]}")
            if r.status_code in (401, 403):
                raise RuntimeError(f"R2 upload unauthorized ({r.status_code}) — check the API token's R2 permissions")
    log(f"  {vid}: uploaded {ok}/{len(keys)} objects")
    return f"{vid}" if ok == len(keys) else None


# ─── Main loop ───────────────────────────────────────────────────────────────

def process_one(vid: str) -> bool:
    base, s = sb()
    workdir = Path(tempfile.mkdtemp(prefix=f"live-{vid}-"))
    try:
        info = remux_video(vid, workdir)
        frames = extract_frames(workdir, info["duration"])
        path = r2_upload(workdir, vid)
        if path:
            mark_done(vid, {
                "storage_path": path,
                "duration_secs": info["duration"],
                "cover_path": f"{path}/cover.jpg",
                "frame_count": max(0, frames - 1),
            })
            log(f"  {vid}: DONE ({info['duration']}s, {frames} frames)")
            return True
        return False
    finally:
        for p in workdir.iterdir():
            p.unlink(missing_ok=True)
        workdir.rmdir()


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
            todo = pending_videos(40)
            log(f"pending remux: {len(todo)}")
            for row in todo:
                if time.time() >= deadline:
                    break
                try:
                    process_one(str(row["id"]))
                except Exception as e:
                    log(f"  {row['id']}: FAILED {e}")
                    mark_done(str(row["id"]), {"storage_path": None})
        except Exception as e:
            log(f"cycle failed: {e}")
        if args.once:
            break
        time.sleep(60)
    log("session over")


if __name__ == "__main__":
    main()
