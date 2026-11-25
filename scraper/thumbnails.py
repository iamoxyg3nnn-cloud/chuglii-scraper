"""Sprite-storyboard generation for zero-latency timeline hover previews.

At ingest (or backfill) we extract one low-res frame every `interval`
seconds from the video, tile them into 10x10 JPEG sprite sheets, and store
the sheet bytes in `scraper.preview_sprite_blobs` (Postgres bytea - no
external storage needed). The frontend edge worker serves sheets via
scraper_get_preview_sprite with immutable caching, and the player renders
the matching tile on timeline hover instantly - no streaming, no decoding.

ffprobe/ffmpeg must be on PATH (preinstalled on GitHub ubuntu runners).
"""

from __future__ import annotations

import math
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from db import get_supabase_client

TARGET_TILES = 600          # cap tiles per video; interval scales up for long videos
TILE_EDGE = 160             # max tile edge in px (aspect preserved)
GRID = 10                   # tiles per sheet row/col (10x10 = 100 per sheet)
JPEG_QUALITY = "-qscale:v 5"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


def _run(cmd: List[str], timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def probe_media(url: str) -> Optional[Dict[str, Any]]:
    """Return {duration, width, height} for an HLS manifest or direct file."""
    try:
        r = _run([
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "format=duration:stream=width,height",
            "-of", "json", url,
        ], timeout=60)
        if r.returncode != 0:
            return None
        import json
        info = json.loads(r.stdout or "{}")
        duration = float(info.get("format", {}).get("duration") or 0)
        streams = info.get("streams") or []
        width = int(streams[0].get("width") or 0) if streams else 0
        height = int(streams[0].get("height") or 0) if streams else 0
        if duration <= 0 or width <= 0 or height <= 0:
            return None
        return {"duration": duration, "width": width, "height": height}
    except Exception:
        return None


def lowest_variant_url(manifest_url: str) -> str:
    """For a master HLS playlist, return the lowest-bandwidth variant URL."""
    try:
        r = httpx.get(manifest_url, timeout=20, follow_redirects=True,
                      headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        lines = [l.strip() for l in r.text.splitlines() if l.strip()]
        if not any(l.startswith("#EXT-X-STREAM-INF") for l in lines):
            return manifest_url  # already a media playlist
        best: Optional[tuple[int, str]] = None
        for i, l in enumerate(lines):
            if l.startswith("#EXT-X-STREAM-INF"):
                bw = 0
                for part in l.split(":")[1].split(","):
                    k, _, v = part.partition("=")
                    if k.strip().upper() == "BANDWIDTH":
                        bw = int(float(v.strip().strip('"')))
                uri = lines[i + 1] if i + 1 < len(lines) else ""
                if uri and not uri.startswith("#"):
                    abs_uri = str(httpx.URL(manifest_url).join(uri))
                    if best is None or bw < best[0]:
                        best = (bw, abs_uri)
        return best[1] if best else manifest_url
    except Exception:
        return manifest_url


def generate_sprites(video_url: str, workdir: Path) -> Optional[Dict[str, Any]]:
    """Extract frames and tile them into sprite sheets inside `workdir`.

    Returns metadata dict (without sheet URLs) or None on failure.
    """
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        return None

    info = probe_media(video_url)
    if not info:
        return None
    duration = info["duration"]

    interval = max(10, math.ceil(duration / TARGET_TILES))
    count = max(1, math.ceil(duration / interval))
    sheets = math.ceil(count / (GRID * GRID))

    # Uniform tile size for the whole video (aspect preserved, even dims).
    if info["width"] >= info["height"]:
        tile_w = TILE_EDGE
        tile_h = max(2, round(TILE_EDGE * info["height"] / info["width"] / 2) * 2)
    else:
        tile_h = TILE_EDGE
        tile_w = max(2, round(TILE_EDGE * info["width"] / info["height"] / 2) * 2)

    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-user_agent", USER_AGENT,
        "-i", video_url,
        "-vf", f"fps=1/{interval},scale={tile_w}:{tile_h},tile={GRID}x{GRID}",
        "-frames:v", str(sheets),
        "-qscale:v", "5",
        str(workdir / "sheet_%03d.jpg"),
    ]
    try:
        r = _run(cmd, timeout=1800)
    except subprocess.TimeoutExpired:
        return None
    files = sorted(workdir.glob("sheet_*.jpg"))
    if r.returncode != 0 or not files:
        return None

    return {
        "interval": interval,
        "tileW": tile_w,
        "tileH": tile_h,
        "cols": GRID,
        "rows": GRID,
        "count": count,
        "sheets": len(files),
    }


def store_sprites(table_name: str, video_id: str, meta: Dict[str, Any],
                  workdir: Path) -> bool:
    """Upload sheet JPEGs into preview_sprite_blobs and stamp preview_thumbs."""
    client = get_supabase_client()
    files = sorted(workdir.glob("sheet_*.jpg"))
    if not files:
        return False
    for idx, f in enumerate(files):
        data = f.read_bytes()
        hex_payload = "\\x" + data.hex()
        r = client.rpc("upsert_preview_sprite", {
            "p_table": table_name,
            "p_id": video_id,
            "p_sheet": idx,
            "p_data": hex_payload,
            "p_meta": meta,
        }).execute()
        if getattr(r, "data", None) is None and getattr(r, "error", None):
            return False
    return True


def process_row(table_name: str, row: Dict[str, Any]) -> bool:
    """Generate + store a storyboard for one video row. Returns success."""
    url = row.get("full_video_url")
    if not url:
        return False
    video_id = str(row.get("id"))
    stream_url = lowest_variant_url(url) if ".m3u8" in url else url
    with tempfile.TemporaryDirectory(prefix="thumbs-") as td:
        workdir = Path(td)
        meta = generate_sprites(stream_url, workdir)
        if not meta:
            return False
        return store_sprites(table_name, video_id, meta, workdir)


def backfill_table(table_name: str, limit: int = 2) -> int:
    """Generate storyboards for up to `limit` rows missing preview_thumbs."""
    client = get_supabase_client()
    res = (
        client.table(table_name)
        .select("id, full_video_url")
        .is_("preview_thumbs", "null")
        .not_.is_("full_video_url", "null")
        .order("scraped_at", desc=True)
        .limit(limit)
        .execute()
    )
    rows = res.data or []
    done = 0
    for row in rows:
        try:
            if process_row(table_name, row):
                done += 1
                print(f"[thumbs] {table_name}/{row['id']}: storyboard generated")
            else:
                print(f"[thumbs] {table_name}/{row['id']}: generation failed")
        except Exception as e:
            print(f"[thumbs] {table_name}/{row['id']}: error {e}")
    return done
