"""Header-only media probing for OnlyFans sources.

Several sources publish no duration metadata at all:

* influencersgonewild's detail pages contain no ``duration`` field of any kind
  (1,365 rows were once stored with the table default 600 s = "10:00"), and
* sheeshfans only sometimes carries ``og:video:duration``, and its embed pages
  are script-rendered, so the served HTML says nothing.

Guessing is not an option and neither is a default, so the media itself is
asked. ``ffprobe`` with a tiny probe window reads the container header without
downloading the file.
"""
from __future__ import annotations

import json
import shutil
import subprocess
from typing import Optional, Tuple

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def ffprobe_available() -> bool:
    return shutil.which("ffprobe") is not None


def _run(url: str, referer: Optional[str], timeout: int) -> dict:
    exe = shutil.which("ffprobe")
    if not exe or not url:
        return {}
    cmd = [
        exe, "-v", "error",
        "-probesize", "32768",
        "-analyzeduration", "500000",
        # NOTE: do NOT add -rw_timeout here. On Windows it makes ffmpeg's TLS
        # layer fail with "Error in the pull function" (IO error -138) against
        # several of these CDNs, so every probe returned nothing and durations
        # silently fell back to the table default. The subprocess timeout below
        # already bounds the call.
        "-show_entries", "format=duration:stream=height",
        "-of", "json",
        "-user_agent", DEFAULT_UA,
    ]
    if referer:
        cmd += ["-headers", f"Referer: {referer}\r\n"]
    cmd.append(url)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return json.loads(proc.stdout or "{}")
    except Exception:
        return {}


def probe_media(
    url: str,
    referer: Optional[str] = None,
    timeout: int = 12,
) -> Tuple[Optional[int], Optional[int]]:
    """``(duration_seconds, height)`` in ONE header-only ffprobe call.

    Both values come from the same invocation because probing is the most
    expensive part of enrichment. Either value is ``None`` when the container
    does not expose it (or cannot be reached at all).
    """
    data = _run(url, referer, timeout)

    duration: Optional[int] = None
    try:
        seconds = float((data.get("format") or {}).get("duration"))
        duration = int(round(seconds)) if seconds > 0 else None
    except (TypeError, ValueError):
        duration = None

    heights = [
        int(stream.get("height"))
        for stream in (data.get("streams") or [])
        if isinstance(stream.get("height"), (int, float)) and stream.get("height")
    ]
    return duration, (max(heights) if heights else None)


# ─── MP4 header parsing (no ffmpeg) ─────────────────────────────────────────

def mp4_duration_from_header(
    url: str,
    referer: Optional[str] = None,
    timeout: float = 8.0,
    window: int = 524288,
) -> Optional[int]:
    """Duration of an MP4/MOV from its ``moov/mvhd`` atom, via two small GETs.

    ffprobe is the general answer, but against these CDNs it is both slow (5 s
    per file at concurrency 1) and flaky under parallel TLS handshakes, so
    measuring ~4,000 videos that way takes hours. The duration lives in a fixed
    layout that a couple of ranged requests can read:

        mvhd: size(4) 'mvhd'(4) version(1) flags(3) created(4) modified(4)
              timescale(4) duration(4)

    ``moov`` is usually at the front (faststart) but can be at the end, so the
    head is tried first and the tail second.
    """
    try:
        import httpx
    except Exception:                                    # pragma: no cover
        return None

    headers = {"user-agent": DEFAULT_UA, "accept": "*/*"}
    if referer:
        headers["referer"] = referer

    def _get(byte_range: str) -> bytes:
        try:
            with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                res = client.get(url, headers={**headers, "range": byte_range})
                if res.status_code in (200, 206):
                    return res.content
        except Exception:
            pass
        return b""

    # Tail first: these CDNs serve non-faststart files, where moov (and mvhd
    # right at its start) sits at the end, so one request usually settles it.
    for byte_range in (f"bytes=-{window}", f"bytes=0-{window - 1}"):
        blob = _get(byte_range)
        seconds = _mvhd_duration(blob)
        if seconds:
            return seconds
    return None


def _mvhd_duration(blob: bytes) -> Optional[int]:
    """Parse ``mvhd`` out of a raw byte window, or return None."""
    if not blob:
        return None
    idx = blob.find(b"mvhd")
    while idx != -1:
        try:
            version = blob[idx + 4]
            if version == 1:
                # 64-bit creation/modification, then timescale (4) + duration (8)
                timescale = int.from_bytes(blob[idx + 24:idx + 28], "big")
                duration = int.from_bytes(blob[idx + 28:idx + 36], "big")
            else:
                # creation(4) modification(4) timescale(4) duration(4)
                timescale = int.from_bytes(blob[idx + 16:idx + 20], "big")
                duration = int.from_bytes(blob[idx + 20:idx + 24], "big")
        except IndexError:
            return None
        if timescale > 0 and duration > 0:
            seconds = duration / timescale
            # 0xFFFFFFFF marks an unknown duration in fragmented MP4s.
            if 0 < seconds < 24 * 3600:
                return int(round(seconds))
        idx = blob.find(b"mvhd", idx + 1)
    return None
