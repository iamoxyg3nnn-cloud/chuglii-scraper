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
