"""
scanner/detect.py
Detect QR từ file video đã tải.
"""

import asyncio
import logging
import threading
from datetime import datetime
from pathlib import Path

import httpx

from core.config import API_BASE_URL, API_SECRET
from scanner.detect_fast import detect_video_fast

logger = logging.getLogger(__name__)

_headers = {"X-Secret": API_SECRET, "Content-Type": "application/json"}


async def _post_scan(cam_id: int, scan: dict) -> bool:
    async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=10) as client:
        try:
            r = await client.post("/scans", json=scan, headers=_headers)
            r.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"POST /scans failed: {e}")
            return False


async def process_video(
    video_path:   Path,
    cam_id:       int,
    chunk_start:  datetime,
    chunk_end:    datetime,
    delete_after: bool = True,
    cancel_event: threading.Event | None = None,
) -> int:
    scans = await asyncio.to_thread(
        detect_video_fast, video_path, cam_id, chunk_start, chunk_end, cancel_event
    )
    saved = 0
    for scan in scans:
        ok = await _post_scan(cam_id, scan)
        if ok:
            saved += 1

    if delete_after:
        try:
            video_path.unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"[CAM {cam_id}] Cannot delete {video_path}: {e}")

    return saved
