"""
scanner/downloader.py
Download footage chunks từ NVR. Semaphore giới hạn concurrent streams.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path

from core.nvr import get_playback
from core.config import CHUNK_MINUTES, SEMAPHORE_COUNT, TEMP_VIDEO_DIR

logger = logging.getLogger("scanner.downloader")

_semaphore = asyncio.Semaphore(SEMAPHORE_COUNT)


def chunk_ranges(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    chunks, cur = [], start
    while cur < end:
        nxt = min(cur + timedelta(minutes=CHUNK_MINUTES), end)
        chunks.append((cur, nxt))
        cur = nxt
    return chunks


async def download_cam_chunks(
    cam_id:     int,
    date:       datetime,
    start_hour: int = 8,
    end_hour:   int = 19,
) -> list[tuple[Path, datetime, datetime]]:
    footage_start = date.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    footage_end   = date.replace(hour=end_hour,   minute=0, second=0, microsecond=0)
    chunks        = chunk_ranges(footage_start, footage_end)

    out_dir = TEMP_VIDEO_DIR / str(cam_id) / date.strftime("%Y%m%d")
    out_dir.mkdir(parents=True, exist_ok=True)

    results: list[tuple[Path, datetime, datetime]] = []
    playback = get_playback()

    async with _semaphore:
        for idx, (c_start, c_end) in enumerate(chunks):
            out = out_dir / f"chunk_{idx:03d}.mp4"
            if out.exists() and out.stat().st_size > 0:
                results.append((out, c_start, c_end))
                continue
            ok = await playback.async_download_clip(cam_id, c_start, c_end, out)
            if ok:
                results.append((out, c_start, c_end))
            else:
                logger.error(f"[CAM {cam_id}] chunk {idx} FAILED")

    return results
