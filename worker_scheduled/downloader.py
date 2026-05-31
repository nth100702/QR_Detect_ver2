"""
downloader.py
Chia footage thành chunk 5 phút, download từng chunk.
Semaphore giới hạn số cam download song song.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path

from shared.hikvision_playback import get_playback
from worker_scheduled.config import CHUNK_MINUTES, SEMAPHORE_COUNT, TEMP_VIDEO_DIR

logger = logging.getLogger("downloader")

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
    """
    Tải toàn bộ chunks cho 1 cam, 1 ngày.
    Trả về list (path, chunk_start, chunk_end) cho các chunk thành công.
    Detect QR KHÔNG nằm ở đây — gọi riêng sau khi download xong.
    """
    footage_start = date.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    footage_end   = date.replace(hour=end_hour,   minute=0, second=0, microsecond=0)
    chunks        = chunk_ranges(footage_start, footage_end)

    out_dir = TEMP_VIDEO_DIR / str(cam_id) / date.strftime("%Y%m%d")
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"[CAM {cam_id}] {len(chunks)} chunks ({start_hour}h–{end_hour}h)")

    results: list[tuple[Path, datetime, datetime]] = []
    playback = get_playback()

    async with _semaphore:
        logger.info(f"[CAM {cam_id}] Acquired semaphore")
        for idx, (c_start, c_end) in enumerate(chunks):
            out = out_dir / f"chunk_{idx:03d}.mp4"

            if out.exists() and out.stat().st_size > 0:
                logger.info(f"[CAM {cam_id}] chunk {idx} cached, skip")
                results.append((out, c_start, c_end))
                continue

            logger.info(
                f"[CAM {cam_id}] chunk {idx+1}/{len(chunks)} "
                f"{c_start:%H:%M}–{c_end:%H:%M}"
            )
            ok = await playback.async_download_clip(cam_id, c_start, c_end, out)
            if ok:
                results.append((out, c_start, c_end))
                logger.info(f"[CAM {cam_id}] chunk {idx} ✓ ({out.stat().st_size//1024} KB)")
            else:
                logger.error(f"[CAM {cam_id}] chunk {idx} FAILED")

        logger.info(f"[CAM {cam_id}] Released semaphore — {len(results)}/{len(chunks)} OK")

    return results


async def download_full_day(
    cam_ids:    list[int],
    date:       datetime,
    start_hour: int = 8,
    end_hour:   int = 19,
) -> dict[int, list[tuple[Path, datetime, datetime]]]:
    """
    Tải toàn bộ footage 8h–19h cho nhiều cam trong 1 ngày, chạy song song.
    Trả về dict {cam_id: [(path, chunk_start, chunk_end), ...]} cho các chunk thành công.

    Mỗi cam vẫn bị giới hạn bởi _semaphore (SEMAPHORE_COUNT) — các cam
    chờ nhau thay vì kéo bandwidth cùng lúc.

    Ví dụ:
        results = await download_full_day([1, 2, 3], datetime(2025, 1, 15))
        for cam_id, chunks in results.items():
            print(f"CAM {cam_id}: {len(chunks)} chunks OK")
    """
    tasks = {
        cam_id: asyncio.create_task(
            download_cam_chunks(cam_id, date, start_hour, end_hour),
            name=f"download_cam_{cam_id}",
        )
        for cam_id in cam_ids
    }

    results: dict[int, list[tuple[Path, datetime, datetime]]] = {}
    for cam_id, task in tasks.items():
        try:
            results[cam_id] = await task
        except Exception as e:
            logger.error(f"[CAM {cam_id}] download_full_day error: {e}")
            results[cam_id] = []

    total_chunks  = sum(len(v) for v in results.values())
    total_cameras = sum(1 for v in results.values() if v)
    logger.info(
        f"download_full_day done — "
        f"{total_cameras}/{len(cam_ids)} cam(s) OK, "
        f"{total_chunks} chunk(s) total"
    )
    return results