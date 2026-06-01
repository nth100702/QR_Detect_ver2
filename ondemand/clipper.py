"""
ondemand/clipper.py
Tải clip ±2 phút quanh thời điểm detect QR theo yêu cầu on-demand.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from core.nvr import get_playback
from core.config import TEMP_CLIP_DIR

logger = logging.getLogger("ondemand.clipper")

CLIP_PRE_SEC  = 120
CLIP_POST_SEC = 120


async def fetch_clip(
    job_id:      str,
    scan_id:     int,
    cam_id:      int,
    detected_at: datetime,
) -> Path | None:
    start_dt = detected_at - timedelta(seconds=CLIP_PRE_SEC)
    stop_dt  = detected_at + timedelta(seconds=CLIP_POST_SEC)
    out_path = TEMP_CLIP_DIR / f"{job_id}_{scan_id}.mp4"

    logger.info(
        f"[JOB {job_id}][SCAN {scan_id}][CAM {cam_id}] "
        f"Clip {start_dt:%H:%M:%S}–{stop_dt:%H:%M:%S} → {out_path.name}"
    )

    if not TEMP_CLIP_DIR.exists():
        logger.error(f"TEMP_CLIP_DIR không tồn tại: {TEMP_CLIP_DIR}")
        return None

    try:
        ok = await get_playback().async_download_clip(
            cam_id=cam_id, start_dt=start_dt, stop_dt=stop_dt, output_path=out_path,
        )
    except Exception:
        logger.exception(f"[JOB {job_id}][SCAN {scan_id}] Exception khi download clip")
        out_path.unlink(missing_ok=True)
        return None

    file_exists = out_path.exists()
    file_size   = out_path.stat().st_size if file_exists else -1

    if ok and file_exists and file_size > 0:
        logger.info(f"[JOB {job_id}][SCAN {scan_id}] ✓ ({file_size // 1024} KB)")
        return out_path

    reasons = []
    if not ok:           reasons.append("download_clip=False")
    if not file_exists:  reasons.append("file không tồn tại")
    elif file_size == 0: reasons.append("file rỗng")
    logger.error(f"[JOB {job_id}][SCAN {scan_id}][CAM {cam_id}] FAILED — {', '.join(reasons)}")
    out_path.unlink(missing_ok=True)
    return None
