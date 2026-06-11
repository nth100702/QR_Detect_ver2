"""
ondemand/clipper.py
Tải clip ±2 phút quanh thời điểm detect QR theo yêu cầu on-demand.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path

from core.nvr import get_playback
from core.config import TEMP_CLIP_DIR

logger = logging.getLogger("ondemand.clipper")

CLIP_PRE_SEC    = 120
CLIP_POST_SEC   = 120
CLIP_TIMEOUT_SEC = 300  # cancel nếu tải quá 5 phút


async def fetch_clip(
    job_id:      str,
    scan_id:     int,
    cam_id:      int,
    detected_at: datetime,
    qr_value:    str = "",
) -> Path | None:
    start_dt = detected_at - timedelta(seconds=CLIP_PRE_SEC)
    stop_dt  = detected_at + timedelta(seconds=CLIP_POST_SEC)

    # Sanitize qr_value to be safe as a filename component
    safe_qr  = "".join(c if c.isalnum() or c in "-_." else "_" for c in qr_value)[:60]
    ts_str   = detected_at.strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_qr}_{ts_str}_cam{cam_id}.mp4" if safe_qr else f"{job_id}_{scan_id}.mp4"
    out_path = TEMP_CLIP_DIR / filename

    logger.info(
        f"[JOB {job_id}][SCAN {scan_id}][CAM {cam_id}] "
        f"Clip {start_dt:%H:%M:%S}–{stop_dt:%H:%M:%S} → {out_path.name}"
    )

    if not TEMP_CLIP_DIR.exists():
        logger.error(f"TEMP_CLIP_DIR không tồn tại: {TEMP_CLIP_DIR}")
        raise RuntimeError("Thư mục lưu clip không tồn tại trên server.")

    try:
        ok = await asyncio.wait_for(
            get_playback().async_download_clip(
                cam_id=cam_id, start_dt=start_dt, stop_dt=stop_dt, output_path=out_path,
            ),
            timeout=CLIP_TIMEOUT_SEC,
        )
    except asyncio.TimeoutError:
        out_path.unlink(missing_ok=True)
        logger.error(f"[JOB {job_id}][SCAN {scan_id}][CAM {cam_id}] Timeout sau {CLIP_TIMEOUT_SEC}s")
        raise RuntimeError(f"Tải footage quá {CLIP_TIMEOUT_SEC // 60} phút, đã tự động hủy.")
    except Exception:
        logger.exception(f"[JOB {job_id}][SCAN {scan_id}] Exception khi download clip")
        out_path.unlink(missing_ok=True)
        raise

    file_exists = out_path.exists()
    file_size   = out_path.stat().st_size if file_exists else -1

    if ok and file_exists and file_size > 0:
        logger.info(f"[JOB {job_id}][SCAN {scan_id}] ✓ ({file_size // 1024} KB)")
        return out_path

    reasons = []
    if not ok:           reasons.append("download_clip=False")
    if not file_exists:  reasons.append("file không tồn tại")
    elif file_size == 0: reasons.append("file rỗng")
    msg = ", ".join(reasons)
    logger.error(f"[JOB {job_id}][SCAN {scan_id}][CAM {cam_id}] FAILED — {msg}")
    out_path.unlink(missing_ok=True)
    raise RuntimeError(f"Tải footage thất bại: {msg}")
