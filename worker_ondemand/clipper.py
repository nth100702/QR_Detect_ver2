"""
clipper.py
Tải clip ±2 phút quanh thời điểm detect QR theo yêu cầu on-demand.
Dùng bởi worker_ondemand.py.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from shared.hikvision_playback import get_playback
from worker_scheduled.config import TEMP_CLIP_DIR

logger = logging.getLogger("clipper")

CLIP_PRE_SEC  = 120   # 2 phút trước detected_at
CLIP_POST_SEC = 120   # 2 phút sau  detected_at


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

    # ── Kiểm tra thư mục output tồn tại ──────────────────────────────────
    if not TEMP_CLIP_DIR.exists():
        logger.error(f"[JOB {job_id}][SCAN {scan_id}] TEMP_CLIP_DIR không tồn tại: {TEMP_CLIP_DIR}")
        return None

    # ── Gọi download, bắt exception ──────────────────────────────────────
    try:
        ok = await get_playback().async_download_clip(
            cam_id=cam_id,
            start_dt=start_dt,
            stop_dt=stop_dt,
            output_path=out_path,
        )
        logger.debug(f"[JOB {job_id}][SCAN {scan_id}] ok={ok!r}")
        logger.debug(f"[JOB {job_id}][SCAN {scan_id}] out_path={out_path}")
        logger.debug(f"[JOB {job_id}][SCAN {scan_id}] exists={out_path.exists()}, size={out_path.stat().st_size if out_path.exists() else 'N/A'}")
    except Exception:
        logger.exception(f"[JOB {job_id}][SCAN {scan_id}][CAM {cam_id}] Exception khi gọi async_download_clip")
        out_path.unlink(missing_ok=True)
        return None

    # ── Phân tích kết quả ─────────────────────────────────────────────────
    logger.debug(f"[JOB {job_id}][SCAN {scan_id}] async_download_clip returned: {ok!r}")

    file_exists = out_path.exists()
    file_size   = out_path.stat().st_size if file_exists else -1

    logger.debug(
        f"[JOB {job_id}][SCAN {scan_id}] "
        f"file_exists={file_exists}, file_size={file_size}"
        f"start_dt={start_dt.strftime('%Y-%m-%d %H:%M:%S')} | "
        f"stop_dt={stop_dt.strftime('%Y-%m-%d %H:%M:%S')} | "
    )

    if ok and file_exists and file_size > 0:
        logger.info(
            f"[JOB {job_id}][SCAN {scan_id}] ✓ ({file_size // 1024} KB)"
        )
        return out_path

    # ── Log lý do thất bại cụ thể ─────────────────────────────────────────
    reasons = []
    if not ok:
        reasons.append(f"download_clip=False")
    if not file_exists:
        reasons.append("file không tồn tại")
    elif file_size == 0:
        reasons.append("file rỗng (0 bytes)")

    logger.error(
        f"[JOB {job_id}][SCAN {scan_id}][CAM {cam_id}] FAILED — {', '.join(reasons)}"
    )
    out_path.unlink(missing_ok=True)
    return None