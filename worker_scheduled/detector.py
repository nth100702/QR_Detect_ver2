"""
worker-scheduled/detector.py
Detect QR từ file video đã tải:
  - Sample 1 fps (tiết kiệm CPU)
  - Chỉ lưu QR hợp lệ (prefix SPXVN)
  - Xác định shift (MORNING / AFTERNOON) theo thời gian detect
  - POST kết quả lên API (không ghi DB trực tiếp)
  - Xóa file video sau khi xử lý xong

Logic clip giữ lại từ server.py:
  - CLIP_MIN_DURATION  = 30s   — switch QR chỉ khi clip hiện tại >= 30s
  - Pre-buffer         = 2s    — giữ thông tin thời điểm QR xuất hiện
  - QR biến mất        — KHÔNG đóng clip, tiếp tục ghi metadata
  - Detect mỗi 5 frame — sau đó sample theo fps
"""

import asyncio
import logging
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import cv2
import httpx
from pyzbar.pyzbar import decode

from shared.models import is_valid_qr, get_shift
from .config import API_BASE_URL, API_SECRET, DETECT_SAMPLE_FPS

logger = logging.getLogger(__name__)

CLIP_MIN_DURATION = 30   # giây — tối thiểu trước khi switch QR
FRAME_DETECT_INTERVAL = 5  # detect mỗi N frame

_headers = {"X-Secret": API_SECRET, "Content-Type": "application/json"}


async def _post_scan(cam_id: int, scan: dict) -> bool:
    """Gửi 1 QR scan lên API."""
    async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=10) as client:
        try:
            r = await client.post("/scans", json=scan, headers=_headers)
            r.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"POST /scans failed: {e}")
            return False


def _detect_video(
    video_path: Path,
    cam_id:     int,
    chunk_start: datetime,
    chunk_end:   datetime,
    cancel_event: threading.Event | None = None,
) -> list[dict]:
    """
    Đọc video, detect QR theo sample fps, trả về list scan records.
    Chạy trong thread (blocking I/O + CPU).

    Dùng grab() cho frame bỏ qua thay vì read() — duy trì HEVC decoder state,
    tránh "Could not find ref with POC" warnings, nhanh ~5-10x so với read all.
    """
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        logger.error(f"Cannot open video: {video_path}")
        return []

    fps          = cap.get(cv2.CAP_PROP_FPS) or 25.0
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    duration_sec = total_frames / fps

    # Bước nhảy frame: grab() các frame bỏ qua thay vì seek() —
    # grab() advance vị trí mà không decode full, duy trì HEVC decoder state
    # → không có "Could not find ref with POC" warnings, nhanh ~5-10x so với read all
    step      = max(1, int(fps / DETECT_SAMPLE_FPS))
    n_samples = max(1, int(total_frames / step))

    logger.info(
        f"[CAM {cam_id}] Detecting {video_path.name} | "
        f"{fps:.1f}fps {duration_sec:.0f}s → {n_samples} samples (grab-skip, step={step})"
    )

    scans:         list[dict] = []
    seen_qrs:      dict[str, datetime] = {}
    current_qr:    str | None = None
    clip_start_ts: datetime | None = None

    frame_idx = 0
    sampled   = 0

    while True:
        # Grab các frame cần bỏ qua (fast, không decode, giữ decoder state)
        if frame_idx % step != 0:
            if not cap.grab():
                break
            frame_idx += 1
            continue

        # Frame cần detect — decode đầy đủ
        ret, frame = cap.read()
        if not ret:
            break

        if cancel_event and cancel_event.is_set():
            logger.warning(f"[CAM {cam_id}] _detect_video cancelled at frame {frame_idx}")
            break

        sampled += 1
        actual_msec = cap.get(cv2.CAP_PROP_POS_MSEC)
        detected_at = chunk_start + timedelta(seconds=actual_msec / 1000.0)

        # Resize xuống 720p nếu frame quá lớn — giảm thời gian pyzbar decode
        h, w = frame.shape[:2]
        if w > 1280:
            scale = 1280.0 / w
            frame = cv2.resize(frame, (int(w * scale), int(h * scale)))

        try:
            results = decode(frame)
        except Exception as e:
            logger.debug(f"[CAM {cam_id}] decode error at frame {frame_idx}: {e}")
            frame_idx += 1
            continue

        for obj in results:
            try:
                qr = obj.data.decode("utf-8", errors="replace")
            except Exception:
                continue

            if not is_valid_qr(qr):
                continue

            if qr in seen_qrs:
                continue

            if current_qr and current_qr != qr and clip_start_ts:
                elapsed = (detected_at - clip_start_ts).total_seconds()
                if elapsed < CLIP_MIN_DURATION:
                    logger.debug(
                        f"[CAM {cam_id}] QR switch ignored (clip only {elapsed:.0f}s < "
                        f"{CLIP_MIN_DURATION}s): {current_qr} → {qr}"
                    )
                    continue

            shift = get_shift(detected_at)
            if shift in ("OUT OF WORK", "LUNCH BREAK"):
                logger.debug(f"[CAM {cam_id}] QR at {detected_at.strftime('%H:%M')} outside shift, skip")
                continue

            seen_qrs[qr] = detected_at
            current_qr   = qr
            clip_start_ts = detected_at

            scans.append({
                "cam_id":      cam_id,
                "qr_value":    qr,
                "detected_at": detected_at.isoformat(),
                "chunk_start": chunk_start.isoformat(),
                "chunk_end":   chunk_end.isoformat(),
                "shift":       shift,
            })

            logger.info(
                f"[CAM {cam_id}] QR detected: {qr} @ "
                f"{detected_at.strftime('%H:%M:%S')} ({shift})"
            )

        frame_idx += 1

    cap.release()
    logger.info(f"[CAM {cam_id}] {video_path.name} → {len(scans)} QR(s) | {sampled} frames sampled")
    return scans


async def process_video(
    video_path:  Path,
    cam_id:      int,
    chunk_start: datetime,
    chunk_end:   datetime,
    delete_after: bool = True,
) -> int:
    """
    Detect QR từ 1 file video, POST kết quả lên API, xóa file nếu thành công.
    Trả về số scan đã ghi thành công.

    Detect chạy trong asyncio.to_thread() — không block event loop,
    không chiếm semaphore NVR (nằm ngoài downloader semaphore).
    """
    scans = await asyncio.to_thread(
        _detect_video, video_path, cam_id, chunk_start, chunk_end
    )

    saved = 0
    for scan in scans:
        ok = await _post_scan(cam_id, scan)
        if ok:
            saved += 1

    if delete_after:
        try:
            video_path.unlink(missing_ok=True)
            logger.debug(f"[CAM {cam_id}] Deleted temp file: {video_path}")
        except Exception as e:
            logger.warning(f"[CAM {cam_id}] Cannot delete {video_path}: {e}")

    return saved


async def process_cam_chunks(
    cam_id:      int,
    chunks:      list[Path],          # output của downloader.download_cam_chunks()
    chunk_start_times: list[datetime], # thời điểm bắt đầu từng chunk
    chunk_end_times:   list[datetime],
) -> int:
    """Xử lý tuần tự từng chunk của 1 cam. Trả về tổng số scan."""
    total = 0
    for path, c_start, c_end in zip(chunks, chunk_start_times, chunk_end_times):
        n = await process_video(path, cam_id, c_start, c_end)
        total += n
    return total