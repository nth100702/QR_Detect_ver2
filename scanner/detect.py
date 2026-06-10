"""
scanner/detect.py
Detect QR từ file video đã tải.
"""

import asyncio
import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import cv2
import httpx
import zxingcpp

from core.models import is_valid_qr, get_shift
from core.config import API_BASE_URL, API_SECRET

logger = logging.getLogger(__name__)

BURST = 2   # số frame decode liên tiếp
SKIP  = 3   # số frame skip sau burst
CYCLE = BURST + SKIP  # = 5

# Dedup: cùng mã QR trong vòng N giây → chỉ ghi 1 lần
QR_COOLDOWN_SEC = 4 * 3600  # 4 tiếng

# Lưu thời điểm ghi nhận cuối của từng QR, chia theo cam_id
# key: (cam_id, qr_value) → datetime
_last_seen: dict[tuple[int, str], datetime] = {}

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


def _detect_video(
    video_path:   Path,
    cam_id:       int,
    chunk_start:  datetime,
    chunk_end:    datetime,
    cancel_event: threading.Event | None = None,
) -> list[dict]:
    """
    Đọc video, detect QR với adaptive sampling + time-window dedup.
    - Idle mode  : sample thưa (_IDLE_FPS) khi không thấy QR
    - Active mode: sample dày (_ACTIVE_FPS) khi vừa thấy QR
    - Dedup      : cùng mã trong QR_COOLDOWN_SEC giây → bỏ qua
    Dùng grab() cho frame bỏ qua để duy trì HEVC decoder state.
    """
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        logger.error(f"Cannot open video: {video_path}")
        return []

    fps          = cap.get(cv2.CAP_PROP_FPS) or 25.0
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    duration_sec = total_frames / fps

    logger.info(
        f"[CAM {cam_id}] Detecting {video_path.name} | "
        f"{fps:.1f}fps {duration_sec:.0f}s | burst={BURST} skip={SKIP}"
    )

    scans: list[dict] = []

    frame_idx = 0
    sampled   = 0

    while True:
        if frame_idx % CYCLE >= BURST:
            if not cap.grab():
                break
            frame_idx += 1
            continue

        ret, frame = cap.read()
        if not ret:
            break

        if cancel_event and cancel_event.is_set():
            logger.warning(f"[CAM {cam_id}] _detect_video cancelled at frame {frame_idx}")
            break

        sampled += 1
        actual_msec = cap.get(cv2.CAP_PROP_POS_MSEC)
        detected_at = chunk_start + timedelta(seconds=actual_msec / 1000.0)

        h, w = frame.shape[:2]
        if w > 1920:
            scale = 1920.0 / w
            frame = cv2.resize(frame, (int(w * scale), int(h * scale)))

        try:
            gray    = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            results = zxingcpp.read_barcodes(gray, formats=zxingcpp.BarcodeFormat.QRCode)
        except Exception as e:
            logger.debug(f"[CAM {cam_id}] decode error at frame {frame_idx}: {e}")
            frame_idx += 1
            continue

        for r in results:
            qr = r.text
            if not qr or not is_valid_qr(qr):
                continue

            # Dedup: bỏ qua nếu cùng mã vừa được ghi trong cooldown window
            key  = (cam_id, qr)
            prev = _last_seen.get(key)
            if prev is not None and (detected_at - prev).total_seconds() < QR_COOLDOWN_SEC:
                continue

            _last_seen[key] = detected_at
            shift = get_shift(detected_at)

            scans.append({
                "cam_id":      cam_id,
                "qr_value":    qr,
                "detected_at": detected_at.isoformat(),
                "chunk_start": chunk_start.isoformat(),
                "chunk_end":   chunk_end.isoformat(),
                "shift":       shift,
            })
            logger.info(f"[CAM {cam_id}] QR detected: {qr} @ {detected_at:%H:%M:%S} ({shift})")

        frame_idx += 1

    cap.release()
    logger.info(f"[CAM {cam_id}] {video_path.name} → {len(scans)} QR(s) | {sampled} frames sampled")
    return scans


async def process_video(
    video_path:   Path,
    cam_id:       int,
    chunk_start:  datetime,
    chunk_end:    datetime,
    delete_after: bool = True,
    cancel_event: threading.Event | None = None,
) -> int:
    scans = await asyncio.to_thread(
        _detect_video, video_path, cam_id, chunk_start, chunk_end, cancel_event
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
