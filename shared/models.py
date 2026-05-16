"""
shared/models.py
Dataclass dùng chung giữa api, worker-scheduled, worker-ondemand.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import re

@dataclass
class Camera:
    id:          int
    name:        str
    shift:       str          # 'morning' | 'night'
    nvr_channel: int
    status:      str = "active"


@dataclass
class QrScan:
    cam_id:      int
    qr_value:    str
    detected_at: datetime
    chunk_start: datetime
    chunk_end:   datetime
    shift:       str           # 'MORNING' | 'AFTERNOON'
    id:          Optional[int] = None
    clip_file:   Optional[str] = None
    created_at:  Optional[datetime] = None


@dataclass
class OndemandJob:
    """Job tải ±2 phút footage theo yêu cầu từ dashboard."""
    job_id:       str
    qr_value:     str
    scan_ids:     list[int] = field(default_factory=list)
    status:       str = "pending"   # pending | running | done | error
    error_msg:    str = ""
    clip_files:   list[str] = field(default_factory=list)
    created_at:   datetime = field(default_factory=datetime.now)


# ─── Ca làm việc ────────────────────────────────────────────────
from datetime import time as dtime

WORK_START    = dtime(8,  0)
LUNCH_START   = dtime(12, 0)
LUNCH_END     = dtime(13, 0)
WORK_END      = dtime(17, 0)

QR_VALID_PREFIX   = ""
QR_INVALID_PREFIX = ""  # giữ lại nếu muốn filter thêm theo prefix

# Các pattern bị loại bất kể QR_VALID_PREFIX
_INVALID_PATTERNS = [
    re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),          # email
    re.compile(r"^(\+84|84|0)(3|5|7|8|9)\d{8}$"),        # SĐT Việt Nam
]


def is_valid_qr(qr: str) -> bool:
    # Loại email và số điện thoại
    for pattern in _INVALID_PATTERNS:
        if pattern.match(qr.strip()):
            return False

    # Loại theo prefix tùy chỉnh
    if QR_INVALID_PREFIX and qr.startswith(QR_INVALID_PREFIX):
        return False

    if not QR_VALID_PREFIX:
        return True
    return qr.startswith(QR_VALID_PREFIX)


def get_shift(dt: datetime) -> str:
    """
    Trả về tên ca làm việc tại thời điểm dt.
    Dùng để ghi vào cột `shift` trong qr_scans.
    """
    t = dt.time()
    if t < WORK_START or t >= WORK_END:
        return "OUT OF WORK"
    if LUNCH_START <= t < LUNCH_END:
        return "LUNCH BREAK"
    if t < LUNCH_START:
        return "MORNING"
    return "AFTERNOON"