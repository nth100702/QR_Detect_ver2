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
QR_INVALID_PREFIX = ""

def is_valid_qr(qr: str) -> bool:
    v = qr.strip()
    
    # Phải bắt đầu bằng chữ cái và không chứa @
    if not v or not v[0].isalpha() or "@" in v:
        return False

    # Loại theo invalid prefix tùy chỉnh
    if QR_INVALID_PREFIX and v.startswith(QR_INVALID_PREFIX):
        return False

    # Nếu có valid prefix thì phải khớp
    if QR_VALID_PREFIX:
        return v.startswith(QR_VALID_PREFIX)

    return True


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