from dataclasses import dataclass, field
from datetime import datetime, time as dtime
from typing import Optional


@dataclass
class Camera:
    id:          int
    name:        str
    shift:       str
    nvr_channel: int
    status:      str = "active"


@dataclass
class QrScan:
    cam_id:      int
    qr_value:    str
    detected_at: datetime
    chunk_start: datetime
    chunk_end:   datetime
    shift:       str
    id:          Optional[int] = None
    clip_file:   Optional[str] = None
    created_at:  Optional[datetime] = None


@dataclass
class OndemandJob:
    job_id:     str
    qr_value:   str
    scan_ids:   list[int] = field(default_factory=list)
    status:     str = "pending"
    error_msg:  str = ""
    clip_files: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)


WORK_START  = dtime(8,  0)
LUNCH_START = dtime(12, 0)
LUNCH_END   = dtime(13, 0)
WORK_END    = dtime(17, 0)

QR_VALID_PREFIX   = ""
QR_INVALID_PREFIX = ""


def is_valid_qr(qr: str) -> bool:
    v = qr.strip()
    if not v or not v[0].isalpha() or "@" in v:
        return False
    if QR_INVALID_PREFIX and v.startswith(QR_INVALID_PREFIX):
        return False
    if QR_VALID_PREFIX:
        return v.startswith(QR_VALID_PREFIX)
    return True


def get_shift(dt: datetime) -> str:
    t = dt.time()
    if t < WORK_START or t >= WORK_END:
        return "OUT OF WORK"
    if LUNCH_START <= t < LUNCH_END:
        return "LUNCH BREAK"
    if t < LUNCH_START:
        return "MORNING"
    return "AFTERNOON"
