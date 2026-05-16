"""
config.py
Tất cả cấu hình đọc từ file .env (hoặc environment variables).
Import file này từ bất kỳ module nào trong project.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env từ thư mục gốc project
# load_dotenv(Path(__file__).parent / ".env")
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

# ── NVR ──────────────────────────────────────────────────────────
NVR_HOST = os.environ["NVR_HOST"]
NVR_PORT = int(os.environ.get("NVR_PORT", "8000"))
NVR_USER = os.environ.get("NVR_USER", "trunghieu")
NVR_PASS = os.environ["NVR_PASS"]

# {cam_id_trong_db: nvr_channel_number}
# Thêm/sửa tại đây khi cắm thêm camera
CHANNEL_MAP: dict[int, int] = {
    1: 1,
    5: 5,
}

# ── HCNetSDK ─────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[1]

SDK_PATH = os.environ.get(
    "HCNET_SDK_PATH",
    str(BASE_DIR / "sdk" / "lib" / "HCNetSDK.dll")
)

# ── Database ─────────────────────────────────────────────────────
DATABASE_URL = os.environ["DATABASE_URL"]
# Ví dụ: postgresql://qruser:password@localhost:5432/qrscanner

# ── Redis (chỉ dùng bởi worker-ondemand) ────────────────────────
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")

# ── API ──────────────────────────────────────────────────────────
API_HOST   = os.environ.get("API_HOST", "0.0.0.0")
API_PORT   = int(os.environ.get("API_PORT", "8000"))
API_SECRET = os.environ["API_SECRET"]
API_BASE_URL = os.environ.get(
    "API_BASE_URL",
    "http://localhost:8000"
)
# ── Ca làm việc ──────────────────────────────────────────────────
from dataclasses import dataclass, field

@dataclass
class Shift:
    name:               str
    cron_hour:          int
    cron_minute:        int
    cam_ids:            list[int]
    footage_start_hour: int = 8
    footage_end_hour:   int = 19

SHIFTS: list[Shift] = [
    Shift(name="morning", cron_hour=7,  cron_minute=0, cam_ids=[1, 2]),
    Shift(name="night",   cron_hour=20, cron_minute=0, cam_ids=[6, 7, 8, 9, 10]),
]

CHUNK_MINUTES    = 5    # mỗi chunk playback
SEMAPHORE_COUNT  = 2    # số cam download song song
DETECT_SAMPLE_FPS = 1   # sample 1 fps khi detect QR

# ── Thư mục tạm ──────────────────────────────────────────────────
BASE_DIR         = Path(__file__).parent
TEMP_VIDEO_DIR   = Path(os.environ.get("TEMP_VIDEO_DIR",  str(BASE_DIR / "temp_videos")))
TEMP_CLIP_DIR    = Path(os.environ.get("TEMP_CLIP_DIR",   str(BASE_DIR / "temp_clips")))
LOG_DIR          = BASE_DIR / "logs"

for _d in (TEMP_VIDEO_DIR, TEMP_CLIP_DIR, LOG_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ── QR ───────────────────────────────────────────────────────────
QR_VALID_PREFIX  = "SPXVN"
CLIP_MIN_DURATION = 30   # giây — switch QR chỉ khi clip hiện tại >= 30s

API_SECRET = os.environ["API_SECRET"]