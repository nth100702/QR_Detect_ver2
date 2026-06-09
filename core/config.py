import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env")

# ── NVR ──────────────────────────────────────────────────────────
NVR_HOST = os.environ["NVR_HOST"]
NVR_PORT = int(os.environ.get("NVR_PORT", "8000"))
NVR_USER = os.environ.get("NVR_USER", "trunghieu")
NVR_PASS = os.environ["NVR_PASS"]

# ── HCNetSDK ─────────────────────────────────────────────────────
SDK_PATH = os.environ.get(
    "HCNET_SDK_PATH",
    str(_ROOT / "sdk" / "lib" / "HCNetSDK.dll")
)

# ── Database ─────────────────────────────────────────────────────
DATABASE_URL = os.environ["DATABASE_URL"]

# ── Redis ────────────────────────────────────────────────────────
REDIS_URL = os.environ["REDIS_URL"]

# ── API ──────────────────────────────────────────────────────────
API_HOST     = os.environ.get("API_HOST", "0.0.0.0")
API_PORT     = int(os.environ.get("API_PORT", "8000"))
API_SECRET   = os.environ["API_SECRET"]
VIEWER_SECRET = os.environ["VIEWER_SECRET"]
API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")

# ── Thư mục ──────────────────────────────────────────────────────
BASE_DIR      = _ROOT
TEMP_VIDEO_DIR = Path(os.environ.get("TEMP_VIDEO_DIR", str(_ROOT / "temp_videos")))
TEMP_CLIP_DIR  = Path(os.environ.get("TEMP_CLIP_DIR",  str(_ROOT / "temp_clips")))
LOG_DIR        = _ROOT / "logs"

for _d in (TEMP_VIDEO_DIR, TEMP_CLIP_DIR, LOG_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ── Scanner ──────────────────────────────────────────────────────
CHUNK_MINUTES     = 5
SEMAPHORE_COUNT   = 6
DETECT_SAMPLE_FPS = 0.5
DETECT_WORKERS    = 3   # CPU-bound: 4 core → 3 workers, giữ 1 core cho API/DL
NVR_CHANNELS      = 4   # I/O-bound: tăng lên 4, test thêm nếu NVR chịu được
SEGMENT_HOURS     = 0.5    # 30 phút
CLIP_MIN_DURATION = 30

# ── QR ───────────────────────────────────────────────────────────
QR_VALID_PREFIX = "SPXVN"

# ── Ca làm việc (dùng cho APScheduler) ───────────────────────────
@dataclass
class Shift:
    name:               str
    cron_hour:          int
    cron_minute:        int
    footage_start_hour: int = 8
    footage_end_hour:   int = 19

SHIFTS: list[Shift] = [
    Shift(name="morning", cron_hour=7,  cron_minute=0),
    Shift(name="night",   cron_hour=20, cron_minute=0),
]
