"""
shared/hikvision_playback.py
Tải clip playback từ NVR Hikvision qua HCNetSDK.

Cải tiến so với bản cũ:
  1. download_clip() poll NET_DVR_PlayBackGetPos thay vì sleep cố định
  2. Hỗ trợ asyncio qua asyncio.to_thread()
  3. NVRConfig đọc từ os.environ (không hardcode)
"""

import asyncio
import ctypes
from ctypes import *
import os
import threading
import time
import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime, timedelta
from worker_scheduled.config import SDK_PATH

# ─────────────────────────────────────────────
# LOGGER
# ─────────────────────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "hikvision_playback.log"

logger = logging.getLogger("hikvision_playback")
logger.setLevel(logging.DEBUG)

_fmt = logging.Formatter(
    fmt="%(asctime)s [%(levelname)-8s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_fh = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(_fmt)
_ch = logging.StreamHandler()
_ch.setLevel(logging.INFO)
_ch.setFormatter(_fmt)
logger.addHandler(_fh)
logger.addHandler(_ch)

# ─────────────────────────────────────────────
# Load SDK
# ─────────────────────────────────────────────
# SDK_PATH = os.environ.get("HCNET_SDK_PATH", "./HCNetSDK.dll")

try:
    sdk = ctypes.CDLL(SDK_PATH)
    logger.info(f"HCNetSDK loaded from {SDK_PATH}")
except OSError as e:
    sdk = None
    logger.error(f"Cannot load HCNetSDK: {e}")


# ─────────────────────────────────────────────
# Structs
# ─────────────────────────────────────────────
class NET_DVR_DEVICEINFO_V30(Structure):
    _fields_ = [
        ("sSerialNumber",    c_byte * 48),
        ("byAlarmInPortNum", c_byte),
        ("byAlarmOutPortNum",c_byte),
        ("byDiskNum",        c_byte),
        ("byDVRType",        c_byte),
        ("byChanNum",        c_byte),
        ("byStartChan",      c_byte),
        ("byAudioChanNum",   c_byte),
        ("byIPChanNum",      c_byte),
        ("byRes",            c_byte * 36),
    ]


class NET_DVR_TIME(Structure):
    _fields_ = [
        ("dwYear",   c_uint),
        ("dwMonth",  c_uint),
        ("dwDay",    c_uint),
        ("dwHour",   c_uint),
        ("dwMinute", c_uint),
        ("dwSecond", c_uint),
    ]


STREAM_ID_LEN = 32


class NET_DVR_STREAM_INFO(Structure):
    _fields_ = [
        ("dwSize",    c_uint),
        ("byID",      c_byte * STREAM_ID_LEN),
        ("dwChannel", c_uint),
        ("byRes",     c_byte * 32),
    ]


class NET_DVR_VOD_PARA(Structure):
    _fields_ = [
        ("dwSize",       c_uint),
        ("struIDInfo",   NET_DVR_STREAM_INFO),
        ("struBeginTime",NET_DVR_TIME),
        ("struEndTime",  NET_DVR_TIME),
        ("hWnd",         c_void_p),
        ("byDrawFrame",  c_byte),
        ("byVolumeType", c_byte),
        ("byVolumeNum",  c_byte),
        ("byRes1",       c_byte),
        ("dwFileIndex",  c_uint),
        ("byRes2",       c_byte * 24),
    ]


# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────
PLAY_START  = 1
PLAY_STOP   = 2
# PlayBackGetPos trả về 0–100 (%), 100 = hoàn tất, -1 = lỗi
POS_DONE    = 100
POS_ERROR   = -1


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def _dt_to_sdk(dt: datetime) -> NET_DVR_TIME:
    return NET_DVR_TIME(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)


@dataclass
class NVRConfig:
    """
    Đọc config từ environment variables.
    Fallback sang giá trị mặc định chỉ dùng khi dev/test.
    """
    ip:          str  = field(default_factory=lambda: os.environ.get("NVR_HOST", ""))
    port:        int  = field(default_factory=lambda: int(os.environ.get("NVR_PORT", "8000")))
    username:    str  = field(default_factory=lambda: os.environ.get("NVR_USER", "trunghieu"))
    password:    str  = field(default_factory=lambda: os.environ.get("NVR_PASS", ""))
    channel_map: dict = field(default_factory=dict)  # {cam_id (DB) → NVR channel}
    # channel_map: dict = field(default_factory=lambda: {
    # 1: 1,
    # 5: 5,
    # })

    def __post_init__(self):
        if not self.ip:
            raise ValueError("NVR_HOST environment variable is not set")
        if not self.password:
            raise ValueError("NVR_PASS environment variable is not set")


# ─────────────────────────────────────────────
# Core
# ─────────────────────────────────────────────
class HikvisionPlayback:
    """
    Quản lý kết nối NVR và tải playback clip.

    Thread-safe: login/logout dùng lock riêng.
    Mỗi download_clip() mở handle riêng, không share.
    """

    # Poll NET_DVR_PlayBackGetPos mỗi POLL_INTERVAL giây
    POLL_INTERVAL_SEC  = 2
    # Timeout tổng (giây) trước khi bỏ cuộc — 10 phút đủ cho clip 5 phút
    DEFAULT_TIMEOUT_SEC = 600
    # % tiến độ tối thiểu trong một chu kỳ STALL_WINDOW để coi là đang chạy
    STALL_THRESHOLD_PCT = 1
    STALL_WINDOW_SEC    = 60   # nếu không tăng 1% trong 60s → stall

    def __init__(self, cfg: NVRConfig):
        if sdk is None:
            raise RuntimeError("HCNetSDK not loaded")
        self.cfg    = cfg
        self._lock  = threading.Lock()
        self._uid   = -1
        self._init_sdk()

    # ── SDK init ──────────────────────────────
    def _init_sdk(self):
        sdk.NET_DVR_Init()
        sdk.NET_DVR_SetConnectTime(2000, 1)
        sdk.NET_DVR_SetReconnect(10000, True)
        logger.info("HCNetSDK initialized")

    def login(self, force: bool = False) -> bool:
        with self._lock:
            if self._uid >= 0 and not force:
                return True

            # Force re-login: logout session cũ trước
            if self._uid >= 0 and force:
                logger.info(f"Force re-login — logout uid={self._uid} trước")
                sdk.NET_DVR_Logout(self._uid)
                self._uid = -1

            sdk.NET_DVR_Login_V30.restype  = c_long
            sdk.NET_DVR_Login_V30.argtypes = [
                c_char_p, c_uint16, c_char_p, c_char_p,
                POINTER(NET_DVR_DEVICEINFO_V30),
            ]

            logger.info(f"Logging in to NVR {self.cfg.ip}:{self.cfg.port} ...")
            dev_info = NET_DVR_DEVICEINFO_V30()
            uid = sdk.NET_DVR_Login_V30(
                self.cfg.ip.encode(),
                self.cfg.port,
                self.cfg.username.encode(),
                self.cfg.password.encode(),
                byref(dev_info),
            )

            if uid < 0:
                err = sdk.NET_DVR_GetLastError()
                logger.error(f"NVR login FAILED — error code: {err}")
                return False

            self._uid = uid
            logger.info(f"NVR login OK — uid={uid}")
            return True

    def logout(self):
        with self._lock:
            if self._uid >= 0:
                sdk.NET_DVR_Logout(self._uid)
                logger.info(f"NVR logout — uid={self._uid}")
                self._uid = -1
        sdk.NET_DVR_Cleanup()
        logger.info("HCNetSDK cleanup done")

    # ── Download (blocking, dùng trong thread) ──
    def download_clip(
        self,
        cam_id:      int,
        start_dt:    datetime,
        stop_dt:     datetime,
        output_path: str | Path,
        timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    ) -> bool:
        """
        Tải clip từ NVR về output_path.

        Dùng NET_DVR_PlayBackGetPos để poll tiến độ thực sự thay vì sleep cố định.
        Trả về True khi hoàn tất, False nếu lỗi / timeout / stall.

        Parameters
        ----------
        cam_id      : ID trong DB — map sang NVR channel qua cfg.channel_map
        start_dt    : thời điểm bắt đầu clip
        stop_dt     : thời điểm kết thúc clip
        output_path : đường dẫn file .mp4 đầu ra
        timeout_sec : giới hạn thời gian chờ tổng
        """
        t0 = time.monotonic()
        logger.info(
            f"[CAM {cam_id}] download_clip | "
            f"{start_dt.strftime('%Y-%m-%d %H:%M:%S')} → {stop_dt.strftime('%H:%M:%S')}"
        )

        if not self.login():
            logger.error(f"[CAM {cam_id}] Abort — NVR not logged in")
            return False

        channel = self.cfg.channel_map.get(cam_id)
        if channel is None:
            logger.error(f"[CAM {cam_id}] Abort — no channel mapping for cam_id={cam_id}")
            return False

        # ── Build VOD params ──
        vod = NET_DVR_VOD_PARA()
        ctypes.memset(byref(vod), 0, sizeof(vod))
        vod.dwSize               = sizeof(vod)
        vod.struIDInfo.dwSize    = sizeof(NET_DVR_STREAM_INFO)
        vod.struIDInfo.dwChannel = channel
        vod.struBeginTime        = _dt_to_sdk(start_dt)
        vod.struEndTime          = _dt_to_sdk(stop_dt)
        vod.hWnd                 = None

        # ── Open playback handle ──
        sdk.NET_DVR_PlayBackByTime_V40.restype  = c_long
        sdk.NET_DVR_PlayBackByTime_V40.argtypes = [c_long, POINTER(NET_DVR_VOD_PARA)]

        with self._lock:
            logger.debug(f"[CAM {cam_id}] PlayBackByTime_V40 — uid={self._uid}, channel={channel}")
            handle = sdk.NET_DVR_PlayBackByTime_V40(self._uid, byref(vod))

        if handle <= 0:
            err = sdk.NET_DVR_GetLastError()
            logger.warning(
                f"[CAM {cam_id}] PlayBackByTime_V40 FAILED — "
                f"handle={handle}, SDK error: {err} — thử re-login..."
            )
            # Session NVR có thể đã expire → force re-login rồi thử lại 1 lần
            if not self.login(force=True):
                logger.error(f"[CAM {cam_id}] Re-login FAILED — abort")
                return False
            with self._lock:
                handle = sdk.NET_DVR_PlayBackByTime_V40(self._uid, byref(vod))
            if handle <= 0:
                err = sdk.NET_DVR_GetLastError()
                logger.error(
                    f"[CAM {cam_id}] PlayBackByTime_V40 vẫn FAILED sau re-login — "
                    f"handle={handle}, SDK error: {err}"
                )
                return False
            logger.info(f"[CAM {cam_id}] Re-login thành công, handle={handle}")

        logger.info(f"[CAM {cam_id}] Playback handle={handle}, channel={channel}")

        # ── Set save path ──
        sdk.NET_DVR_PlayBackSaveData.restype  = c_bool
        sdk.NET_DVR_PlayBackSaveData.argtypes = [c_long, c_char_p]

        out_bytes = str(output_path).encode("gbk")
        if not sdk.NET_DVR_PlayBackSaveData(handle, out_bytes):
            err = sdk.NET_DVR_GetLastError()
            logger.error(
                f"[CAM {cam_id}] PlayBackSaveData FAILED — "
                f"handle={handle}, path={output_path}, SDK error: {err}"
            )
            sdk.NET_DVR_StopPlayBack(handle)
            return False

        # ── Start playback ──
        sdk.NET_DVR_PlayBackControl.restype  = c_bool
        sdk.NET_DVR_PlayBackControl.argtypes = [c_long, c_uint, c_void_p, c_uint]
        ctrl_ok = sdk.NET_DVR_PlayBackControl(handle, PLAY_START, None, 0)
        if not ctrl_ok:
            err = sdk.NET_DVR_GetLastError()
            logger.error(
                f"[CAM {cam_id}] PlayBackControl(PLAY_START) FAILED — "
                f"handle={handle}, SDK error: {err}"
            )
            sdk.NET_DVR_StopPlayBack(handle)
            return False
        logger.info(f"[CAM {cam_id}] Playback started → {output_path}")

        # ── Poll tiến độ ──
        # sdk.NET_DVR_PlayBackGetPos.restype  = c_int
        # sdk.NET_DVR_PlayBackGetPos.argtypes = [c_long]
        sdk.NET_DVR_GetDownloadPos.restype  = c_int
        sdk.NET_DVR_GetDownloadPos.argtypes = [c_long]

        last_pct        = 0
        stall_since     = time.monotonic()
        success         = False

        while True:
            elapsed = time.monotonic() - t0
            if elapsed > timeout_sec:
                logger.error(
                    f"[CAM {cam_id}] Timeout after {elapsed:.0f}s "
                    f"(limit {timeout_sec}s) at {last_pct}%"
                )
                break

            # pct = sdk.NET_DVR_PlayBackGetPos(handle)
            pct = sdk.NET_DVR_GetDownloadPos(handle)

            if pct == POS_ERROR:
                err = sdk.NET_DVR_GetLastError()
                logger.error(f"[CAM {cam_id}] PlayBackGetPos error — SDK error: {err}")
                break

            if pct > last_pct:
                logger.debug(f"[CAM {cam_id}] Progress: {pct}%")
                last_pct    = pct
                stall_since = time.monotonic()

            # Kiểm tra stall
            if (time.monotonic() - stall_since) > self.STALL_WINDOW_SEC and pct < POS_DONE:
                logger.error(
                    f"[CAM {cam_id}] Stall detected — stuck at {pct}% "
                    f"for {self.STALL_WINDOW_SEC}s"
                )
                break

            if pct >= POS_DONE:
                success = True
                logger.info(
                    f"[CAM {cam_id}] Download complete ✓ — "
                    f"elapsed {time.monotonic() - t0:.1f}s"
                )
                break

            time.sleep(self.POLL_INTERVAL_SEC)

        # ── Cleanup ──
        sdk.NET_DVR_StopPlayBack(handle)
        return success

    # ── Async wrapper (dùng trong asyncio pipeline) ──
    async def async_download_clip(
        self,
        cam_id:      int,
        start_dt:    datetime,
        stop_dt:     datetime,
        output_path: str | Path,
        timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    ) -> bool:
        """
        Async wrapper — chạy download_clip() trong thread pool.
        Không block event loop, không chiếm asyncio semaphore trong lúc sleep.
        """
        return await asyncio.to_thread(
            self.download_clip,
            cam_id, start_dt, stop_dt, output_path, timeout_sec,
        )


# ─────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────
_playback_instance: HikvisionPlayback | None = None


async def init_playback(cfg: NVRConfig | None = None):
    global _playback_instance
    from shared.db import get_channel_map

    if cfg is None:
        cfg = NVRConfig()

    cfg.channel_map = await get_channel_map()
    logger.info(f"channel_map loaded from DB: {cfg.channel_map}")  # ← xem log này ra gì

    _playback_instance = HikvisionPlayback(cfg)
    _playback_instance.login()


async def refresh_channel_map():
    """Gọi sau khi thêm/sửa/xóa camera — không cần restart."""
    if _playback_instance is None:
        return
    from shared.db import get_channel_map
    _playback_instance.cfg.channel_map = await get_channel_map()
    logger.info(f"channel_map refreshed: {_playback_instance.cfg.channel_map}")


def get_playback() -> HikvisionPlayback:
    if _playback_instance is None:
        raise RuntimeError("Playback not initialized. Call init_playback() first.")
    logger.info(f"current channel_map: {_playback_instance.cfg.channel_map}")  # ← thêm dòng này
    return _playback_instance