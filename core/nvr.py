"""
core/nvr.py
HikvisionPlayback — tải clip playback từ NVR qua HCNetSDK.
"""

import asyncio
import ctypes
from ctypes import *
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

from core.config import SDK_PATH, LOG_DIR

logger = logging.getLogger("hikvision_playback")
logger.setLevel(logging.DEBUG)

_fmt = logging.Formatter(fmt="%(asctime)s [%(levelname)-8s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
_fh  = RotatingFileHandler(LOG_DIR / "hikvision_playback.log", maxBytes=5*1024*1024, backupCount=5, encoding="utf-8")
_fh.setLevel(logging.DEBUG); _fh.setFormatter(_fmt)
_ch  = logging.StreamHandler()
_ch.setLevel(logging.INFO); _ch.setFormatter(_fmt)
logger.addHandler(_fh); logger.addHandler(_ch)

try:
    sdk = ctypes.CDLL(SDK_PATH)
    logger.info(f"HCNetSDK loaded from {SDK_PATH}")
except OSError as e:
    sdk = None
    logger.error(f"Cannot load HCNetSDK: {e}")


class NET_DVR_DEVICEINFO_V30(Structure):
    _fields_ = [
        ("sSerialNumber",    c_byte * 48), ("byAlarmInPortNum",  c_byte),
        ("byAlarmOutPortNum", c_byte),     ("byDiskNum",         c_byte),
        ("byDVRType",        c_byte),      ("byChanNum",         c_byte),
        ("byStartChan",      c_byte),      ("byAudioChanNum",    c_byte),
        ("byIPChanNum",      c_byte),      ("byRes",             c_byte * 36),
    ]


class NET_DVR_TIME(Structure):
    _fields_ = [
        ("dwYear", c_uint), ("dwMonth", c_uint), ("dwDay",    c_uint),
        ("dwHour", c_uint), ("dwMinute", c_uint), ("dwSecond", c_uint),
    ]


STREAM_ID_LEN = 32


class NET_DVR_STREAM_INFO(Structure):
    _fields_ = [
        ("dwSize", c_uint), ("byID", c_byte * STREAM_ID_LEN),
        ("dwChannel", c_uint), ("byStreamType", c_byte), ("byRes", c_byte * 31),
    ]


class NET_DVR_VOD_PARA(Structure):
    _fields_ = [
        ("dwSize", c_uint), ("struIDInfo", NET_DVR_STREAM_INFO),
        ("struBeginTime", NET_DVR_TIME), ("struEndTime", NET_DVR_TIME),
        ("hWnd", c_void_p), ("byDrawFrame", c_byte), ("byVolumeType", c_byte),
        ("byVolumeNum", c_byte), ("byRes1", c_byte), ("dwFileIndex", c_uint),
        ("byRes2", c_byte * 24),
    ]


PLAY_START = 1
PLAY_STOP  = 2
POS_DONE   = 100
POS_ERROR  = -1


def _dt_to_sdk(dt: datetime) -> NET_DVR_TIME:
    return NET_DVR_TIME(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)


@dataclass
class NVRConfig:
    ip:          str  = field(default_factory=lambda: os.environ.get("NVR_HOST", ""))
    port:        int  = field(default_factory=lambda: int(os.environ.get("NVR_PORT", "8000")))
    username:    str  = field(default_factory=lambda: os.environ.get("NVR_USER", "trunghieu"))
    password:    str  = field(default_factory=lambda: os.environ.get("NVR_PASS", ""))
    channel_map: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.ip:       raise ValueError("NVR_HOST not set")
        if not self.password: raise ValueError("NVR_PASS not set")


class HikvisionPlayback:
    POLL_INTERVAL_SEC   = 2
    DEFAULT_TIMEOUT_SEC = 600
    STALL_THRESHOLD_PCT = 1
    STALL_WINDOW_SEC    = 300

    def __init__(self, cfg: NVRConfig):
        if sdk is None:
            raise RuntimeError("HCNetSDK not loaded")
        self.cfg         = cfg
        self._login_lock = threading.Lock()
        self._uid        = -1
        self._init_sdk()

    def _init_sdk(self):
        sdk.NET_DVR_Init()
        sdk.NET_DVR_SetConnectTime(2000, 1)
        sdk.NET_DVR_SetReconnect(10000, True)
        logger.info("HCNetSDK initialized")

    def login(self, force: bool = False) -> bool:
        with self._login_lock:
            if self._uid >= 0 and not force:
                return True
            if self._uid >= 0 and force:
                sdk.NET_DVR_Logout(self._uid)
                self._uid = -1

            sdk.NET_DVR_Login_V30.restype  = c_long
            sdk.NET_DVR_Login_V30.argtypes = [
                c_char_p, c_uint16, c_char_p, c_char_p,
                POINTER(NET_DVR_DEVICEINFO_V30),
            ]
            dev_info = NET_DVR_DEVICEINFO_V30()
            uid = sdk.NET_DVR_Login_V30(
                self.cfg.ip.encode(), self.cfg.port,
                self.cfg.username.encode(), self.cfg.password.encode(),
                byref(dev_info),
            )
            if uid < 0:
                logger.error(f"NVR login FAILED — error: {sdk.NET_DVR_GetLastError()}")
                return False
            self._uid = uid
            logger.info(f"NVR login OK — uid={uid}")
            return True

    def logout(self):
        with self._login_lock:
            if self._uid >= 0:
                sdk.NET_DVR_Logout(self._uid)
                self._uid = -1
        sdk.NET_DVR_Cleanup()

    def download_clip(
        self,
        cam_id:       int,
        start_dt:     datetime,
        stop_dt:      datetime,
        output_path:  str | Path,
        timeout_sec:  int = DEFAULT_TIMEOUT_SEC,
        cancel_event: threading.Event | None = None,
    ) -> bool:
        output_path = Path(output_path)
        logger.info(f"[CAM {cam_id}] download_clip {start_dt:%Y-%m-%d %H:%M:%S} → {stop_dt:%H:%M:%S}")

        if cancel_event and cancel_event.is_set():
            return False
        if not self.login():
            return False

        channel = self.cfg.channel_map.get(cam_id)
        if channel is None:
            logger.error(f"[CAM {cam_id}] no channel mapping")
            return False
        logger.debug(f"[CAM {cam_id}] SDK channel={channel}")

        vod = NET_DVR_VOD_PARA()
        ctypes.memset(byref(vod), 0, sizeof(vod))
        vod.dwSize               = sizeof(vod)
        vod.struIDInfo.dwSize       = sizeof(NET_DVR_STREAM_INFO)
        vod.struIDInfo.dwChannel    = channel
        vod.struIDInfo.byStreamType = 1  # 0=main stream, 1=sub stream
        vod.struBeginTime        = _dt_to_sdk(start_dt)
        vod.struEndTime          = _dt_to_sdk(stop_dt)
        vod.hWnd                 = None

        sdk.NET_DVR_PlayBackByTime_V40.restype  = c_long
        sdk.NET_DVR_PlayBackByTime_V40.argtypes = [c_long, POINTER(NET_DVR_VOD_PARA)]

        with self._login_lock:
            uid_snapshot = self._uid
        handle = sdk.NET_DVR_PlayBackByTime_V40(uid_snapshot, byref(vod))

        if handle <= 0:
            logger.warning(f"[CAM {cam_id}] PlayBackByTime_V40 FAILED — trying re-login")
            if not self.login(force=True):
                return False
            with self._login_lock:
                uid_snapshot = self._uid
            handle = sdk.NET_DVR_PlayBackByTime_V40(uid_snapshot, byref(vod))
            if handle <= 0:
                logger.error(f"[CAM {cam_id}] Still FAILED after re-login")
                return False

        sdk.NET_DVR_PlayBackSaveData.restype  = c_bool
        sdk.NET_DVR_PlayBackSaveData.argtypes = [c_long, c_char_p]
        if not sdk.NET_DVR_PlayBackSaveData(handle, str(output_path).encode("gbk")):
            sdk.NET_DVR_StopPlayBack(handle)
            return False

        sdk.NET_DVR_PlayBackControl.restype  = c_bool
        sdk.NET_DVR_PlayBackControl.argtypes = [c_long, c_uint, c_void_p, c_uint]
        if not sdk.NET_DVR_PlayBackControl(handle, PLAY_START, None, 0):
            sdk.NET_DVR_StopPlayBack(handle)
            return False

        sdk.NET_DVR_GetDownloadPos.restype  = c_int
        sdk.NET_DVR_GetDownloadPos.argtypes = [c_long]

        t0          = time.monotonic()
        last_pct    = 0
        stall_since = time.monotonic()
        success     = False
        cancelled   = False

        while True:
            if cancel_event and cancel_event.is_set():
                cancelled = True
                break
            if time.monotonic() - t0 > timeout_sec:
                break

            pct = sdk.NET_DVR_GetDownloadPos(handle)
            if pct == POS_ERROR:
                break
            if pct > last_pct:
                last_pct = pct; stall_since = time.monotonic()
            if (time.monotonic() - stall_since) > self.STALL_WINDOW_SEC and pct < POS_DONE:
                logger.error(f"[CAM {cam_id}] Stall at {pct}%"); break
            if pct >= POS_DONE:
                success = True; break
            time.sleep(self.POLL_INTERVAL_SEC)

        sdk.NET_DVR_StopPlayBack(handle)

        if success:
            prev_size = -1
            for _ in range(10):
                time.sleep(1)
                cur_size = output_path.stat().st_size if output_path.exists() else 0
                if cur_size == prev_size and cur_size > 0:
                    break
                prev_size = cur_size
            else:
                success = False

        if cancelled or not success:
            output_path.unlink(missing_ok=True)

        return success

    async def async_download_clip(
        self,
        cam_id:       int,
        start_dt:     datetime,
        stop_dt:      datetime,
        output_path:  str | Path,
        timeout_sec:  int = DEFAULT_TIMEOUT_SEC,
        cancel_event: threading.Event | None = None,
    ) -> bool:
        return await asyncio.to_thread(
            self.download_clip,
            cam_id, start_dt, stop_dt, output_path, timeout_sec, cancel_event,
        )


_playback_instance: HikvisionPlayback | None = None


async def init_playback(cfg: NVRConfig | None = None):
    global _playback_instance
    from core.db import get_channel_map
    if cfg is None:
        cfg = NVRConfig()
    cfg.channel_map = await get_channel_map()
    logger.info(f"channel_map loaded: {cfg.channel_map}")
    _playback_instance = HikvisionPlayback(cfg)
    _playback_instance.login()


async def refresh_channel_map():
    if _playback_instance is None:
        return
    from core.db import get_channel_map
    _playback_instance.cfg.channel_map = await get_channel_map()
    logger.info(f"channel_map refreshed: {_playback_instance.cfg.channel_map}")


def get_playback() -> HikvisionPlayback:
    if _playback_instance is None:
        raise RuntimeError("Playback not initialized. Call init_playback() first.")
    return _playback_instance
