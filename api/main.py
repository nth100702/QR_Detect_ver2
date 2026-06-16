"""
api.py
FastAPI server — chạy trực tiếp trên Windows:
  python api.py
  hoặc: uvicorn api:app --reload --port 8000

Worker (APScheduler + manual trigger listener) chạy cùng process.
Không cần chạy worker_scheduled.py riêng nữa.

Redis là OPTIONAL:
  - Nếu có Redis  → /jobs hoạt động bình thường (tải footage on-demand)
  - Nếu không có  → /cameras /scans /stats vẫn chạy, chỉ /jobs trả 503
"""

import asyncio
import json
import logging
import logging.handlers
import shutil
import uuid
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path

import pytz
import redis.asyncio as aioredis
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security.api_key import APIKeyHeader
from fastapi.staticfiles import StaticFiles
from pathlib import Path as FilePath
from pydantic import BaseModel

from core import db
from core.nvr import init_playback, get_playback, refresh_channel_map
from core.models import QrScan
from core.config import (
    API_SECRET, VIEWER_SECRET, REDIS_URL, API_HOST, API_PORT, LOG_DIR,
    SHIFTS, CHUNK_MINUTES, TEMP_VIDEO_DIR, TEMP_CLIP_DIR, NVR_CHANNELS, SEGMENT_HOURS
)
from scanner.downloader import download_cam_chunks, chunk_ranges
from scanner.detect import process_video
from scanner.state import (
    set_shift_started, set_shift_finished, update_job_counters,
    set_cam_downloading, set_cam_chunk_progress,
    set_cam_detecting, set_cam_done, set_cam_error,
    clear_stale_jobs,
)

# ── Logger ───────────────────────────────────────────────────────
log_file = LOG_DIR / "api.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.handlers.RotatingFileHandler(
            log_file, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
        ),
        logging.StreamHandler(),
    ],
)
logger  = logging.getLogger("api")
wlogger = logging.getLogger("worker_scheduled")
TZ      = pytz.timezone("Asia/Ho_Chi_Minh")

QUEUE_KEY  = "jobs:ondemand"
STATUS_TTL = 3600

# ── Auth ─────────────────────────────────────────────────────────
_api_key_header = APIKeyHeader(name="X-Secret", auto_error=True)

def _resolve_role(key: str) -> str:
    """Trả về admin | viewer | raises 401."""
    if key == API_SECRET:
        return "admin"
    if key == VIEWER_SECRET:
        return "viewer"
    raise HTTPException(status_code=401, detail="Invalid API secret")

def verify_secret(key: str = Security(_api_key_header)):
    """Cho phép cả admin lẫn viewer (GET endpoints)."""
    _resolve_role(key)
    return key

def verify_admin(key: str = Security(_api_key_header)):
    """Chỉ cho phép admin (POST/DELETE/trigger)."""
    if _resolve_role(key) != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    return key

# ── Redis helper ─────────────────────────────────────────────────
_redis: aioredis.Redis | None = None

def require_redis():
    if _redis is None:
        raise HTTPException(
            status_code=503,
            detail="Redis không khả dụng. Cài Redis để dùng tính năng tải footage on-demand.",
        )
    return _redis

# ── Scan queue + trigger state ───────────────────────────────────
_scan_queue:     asyncio.Queue      = asyncio.Queue()
_trigger_paused: bool               = False
_active_tasks:   set[asyncio.Task]  = set()
# Theo dõi folder nào đang được task active sử dụng
# key = task name, value = set[Path] các folder đang dùng
_active_dirs:    dict[str, set]     = {}

# Lock per (cam_id, date) — ngăn cron và manual trigger cùng xử lý
# 1 cam + 1 ngày cùng lúc (tránh conflict file + duplicate scan)
_cam_locks: dict[str, asyncio.Lock] = {}
_cancel_events: dict[str, threading.Event] = {}

# ── Daily cron guard: chỉ cho 1 cron_daily chạy tại 1 thời điểm ─
# Nếu job trước chưa xong, ngày mới đẩy vào _pending_scan_dates
# và sẽ được xử lý backfill ngay sau khi job hiện tại hoàn thành.
_daily_scan_running: bool       = False
_pending_scan_dates: list[str]  = []   # ISO date strings "YYYY-MM-DD", FIFO

def get_scan_trigger_queue() -> asyncio.Queue:
    return _scan_queue

# ── Worker: download + detect pipeline ──────────────────────────
# async def _download_with_progress(
#     cam_id:       int,
#     date:         datetime,
#     start_hour:   int,
#     end_hour:     int,
#     start_minute: int = 0,
#     end_minute:   int = 0,
# ):
#     from scanner.downloader import _semaphore
#     from core.nvr import get_playback

#     footage_start = TZ.normalize(date.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0))
#     footage_end   = TZ.normalize(date.replace(hour=end_hour,   minute=end_minute,   second=0, microsecond=0))
#     chunks        = chunk_ranges(footage_start, footage_end)
#     total         = len(chunks)

#     set_cam_downloading(cam_id, total)
#     out_dir = TEMP_VIDEO_DIR / str(cam_id) / date.strftime("%Y%m%d")
#     out_dir.mkdir(parents=True, exist_ok=True)

#     # Đăng ký folder này với task hiện tại để cleanup an toàn
#     task_name = asyncio.current_task().get_name() if asyncio.current_task() else "_scheduler"
#     _active_dirs.setdefault(task_name, set()).add(out_dir)

#     results  = []
#     playback = get_playback()

#     # async with _semaphore:
#     #     for idx, (c_start, c_end) in enumerate(chunks):
#     #         label = f"{c_start:%H:%M}–{c_end:%H:%M}"
#     #         set_cam_chunk_progress(cam_id, idx, label, total)

#     #         out = out_dir / f"chunk_{c_start:%H%M}.mp4"
#     #         if out.exists() and out.stat().st_size > 0:
#     #             wlogger.debug(f"[CAM {cam_id}] {label} cached, skip")
#     #             results.append((out, c_start, c_end))
#     #             continue

#     #         ok = await playback.async_download_clip(cam_id, c_start, c_end, out)
#     #         if ok:
#     #             results.append((out, c_start, c_end))
#     #         else:
#     #             wlogger.error(f"[CAM {cam_id}] {label} FAILED")

#     # return results


#     for idx, (c_start, c_end) in enumerate(chunks):
#         label = f"{c_start:%H:%M}–{c_end:%H:%M}"
#         out = out_dir / f"chunk_{c_start:%H%M}.mp4"
#         if out.exists() and out.stat().st_size > 0:
#             wlogger.debug(f"[CAM {cam_id}] {label} cached, skip")
#             results.append((out, c_start, c_end))
#             continue
 
#         async with _semaphore:
#             ok = await playback.async_download_clip(cam_id, c_start, c_end, out)
#         await set_cam_chunk_progress(cam_id, idx + 1, label, total)  # update sau download
#         if ok:
#             results.append((out, c_start, c_end))
#         else:
#             wlogger.error(f"[CAM {cam_id}] {label} FAILED")
 
#     return results


# async def scan_date(
#     cam_ids:      list[int],
#     target_date:  datetime,
#     start_hour:   int,
#     end_hour:     int,
#     start_minute: int = 0,
#     end_minute:   int = 0,
#     label:        str = "",
# ) -> int:
#     from core.db import get_channel_map
#     get_playback().cfg.channel_map = await get_channel_map()
#     wlogger.info(f"Channel map: {get_playback().cfg.channel_map}")
#     date_str = target_date.strftime("%Y-%m-%d")
#     label    = label or f"scan {date_str} {start_hour:02d}:{start_minute:02d}–{end_hour:02d}:{end_minute:02d}"

#     wlogger.info(f"=== [{label}] cams={cam_ids} ===")
#     set_shift_started(label, date_str, cam_ids)

#     dl_tasks = [
#         _download_with_progress(
#             cam_id, target_date,
#             start_hour, end_hour,
#             start_minute, end_minute,
#         )
#         for cam_id in cam_ids
#     ]
#     cam_results = await asyncio.gather(*dl_tasks, return_exceptions=True)

#     async def _detect_cam(cid, chunks):
#         n = 0
#         for (path, c_start, c_end) in chunks:
#             n += await process_video(path, cid, c_start, c_end)
#         await set_cam_done(cid, n)
#         return n

#     detect_tasks = []
#     for cam_id, chunks in zip(cam_ids, cam_results):
#         if isinstance(chunks, Exception):
#             wlogger.error(f"[CAM {cam_id}] Download exception: {chunks}")
#             await set_cam_error(cam_id, str(chunks))
#             continue
#         if not chunks:
#             wlogger.warning(f"[CAM {cam_id}] Không có chunk nào tải được")
#             await set_cam_done(cam_id, 0)
#             continue
#         await set_cam_detecting(cam_id)
#         detect_tasks.append(_detect_cam(cam_id, chunks))

#     results     = await asyncio.gather(*detect_tasks, return_exceptions=True)
#     total_scans = sum(r for r in results if isinstance(r, int))

#     set_shift_finished(total_scans)
#     wlogger.info(f"=== [{label}] done | total_scans={total_scans} ===")
#     return total_scans
async def download_full_clip(
    cam_id: int,
    date: datetime,
    start_hour: int,
    end_hour: int,
):
    from core.nvr import get_playback
    from core.config import SEMAPHORE_COUNT
    footage_start = date.replace(
        hour=start_hour,
        minute=0,
        second=0,
        microsecond=0,
    )

    footage_end = date.replace(
        hour=end_hour,
        minute=0,
        second=0,
        microsecond=0,
    )

    out_dir = TEMP_VIDEO_DIR / str(cam_id) / date.strftime("%Y%m%d")
    out_dir.mkdir(parents=True, exist_ok=True)

    out = out_dir / "full.mp4"

    playback = get_playback()
    _semaphore = asyncio.Semaphore(SEMAPHORE_COUNT)
    async with _semaphore:
        logger.info(
            f"[CAM {cam_id}] FULL DOWNLOAD "
            f"{footage_start:%H:%M} -> {footage_end:%H:%M}"
        )

        ok = await playback.async_download_clip(
            cam_id,
            footage_start,
            footage_end,
            out,
        )

    if not ok:
        logger.error(f"[CAM {cam_id}] FULL DOWNLOAD FAILED")
        return None

    logger.info(
        f"[CAM {cam_id}] FULL DOWNLOAD OK "
        f"({out.stat().st_size // 1024} KB)"
    )

    return out

# async def _download_detect_pipeline(
#     cam_id:       int,
#     date:         datetime,
#     start_hour:   int,
#     end_hour:     int,
#     start_minute: int = 0,
#     end_minute:   int = 0,
#     job_id:       str = "",
# ) -> int:
#     """
#     Pipeline download → detect song song dùng producer/consumer queue.

#     Thay vì: [dl1][det1][dl2][det2]...  (tuần tự, lãng phí NVR slot khi detect)
#     Thành:   [dl1][dl2][dl3]...          (producer liên tục download)
#                   [det1][det2][det3]...  (DETECT_WORKERS consumer detect song song)

#     - Producer: download từng chunk, đẩy vào queue (maxsize=DETECT_WORKERS+2 để buffer nhỏ)
#     - Consumer pool: DETECT_WORKERS coroutine chạy detect song song qua ThreadPoolExecutor
#     - Lock per (cam_id, date): tránh conflict khi cron và manual trigger trùng cam+ngày
#     """
#     from scanner.downloader import _semaphore, chunk_ranges
#     from core.config import DETECT_WORKERS
#     from core.nvr import get_playback
#     from concurrent.futures import ThreadPoolExecutor

#     lock_key = f"{cam_id}_{date.strftime('%Y%m%d')}"
#     if lock_key not in _cam_locks:
#         _cam_locks[lock_key] = asyncio.Lock()

#     async with _cam_locks[lock_key]:
#         wlogger.info(f"[CAM {cam_id}] Lock acquired: {lock_key} (job={job_id})")

#         footage_start = TZ.normalize(
#             date.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
#         )
#         footage_end = TZ.normalize(
#             date.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)
#         )
#         chunks    = chunk_ranges(footage_start, footage_end)
#         total     = len(chunks)
#         out_dir   = TEMP_VIDEO_DIR / str(cam_id) / date.strftime("%Y%m%d")
#         out_dir.mkdir(parents=True, exist_ok=True)

#         task_name = asyncio.current_task().get_name() if asyncio.current_task() else "_scheduler"
#         _active_dirs.setdefault(task_name, set()).add(out_dir)

#         def _is_cancelled() -> bool:
#             ev = _cancel_events.get(task_name)
#             return ev is not None and ev.is_set()

#         set_cam_downloading(cam_id, total, job_id=job_id)
#         playback    = get_playback()
#         total_saved = 0
#         saved_lock  = asyncio.Lock()

#         # Queue buffer nhỏ — tránh download quá nhiều trước khi detect kịp
#         # maxsize = DETECT_WORKERS + 2: luôn có sẵn chunk cho consumer mà không tốn disk
#         dl_queue: asyncio.Queue = asyncio.Queue(maxsize=DETECT_WORKERS + 2)

#         # ── Producer: download tuần tự, đẩy chunk vào queue ──────
#         async def _producer():
#             for idx, (c_start, c_end) in enumerate(chunks):
#                 if _is_cancelled():
#                     wlogger.warning(f"[CAM {cam_id}] Producer cancelled tại chunk {idx+1}/{total}")
#                     break

#                 label = f"{c_start:%H:%M}–{c_end:%H:%M}"
#                 out   = out_dir / f"chunk_{c_start:%H%M}.mp4"

#                 if out.exists() and out.stat().st_size > 0:
#                     wlogger.debug(f"[CAM {cam_id}] {label} cached, skip download")
#                 else:
#                     async with _semaphore:
#                         ok = await playback.async_download_clip(
#                             cam_id, c_start, c_end, out,
#                             cancel_event=_cancel_events.get(task_name),
#                         )
#                     if not ok:
#                         wlogger.error(f"[CAM {cam_id}] {label} download FAILED — bỏ qua")
#                         await set_cam_chunk_progress(cam_id, idx + 1, label, total, job_id=job_id)
#                         continue

#                 await set_cam_chunk_progress(cam_id, idx + 1, label, total, job_id=job_id)

#                 if _is_cancelled():
#                     wlogger.warning(f"[CAM {cam_id}] Producer cancelled sau download {label}")
#                     break

#                 # Đẩy vào queue — sẽ block nếu queue đầy (consumer chưa kịp detect)
#                 await dl_queue.put((out, c_start, c_end, idx))

#             # Gửi sentinel cho từng consumer để báo hết chunk
#             for _ in range(DETECT_WORKERS):
#                 await dl_queue.put(None)

#         # ── Consumer: nhận chunk từ queue, detect song song ───────
#         executor = ThreadPoolExecutor(
#             max_workers=DETECT_WORKERS,
#             thread_name_prefix=f"detect_cam{cam_id}",
#         )

#         async def _consumer(worker_id: int):
#             nonlocal total_saved
#             while True:
#                 item = await dl_queue.get()
#                 if item is None:
#                     dl_queue.task_done()
#                     break

#                 out, c_start, c_end, idx = item
#                 label = f"{c_start:%H:%M}–{c_end:%H:%M}"

#                 if _is_cancelled():
#                     # Xóa file nếu bị cancel
#                     try:
#                         out.unlink(missing_ok=True)
#                     except Exception:
#                         pass
#                     dl_queue.task_done()
#                     wlogger.warning(f"[CAM {cam_id}] Consumer-{worker_id} cancelled tại {label}")
#                     break

#                 await set_cam_detecting(cam_id, job_id=job_id)

#                 loop  = asyncio.get_event_loop()
#                 saved = await loop.run_in_executor(
#                     executor,
#                     lambda o=out, cs=c_start, ce=c_end: __import__(
#                         "worker_scheduled.detector", fromlist=["_detect_video"]
#                     )._detect_video(
#                         o, cam_id, cs, ce,
#                         cancel_event=_cancel_events.get(task_name),
#                         delete_after=True,
#                     ),
#                 )

#                 # POST kết quả
#                 from scanner.detect import _post_scan
#                 n = 0
#                 for scan in saved:
#                     ok = await _post_scan(cam_id, scan)
#                     if ok:
#                         n += 1

#                 async with saved_lock:
#                     total_saved += n

#                 wlogger.info(
#                     f"[CAM {cam_id}] worker-{worker_id} chunk {idx+1}/{total} {label} "
#                     f"→ {n} QR(s) | total={total_saved} (job={job_id})"
#                 )
#                 dl_queue.task_done()

#         # ── Chạy producer + consumers song song ──────────────────
#         await set_cam_detecting(cam_id, job_id=job_id)
#         consumers = [_consumer(i) for i in range(DETECT_WORKERS)]
#         await asyncio.gather(_producer(), *consumers)
#         executor.shutdown(wait=False)

#         wlogger.info(f"[CAM {cam_id}] Lock released: {lock_key} (job={job_id})")
#         return total_saved
async def _download_detect_pipeline(
    cam_id:       int,
    date:         datetime,
    start_hour:   int,
    end_hour:     int,
    start_minute: int = 0,
    end_minute:   int = 0,
    job_id:       str = "",   # ← BƯỚC 3: nhận job_id để ghi state đúng slot
) -> int:
    """
    Pipeline download → detect → delete per chunk.
    Trả về tổng số QR scan đã POST thành công cho cam này.
 
    BƯỚC 2: Lock per (cam_id, date) — nếu cron đang chạy cam này cùng ngày,
    manual trigger sẽ chờ cron xong rồi mới chạy (không conflict file).
    """
    from scanner.downloader import _semaphore, chunk_ranges
    from core.nvr import get_playback
 
    # ── BƯỚC 2: Acquire lock per (cam_id, date) ──────────────────
    lock_key = f"{cam_id}_{date.strftime('%Y%m%d')}"
    if lock_key not in _cam_locks:
        _cam_locks[lock_key] = asyncio.Lock()
 
    async with _cam_locks[lock_key]:
        wlogger.info(f"[CAM {cam_id}] Lock acquired: {lock_key} (job={job_id})")
 
        footage_start = TZ.normalize(
            date.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
        )
        footage_end = TZ.normalize(
            date.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)
        )
        chunks = chunk_ranges(footage_start, footage_end)
        total  = len(chunks)
 
        out_dir = TEMP_VIDEO_DIR / str(cam_id) / date.strftime("%Y%m%d")
        out_dir.mkdir(parents=True, exist_ok=True)
 
        task_name = asyncio.current_task().get_name() if asyncio.current_task() else "_scheduler"
        _active_dirs.setdefault(task_name, set()).add(out_dir)
        # _is_cancelled() check động mỗi lần — tránh lấy None lúc task mới start
        def _is_cancelled() -> bool:
            ev = _cancel_events.get(task_name)
            return ev is not None and ev.is_set()

        set_cam_downloading(cam_id, total, job_id=job_id)   # ← BƯỚC 3: truyền job_id
        playback     = get_playback()
        total_saved  = 0
 
        for idx, (c_start, c_end) in enumerate(chunks):
            # ── Check cancel trước mỗi chunk ─────────────────────
            if _is_cancelled():
                wlogger.warning(f"[CAM {cam_id}] Cancel detected — dừng tại chunk {idx+1}/{total}")
                break

            label = f"{c_start:%H:%M}–{c_end:%H:%M}"
            out   = out_dir / f"chunk_{c_start:%H%M}.mp4"

            # ── Download ─────────────────────────────────────────
            if out.exists() and out.stat().st_size > 0:
                wlogger.debug(f"[CAM {cam_id}] {label} cached, skip download")
            else:
                async with _semaphore:
                    ok = await playback.async_download_clip(cam_id, c_start, c_end, out,
                                                            cancel_event=_cancel_events.get(task_name),)
                if not ok:
                    wlogger.error(f"[CAM {cam_id}] {label} download FAILED — bỏ qua chunk")
                    await set_cam_chunk_progress(cam_id, idx + 1, label, total, job_id=job_id)
                    continue
 
            await set_cam_chunk_progress(cam_id, idx + 1, label, total, job_id=job_id)

            # ── Check cancel sau download, trước detect ───────────
            if _is_cancelled():
                wlogger.warning(f"[CAM {cam_id}] Cancel detected sau download {label} — dừng")
                break

            # ── Detect ngay ──────────────────────────────────────
            await set_cam_detecting(cam_id, job_id=job_id)
            saved = await process_video(
                video_path   = out,
                cam_id       = cam_id,
                chunk_start  = c_start,
                chunk_end    = c_end,
                delete_after = True,
                cancel_event = _cancel_events.get(task_name),
            )
            total_saved += saved
            wlogger.info(
                f"[CAM {cam_id}] chunk {idx+1}/{total} {label} "
                f"→ {saved} QR(s) | total={total_saved} (job={job_id})"
            )
 
        wlogger.info(f"[CAM {cam_id}] Lock released: {lock_key} (job={job_id})")
        return total_saved
import subprocess as _subprocess
import re as _re

def _collect_split_parts(base_path: Path) -> list[Path]:
    """
    HCNetSDK tự động tạo thêm _1, _2, ... khi recording vượt NVR segment
    boundary hoặc giới hạn 1 GB. Hàm này collect tất cả parts theo đúng thứ tự.
    Ví dụ: seg_0800_1200.mp4, seg_0800_1200_1.mp4, seg_0800_1200_2.mp4
    """
    stem   = base_path.stem
    parent = base_path.parent
    pat    = _re.compile(rf"^{_re.escape(stem)}(_\d+)?\.mp4$")
    return sorted(
        p for p in parent.iterdir()
        if p.is_file() and pat.match(p.name)
    )

def _is_valid_mp4(path: Path, expected_minutes: float, tolerance: float = 0.85) -> bool:
    try:
        r = _subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1",
             str(path)],
            capture_output=True, text=True, timeout=15,
        )
        if r.returncode != 0:
            return False
        actual_sec   = float(r.stdout.strip())
        expected_sec = expected_minutes * 60
        ratio        = actual_sec / expected_sec
        if ratio < tolerance:
            wlogger.warning(
                f"[VALIDATE] {path.name} {actual_sec:.0f}s / {expected_sec:.0f}s "
                f"= {ratio:.0%} < {tolerance:.0%} → reject"
            )
            return False
        return True
    except Exception as e:
        wlogger.warning(f"[VALIDATE] ffprobe error {path}: {e}")
        return False


async def scan_date_bulk(
    cam_ids:       list[int],
    target_date:   datetime,
    start_hour:    int,
    end_hour:      int,
    start_minute:  int = 0,
    end_minute:    int = 0,
    job_id:        str = "",
    label:         str = "",
    nvr_channels:  int = NVR_CHANNELS,
    segment_hours: float = SEGMENT_HOURS,
) -> int:
    from core.nvr import get_playback
    from scanner.detect import _post_scan
    from scanner.detect_fast import detect_video_fast as _detect_video
    from core.config import DETECT_WORKERS
    from concurrent.futures import ThreadPoolExecutor

    await refresh_channel_map()
    playback  = get_playback()
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "_scheduler"

    # ── Tạo danh sách segment jobs theo round-robin ─────────────────
    # Round-robin: seg0 của tất cả cam, rồi seg1, rồi seg2, ...
    # Tránh 2 worker cùng tải cùng channel (NVR reject concurrent request
    # cho cùng 1 channel — dẫn đến download fail im lặng).
    #
    # Thứ tự cam-by-cam:    cam2-s0, cam2-s1, cam1-s0, cam1-s1
    # Thứ tự round-robin:   cam2-s0, cam1-s0, cam2-s1, cam1-s1  ← đúng
    from itertools import zip_longest

    seg_start_base = TZ.normalize(
        target_date.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    )
    seg_end_total = TZ.normalize(
        target_date.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)
    )

    per_cam_jobs: list[list] = []
    for cam_id in cam_ids:
        out_dir = TEMP_VIDEO_DIR / str(cam_id) / target_date.strftime("%Y%m%d")
        out_dir.mkdir(parents=True, exist_ok=True)
        _active_dirs.setdefault(task_name, set()).add(out_dir)

        cam_jobs = []
        cursor = seg_start_base
        while cursor < seg_end_total:
            seg_end = min(cursor + timedelta(hours=segment_hours), seg_end_total)
            out = out_dir / f"seg_{cursor:%H%M}_{seg_end:%H%M}.mp4"
            cam_jobs.append((cam_id, cursor, seg_end, out))
            cursor = seg_end
        per_cam_jobs.append(cam_jobs)

    # Interleave: [cam2-s0, cam1-s0, cam2-s1, cam1-s1, ...]
    all_jobs = [
        job
        for slot in zip_longest(*per_cam_jobs)
        for job in slot
        if job is not None
    ]

    total_jobs = len(all_jobs)
    wlogger.info(
        f"[bulk] {len(cam_ids)} cam → {total_jobs} segment jobs | "
        f"nvr_channels={nvr_channels} detect_workers={DETECT_WORKERS} job_id={job_id}"
    )
    _label = label or f"bulk {target_date:%Y-%m-%d}"
    set_shift_started(_label, target_date.strftime("%Y-%m-%d"), cam_ids, job_id=job_id, segments_total=total_jobs)

    # ── Queues ──────────────────────────────────────────────────────
    dl_queue:     asyncio.Queue = asyncio.Queue()
    detect_queue: asyncio.Queue = asyncio.Queue(maxsize=DETECT_WORKERS * 3)  # back-pressure: DL block khi detect chưa kịp

    for job in all_jobs:
        await dl_queue.put(job)

    total_saved   = 0
    total_dl_done = 0
    saved_lock    = asyncio.Lock()
    # Per-channel lock: tránh 2 DL worker tải cùng 1 channel NVR đồng thời
    # khi download speeds khác nhau (cam nhanh xong trước, pick segment tiếp theo
    # của cùng cam_id trước khi worker khác release channel đó)
    _channel_locks: dict[int, asyncio.Lock] = {}

    def _is_cancelled() -> bool:
        ev = _cancel_events.get(task_name)
        return ev is not None and ev.is_set()

    # ── DL worker ──────────────────────────────────────────────────
    async def _dl_worker(worker_id: int):
        nonlocal total_dl_done
        while True:
            try:
                cam_id, seg_start, seg_end, out = dl_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            if _is_cancelled():
                break

            label = f"cam{cam_id} {seg_start:%H:%M}→{seg_end:%H:%M}"

            # ── Cache hit: collect tất cả split parts đã có ────────
            cached = _collect_split_parts(out)
            if cached:
                wlogger.debug(f"[DL-{worker_id}] {label} cached ({len(cached)} part(s))")
                for part in cached:
                    await detect_queue.put((cam_id, seg_start, seg_end, part))
                continue

            # ── Download với per-channel lock ─────────────────────
            # Đảm bảo không có 2 worker tải cùng channel NVR đồng thời
            if cam_id not in _channel_locks:
                _channel_locks[cam_id] = asyncio.Lock()
            await update_job_counters(job_id, dl_delta=+1)
            async with _channel_locks[cam_id]:
                wlogger.info(f"[DL-{worker_id}] Downloading {label}")
                await playback.async_download_clip(
                    cam_id, seg_start, seg_end, out,
                    cancel_event=_cancel_events.get(task_name),
                )

            # ── Collect tất cả parts SDK tạo ra (base + _1, _2, ...) ──
            all_parts   = _collect_split_parts(out)
            valid_parts = [p for p in all_parts if p.stat().st_size > 1024]

            if not valid_parts:
                await update_job_counters(job_id, dl_delta=-1)
                wlogger.error(f"[DL-{worker_id}] FAILED {label} — no valid parts found")
                for p in all_parts:
                    try: p.unlink(missing_ok=True)
                    except: pass
                continue

            await update_job_counters(job_id, dl_delta=-1, dl_done_delta=+1)
            total_mb = sum(p.stat().st_size for p in valid_parts) // 1024 // 1024
            wlogger.info(
                f"[DL-{worker_id}] Done {label} "
                f"({len(valid_parts)} part(s), {total_mb}MB total)"
            )
            async with saved_lock:
                total_dl_done += 1

            for part in valid_parts:
                await detect_queue.put((cam_id, seg_start, seg_end, part))

    # ── Detect workers ───────────────────────────────────────────────
    executor = ThreadPoolExecutor(
        max_workers=DETECT_WORKERS,
        thread_name_prefix="bulk_detect",
    )

    async def _detect_worker(worker_id: int):
        nonlocal total_saved

        while True:
            item = await detect_queue.get()
            if item is None:
                detect_queue.task_done()
                break

            cam_id, seg_start, seg_end, out = item

            if _is_cancelled():
                try: out.unlink(missing_ok=True)
                except: pass
                detect_queue.task_done()
                continue

            label = f"cam{cam_id} {seg_start:%H:%M}→{seg_end:%H:%M} [{out.name}]"
            wlogger.info(f"[DETECT-{worker_id}] {label}")

            await update_job_counters(job_id, detect_delta=+1)
            loop = asyncio.get_event_loop()
            try:
                saved = await loop.run_in_executor(
                    executor,
                    lambda o=out, ci=cam_id, cs=seg_start, ce=seg_end: _detect_video(
                        o, ci, cs, ce,
                        cancel_event=_cancel_events.get(task_name),
                    ),
                )
            except Exception as e:
                await update_job_counters(job_id, detect_delta=-1)
                wlogger.error(f"[DETECT-{worker_id}] {label} exception: {e}")
                try: out.unlink(missing_ok=True)
                except: pass
                detect_queue.task_done()
                continue

            await update_job_counters(job_id, detect_delta=-1, detect_done_delta=+1)
            n = 0
            for scan in (saved or []):
                ok = await _post_scan(cam_id, scan)
                if ok:
                    n += 1

            # Xóa file sau detect + post xong
            try: out.unlink(missing_ok=True)
            except: pass

            async with saved_lock:
                total_saved += n

            wlogger.info(
                f"[DETECT-{worker_id}] {label} → {n} QR(s) | total={total_saved}"
            )
            detect_queue.task_done()

    # ── Chạy ────────────────────────────────────────────────────────
    # [FIX #3] Chạy nvr_channels DL workers song song thay vì 1 worker
    async def _dl_pool():
        dl_workers = [_dl_worker(i) for i in range(nvr_channels)]
        await asyncio.gather(*dl_workers)
        # Gửi sentinel cho từng detect worker sau khi TẤT CẢ DL xong
        for _ in range(DETECT_WORKERS):
            await detect_queue.put(None)

    detect_workers_tasks = [
        asyncio.create_task(_detect_worker(i)) for i in range(DETECT_WORKERS)
    ]
    await asyncio.gather(
        _dl_pool(),
        *detect_workers_tasks,
    )
    executor.shutdown(wait=False)

    _active_dirs.pop(task_name, None)

    set_shift_finished(total_saved, job_id=job_id)
    wlogger.info(f"[bulk] Hoàn tất | total_scans={total_saved} job_id={job_id}")
    return total_saved


async def scan_date(
    cam_ids:      list[int],
    target_date:  datetime,
    start_hour:   int,
    end_hour:     int,
    start_minute: int = 0,
    end_minute:   int = 0,
    label:        str = "",
    job_id:       str = "",   # ← BƯỚC 3: nhận job_id từ run_shift / _run_scan_job
) -> int:
    from core.db import get_channel_map
    get_playback().cfg.channel_map = await get_channel_map()
    wlogger.info(f"Channel map: {get_playback().cfg.channel_map}")
 
    date_str = target_date.strftime("%Y-%m-%d")
    label    = label or f"scan {date_str} {start_hour:02d}:{start_minute:02d}–{end_hour:02d}:{end_minute:02d}"
 
    wlogger.info(f"=== [{label}] cams={cam_ids} job_id={job_id} ===")
    set_shift_started(label, date_str, cam_ids, job_id=job_id)   # ← BƯỚC 3
 
    pipeline_tasks = [
        _download_detect_pipeline(
            cam_id, target_date,
            start_hour, end_hour,
            start_minute, end_minute,
            job_id=job_id,   # ← BƯỚC 3
        )
        for cam_id in cam_ids
    ]
    results = await asyncio.gather(*pipeline_tasks, return_exceptions=True)
 
    total_scans = 0
    for cam_id, result in zip(cam_ids, results):
        if isinstance(result, Exception):
            wlogger.error(f"[CAM {cam_id}] Pipeline exception: {result}")
            await set_cam_error(cam_id, str(result), job_id=job_id)
        else:
            total_scans += result
            await set_cam_done(cam_id, result, job_id=job_id)
 
    set_shift_finished(total_scans, job_id=job_id)   # ← BƯỚC 3
    wlogger.info(f"=== [{label}] done | total_scans={total_scans} job_id={job_id} ===")
    return total_scans

async def run_shift(shift_name: str):
    shift = next((s for s in SHIFTS if s.name == shift_name), None)
    if not shift:
        wlogger.error(f"Unknown shift: {shift_name}")
        return
 
    pool = db.get_pool()
    rows = await pool.fetch(
        "SELECT id FROM cameras WHERE shift = $1 AND status = 'active'",
        shift_name,
    )
    cam_ids = [r["id"] for r in rows]
 
    if not cam_ids:
        wlogger.warning(f"[{shift_name}] Không có camera active nào trong DB — bỏ qua")
        return
 
    wlogger.info(f"[{shift_name}] Camera active từ DB: {cam_ids}")
 
    # ── BƯỚC 3: Tạo job_id cho cron ──────────────────────────────
    job_id = f"cron_{shift_name}_{datetime.now(TZ):%Y%m%d_%H%M}"

    yesterday = (datetime.now(TZ) - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    await scan_date(
        cam_ids     = cam_ids,
        target_date = yesterday,
        start_hour  = shift.footage_start_hour,
        end_hour    = shift.footage_end_hour,
        label       = shift.name,
        job_id      = job_id,   # ← BƯỚC 3
    )

async def _daily_scan_worker(first_date: datetime, first_cam_ids: list[int]):
    """Task chạy nền: xử lý first_date, rồi drain hết _pending_scan_dates."""
    global _daily_scan_running

    async def _scan_one(target_date: datetime, cam_ids: list[int]):
        job_id = f"cron_daily_{target_date:%Y%m%d}"
        wlogger.info(f"[daily] {len(cam_ids)} cam | date={target_date:%Y-%m-%d} | job_id={job_id}")
        await scan_date_bulk(
            cam_ids       = cam_ids,
            target_date   = target_date,
            start_hour    = 8,
            end_hour      = 19,
            job_id        = job_id,
            label         = f"[Cron] {len(cam_ids)} cams · {target_date:%Y-%m-%d} 08:00–19:00",
            nvr_channels  = NVR_CHANNELS,
            segment_hours = SEGMENT_HOURS,
        )
        wlogger.info(f"[daily] job xong: {target_date:%Y-%m-%d}")

    try:
        await _scan_one(first_date, first_cam_ids)
    except Exception as e:
        wlogger.error(f"[daily] {first_date:%Y-%m-%d} lỗi: {e}")

    # Backfill: drain queue các ngày bị miss khi job này đang chạy
    while _pending_scan_dates:
        pending_date_str = _pending_scan_dates.pop(0)
        wlogger.info(f"[daily] backfill ngày bị miss: {pending_date_str}")
        try:
            pending_date = TZ.localize(datetime.strptime(pending_date_str, "%Y-%m-%d"))
            pool = db.get_pool()
            rows = await pool.fetch("SELECT id FROM cameras WHERE status = 'active'")
            backfill_cam_ids = [r["id"] for r in rows]
            if backfill_cam_ids:
                await _scan_one(pending_date, backfill_cam_ids)
        except Exception as e:
            wlogger.error(f"[daily] backfill {pending_date_str} lỗi: {e}")

    _daily_scan_running = False


async def run_daily_scan():
    """Cron job chạy 1 lần/ngày lúc 20:00 — xử lý toàn bộ cam active.

    Nếu job trước chưa xong, ngày hôm nay được enqueue vào _pending_scan_dates
    để chạy backfill ngay sau khi job hiện tại hoàn thành (không chạy song song).
    """
    global _daily_scan_running

    pool = db.get_pool()
    rows = await pool.fetch("SELECT id FROM cameras WHERE status = 'active'")
    cam_ids = [r["id"] for r in rows]

    if not cam_ids:
        wlogger.warning("[daily] Không có camera active — bỏ qua")
        return

    today = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    today_str = today.strftime("%Y-%m-%d")

    if _daily_scan_running:
        if today_str not in _pending_scan_dates:
            _pending_scan_dates.append(today_str)
            wlogger.warning(
                f"[daily] Job trước chưa xong — enqueue backfill cho {today_str} "
                f"(pending queue: {_pending_scan_dates})"
            )
        else:
            wlogger.warning(f"[daily] {today_str} đã có trong pending queue, bỏ qua")
        return

    _daily_scan_running = True
    asyncio.create_task(
        _daily_scan_worker(today, cam_ids),
        name=f"cron_daily_{today:%Y%m%d}",
    )


def _get_all_active_dirs() -> set:
    """Trả về tập hợp tất cả folder đang được task active sử dụng."""
    result = set()
    for dirs in _active_dirs.values():
        result.update(dirs)
    return result

async def cleanup_old_scans():
    cutoff = datetime.now(TZ) - timedelta(days=30)
    pool = db.get_pool()
    result = await pool.execute(
        "DELETE FROM qr_scans WHERE detected_at < $1",
        cutoff,
    )
    count = int(result.split()[-1]) if result else 0
    wlogger.info(f"[cleanup] Đã xóa {count} QR scan cũ hơn 30 ngày (trước {cutoff.date()})")

def _cleanup_empty_date_dirs(skip_dirs: set | None = None) -> int:
    """
    Xóa các folder ngày (YYYYMMDD) rỗng trong TEMP_VIDEO_DIR.
    Bỏ qua các folder trong skip_dirs (đang được task active dùng).
    Trả về số folder đã xóa.
    """
    skip_dirs = skip_dirs or set()
    removed   = 0
    if not TEMP_VIDEO_DIR.exists():
        return 0
    for cam_dir in TEMP_VIDEO_DIR.iterdir():
        if not cam_dir.is_dir():
            continue
        for date_dir in cam_dir.iterdir():
            if not date_dir.is_dir():
                continue
            if date_dir in skip_dirs:
                wlogger.debug(f"[Cleanup] Bỏ qua folder đang dùng: {date_dir}")
                continue
            try:
                # Chỉ xóa nếu thực sự rỗng
                if not any(date_dir.iterdir()):
                    date_dir.rmdir()
                    wlogger.info(f"[Cleanup] Xóa folder rỗng: {date_dir}")
                    removed += 1
            except Exception as e:
                wlogger.warning(f"[Cleanup] Không thể xóa {date_dir}: {e}")
    return removed
async def cleanup_empty_dirs():
    """Job APScheduler — gọi _cleanup_empty_date_dirs không skip folder nào."""
    active_dirs = {info["out_dir"] for info in _active_tasks.values() if "out_dir" in info}
    removed = _cleanup_empty_date_dirs(skip_dirs=active_dirs)
    wlogger.info(f"[cleanup] Scheduled cleanup: {removed} folder rỗng đã xóa")

CLIP_MAX_AGE_HOURS = 24     # xóa footage on-demand cũ hơn 24h
CLIP_MAX_SIZE_GB   = 10     # nếu thư mục vượt 10GB → xóa file cũ nhất trước

async def _cleanup_stale_clips(max_age_hours: int = 2, skip_dirs: set | None = None) -> int:
    skip_dirs = skip_dirs or set()
    removed   = 0
    cutoff    = datetime.now().timestamp() - max_age_hours * 3600
    if not TEMP_CLIP_DIR.exists():
        return 0
    pool = db.get_pool()
    for mp4 in TEMP_CLIP_DIR.rglob("*.mp4"):
        if not mp4.is_file():
            continue
        if mp4.parent in skip_dirs:
            continue
        try:
            if mp4.stat().st_mtime < cutoff:
                mp4.unlink(missing_ok=True)
                wlogger.info(f"[Cleanup] Xóa clip cũ ({max_age_hours}h): {mp4}")
                removed += 1
                if pool:
                    await pool.execute(
                        "UPDATE qr_scans SET clip_file = NULL WHERE clip_file = $1",
                        str(mp4),
                    )
        except Exception as e:
            wlogger.warning(f"[Cleanup] Không thể xóa {mp4}: {e}")
    return removed


async def run_periodic_cleanup():
    active = _get_all_active_dirs()
    wlogger.info(f"[Cleanup] Bắt đầu | active_dirs={len(active)}")
    n_dirs  = _cleanup_empty_date_dirs(skip_dirs=active)
    n_clips = await _cleanup_stale_clips(max_age_hours=24, skip_dirs=active)
    n_size  = await asyncio.to_thread(_cleanup_clips_by_size)
    wlogger.info(f"[Cleanup] Xong | dirs_removed={n_dirs} clips_removed={n_clips} size_removed={n_size}")

def _cleanup_clips_by_size() -> int:
    """
    Lớp 2 — Nếu TEMP_CLIP_DIR vượt CLIP_MAX_SIZE_GB (10GB),
    xóa file cũ nhất trước cho đến khi về dưới ngưỡng.
    Chạy sau lớp 1 để tránh tình huống disk đầy đột ngột.
    """
    if not TEMP_CLIP_DIR.exists():
        return 0
 
    # Tính tổng dung lượng hiện tại
    def _dir_size_gb() -> float:
        return sum(
            f.stat().st_size for f in TEMP_CLIP_DIR.rglob("*.mp4") if f.is_file()
        ) / (1024 ** 3)
 
    current_gb = _dir_size_gb()
    if current_gb <= CLIP_MAX_SIZE_GB:
        wlogger.debug(f"[ClipCleanup] Size OK: {current_gb:.2f}GB / {CLIP_MAX_SIZE_GB}GB")
        return 0
 
    wlogger.warning(
        f"[ClipCleanup] Vượt ngưỡng: {current_gb:.2f}GB > {CLIP_MAX_SIZE_GB}GB "
        f"— bắt đầu xóa file cũ nhất"
    )
 
    # Sắp xếp theo mtime tăng dần (cũ nhất trước)
    files = sorted(
        (f for f in TEMP_CLIP_DIR.rglob("*.mp4") if f.is_file()),
        key=lambda f: f.stat().st_mtime,
    )
 
    removed = 0
    for mp4 in files:
        if _dir_size_gb() <= CLIP_MAX_SIZE_GB:
            break
        try:
            size_mb = mp4.stat().st_size / (1024 ** 2)
            mp4.unlink(missing_ok=True)
            wlogger.info(f"[ClipCleanup] Xóa do vượt size: {mp4.name} ({size_mb:.1f}MB)")
            removed += 1
        except Exception as e:
            wlogger.warning(f"[ClipCleanup] Không thể xóa {mp4}: {e}")
 
    wlogger.info(
        f"[ClipCleanup] Sau xóa: {_dir_size_gb():.2f}GB | removed={removed}"
    )
    return removed

async def _run_scan_job(job: dict):
    # ── BƯỚC 3: Tạo job_id cho manual trigger ────────────────────
    job_id = f"manual_{job['date']}_{job['start_time'].replace(':', '')}_{datetime.now(TZ):%H%M%S}"
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else job_id
 
    cam_ids = job["cam_ids"]
    cam_str = f"CAM {cam_ids[0]}" if len(cam_ids) == 1 else f"{len(cam_ids)} cams"
    label   = f"[Manual] {cam_str} · {job['date']} {job['start_time']}–{job['end_time']}"
    try:
        sh, sm = map(int, job["start_time"].split(":"))
        eh, em = map(int, job["end_time"].split(":"))
        target = TZ.localize(datetime.strptime(job["date"], "%Y-%m-%d"))
        await scan_date_bulk(
            cam_ids      = cam_ids,
            target_date  = target,
            start_hour   = sh,
            end_hour     = eh,
            start_minute = sm,
            end_minute   = em,
            job_id       = job_id,
            label        = label,
        )
    except asyncio.CancelledError:
        wlogger.warning(f"[{label}] bị cancel — đang cleanup folder...")
        my_dirs      = _active_dirs.pop(task_name, set())
        other_active = _get_all_active_dirs()
        for d in my_dirs:
            if d in other_active:
                wlogger.debug(f"[{label}] Bỏ qua {d} — task khác đang dùng")
                continue
            try:
                if d.exists():
                    shutil.rmtree(d, ignore_errors=True)
                    wlogger.info(f"[{label}] Đã xóa folder sau cancel: {d}")
            except Exception as e:
                wlogger.warning(f"[{label}] Không thể xóa {d}: {e}")
        raise
    except Exception as e:
        wlogger.exception(f"[{label}] lỗi: {e}")
        my_dirs      = _active_dirs.pop(task_name, set())
        other_active = _get_all_active_dirs()
        for d in my_dirs:
            if d in other_active:
                continue
            try:
                if d.exists():
                    shutil.rmtree(d, ignore_errors=True)
                    wlogger.info(f"[{label}] Đã xóa folder sau lỗi: {d}")
            except Exception:
                pass
    finally:
        current = asyncio.current_task()
        _active_tasks.discard(current)
        _active_dirs.pop(task_name, None)
        _cancel_events.pop(task_name, None)

# Helper: kiểm tra cam đang được lock (đang xử lý cùng ngày)
def _get_running_cams(cam_ids: list[int], date_str: str) -> list[int]:
    """Trả về list cam_id đang bị lock cho ngày date_str."""
    running = []
    for cam_id in cam_ids:
        key  = f"{cam_id}_{date_str.replace('-', '')}"
        lock = _cam_locks.get(key)
        if lock and lock.locked():
            running.append(cam_id)
    return running

async def listen_scan_triggers(queue: asyncio.Queue):
    wlogger.info("Listening for manual scan triggers...")
    while True:
        try:
            job = await queue.get()

            if _trigger_paused:
                wlogger.warning(
                    f"Trigger PAUSED — bỏ qua job: {job['date']} "
                    f"{job['start_time']}–{job['end_time']} cams={job['cam_ids']}"
                )
                queue.task_done()
                continue

            # Tạo job_id 1 lần duy nhất ở đây, truyền vào job dict
            # để _run_scan_job đọc lại — task_name = job_id = đúng 1 giá trị
            job["job_id"] = (
                f"manual_{job['date']}_{job['start_time'].replace(':', '')}"
                f"_{datetime.now(TZ):%H%M%S}"
            )
            task = asyncio.create_task(_run_scan_job(job), name=job["job_id"])
            _active_tasks.add(task)
            wlogger.info(
                f"Trigger spawned task [{job['job_id']}] | "
                f"active_tasks={len(_active_tasks)} | queue_pending={queue.qsize()}"
            )
            queue.task_done()

        except asyncio.CancelledError:
            wlogger.info("Trigger listener cancelled — shutting down")
            raise
        except Exception as e:
            wlogger.exception(f"Trigger listener error: {e}")

async def _run_ondemand_worker(redis: aioredis.Redis):
    """Background loop: lắng nghe Redis queue, xử lý on-demand clip jobs."""
    from ondemand.clipper import fetch_clip
    import httpx

    async def _set_status(job_id: str, payload: dict):
        await redis.setex(f"job:status:{job_id}", STATUS_TTL, json.dumps(payload))

    while True:
        try:
            result = await redis.brpop(QUEUE_KEY, timeout=5)
            if result is None:
                continue
            _, raw = result
            try:
                job = json.loads(raw)
            except json.JSONDecodeError as e:
                logger.error(f"[ondemand] Invalid job JSON: {e}")
                continue

            job_id   = job.get("job_id", "unknown")
            qr_value = job.get("qr_value", "")
            records  = job.get("records", [])

            logger.info(f"[ondemand] JOB {job_id} | qr={qr_value} | {len(records)} scan(s)")
            await _set_status(job_id, {
                "job_id": job_id, "status": "running",
                "qr_value": qr_value, "total": len(records), "done": 0,
            })

            clip_files = []
            job_error  = None
            for idx, rec in enumerate(records):
                from datetime import timezone, timedelta as _td
                ICT = timezone(_td(hours=7))
                detected_at = datetime.fromisoformat(rec["detected_at"])
                if detected_at.tzinfo is not None:
                    detected_at = detected_at.astimezone(ICT).replace(tzinfo=None)

                try:
                    clip_path = await fetch_clip(job_id, rec["scan_id"], rec["cam_id"], detected_at, qr_value)
                except Exception as e:
                    job_error = str(e)
                    logger.error(f"[ondemand] JOB {job_id} fetch_clip failed: {e}")
                    await _set_status(job_id, {
                        "job_id": job_id, "status": "error",
                        "qr_value": qr_value, "total": len(records),
                        "done": idx, "error_msg": job_error,
                    })
                    break

                if clip_path:
                    clip_files.append(str(clip_path))
                    async with httpx.AsyncClient(base_url=f"http://localhost:{API_PORT}", timeout=10) as client:
                        try:
                            await client.patch(
                                f"/scans/{rec['scan_id']}/clip",
                                json={"clip_file": str(clip_path)},
                                headers={"X-Secret": API_SECRET},
                            )
                        except Exception as e:
                            logger.warning(f"[ondemand] PATCH clip failed: {e}")

                await _set_status(job_id, {
                    "job_id": job_id, "status": "running",
                    "qr_value": qr_value, "total": len(records), "done": idx + 1,
                })

            if not job_error:
                await _set_status(job_id, {
                    "job_id": job_id, "status": "done",
                    "qr_value": qr_value, "total": len(records),
                    "done": len(records), "clip_files": clip_files,
                })
                logger.info(f"[ondemand] JOB {job_id} done | {len(clip_files)}/{len(records)} clips")

        except aioredis.RedisError as e:
            logger.error(f"[ondemand] Redis error: {e}, retrying in 5s")
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"[ondemand] Unexpected error: {e}")
            await asyncio.sleep(1)


# ── Lifespan ─────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _redis

    await db.init_pool()
    logger.info("DB pool ready")

    clear_stale_jobs()
    logger.info("Stale jobs cleared")

    try:
        r = aioredis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2, socket_timeout=None)
        await r.ping()
        _redis = r
        logger.info("Redis ready")
    except Exception as e:
        _redis = None
        logger.warning(
            f"⚠️  Redis không khả dụng: {e}\n"
            "    → /cameras /scans /stats vẫn hoạt động bình thường.\n"
            "    → /jobs sẽ trả lỗi 503 cho đến khi có Redis."
        )

    try:
        await init_playback()
        logger.info("HikvisionPlayback ready")
    except Exception as e:
        logger.error(f"HikvisionPlayback init failed: {e} — worker sẽ không hoạt động")

    scheduler = AsyncIOScheduler(timezone=TZ)
    scheduler.add_job(
        run_daily_scan,
        CronTrigger(hour=19, minute=0, timezone=TZ),
        id="daily_scan",
        misfire_grace_time=3600,
        replace_existing=True,
    )
    logger.info("Scheduled daily scan at 19:00 ICT")

    scheduler.add_job(
        run_periodic_cleanup,
        trigger=CronTrigger(minute=30, timezone=TZ),  # mỗi giờ lúc :30
        id="clip_cleanup",
        misfire_grace_time=300,
        replace_existing=True,
    )
    logger.info("Scheduled clip cleanup every hour at :30")

    # Cleanup định kỳ: mỗi 30 phút — dọn folder rỗng + clip on-demand cũ > 2h
    # Chạy lúc :00 và :30 của mỗi giờ; bỏ qua folder đang được task active dùng
    scheduler.add_job(
        # run_periodic_cleanup,
        cleanup_old_scans,
        trigger=CronTrigger(minute="0,30", timezone=TZ),
        id="periodic_cleanup",
        misfire_grace_time=120,
        replace_existing=True,
    )
    
    scheduler.add_job(
        cleanup_empty_dirs,
        trigger=CronTrigger(hour=3, minute=5, timezone=TZ),
        id="cleanup_empty_dirs",
        misfire_grace_time=3600,
        replace_existing=True,
    )
    
    logger.info("Scheduled periodic cleanup every 30 minutes")

    scheduler.start()
    logger.info("APScheduler running")

    trigger_task = asyncio.create_task(
        listen_scan_triggers(_scan_queue),
        name="scan_trigger_listener",
    )
    def _on_trigger_task_done(t: asyncio.Task):
        if t.cancelled():
            return
        exc = t.exception()
        if exc:
            wlogger.error(f"Trigger listener crashed unexpectedly: {exc}", exc_info=exc)
    trigger_task.add_done_callback(_on_trigger_task_done)
    logger.info("Scan trigger listener started")

    ondemand_task: asyncio.Task | None = None
    if _redis:
        ondemand_task = asyncio.create_task(
            _run_ondemand_worker(_redis),
            name="ondemand_worker",
        )
        logger.info("On-demand clip worker started")

    logger.info("API + Worker started")
    yield

    trigger_task.cancel()
    try:
        await trigger_task
    except asyncio.CancelledError:
        pass
    logger.info("Scan trigger listener stopped")

    if ondemand_task:
        ondemand_task.cancel()
        try:
            await ondemand_task
        except asyncio.CancelledError:
            pass
        logger.info("On-demand worker stopped")

    if _active_tasks:
        logger.info(f"Cancelling {len(_active_tasks)} active scan task(s)...")
        for t in list(_active_tasks):
            t.cancel()
        await asyncio.gather(*_active_tasks, return_exceptions=True)
        logger.info("All scan tasks cancelled")

    scheduler.shutdown(wait=False)
    logger.info("APScheduler stopped")

    await db.close_pool()
    if _redis:
        await _redis.aclose()
    logger.info("API shutdown complete")


app = FastAPI(title="Hikvision QR API", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Static files ─────────────────────────────────────────────────
STATIC_DIR = FilePath(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/ui", include_in_schema=False)
async def serve_ui():
    html = STATIC_DIR / "dashboard.html"
    if not html.exists():
        raise HTTPException(404, "dashboard.html not found in static/")
    return FileResponse(html, media_type="text/html")

# ── SSE: /worker/stream ──────────────────────────────────────────
@app.get("/worker/stream")
async def worker_stream(request: Request, secret: str = Query("")):
    if secret not in (API_SECRET, VIEWER_SECRET):
        raise HTTPException(status_code=401, detail="Invalid secret")

    async def event_gen():
        from scanner.state import read_state
        while True:
            if await request.is_disconnected():
                break
            try:
                data    = read_state() or {"shift": None, "cameras": {}}
                payload = json.dumps(data, ensure_ascii=False, default=str)
                yield f"data: {payload}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Access-Control-Allow-Origin": "*",
        },
    )

# ── Schemas ──────────────────────────────────────────────────────
class CameraCreate(BaseModel):
    name:        str
    shift:       str
    nvr_channel: int

class CameraUpdate(BaseModel):
    name:        str
    shift:       str
    nvr_channel: int

class ScanCreate(BaseModel):
    cam_id:      int
    qr_value:    str
    detected_at: str
    chunk_start: str
    chunk_end:   str
    shift:       str

class ClipUpdate(BaseModel):
    clip_file: str

class JobCreate(BaseModel):
    qr_value: str
    records:  list[dict]

class ScanTrigger(BaseModel):
    cam_ids:    list[int]
    date:       str
    start_time: str = "08:00"
    end_time:   str = "19:00"

# ── Health ───────────────────────────────────────────────────────
@app.get("/auth/role")
async def get_role(key: str = Security(_api_key_header)):
    """Trả về role của secret hiện tại để frontend phân quyền UI."""
    return {"role": _resolve_role(key)}

@app.get("/health")
async def health(_=Depends(verify_secret)):
    redis_ok = False
    if _redis:
        try:
            await _redis.ping()
            redis_ok = True
        except Exception:
            pass
    return {
        "status":       "ok",
        "redis":        redis_ok,
        "jobs_enabled": redis_ok,
        "time":         datetime.now().isoformat(),
    }

from core.nvr import refresh_channel_map
# ── Cameras ──────────────────────────────────────────────────────
@app.get("/cameras")
async def get_cameras(_=Depends(verify_secret)):
    cams = await db.list_cameras(status=None)
    return {"data": [c.__dict__ for c in cams]}

@app.post("/cameras", status_code=201)
async def create_camera(body: CameraCreate, _=Depends(verify_admin)):
    if body.shift not in ("morning", "night"):
        raise HTTPException(400, "shift phải là 'morning' hoặc 'night'")
    cam_id = await db.insert_camera(body.name, body.shift, body.nvr_channel)
    await refresh_channel_map()   # ← sync channel map sau khi thêm
    return {"id": cam_id, "msg": "Camera created"}

@app.delete("/cameras/{cam_id}")
async def remove_camera(cam_id: int, _=Depends(verify_admin)):
    ok = await db.delete_camera(cam_id)
    if not ok:
        raise HTTPException(404, f"Camera {cam_id} not found")
    await refresh_channel_map()   # ← xóa camera khỏi map
    return {"msg": f"Camera {cam_id} deactivated"}

@app.post("/cameras/{cam_id}/restore")
async def restore_camera(cam_id: int, _=Depends(verify_admin)):
    ok = await db.restore_camera(cam_id)
    if not ok:
        raise HTTPException(404, f"Camera {cam_id} không tìm thấy hoặc chưa bị xóa")
    await refresh_channel_map()   # ← thêm lại vào map khi restore
    return {"msg": f"Camera {cam_id} đã được khôi phục"}

@app.put("/cameras/{cam_id}")
async def update_camera(cam_id: int, body: CameraUpdate, _=Depends(verify_admin)):
    if body.shift not in ("morning", "night"):
        raise HTTPException(400, "shift phải là morning hoặc night")
    pool = db.get_pool()
    res  = await pool.execute(
        """
        UPDATE cameras
        SET name = $1, shift = $2, nvr_channel = $3
        WHERE id = $4 AND status = 'active'
        """,
        body.name, body.shift, body.nvr_channel, cam_id,
    )
    if res == "UPDATE 0":
        raise HTTPException(404, f"Camera {cam_id} not found")
    await refresh_channel_map()   # ← cập nhật channel nếu đổi nvr_channel
    return {"msg": "updated"}

# ── Scans ────────────────────────────────────────────────────────
@app.get("/scans")
async def get_scans_api(
    cam_id:   Optional[int] = Query(None),
    qr_value: Optional[str] = Query(None),
    date:     Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit:    int           = Query(50, ge=1, le=500),
    offset:   int           = Query(0, ge=0),
    _=Depends(verify_secret),
):
    total, rows = await db.get_scans(cam_id, qr_value, date, limit, offset)
    return {"total": total, "limit": limit, "offset": offset, "data": rows}

@app.post("/scans", status_code=201)
async def create_scan(body: ScanCreate, _=Depends(verify_secret)):
    def _parse_dt(s: str) -> datetime:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:       # naive → gán Asia/Ho_Chi_Minh
            dt = TZ.localize(dt)
        return dt

    scan = QrScan(
        cam_id      = body.cam_id,
        qr_value    = body.qr_value,
        detected_at = _parse_dt(body.detected_at),
        chunk_start = _parse_dt(body.chunk_start),
        chunk_end   = _parse_dt(body.chunk_end),
        shift       = body.shift,
    )
    scan_id = await db.insert_qr(scan)
    return {"msg": "created" if scan_id else "duplicate", "id": scan_id}

@app.patch("/scans/{scan_id}/clip")
async def patch_clip(scan_id: int, body: ClipUpdate, _=Depends(verify_secret)):
    await db.update_clip_path(scan_id, body.clip_file)
    return {"msg": "updated"}

# ── Stats ────────────────────────────────────────────────────────
@app.get("/stats")
async def stats(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    _=Depends(verify_secret),
):
    return {
        "today":  await db.get_stats_today(),
        "by_cam": await db.get_stats_by_cam(date),
    }

# ── Jobs (yêu cầu Redis) ─────────────────────────────────────────
@app.post("/jobs", status_code=202)
async def create_job(body: JobCreate, r=Depends(require_redis), _=Depends(verify_secret)):
    if not body.records:
        raise HTTPException(400, "records không được rỗng")
    job_id  = str(uuid.uuid4())
    payload = {"job_id": job_id, "qr_value": body.qr_value, "records": body.records}
    await r.setex(
        f"job:status:{job_id}", STATUS_TTL,
        json.dumps({"job_id": job_id, "status": "pending", "qr_value": body.qr_value}),
    )
    await r.lpush(QUEUE_KEY, json.dumps(payload))
    logger.info(f"Job {job_id} queued | {len(body.records)} record(s)")
    return {"job_id": job_id, "status": "pending"}

@app.get("/jobs/{job_id}")
async def get_job(job_id: str, r=Depends(require_redis), _=Depends(verify_secret)):
    raw = await r.get(f"job:status:{job_id}")
    if not raw:
        raise HTTPException(404, f"Job {job_id} not found")
    return json.loads(raw)

# ── Worker status ────────────────────────────────────────────────
@app.get("/worker/status")
async def worker_status(_=Depends(verify_secret)):
    try:
        from scanner.state import read_state
        return read_state() or {"shift": None, "cameras": {}}
    except Exception as e:
        return {"shift": None, "cameras": {}, "error": str(e)}

@app.delete("/worker/jobs/done")
async def clear_done_jobs(_=Depends(verify_admin)):
    """Xóa tất cả job done/error/cancelled khỏi worker_state.json."""
    from scanner.state import clear_done_jobs as _clear
    removed = _clear()
    return {"msg": f"Đã xóa {removed} job", "removed": removed}

# ── Scan trigger ─────────────────────────────────────────────────
@app.post("/scan/trigger", status_code=202)
async def trigger_scan(body: ScanTrigger, _=Depends(verify_admin)):
    if _trigger_paused:
        raise HTTPException(503, "Trigger đang bị tạm dừng. Gọi POST /scan/resume trước.")
 
    try:
        datetime.strptime(body.date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "date phải là định dạng YYYY-MM-DD")
 
    try:
        sh, sm = body.start_time.split(":")
        eh, em = body.end_time.split(":")
        if int(sh) * 60 + int(sm) >= int(eh) * 60 + int(em):
            raise HTTPException(400, "start_time phải trước end_time")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(400, "start_time/end_time phải là định dạng HH:MM")
 
    if not body.cam_ids:
        raise HTTPException(400, "cam_ids không được rỗng")

    MAX_CAMS_PER_TRIGGER  = 10
    MAX_MANUAL_JOBS_TOTAL = 3

    if len(body.cam_ids) > MAX_CAMS_PER_TRIGGER:
        raise HTTPException(400, f"Tối đa {MAX_CAMS_PER_TRIGGER} cam mỗi lần trigger (nhận {len(body.cam_ids)}).")

    manual_running = sum(
        1 for t in _active_tasks if t.get_name().startswith("manual_")
    )
    if manual_running >= MAX_MANUAL_JOBS_TOTAL:
        raise HTTPException(429, f"Đang có {manual_running} manual job chạy. Tối đa {MAX_MANUAL_JOBS_TOTAL}, chờ job xong hoặc cancel bớt.")

    # ── BƯỚC 4: Phát hiện conflict ────────────────────────────────
    date_key  = body.date.replace("-", "")
    conflicts = [
        cam_id for cam_id in body.cam_ids
        if _cam_locks.get(f"{cam_id}_{date_key}", asyncio.Lock()).locked()
    ]
 
    job = {
        "cam_ids":    body.cam_ids,
        "date":       body.date,
        "start_time": body.start_time,
        "end_time":   body.end_time,
        "queued_at":  datetime.now(TZ).isoformat(),
    }
    await _scan_queue.put(job)
    logger.info(f"Scan trigger queued: {job} | conflicts={conflicts}")
 
    return {
        "msg":           (
            "Scan job queued — một số cam đang được xử lý, sẽ tự động chờ"
            if conflicts else
            "Scan job queued — worker sẽ xử lý ngay"
        ),
        "cam_ids":       body.cam_ids,
        "date":          body.date,
        "start_time":    body.start_time,
        "end_time":      body.end_time,
        "active_tasks":  len(_active_tasks),
        "queue_pending": _scan_queue.qsize(),
        # ── BƯỚC 4: Trả về danh sách cam đang conflict ───────────
        "conflicts":     conflicts,   # [] nếu không có conflict
    }

@app.get("/clips/{scan_id}")
async def download_clip(scan_id: int, _=Depends(verify_secret)):
    pool = db.get_pool()
    row = await pool.fetchrow(
        "SELECT clip_file FROM qr_scans WHERE id = $1", scan_id
    )

    # 1. Có record không?
    if not row:
        raise HTTPException(404, f"Không tìm thấy scan id={scan_id}")

    # 2. Có clip_file không?
    if not row["clip_file"]:
        raise HTTPException(404, "Scan chưa có clip")

    path = FilePath(row["clip_file"])

    # 3. File có tồn tại không? Nếu không → tự null DB để UI không hiện stale
    if not path.exists():
        await pool.execute(
            "UPDATE qr_scans SET clip_file = NULL WHERE id = $1", scan_id
        )
        raise HTTPException(404, "Clip đã bị xóa khỏi server (đã cập nhật trạng thái).")

    # 4. Serve
    return FileResponse(
        path,
        media_type="video/mp4",
        filename=path.name,
        headers={"Content-Disposition": f'attachment; filename="{path.name}"'}
    )

@app.post("/scan/pause")
async def pause_trigger(_=Depends(verify_admin)):
    global _trigger_paused
    _trigger_paused = True
    logger.warning(f"Trigger PAUSED | active_tasks={len(_active_tasks)}")
    return {"msg": "Trigger đã tạm dừng", "active_tasks": len(_active_tasks)}

@app.post("/scan/resume")
async def resume_trigger(_=Depends(verify_admin)):
    global _trigger_paused
    _trigger_paused = False
    logger.info("Trigger RESUMED")
    return {"msg": "Trigger đã tiếp tục"}

@app.post("/scan/cancel")
async def cancel_active_tasks(_=Depends(verify_admin)):
    count = len(_active_tasks)
    for t in list(_active_tasks):
        # Set event trước
        event = threading.Event()
        event.set()
        _cancel_events[t.get_name()] = event
        t.cancel()
    logger.warning(f"Cancelled {count} active scan task(s)")
    return {"msg": f"Đã cancel {count} task đang chạy", "cancelled": count}

@app.get("/scan/tasks")
async def list_scan_tasks(_=Depends(verify_secret)):
    """
    Trả về danh sách asyncio task đang chạy hoặc pending.
    Frontend dùng để map task_name → job để cancel per-task.
    task_name format: scan_{date}_{HHmm}  — ví dụ: scan_2025-01-15_0800
    """
    tasks = []
    for t in list(_active_tasks):
        tasks.append({
            "task_name": t.get_name(),
            "done":      t.done(),
            "cancelled": t.cancelled(),
        })
    return {
        "active_tasks":  len(_active_tasks),
        "queue_pending": _scan_queue.qsize(),
        "paused":        _trigger_paused,
        "tasks":         tasks,
    }
 
 
@app.post("/scan/cancel/{task_name}")
async def cancel_one_task(task_name: str, _=Depends(verify_admin)):
    target = next((t for t in list(_active_tasks) if t.get_name() == task_name), None)
    if target is None:
        raise HTTPException(404, f"Task '{task_name}' không tồn tại hoặc đã kết thúc")
    if target.done():
        raise HTTPException(400, f"Task '{task_name}' đã kết thúc rồi")

    # ── THÊM: tạo và set cancel_event để thread detect thoát sớm ─
    event = threading.Event()
    event.set()
    _cancel_events[task_name] = event

    target.cancel()
    logger.warning(f"Per-task cancel: {task_name}")

    # Ghi state cancelled ngay — task_name == job_id
    from scanner.state import set_job_cancelled
    set_job_cancelled(task_name)

    return {"msg": f"Đã gửi cancel tới task '{task_name}'", "task_name": task_name}


@app.post("/jobs/cleanup")
async def manual_cleanup(_=Depends(verify_admin)):
    """
    Xóa thủ công folder ngày rỗng và clip on-demand cũ.
    An toàn: không đụng đến folder đang được task active sử dụng.
    """
    active = _get_all_active_dirs()
    n_dirs  = _cleanup_empty_date_dirs(skip_dirs=active)
    n_clips = await _cleanup_stale_clips(max_age_hours=2, skip_dirs=active)
    logger.info(f"[ManualCleanup] dirs_removed={n_dirs} clips_removed={n_clips}")
    return {
        "msg":           "Cleanup hoàn tất",
        "dirs_removed":  n_dirs,
        "clips_removed": n_clips,
        "active_dirs":   [str(d) for d in active],
    }

# ── DEBUG──────────────────────────────────────────────────

@app.post("/debug/clip-cleanup")
async def debug_clip_cleanup(_=Depends(verify_admin)):
    n_clips = await _cleanup_stale_clips(max_age_hours=24)
    n_size  = await asyncio.to_thread(_cleanup_clips_by_size)
    return {
        "clips_removed": n_clips,
        "size_removed":  n_size,
    }


# ── Entry point ──────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    import uvicorn
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    uvicorn.run("api.main:app", host=API_HOST, port=API_PORT, reload=False)