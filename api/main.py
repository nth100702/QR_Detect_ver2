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
import re
import shutil
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

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

from shared import db
from shared.hikvision_playback import init_playback, get_playback
from shared.models import QrScan
from worker_scheduled.config import (
    API_SECRET, REDIS_URL, API_HOST, API_PORT, LOG_DIR,
    SHIFTS, CHUNK_MINUTES, TEMP_VIDEO_DIR,
)
from worker_scheduled.downloader import download_cam_chunks, chunk_ranges
from worker_scheduled.detector import process_video
from worker_scheduled.worker_state import (
    set_shift_started, set_shift_finished,
    set_cam_downloading, set_cam_chunk_progress,
    set_cam_detecting, set_cam_done, set_cam_error,
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

def verify_secret(key: str = Security(_api_key_header)):
    if key != API_SECRET:
        raise HTTPException(status_code=401, detail="Invalid API secret")
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

def get_scan_trigger_queue() -> asyncio.Queue:
    return _scan_queue

# ── Worker: download + detect pipeline ──────────────────────────
async def _download_with_progress(
    cam_id:       int,
    date:         datetime,
    start_hour:   int,
    end_hour:     int,
    start_minute: int = 0,
    end_minute:   int = 0,
):
    from worker_scheduled.downloader import _semaphore
    from shared.hikvision_playback import get_playback

    footage_start = TZ.normalize(date.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0))
    footage_end   = TZ.normalize(date.replace(hour=end_hour,   minute=end_minute,   second=0, microsecond=0))
    chunks        = chunk_ranges(footage_start, footage_end)
    total         = len(chunks)

    set_cam_downloading(cam_id, total)
    out_dir = TEMP_VIDEO_DIR / str(cam_id) / date.strftime("%Y%m%d")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Đăng ký folder này với task hiện tại để cleanup an toàn
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else "_scheduler"
    _active_dirs.setdefault(task_name, set()).add(out_dir)

    results  = []
    playback = get_playback()

    async with _semaphore:
        for idx, (c_start, c_end) in enumerate(chunks):
            label = f"{c_start:%H:%M}–{c_end:%H:%M}"
            set_cam_chunk_progress(cam_id, idx, label, total)

            out = out_dir / f"chunk_{c_start:%H%M}.mp4"
            if out.exists() and out.stat().st_size > 0:
                wlogger.debug(f"[CAM {cam_id}] {label} cached, skip")
                results.append((out, c_start, c_end))
                continue

            ok = await playback.async_download_clip(cam_id, c_start, c_end, out)
            if ok:
                results.append((out, c_start, c_end))
            else:
                wlogger.error(f"[CAM {cam_id}] {label} FAILED")

    return results


async def scan_date(
    cam_ids:      list[int],
    target_date:  datetime,
    start_hour:   int,
    end_hour:     int,
    start_minute: int = 0,
    end_minute:   int = 0,
    label:        str = "",
) -> int:
    from shared.db import get_channel_map
    get_playback().cfg.channel_map = await get_channel_map()
    wlogger.info(f"Channel map: {get_playback().cfg.channel_map}")
    date_str = target_date.strftime("%Y-%m-%d")
    label    = label or f"scan {date_str} {start_hour:02d}:{start_minute:02d}–{end_hour:02d}:{end_minute:02d}"

    wlogger.info(f"=== [{label}] cams={cam_ids} ===")
    set_shift_started(label, date_str, cam_ids)

    dl_tasks = [
        _download_with_progress(
            cam_id, target_date,
            start_hour, end_hour,
            start_minute, end_minute,
        )
        for cam_id in cam_ids
    ]
    cam_results = await asyncio.gather(*dl_tasks, return_exceptions=True)

    async def _detect_cam(cid, chunks):
        n = 0
        for (path, c_start, c_end) in chunks:
            n += await process_video(path, cid, c_start, c_end)
        set_cam_done(cid, n)
        return n

    detect_tasks = []
    for cam_id, chunks in zip(cam_ids, cam_results):
        if isinstance(chunks, Exception):
            wlogger.error(f"[CAM {cam_id}] Download exception: {chunks}")
            set_cam_error(cam_id, str(chunks))
            continue
        if not chunks:
            wlogger.warning(f"[CAM {cam_id}] Không có chunk nào tải được")
            set_cam_done(cam_id, 0)
            continue
        set_cam_detecting(cam_id)
        detect_tasks.append(_detect_cam(cam_id, chunks))

    results     = await asyncio.gather(*detect_tasks, return_exceptions=True)
    total_scans = sum(r for r in results if isinstance(r, int))

    set_shift_finished(total_scans)
    wlogger.info(f"=== [{label}] done | total_scans={total_scans} ===")
    return total_scans


async def run_shift(shift_name: str):
    shift = next((s for s in SHIFTS if s.name == shift_name), None)
    if not shift:
        wlogger.error(f"Unknown shift: {shift_name}")
        return

    yesterday = (datetime.now(TZ) - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    await scan_date(
        cam_ids     = shift.cam_ids,
        target_date = yesterday,
        start_hour  = shift.footage_start_hour,
        end_hour    = shift.footage_end_hour,
        label       = shift.name,
    )


def _get_all_active_dirs() -> set:
    """Trả về tập hợp tất cả folder đang được task active sử dụng."""
    result = set()
    for dirs in _active_dirs.values():
        result.update(dirs)
    return result


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


def _cleanup_stale_clips(max_age_hours: int = 2, skip_dirs: set | None = None) -> int:
    """
    Xóa clip on-demand (.mp4) trong TEMP_VIDEO_DIR cũ hơn max_age_hours giờ.
    On-demand clip được nhận biết bằng mtime, không phải chunk_ prefix
    (chunk_ đã bị xóa ngay sau detect; clip on-demand là file còn sót lại).

    Lý do chọn 2 giờ:
      - Ca dài nhất ~12h → clip on-demand tải về dùng xem ngay, 2h là đủ
      - Không xóa quá sớm phòng client đang stream
      - Không để quá lâu (disk drain)
    """
    skip_dirs = skip_dirs or set()
    removed   = 0
    cutoff    = datetime.now().timestamp() - max_age_hours * 3600
    if not TEMP_VIDEO_DIR.exists():
        return 0
    for mp4 in TEMP_VIDEO_DIR.rglob("*.mp4"):
        if not mp4.is_file():
            continue
        if mp4.parent in skip_dirs:
            continue
        try:
            if mp4.stat().st_mtime < cutoff:
                mp4.unlink(missing_ok=True)
                wlogger.info(f"[Cleanup] Xóa clip cũ ({max_age_hours}h): {mp4}")
                removed += 1
        except Exception as e:
            wlogger.warning(f"[Cleanup] Không thể xóa {mp4}: {e}")
    return removed


async def run_periodic_cleanup():
    """APScheduler job: dọn folder rỗng + clip on-demand cũ."""
    active = _get_all_active_dirs()
    wlogger.info(f"[Cleanup] Bắt đầu | active_dirs={len(active)}")
    n_dirs  = _cleanup_empty_date_dirs(skip_dirs=active)
    n_clips = _cleanup_stale_clips(max_age_hours=2, skip_dirs=active)
    wlogger.info(f"[Cleanup] Xong | dirs_removed={n_dirs} clips_removed={n_clips}")


async def _run_scan_job(job: dict):
    label = f"manual {job['date']} {job['start_time']}–{job['end_time']}"
    task_name = asyncio.current_task().get_name() if asyncio.current_task() else label
    try:
        sh, sm = map(int, job["start_time"].split(":"))
        eh, em = map(int, job["end_time"].split(":"))
        target = TZ.localize(datetime.strptime(job["date"], "%Y-%m-%d"))
        await scan_date(
            cam_ids      = job["cam_ids"],
            target_date  = target,
            start_hour   = sh,
            end_hour     = eh,
            start_minute = sm,
            end_minute   = em,
            label        = label,
        )
    except asyncio.CancelledError:
        wlogger.warning(f"[{label}] bị cancel — đang cleanup folder...")
        # Cleanup an toàn: chỉ xóa folder của task này,
        # không đụng đến folder của task khác đang chạy song song
        my_dirs = _active_dirs.pop(task_name, set())
        other_active = _get_all_active_dirs()   # dirs của các task còn lại
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
    finally:
        current = asyncio.current_task()
        _active_tasks.discard(current)
        _active_dirs.pop(task_name, None)   # đảm bảo luôn được dọn


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

            label = f"scan_{job['date']}_{job['start_time'].replace(':', '')}"
            task  = asyncio.create_task(_run_scan_job(job), name=label)
            _active_tasks.add(task)
            wlogger.info(
                f"Trigger spawned task [{label}] | "
                f"active_tasks={len(_active_tasks)} | queue_pending={queue.qsize()}"
            )
            queue.task_done()

        except asyncio.CancelledError:
            wlogger.info("Trigger listener cancelled — shutting down")
            raise
        except Exception as e:
            wlogger.exception(f"Trigger listener error: {e}")

# ── Lifespan ─────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _redis

    await db.init_pool()
    logger.info("DB pool ready")

    try:
        r = aioredis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2)
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
    for shift in SHIFTS:
        scheduler.add_job(
            run_shift,
            trigger=CronTrigger(hour=shift.cron_hour, minute=shift.cron_minute, timezone=TZ),
            args=[shift.name],
            id=f"shift_{shift.name}",
            misfire_grace_time=300,
            replace_existing=True,
        )
        logger.info(
            f"Scheduled shift [{shift.name}] "
            f"{shift.cron_hour:02d}:{shift.cron_minute:02d} ICT | cams={shift.cam_ids}"
        )

    # Cleanup định kỳ: mỗi 30 phút — dọn folder rỗng + clip on-demand cũ > 2h
    # Chạy lúc :00 và :30 của mỗi giờ; bỏ qua folder đang được task active dùng
    scheduler.add_job(
        run_periodic_cleanup,
        trigger=CronTrigger(minute="0,30", timezone=TZ),
        id="periodic_cleanup",
        misfire_grace_time=120,
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

    logger.info("API + Worker started")
    yield

    trigger_task.cancel()
    try:
        await trigger_task
    except asyncio.CancelledError:
        pass
    logger.info("Scan trigger listener stopped")

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
    if secret != API_SECRET:
        raise HTTPException(status_code=401, detail="Invalid secret")

    async def event_gen():
        from worker_scheduled.worker_state import read_state
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

from shared.hikvision_playback import refresh_channel_map
# ── Cameras ──────────────────────────────────────────────────────
@app.get("/cameras")
async def get_cameras(_=Depends(verify_secret)):
    cams = await db.list_cameras(status=None)
    return {"data": [c.__dict__ for c in cams]}

@app.post("/cameras", status_code=201)
async def create_camera(body: CameraCreate, _=Depends(verify_secret)):
    if body.shift not in ("morning", "night"):
        raise HTTPException(400, "shift phải là 'morning' hoặc 'night'")
    cam_id = await db.insert_camera(body.name, body.shift, body.nvr_channel)
    await refresh_channel_map()   # ← sync channel map sau khi thêm
    return {"id": cam_id, "msg": "Camera created"}

@app.delete("/cameras/{cam_id}")
async def remove_camera(cam_id: int, _=Depends(verify_secret)):
    ok = await db.delete_camera(cam_id)
    if not ok:
        raise HTTPException(404, f"Camera {cam_id} not found")
    await refresh_channel_map()   # ← xóa camera khỏi map
    return {"msg": f"Camera {cam_id} deactivated"}

@app.post("/cameras/{cam_id}/restore")
async def restore_camera(cam_id: int, _=Depends(verify_secret)):
    ok = await db.restore_camera(cam_id)
    if not ok:
        raise HTTPException(404, f"Camera {cam_id} không tìm thấy hoặc chưa bị xóa")
    await refresh_channel_map()   # ← thêm lại vào map khi restore
    return {"msg": f"Camera {cam_id} đã được khôi phục"}

@app.put("/cameras/{cam_id}")
async def update_camera(cam_id: int, body: CameraUpdate, _=Depends(verify_secret)):
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
        from worker_scheduled.worker_state import read_state
        return read_state() or {"shift": None, "cameras": {}}
    except Exception as e:
        return {"shift": None, "cameras": {}, "error": str(e)}

# ── Scan trigger ─────────────────────────────────────────────────
@app.post("/scan/trigger", status_code=202)
async def trigger_scan(body: ScanTrigger, _=Depends(verify_secret)):
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

    job = {
        "cam_ids":    body.cam_ids,
        "date":       body.date,
        "start_time": body.start_time,
        "end_time":   body.end_time,
        "queued_at":  datetime.now(TZ).isoformat(),
    }
    await _scan_queue.put(job)
    logger.info(f"Scan trigger queued: {job}")
    return {
        "msg":           "Scan job queued — worker sẽ xử lý ngay",
        "cam_ids":       body.cam_ids,
        "date":          body.date,
        "start_time":    body.start_time,
        "end_time":      body.end_time,
        "active_tasks":  len(_active_tasks),
        "queue_pending": _scan_queue.qsize(),
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

    # 3. File có tồn tại không?
    if not path.exists():
        raise HTTPException(404, f"File không tồn tại trên disk: {path}")

    # 4. Serve
    return FileResponse(
        path,
        media_type="video/mp4",
        filename=path.name,
        headers={"Content-Disposition": f'attachment; filename="{path.name}"'}
    )

@app.post("/scan/pause")
async def pause_trigger(_=Depends(verify_secret)):
    global _trigger_paused
    _trigger_paused = True
    logger.warning(f"Trigger PAUSED | active_tasks={len(_active_tasks)}")
    return {"msg": "Trigger đã tạm dừng", "active_tasks": len(_active_tasks)}

@app.post("/scan/resume")
async def resume_trigger(_=Depends(verify_secret)):
    global _trigger_paused
    _trigger_paused = False
    logger.info("Trigger RESUMED")
    return {"msg": "Trigger đã tiếp tục"}

@app.post("/scan/cancel")
async def cancel_active_tasks(_=Depends(verify_secret)):
    count = len(_active_tasks)
    for t in list(_active_tasks):
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
async def cancel_one_task(task_name: str, _=Depends(verify_secret)):
    """
    Cancel một task cụ thể theo tên.
    task_name lấy từ GET /scan/tasks → tasks[].task_name
    Backend sẽ cleanup folder của task này qua CancelledError handler trong _run_scan_job.
    """
    target = next((t for t in list(_active_tasks) if t.get_name() == task_name), None)
    if target is None:
        raise HTTPException(404, f"Task '{task_name}' không tồn tại hoặc đã kết thúc")
    if target.done():
        raise HTTPException(400, f"Task '{task_name}' đã kết thúc rồi")
    target.cancel()
    logger.warning(f"Per-task cancel: {task_name}")
    return {"msg": f"Đã gửi cancel tới task '{task_name}'", "task_name": task_name}


@app.post("/jobs/cleanup")
async def manual_cleanup(_=Depends(verify_secret)):
    """
    Xóa thủ công folder ngày rỗng và clip on-demand cũ.
    An toàn: không đụng đến folder đang được task active sử dụng.
    """
    active = _get_all_active_dirs()
    n_dirs  = _cleanup_empty_date_dirs(skip_dirs=active)
    n_clips = _cleanup_stale_clips(max_age_hours=2, skip_dirs=active)
    logger.info(f"[ManualCleanup] dirs_removed={n_dirs} clips_removed={n_clips}")
    return {
        "msg":           "Cleanup hoàn tất",
        "dirs_removed":  n_dirs,
        "clips_removed": n_clips,
        "active_dirs":   [str(d) for d in active],
    }

# ── Entry point ──────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    import uvicorn
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    uvicorn.run("api:app", host=API_HOST, port=API_PORT, reload=False)