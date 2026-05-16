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
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

import pytz
import redis.asyncio as aioredis
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel

from shared import db
from shared.hikvision_playback import init_playback
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
# Logger được cấu hình một lần duy nhất, cả API lẫn worker dùng chung.
# worker_scheduled.py cũ có basicConfig riêng — bỏ vì nay chạy cùng process.
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
logger     = logging.getLogger("api")
wlogger    = logging.getLogger("worker_scheduled")  # namespace riêng cho worker
TZ         = pytz.timezone("Asia/Ho_Chi_Minh")

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
    """Dependency — trả 503 nếu Redis chưa sẵn sàng."""
    if _redis is None:
        raise HTTPException(
            status_code=503,
            detail="Redis không khả dụng. Cài Redis để dùng tính năng tải footage on-demand.",
        )
    return _redis

# ── Scan queue (internal — không cần Redis) ──────────────────────
_scan_queue: asyncio.Queue = asyncio.Queue()

def get_scan_trigger_queue() -> asyncio.Queue:
    """Trả về queue dùng để nhận manual scan trigger."""
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

    footage_start = date.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    footage_end   = date.replace(hour=end_hour,   minute=end_minute,   second=0, microsecond=0)
    chunks        = chunk_ranges(footage_start, footage_end)
    total         = len(chunks)

    set_cam_downloading(cam_id, total)
    out_dir = TEMP_VIDEO_DIR / str(cam_id) / date.strftime("%Y%m%d")
    out_dir.mkdir(parents=True, exist_ok=True)

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
    """
    Download + detect QR cho danh sách cam trong khoảng thời gian chỉ định.

    Dùng được từ:
      - Cron ca tự động
      - API POST /scan/trigger
      - Gọi trực tiếp từ code khi cần

    Trả về tổng số QR scan đã ghi vào DB.
    """
    date_str = target_date.strftime("%Y-%m-%d")
    label    = label or f"scan {date_str} {start_hour:02d}:{start_minute:02d}–{end_hour:02d}:{end_minute:02d}"

    wlogger.info(f"=== [{label}] cams={cam_ids} ===")
    set_shift_started(label, date_str, cam_ids)

    # ── Download ─────────────────────────────────────────────────
    dl_tasks = [
        _download_with_progress(
            cam_id, target_date,
            start_hour, end_hour,
            start_minute, end_minute,
        )
        for cam_id in cam_ids
    ]
    cam_results = await asyncio.gather(*dl_tasks, return_exceptions=True)

    # ── Detect — ngoài semaphore NVR ─────────────────────────────
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
    """Gọi bởi APScheduler theo cron."""
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


async def listen_scan_triggers(queue: asyncio.Queue):
    """Vòng lặp nhận manual scan job từ _scan_queue và gọi scan_date()."""
    wlogger.info("Listening for manual scan triggers...")
    while True:
        job = await queue.get()
        try:
            sh, sm = map(int, job["start_time"].split(":"))
            eh, em = map(int, job["end_time"].split(":"))
            target = datetime.strptime(job["date"], "%Y-%m-%d").replace(tzinfo=TZ)

            await scan_date(
                cam_ids      = job["cam_ids"],
                target_date  = target,
                start_hour   = sh,
                end_hour     = eh,
                start_minute = sm,
                end_minute   = em,
                label        = f"manual {job['date']} {job['start_time']}–{job['end_time']}",
            )
        except Exception as e:
            wlogger.exception(f"Scan trigger error: {e}")
        finally:
            queue.task_done()

# ── Lifespan ─────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _redis

    # 1. DB pool
    await db.init_pool()
    logger.info("DB pool ready")

    # 2. Redis (optional)
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

    # 3. HikvisionPlayback
    try:
        init_playback()
        logger.info("HikvisionPlayback ready")
    except Exception as e:
        logger.error(f"HikvisionPlayback init failed: {e} — worker sẽ không hoạt động")

    # 4. APScheduler — cron theo từng ca trong SHIFTS
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
    scheduler.start()
    logger.info("APScheduler running")

    # 5. Manual trigger listener — chạy nền, dùng _scan_queue nội bộ
    trigger_task = asyncio.create_task(
        listen_scan_triggers(_scan_queue),
        name="scan_trigger_listener",
    )
    logger.info("Scan trigger listener started")

    logger.info("API + Worker started")
    yield  # ── server đang chạy ──────────────────────────────────

    # ── Shutdown sạch ────────────────────────────────────────────
    trigger_task.cancel()
    try:
        await trigger_task
    except asyncio.CancelledError:
        pass
    logger.info("Scan trigger listener stopped")

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

# ── Schemas ──────────────────────────────────────────────────────
class CameraCreate(BaseModel):
    name:        str
    shift:       str   # 'morning' | 'night'
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
    date:       str        # YYYY-MM-DD
    start_time: str = "08:00"   # HH:MM
    end_time:   str = "19:00"   # HH:MM

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
    return {"id": cam_id, "msg": "Camera created"}

@app.delete("/cameras/{cam_id}")
async def remove_camera(cam_id: int, _=Depends(verify_secret)):
    ok = await db.delete_camera(cam_id)
    if not ok:
        raise HTTPException(404, f"Camera {cam_id} not found")
    return {"msg": f"Camera {cam_id} deactivated"}

@app.put("/cameras/{cam_id}")
async def update_camera(cam_id: int, body: CameraUpdate, _=Depends(verify_secret)):
    if body.shift not in ("morning", "night"):
        raise HTTPException(400, "shift phải là morning hoặc night")
    pool = db.get_pool()
    res = await pool.execute(
        """
        UPDATE cameras
        SET
            name = $1,
            shift = $2,
            nvr_channel = $3
        WHERE id = $4
          AND status = 'active'
        """,
        body.name,
        body.shift,
        body.nvr_channel,
        cam_id,
    )
    if res == "UPDATE 0":
        raise HTTPException(404, f"Camera {cam_id} not found")
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
    scan = QrScan(
        cam_id=body.cam_id, qr_value=body.qr_value,
        detected_at=datetime.fromisoformat(body.detected_at),
        chunk_start=datetime.fromisoformat(body.chunk_start),
        chunk_end=datetime.fromisoformat(body.chunk_end),
        shift=body.shift,
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
    """Đọc trạng thái realtime của worker từ file JSON."""
    try:
        from worker_scheduled.worker_state import read_state
        return read_state() or {"shift": None, "cameras": {}}
    except Exception as e:
        return {"shift": None, "cameras": {}, "error": str(e)}

# ── Scan trigger (manual playback scan) ─────────────────────────
@app.post("/scan/trigger", status_code=202)
async def trigger_scan(body: ScanTrigger, _=Depends(verify_secret)):
    """
    Trigger scan playback thủ công cho cam + khoảng thời gian chỉ định.
    Dùng asyncio.Queue nội bộ — không cần Redis.
    """
    # Validate date
    try:
        datetime.strptime(body.date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "date phải là định dạng YYYY-MM-DD")

    # Validate time
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
        "queued_at":  datetime.now().isoformat(),
    }
    await _scan_queue.put(job)
    logger.info(f"Scan trigger queued: {job}")
    return {
        "msg":        "Scan job queued — worker sẽ xử lý ngay",
        "cam_ids":    body.cam_ids,
        "date":       body.date,
        "start_time": body.start_time,
        "end_time":   body.end_time,
    }

@app.get("/scan/queue")
async def get_scan_queue(_=Depends(verify_secret)):
    """Xem số job đang chờ trong queue."""
    return {"pending": _scan_queue.qsize()}

# ── Entry point ──────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    import uvicorn
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    uvicorn.run("api:app", host=API_HOST, port=API_PORT, reload=False)