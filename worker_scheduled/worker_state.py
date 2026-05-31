"""
worker_state.py
Module dùng chung để worker_scheduled ghi trạng thái vào file JSON.
API đọc file này và trả về cho dashboard.

Dùng file JSON thay vì Redis/DB → không phụ thuộc thêm gì.

Thay đổi so với version cũ:
  - Structure mới: {"jobs": {job_id: {...}}} thay vì 1 slot "shift" duy nhất
  - Mỗi hàm nhận thêm job_id — ghi đúng slot, không ghi đè nhau
  - Nhiều job chạy song song (cron + manual) hiển thị đúng trên dashboard
  - Cleanup tự động: giữ tối đa 10 job gần nhất (done/error), xóa job cũ hơn
  - Backward-compatible: job_id có default="" → code cũ không cần sửa ngay
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path

from .config import BASE_DIR

STATE_FILE = BASE_DIR / "logs" / "worker_state.json"
MAX_DONE_JOBS = 10   # số job done/error giữ lại tối đa

_state_lock = asyncio.Lock()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _read() -> dict:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"jobs": {}}


def _write(data: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    # Đảm bảo luôn có key "jobs"
    data.setdefault("jobs", {})
    STATE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _cleanup_old_jobs(data: dict):
    """
    Giữ tối đa MAX_DONE_JOBS job có status done/error.
    Job đang running không bao giờ bị xóa.
    """
    jobs = data.get("jobs", {})
    done_jobs = [
        k for k, v in jobs.items()
        if v.get("status") in ("done", "error")
    ]
    # Sắp xếp theo started_at tăng dần → xóa job cũ nhất trước
    done_jobs.sort(key=lambda k: jobs[k].get("started_at", ""))
    for k in done_jobs[:-MAX_DONE_JOBS]:
        del jobs[k]


async def _update_cam(cam_id: int, updates: dict, job_id: str = ""):
    """Read-modify-write an toàn có lock — ghi vào đúng slot job_id."""
    async with _state_lock:
        data = _read()
        job  = data["jobs"].get(job_id, {})
        cam  = job.get("cameras", {}).get(str(cam_id), {})
        cam.update(**updates)
        job.setdefault("cameras", {})[str(cam_id)] = cam
        data["jobs"][job_id] = job
        _write(data)


# ── Public API ────────────────────────────────────────────────────

def set_shift_started(
    shift_name:     str,
    target_date:    str,
    cam_ids:        list[int],
    job_id:         str = "",
    segments_total: int = 0,
):
    """
    Khởi tạo 1 job mới trong state.
    Nếu job_id trùng (trigger lại cùng job) → ghi đè slot đó.
    """
    data = _read()
    data["jobs"][job_id] = {
        "job_id":         job_id,
        "source":         "cron" if job_id.startswith("cron_") else "manual",
        "label":          shift_name,
        "target_date":    target_date,
        "cam_ids":        cam_ids,
        "status":         "running",
        "started_at":     _now(),
        "finished_at":    None,
        "total_scans":    0,
        "segments_total": segments_total,
        "dl_active":      0,
        "dl_done":        0,
        "detect_active":  0,
        "detect_done":    0,
    }
    _cleanup_old_jobs(data)
    _write(data)


async def update_job_counters(
    job_id:           str,
    dl_delta:         int = 0,
    detect_delta:     int = 0,
    dl_done_delta:    int = 0,
    detect_done_delta: int = 0,
):
    """Atomic increment/decrement worker counters cho scan_date_bulk."""
    async with _state_lock:
        data = _read()
        job  = data["jobs"].get(job_id, {})
        if dl_delta:
            job["dl_active"]     = max(0, job.get("dl_active", 0) + dl_delta)
        if detect_delta:
            job["detect_active"] = max(0, job.get("detect_active", 0) + detect_delta)
        if dl_done_delta:
            job["dl_done"]       = job.get("dl_done", 0) + dl_done_delta
        if detect_done_delta:
            job["detect_done"]   = job.get("detect_done", 0) + detect_done_delta
        data["jobs"][job_id] = job
        _write(data)


def set_cam_downloading(cam_id: int, chunks_total: int, job_id: str = ""):
    """Sync — gọi trước asyncio.gather nên không cần lock."""
    data = _read()
    job  = data["jobs"].get(job_id, {})
    cam  = job.get("cameras", {}).get(str(cam_id), {})
    cam.update(status="downloading", chunks_total=chunks_total, updated_at=_now())
    job.setdefault("cameras", {})[str(cam_id)] = cam
    data["jobs"][job_id] = job
    _write(data)


async def set_cam_chunk_progress(
    cam_id:       int,
    chunk_idx:    int,
    chunk_label:  str,
    chunks_total: int,
    job_id:       str = "",
):
    await _update_cam(cam_id, dict(
        status        = "downloading",
        chunks_done   = chunk_idx,
        chunks_total  = chunks_total,
        current_chunk = chunk_label,
        updated_at    = _now(),
    ), job_id=job_id)


async def set_cam_detecting(cam_id: int, job_id: str = ""):
    await _update_cam(cam_id, dict(
        status        = "detecting",
        current_chunk = None,
        updated_at    = _now(),
    ), job_id=job_id)


async def set_cam_done(cam_id: int, scans_found: int, job_id: str = ""):
    await _update_cam(cam_id, dict(
        status        = "done",
        scans_found   = scans_found,
        current_chunk = None,
        updated_at    = _now(),
    ), job_id=job_id)


async def set_cam_error(cam_id: int, msg: str, job_id: str = ""):
    await _update_cam(cam_id, dict(
        status        = "error",
        current_chunk = msg,
        updated_at    = _now(),
    ), job_id=job_id)


def set_shift_finished(total_scans: int, job_id: str = ""):
    """Sync — gọi sau gather nên không cần lock."""
    data  = _read()
    job   = data["jobs"].get(job_id, {})
    job.update(status="done", finished_at=_now(), total_scans=total_scans)
    data["jobs"][job_id] = job
    _cleanup_old_jobs(data)
    _write(data)


def set_job_cancelled(job_id: str):
    """
    Đánh dấu job là cancelled trong state.
    Gọi từ API cancel endpoint ngay sau khi cancel asyncio task.
    """
    data = _read()
    job  = data.get("jobs", {}).get(job_id)
    if job:
        job.update(status="cancelled", finished_at=_now())
        data["jobs"][job_id] = job
        _cleanup_old_jobs(data)
        _write(data)


def read_state() -> dict:
    """
    Trả về toàn bộ state.
    Dashboard đọc data["jobs"] để hiển thị tất cả job đang/đã chạy.
    """
    return _read()

def clear_done_jobs() -> int:
    """
    Xóa tất cả job status done/error/cancelled.
    Job đang running không bị xóa.
    Trả về số job đã xóa.
    """
    data = _read()
    jobs = data.get("jobs", {})
    to_remove = [
        k for k, v in jobs.items()
        if v.get("status") in ("done", "error", "cancelled")
    ]
    for k in to_remove:
        del jobs[k]
    _write(data)
    return len(to_remove)