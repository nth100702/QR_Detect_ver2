"""
worker_state.py
Module dùng chung để worker_scheduled ghi trạng thái vào file JSON.
API đọc file này và trả về cho dashboard.

Dùng file JSON thay vì Redis/DB → không phụ thuộc thêm gì.
"""

import json
import os
from datetime import datetime
from pathlib import Path

from .config import BASE_DIR

STATE_FILE = BASE_DIR / "logs" / "worker_state.json"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _read() -> dict:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write(data: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ── Các hàm worker_scheduled gọi để cập nhật trạng thái ─────────

def set_shift_started(shift_name: str, target_date: str, cam_ids: list[int]):
    data = _read()
    data["shift"] = {
        "name":        shift_name,
        "target_date": target_date,
        "cam_ids":     cam_ids,
        "status":      "running",
        "started_at":  _now(),
        "finished_at": None,
        "total_scans": 0,
    }
    data["cameras"] = {
        str(cam_id): {
            "cam_id":          cam_id,
            "status":          "waiting",   # waiting | downloading | detecting | done | error
            "chunks_total":    0,
            "chunks_done":     0,
            "scans_found":     0,
            "current_chunk":   None,        # "08:00–08:05"
            "updated_at":      _now(),
        }
        for cam_id in cam_ids
    }
    _write(data)


def set_cam_downloading(cam_id: int, chunks_total: int):
    data = _read()
    cam  = data.get("cameras", {}).get(str(cam_id), {})
    cam.update(status="downloading", chunks_total=chunks_total, updated_at=_now())
    data.setdefault("cameras", {})[str(cam_id)] = cam
    _write(data)


def set_cam_chunk_progress(cam_id: int, chunk_idx: int, chunk_label: str, chunks_total: int):
    data = _read()
    cam  = data.get("cameras", {}).get(str(cam_id), {})
    cam.update(
        status        = "downloading",
        chunks_done   = chunk_idx,
        chunks_total  = chunks_total,
        current_chunk = chunk_label,
        updated_at    = _now(),
    )
    data.setdefault("cameras", {})[str(cam_id)] = cam
    _write(data)


def set_cam_detecting(cam_id: int):
    data = _read()
    cam  = data.get("cameras", {}).get(str(cam_id), {})
    cam.update(status="detecting", current_chunk=None, updated_at=_now())
    data.setdefault("cameras", {})[str(cam_id)] = cam
    _write(data)


def set_cam_done(cam_id: int, scans_found: int):
    data = _read()
    cam  = data.get("cameras", {}).get(str(cam_id), {})
    cam.update(status="done", scans_found=scans_found, current_chunk=None, updated_at=_now())
    data.setdefault("cameras", {})[str(cam_id)] = cam
    _write(data)


def set_cam_error(cam_id: int, msg: str):
    data = _read()
    cam  = data.get("cameras", {}).get(str(cam_id), {})
    cam.update(status="error", current_chunk=msg, updated_at=_now())
    data.setdefault("cameras", {})[str(cam_id)] = cam
    _write(data)


def set_shift_finished(total_scans: int):
    data = _read()
    shift = data.get("shift", {})
    shift.update(status="done", finished_at=_now(), total_scans=total_scans)
    data["shift"] = shift
    _write(data)


def read_state() -> dict:
    return _read()