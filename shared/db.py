"""
shared/db.py
asyncpg connection pool + helper functions.
Tất cả worker và api đều import từ đây.
"""

import asyncpg
import os
import logging
from datetime import datetime, date as date_type
from typing import Optional

from .models import QrScan, Camera

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


def _parse_date(date: str | date_type | None) -> date_type | None:
    """Convert string 'YYYY-MM-DD' → datetime.date. Bỏ qua nếu None hoặc đã là date."""
    if date is None:
        return None
    if isinstance(date, date_type):
        return date
    try:
        return date_type.fromisoformat(date)
    except ValueError as e:
        logger.warning(f"Invalid date format '{date}': {e}")
        return None


async def init_pool(dsn: str | None = None, min_size: int = 2, max_size: int = 10):
    """
    Khởi tạo connection pool.
    Gọi 1 lần khi startup (lifespan FastAPI hoặc worker main).
    """
    global _pool
    dsn = dsn or os.environ["DATABASE_URL"]
    _pool = await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size,server_settings={"TimeZone": "Asia/Ho_Chi_Minh"},)
    logger.info(f"asyncpg pool ready (min={min_size} max={max_size})")


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("asyncpg pool closed")


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized. Call init_pool() first.")
    return _pool


# ─── Cameras ─────────────────────────────────────────────────────

async def list_cameras(status: str | None = "active") -> list[Camera]:
    q = "SELECT id, name, shift, nvr_channel, status FROM cameras"
    params = []
    if status:
        q += " WHERE status = $1"
        params.append(status)
    q += " ORDER BY id"
    rows = await get_pool().fetch(q, *params)
    return [Camera(**dict(r)) for r in rows]


async def get_camera(cam_id: int) -> Camera | None:
    row = await get_pool().fetchrow(
        "SELECT id, name, shift, nvr_channel, status FROM cameras WHERE id = $1",
        cam_id,
    )
    return Camera(**dict(row)) if row else None


async def insert_camera(name: str, shift: str, nvr_channel: int) -> int:
    return await get_pool().fetchval(
        "INSERT INTO cameras (name, shift, nvr_channel) VALUES ($1,$2,$3) RETURNING id",
        name, shift, nvr_channel,
    )

async def restore_camera(cam_id: int) -> bool:
    result = await get_pool().execute(
        "UPDATE cameras SET status='active' WHERE id=$1 AND status='deleted'",
        cam_id
    )
    return result == "UPDATE 1"

async def delete_camera(cam_id: int) -> bool:
    result = await get_pool().execute(
        "UPDATE cameras SET status='deleted' WHERE id=$1", cam_id
    )
    return result == "UPDATE 1"


async def get_channel_map() -> dict[int, int]:
    """Trả về {cam_id: nvr_channel} cho tất cả camera active."""
    rows = await get_pool().fetch(
        "SELECT id, nvr_channel FROM cameras WHERE status = 'active'"
    )
    return {r["id"]: r["nvr_channel"] for r in rows}


# ─── QR Scans ────────────────────────────────────────────────────

async def insert_qr(scan: QrScan) -> int | None:
    """
    Insert QR scan. Bỏ qua nếu trùng (cam_id, qr_value, chunk_start).
    Trả về id mới hoặc None nếu duplicate.
    """
    try:
        return await get_pool().fetchval(
            """
            INSERT INTO qr_scans (cam_id, qr_value, detected_at, chunk_start, chunk_end, shift)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (cam_id, qr_value, chunk_start) DO NOTHING
            RETURNING id
            """,
            scan.cam_id, scan.qr_value, scan.detected_at,
            scan.chunk_start, scan.chunk_end, scan.shift,
        )
    except Exception as e:
        logger.error(f"insert_qr error: {e}")
        return None


async def search_by_qr(qr_value: str, date: str | date_type | None = None) -> list[dict]:
    parsed = _parse_date(date)
    q = """
        SELECT s.id, s.cam_id, c.name AS cam_name, s.qr_value,
               s.detected_at, s.chunk_start, s.chunk_end, s.shift, s.clip_file
        FROM qr_scans s
        LEFT JOIN cameras c ON c.id = s.cam_id
        WHERE s.qr_value ILIKE $1
    """
    params: list = [f"%{qr_value}%"]
    if parsed:
        q += " AND DATE(s.detected_at) = $2"
        params.append(parsed)
    q += " ORDER BY s.detected_at DESC LIMIT 200"
    rows = await get_pool().fetch(q, *params)
    return [dict(r) for r in rows]


async def get_scans(
    cam_id:   int | None = None,
    qr_value: str | None = None,
    date:     str | date_type | None = None,
    limit:    int = 50,
    offset:   int = 0,
) -> tuple[int, list[dict]]:
    parsed_date = _parse_date(date)  # FIX: string → date_type

    conditions, params = [], []
    idx = 1

    if cam_id is not None:
        conditions.append(f"s.cam_id = ${idx}"); params.append(cam_id); idx += 1
    if qr_value:
        conditions.append(f"s.qr_value ILIKE ${idx}"); params.append(f"%{qr_value}%"); idx += 1
    if parsed_date:
        conditions.append(f"DATE(s.detected_at) = ${idx}"); params.append(parsed_date); idx += 1

    where  = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    base_q = f"FROM qr_scans s LEFT JOIN cameras c ON c.id = s.cam_id {where}"

    total = await get_pool().fetchval(f"SELECT COUNT(*) {base_q}", *params)
    rows  = await get_pool().fetch(
        f"""
        SELECT s.id, s.cam_id, c.name AS cam_name, s.qr_value,
               s.detected_at, s.shift, s.clip_file
        {base_q}
        ORDER BY s.detected_at DESC
        LIMIT ${idx} OFFSET ${idx+1}
        """,
        *params, limit, offset,
    )
    return total, [dict(r) for r in rows]


async def update_clip_path(scan_id: int, clip_file: str):
    await get_pool().execute(
        "UPDATE qr_scans SET clip_file=$1 WHERE id=$2", clip_file, scan_id
    )


async def get_stats_today() -> dict:
    row = await get_pool().fetchrow(
        """
        SELECT
            COUNT(*)                 AS total_scans,
            COUNT(DISTINCT cam_id)   AS active_cams,
            COUNT(DISTINCT qr_value) AS unique_qrs,
            MAX(detected_at)         AS last_scan
        FROM qr_scans
        WHERE DATE(detected_at) = CURRENT_DATE
        """
    )
    return dict(row) if row else {}


async def get_stats_by_cam(date: str | date_type | None = None) -> list[dict]:
    parsed = _parse_date(date)  # FIX: string → date_type
    cond   = "WHERE DATE(s.detected_at) = $1" if parsed else ""
    params = [parsed] if parsed else []
    rows   = await get_pool().fetch(
        f"""
        SELECT s.cam_id, c.name AS cam_name,
               COUNT(*) AS total_scans, MAX(s.detected_at) AS last_scan
        FROM qr_scans s LEFT JOIN cameras c ON c.id = s.cam_id
        {cond}
        GROUP BY s.cam_id, c.name ORDER BY total_scans DESC
        """,
        *params,
    )
    return [dict(r) for r in rows]