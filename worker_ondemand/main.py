"""
worker-ondemand/main.py
Lắng nghe Redis queue, xử lý job tải clip on-demand.

Job format (JSON):
{
  "job_id":   "uuid",
  "qr_value": "SPXVN...",
  "records":  [{"scan_id": 1, "cam_id": 2, "detected_at": "2025-01-01T10:00:00"}]
}
"""

import asyncio
import json
import logging
import os
from datetime import datetime

import redis.asyncio as aioredis

from shared import db
from shared.hikvision_playback import init_playback
from .clipper import fetch_clip
from dotenv import load_dotenv
from datetime import timezone, timedelta
ICT = timezone(timedelta(hours=7))

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("worker-ondemand")

REDIS_URL    = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
QUEUE_KEY    = "jobs:ondemand"
STATUS_TTL   = 3600   # TTL 1h cho job status key
API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")
API_SECRET   = os.environ.get("API_SECRET", "")


async def set_job_status(redis: aioredis.Redis, job_id: str, payload: dict):
    key = f"job:status:{job_id}"
    await redis.setex(key, STATUS_TTL, json.dumps(payload))


async def process_job(redis: aioredis.Redis, raw: str):
    try:
        job = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid job JSON: {e}")
        return

    job_id   = job.get("job_id", "unknown")
    qr_value = job.get("qr_value", "")
    records  = job.get("records", [])

    logger.info(f"[JOB {job_id}] Received | qr={qr_value} | {len(records)} scan(s)")

    await set_job_status(redis, job_id, {
        "job_id": job_id, "status": "running",
        "qr_value": qr_value, "total": len(records), "done": 0,
    })


    clip_files = []
    for idx, rec in enumerate(records):
        scan_id     = rec["scan_id"]
        cam_id      = rec["cam_id"]
        detected_at = datetime.fromisoformat(rec["detected_at"])
        if detected_at.tzinfo is not None:
            detected_at = detected_at.astimezone(ICT)  # đảm bảo về +7
        detected_at = detected_at.replace(tzinfo=None)

        clip_path = await fetch_clip(job_id, scan_id, cam_id, detected_at)
        if clip_path:
            clip_files.append(str(clip_path))

            # Cập nhật clip_file trong DB qua API
            import httpx
            async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=10) as client:
                try:
                    await client.patch(
                        f"/scans/{scan_id}/clip",
                        json={"clip_file": str(clip_path)},
                        headers={"X-Secret": API_SECRET},
                    )
                except Exception as e:
                    logger.warning(f"[JOB {job_id}] PATCH /scans/{scan_id}/clip failed: {e}")

        await set_job_status(redis, job_id, {
            "job_id": job_id, "status": "running",
            "qr_value": qr_value, "total": len(records), "done": idx + 1,
        })

    await set_job_status(redis, job_id, {
        "job_id": job_id, "status": "done",
        "qr_value": qr_value, "total": len(records),
        "done": len(records), "clip_files": clip_files,
    })

    logger.info(
        f"[JOB {job_id}] Done | {len(clip_files)}/{len(records)} clips downloaded"
    )


async def main():
    # Init DB pool trước — init_playback() cần get_channel_map() từ DB
    try:
        await db.init_pool()
        logger.info("DB pool ready")
    except Exception as e:
        logger.error(f"DB init failed: {e}")
        return

    # Init HikvisionPlayback — load channel_map từ DB
    try:
        await init_playback()
        logger.info("HikvisionPlayback singleton ready")
    except Exception as e:
        logger.error(f"HikvisionPlayback init failed: {e}")
        return

    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    logger.info(f"Connected to Redis: {REDIS_URL}")
    logger.info(f"Listening on queue: {QUEUE_KEY}")

    try:
        while True:
            try:
                # brpop blocks until a job arrives (timeout=0 = forever)
                result = await redis.brpop(QUEUE_KEY, timeout=0)
                if result is None:
                    continue
                _, raw = result
                await process_job(redis, raw)
            except aioredis.RedisError as e:
                logger.error(f"Redis error: {e}, retrying in 5s")
                await asyncio.sleep(5)
            except Exception as e:
                logger.exception(f"Unexpected error: {e}")
                await asyncio.sleep(1)
    finally:
        await db.close_pool()
        await redis.aclose()
        logger.info("Worker shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())