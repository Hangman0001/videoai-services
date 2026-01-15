import os
import json
import time

import redis
from fastapi import APIRouter, HTTPException

REDIS_URL = os.getenv("REDIS_URL")
if not REDIS_URL:
    raise RuntimeError("REDIS_URL is not set")

redis_client = redis.from_url(REDIS_URL, decode_responses=True)

QUEUE_PENDING = "video_jobs:pending"

# Export router instead of app
router = APIRouter()


@router.post("/webhook/video-uploaded")
async def video_uploaded(payload: dict):
    try:
        record = payload["record"]

        job = {
            "id": record["id"],
            "user_id": record["user_id"],
            "video_url": record["video_url"],
            "file_size_mb": record.get("file_size_mb"),
            "duration_seconds": record.get("duration_seconds"),
            "status": "pending",
            "retry_count": 0,
            "created_at": record["created_at"],
            "queued_at": time.time(),
        }

        redis_client.lpush(QUEUE_PENDING, json.dumps(job))

        return {
            "success": True,
            "job_id": job["id"],
            "queued_at": job["queued_at"],
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
