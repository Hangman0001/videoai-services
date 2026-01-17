# """
# Railway Queue Worker

# This service processes video processing jobs from a Redis queue and updates
# the Supabase database with job status and results.

# Responsibilities:
# - Consuming jobs from Redis queue
# - Processing video metadata and status updates
# - Updating Supabase database with job results
# - Handling job failures and retries
# - Health monitoring
# """

# import os
# import time
# import json
# from typing import Dict, Any, Optional

# # Placeholder imports - these would be used in production
# # import redis
# # from supabase import create_client, Client
# # import requests

# # Environment variables (placeholders)
# REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
# SUPABASE_URL = os.getenv("SUPABASE_URL", "https://placeholder.supabase.co")
# SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "placeholder_key")
# RAILWAY_SERVICE_NAME = os.getenv("RAILWAY_SERVICE_NAME", "videoai-queue")


# class QueueWorker:
#     """
#     Redis queue worker for processing video jobs.
    
#     This is a stub implementation. In production, this would:
#     1. Connect to Redis queue
#     2. Poll for new jobs
#     3. Process each job
#     4. Update Supabase with results
#     5. Handle errors and retries
#     """
    
#     def __init__(self):
#         self.redis_url = REDIS_URL
#         self.supabase_url = SUPABASE_URL
#         self.supabase_key = SUPABASE_SERVICE_ROLE_KEY
#         self.running = False
        
#         # Stub: In production, initialize Redis and Supabase clients
#         # self.redis_client = redis.from_url(self.redis_url)
#         # self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
    
#     def health_check(self) -> Dict[str, Any]:
#         """
#         Health check function for monitoring.
#         Returns service status and connection information.
#         """
#         # Stub: In production, this would check Redis and Supabase connections
#         return {
#             "status": "healthy",
#             "service": "railway-queue",
#             "redis_connected": False,  # Stub: would check actual connection
#             "supabase_connected": False,  # Stub: would check actual connection
#             "message": "Health check (stub - no actual connection checks)"
#         }
    
#     def process_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
#         """
#         Process a single job from the queue.
        
#         This is a stub implementation. In production, this would:
#         1. Parse job data
#         2. Update job status to 'processing' in Supabase
#         3. Perform job-specific processing
#         4. Update job status to 'completed' or 'failed' in Supabase
#         5. Handle any errors appropriately
        
#         Args:
#             job_data: Dictionary containing job information (job_id, video_url, etc.)
        
#         Returns:
#             Dictionary with processing result
#         """
#         job_id = job_data.get("job_id", "unknown")
        
#         # Stub: In production, this would:
#         # 1. Update Supabase: supabase.table('jobs').update({'status': 'processing'}).eq('id', job_id).execute()
#         # 2. Process the job based on job_type
#         # 3. Update Supabase with results: supabase.table('jobs').update({'status': 'completed', 'result': result}).eq('id', job_id).execute()
        
#         print(f"Processing job {job_id} (stub - no actual processing)")
        
#         return {
#             "success": True,
#             "job_id": job_id,
#             "status": "completed",
#             "message": f"Job {job_id} processed successfully (stub - no actual processing performed)"
#         }
    
#     def handle_job_failure(self, job_data: Dict[str, Any], error: Exception) -> None:
#         """
#         Handle a failed job.
        
#         This is a stub implementation. In production, this would:
#         1. Log the error
#         2. Update job status to 'failed' in Supabase
#         3. Store error message
#         4. Optionally retry the job if retry count < max_retries
        
#         Args:
#             job_data: Dictionary containing job information
#             error: Exception that caused the failure
#         """
#         job_id = job_data.get("job_id", "unknown")
        
#         # Stub: In production, this would:
#         # supabase.table('jobs').update({
#         #     'status': 'failed',
#         #     'error': str(error),
#         #     'failed_at': datetime.utcnow().isoformat()
#         # }).eq('id', job_id).execute()
        
#         print(f"Job {job_id} failed: {error} (stub - no actual database update)")
    
#     def consume_queue(self, queue_name: str = "video_jobs") -> None:
#         """
#         Main queue consumption loop.
        
#         This is a stub implementation. In production, this would:
#         1. Connect to Redis
#         2. Continuously poll the queue for new jobs
#         3. Process each job
#         4. Handle errors and retries
#         5. Implement graceful shutdown
        
#         Args:
#             queue_name: Name of the Redis queue to consume from
#         """
#         self.running = True
        
#         print(f"Starting queue worker for queue: {queue_name}")
#         print("(Stub - no actual queue consumption)")
        
#         # Stub: In production, this would be a continuous loop:
#         # while self.running:
#         #     try:
#         #         # Blocking pop from Redis queue
#         #         job_data = self.redis_client.blpop(queue_name, timeout=1)
#         #         if job_data:
#         #             job = json.loads(job_data[1])
#         #             result = self.process_job(job)
#         #     except Exception as e:
#         #         self.handle_job_failure(job, e)
#         #         time.sleep(1)
        
#         # Stub: Simulate processing a few jobs
#         for i in range(3):
#             if not self.running:
#                 break
            
#             mock_job = {
#                 "job_id": f"job_{i}",
#                 "video_url": f"https://example.com/video_{i}.mp4",
#                 "job_type": "process_video"
#             }
            
#             try:
#                 result = self.process_job(mock_job)
#                 print(f"Processed: {result}")
#             except Exception as e:
#                 self.handle_job_failure(mock_job, e)
            
#             time.sleep(1)  # Stub: simulate processing time
    
#     def stop(self) -> None:
#         """Stop the worker gracefully."""
#         self.running = False
#         print("Worker stopped")


# def main():
#     """
#     Main entry point for the queue worker.
#     """
#     worker = QueueWorker()
    
#     # Health check
#     health = worker.health_check()
#     print(f"Health check: {health}")
    
#     try:
#         # Start consuming from queue
#         worker.consume_queue("video_jobs")
#     except KeyboardInterrupt:
#         print("\nReceived interrupt signal, shutting down...")
#         worker.stop()
#     except Exception as e:
#         print(f"Worker error: {e}")
#         worker.stop()
#         raise


# if __name__ == "__main__":
#     main()
# import os
# import json
# import time
# import asyncio
# import logging
# from typing import Dict, Any

# import redis
# import aiohttp

# # --------------------------------------------------
# # Logging
# # --------------------------------------------------
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
# )
# logger = logging.getLogger("queue-worker")

# # --------------------------------------------------
# # Environment
# # --------------------------------------------------
# REDIS_URL = os.getenv("REDIS_URL")
# CLOUDFLARE_ROUTER_URL = os.getenv("CLOUDFLARE_ROUTER_URL")

# if not REDIS_URL:
#     raise RuntimeError("REDIS_URL is not set")

# if not CLOUDFLARE_ROUTER_URL:
#     raise RuntimeError("CLOUDFLARE_ROUTER_URL is not set")

# # --------------------------------------------------
# # Queue Names
# # --------------------------------------------------
# QUEUES = {
#     "pending": "video_jobs:pending",
#     "processing": "video_jobs:processing",
#     "routed": "video_jobs:routed",
#     "dead_letter": "video_jobs:dead_letter",
#     "retry_delayed": "video_jobs:retry:delayed",
# }

# MAX_RETRIES = 5
# RETRY_DELAYS = [1, 2, 4, 8, 16]  # seconds


# class QueueWorker:
#     def __init__(self):
#         self.redis = redis.from_url(REDIS_URL, decode_responses=True)
#         logger.info("Connected to Redis")

#     # --------------------------------------------------
#     # Main loop
#     # --------------------------------------------------
#     async def start(self):
#         logger.info("🚀 Phase 2 Queue Worker started")
#         logger.info(f"Router Worker: {CLOUDFLARE_ROUTER_URL}")

#         while True:
#             try:
#                 job_data = self.redis.brpoplpush(
#                     QUEUES["pending"],
#                     QUEUES["processing"],
#                     timeout=1,
#                 )

#                 if job_data:
#                     job = json.loads(job_data)
#                     await self.process_job(job)

#                 await self.process_retry_queue()
#                 await asyncio.sleep(0.1)

#             except Exception as e:
#                 logger.error(f"Worker loop error: {e}")
#                 await asyncio.sleep(5)

#     # --------------------------------------------------
#     # Job processing
#     # --------------------------------------------------
#     async def process_job(self, job: Dict[str, Any]):
#         job_id = job.get("id")
#         retry_count = job.get("retry_count", 0)

#         logger.info(f"Processing job {job_id} (attempt {retry_count + 1})")

#         try:
#             response = await self.call_router_worker(job)

#             if not response.get("success"):
#                 raise RuntimeError(response.get("error", "Router failure"))

#             job["route"] = response["classification"]["route"]
#             job["status"] = "routed"
#             job["routed_at"] = time.time()

#             self.redis.lpush(QUEUES["routed"], json.dumps(job))
#             self.redis.lrem(QUEUES["processing"], 0, json.dumps(job))

#             logger.info(f"Job {job_id} routed → {job['route']}")

#         except Exception as e:
#             logger.error(f"Job {job_id} failed: {e}")
#             await self.handle_failure(job, str(e))

#     # --------------------------------------------------
#     # Router Worker call
#     # --------------------------------------------------
#     async def call_router_worker(self, job: Dict[str, Any]) -> Dict[str, Any]:
#         payload = {
#             "id": job.get("id"),
#             "video_url": job.get("video_url"),
#             "file_size_mb": job.get("file_size_mb"),
#             "duration_seconds": job.get("duration_seconds"),
#         }

#         async with aiohttp.ClientSession() as session:
#             try:
#                 async with session.post(
#                     CLOUDFLARE_ROUTER_URL,
#                     json=payload,
#                     timeout=30,
#                 ) as resp:
#                     if resp.status == 200:
#                         return await resp.json()
#                     return {
#                         "success": False,
#                         "error": f"HTTP {resp.status}",
#                     }
#             except asyncio.TimeoutError:
#                 return {"success": False, "error": "Router timeout"}
#             except Exception as e:
#                 return {"success": False, "error": str(e)}

#     # --------------------------------------------------
#     # Failure & retry
#     # --------------------------------------------------
#     async def handle_failure(self, job: Dict[str, Any], error: str):
#         job_id = job.get("id")
#         retry_count = job.get("retry_count", 0)

#         self.redis.lrem(QUEUES["processing"], 0, json.dumps(job))

#         if retry_count < MAX_RETRIES:
#             delay = RETRY_DELAYS[retry_count]
#             retry_at = time.time() + delay

#             job["retry_count"] = retry_count + 1
#             job["last_error"] = error
#             job["next_retry_at"] = retry_at

#             self.redis.zadd(
#                 QUEUES["retry_delayed"],
#                 {json.dumps(job): retry_at},
#             )

#             logger.info(f"Retry scheduled for job {job_id} in {delay}s")

#         else:
#             job["status"] = "dead_letter"
#             job["final_error"] = error
#             job["failed_at"] = time.time()

#             self.redis.lpush(QUEUES["dead_letter"], json.dumps(job))
#             logger.error(f"Job {job_id} moved to dead_letter")

#     # --------------------------------------------------
#     # Retry queue
#     # --------------------------------------------------
#     async def process_retry_queue(self):
#         now = time.time()
#         jobs = self.redis.zrangebyscore(
#             QUEUES["retry_delayed"],
#             0,
#             now,
#         )

#         for job_str in jobs:
#             self.redis.lpush(QUEUES["pending"], job_str)
#             self.redis.zrem(QUEUES["retry_delayed"], job_str)

#             job = json.loads(job_str)
#             logger.info(f"Retrying job {job.get('id')}")


# # --------------------------------------------------
# # Entry point
# # --------------------------------------------------
# async def main():
#     worker = QueueWorker()
#     await worker.start()


# if __name__ == "__main__":
#     asyncio.run(main())
import os
import json
import time
import asyncio
import logging
from typing import Dict, Any

import redis
import aiohttp

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("queue-worker")

# --------------------------------------------------
# Environment variables
# --------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL")
CLOUDFLARE_ROUTER_URL = os.getenv("CLOUDFLARE_ROUTER_URL")
HF_FARM_URL = os.getenv("HF_FARM_URL")

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is not set")

if not CLOUDFLARE_ROUTER_URL:
    raise RuntimeError("CLOUDFLARE_ROUTER_URL is not set")

if not HF_FARM_URL:
    raise RuntimeError("HF_FARM_URL is not set")

# --------------------------------------------------
# Queue names
# --------------------------------------------------
QUEUES = {
    "pending": "video_jobs:pending",
    "processing": "video_jobs:processing",
    "routed": "video_jobs:routed",
    "dead_letter": "video_jobs:dead_letter",
    "retry_delayed": "video_jobs:retry:delayed",
}

MAX_RETRIES = 5
RETRY_DELAYS = [1, 2, 4, 8, 16]  # seconds


class QueueWorker:
    def __init__(self):
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)
        logger.info("✅ Connected to Redis")

    # --------------------------------------------------
    # Main worker loop
    # --------------------------------------------------
    async def start(self):
        logger.info("🚀 Queue Worker started")
        logger.info(f"Router URL: {CLOUDFLARE_ROUTER_URL}")
        logger.info(f"HF Farm URL: {HF_FARM_URL}")

        while True:
            try:
                job_data = self.redis.brpoplpush(
                    QUEUES["pending"],
                    QUEUES["processing"],
                    timeout=1,
                )

                if job_data:
                    job = json.loads(job_data)
                    await self.process_job(job)

                await self.process_retry_queue()
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Worker loop error: {e}")
                await asyncio.sleep(5)

    # --------------------------------------------------
    # Job processing
    # --------------------------------------------------
    async def process_job(self, job: Dict[str, Any]):
        job_id = job.get("id")
        retry_count = job.get("retry_count", 0)

        logger.info(f"▶️ Processing job {job_id} (attempt {retry_count + 1})")

        try:
            # ----------------------------
            # 1. Call Router Worker
            # ----------------------------
            router_response = await self.call_router_worker(job)

            if not router_response.get("success"):
                raise RuntimeError(router_response.get("error", "Router failure"))

            job["route"] = router_response["route"]
            job["router_decision"] = router_response["decision"]
            job["routed_at"] = time.time()

            logger.info(
                f"🧠 Router selected {job['router_decision']['provider']} "
                f"for job {job_id}"
            )

            # ----------------------------
            # 2. Call HF Farm
            # ----------------------------
            hf_response = await self.call_hf_farm(job)

            if not hf_response.get("success"):
                raise RuntimeError(hf_response.get("error", "HF Farm failure"))

            job["highlights"] = hf_response.get("highlights", [])
            job["highlight_count"] = hf_response.get("highlight_count", 0)
            job["ai_completed_at"] = time.time()
            job["status"] = "completed"

            # ----------------------------
            # 3. Move to routed queue
            # ----------------------------
            self.redis.lpush(QUEUES["routed"], json.dumps(job))
            self.redis.lrem(QUEUES["processing"], 0, json.dumps(job))

            logger.info(
                f"✅ Job {job_id} completed "
                f"({job['highlight_count']} highlights)"
            )

        except Exception as e:
            logger.error(f"❌ Job {job_id} failed: {e}")
            await self.handle_failure(job, str(e))

    # --------------------------------------------------
    # Router Worker call
    # --------------------------------------------------
    async def call_router_worker(self, job: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "id": job.get("id"),
            "video_url": job.get("video_url"),
            "file_size_mb": job.get("file_size_mb", 0),
            "duration_seconds": job.get("duration_seconds"),
            "route": job.get("route"),
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    CLOUDFLARE_ROUTER_URL,
                    json=payload,
                    timeout=30,
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()

                    return {
                        "success": False,
                        "error": f"Router HTTP {resp.status}",
                    }

            except asyncio.TimeoutError:
                return {"success": False, "error": "Router timeout"}
            except Exception as e:
                return {"success": False, "error": str(e)}

    # --------------------------------------------------
    # HF Farm call
    # --------------------------------------------------
    async def call_hf_farm(self, job: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "jobId": job.get("id"),
            "video_url": job.get("video_url"),
            "duration_seconds": job.get("duration_seconds"),
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    HF_FARM_URL,
                    json=payload,
                    timeout=60,
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()

                    return {
                        "success": False,
                        "error": f"HF Farm HTTP {resp.status}",
                    }

            except asyncio.TimeoutError:
                return {"success": False, "error": "HF Farm timeout"}
            except Exception as e:
                return {"success": False, "error": str(e)}

    # --------------------------------------------------
    # Failure & retry handling
    # --------------------------------------------------
    async def handle_failure(self, job: Dict[str, Any], error: str):
        job_id = job.get("id")
        retry_count = job.get("retry_count", 0)

        self.redis.lrem(QUEUES["processing"], 0, json.dumps(job))

        if retry_count < MAX_RETRIES:
            delay = RETRY_DELAYS[retry_count]
            retry_at = time.time() + delay

            job["retry_count"] = retry_count + 1
            job["last_error"] = error
            job["next_retry_at"] = retry_at

            self.redis.zadd(
                QUEUES["retry_delayed"],
                {json.dumps(job): retry_at},
            )

            logger.info(
                f"🔁 Retry scheduled for job {job_id} in {delay}s"
            )

        else:
            job["status"] = "dead_letter"
            job["final_error"] = error
            job["failed_at"] = time.time()

            self.redis.lpush(QUEUES["dead_letter"], json.dumps(job))
            logger.error(f"☠️ Job {job_id} moved to dead_letter")

    # --------------------------------------------------
    # Retry queue processor
    # --------------------------------------------------
    async def process_retry_queue(self):
        now = time.time()
        jobs = self.redis.zrangebyscore(
            QUEUES["retry_delayed"],
            0,
            now,
        )

        for job_str in jobs:
            self.redis.lpush(QUEUES["pending"], job_str)
            self.redis.zrem(QUEUES["retry_delayed"], job_str)

            job = json.loads(job_str)
            logger.info(f"🔄 Retrying job {job.get('id')}")


# --------------------------------------------------
# Entry point
# --------------------------------------------------
async def main():
    worker = QueueWorker()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
