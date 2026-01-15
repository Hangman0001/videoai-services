"""
Railway Queue Service - Unified Entry Point

Starts:
1. FastAPI webhook server (foreground) → Railway exposes public URL
2. Queue worker (background asyncio task) → processes Redis jobs

IMPORTANT:
- Railway only exposes a public URL if an HTTP server runs in foreground
- Queue worker must NOT block the event loop
"""

import os
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
import uvicorn

# ✅ ABSOLUTE IMPORTS (CRITICAL FOR RAILWAY)
from src.webhook import router as webhook_router
from src.worker import QueueWorker

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("railway-queue-service")

# --------------------------------------------------
# Global worker references
# --------------------------------------------------
worker: Optional[QueueWorker] = None
worker_task: Optional[asyncio.Task] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan hook.
    Starts QueueWorker in background on startup.
    Cancels it cleanly on shutdown.
    """
    global worker, worker_task

    logger.info("🚀 Starting Railway Queue Service...")

    # Start queue worker
    worker = QueueWorker()
    worker_task = asyncio.create_task(worker.start())

    logger.info("✅ Queue worker started as background task")

    try:
        yield
    finally:
        logger.info("🛑 Shutting down Railway Queue Service...")
        if worker_task:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
        logger.info("✅ Queue worker stopped")


# --------------------------------------------------
# FastAPI app
# --------------------------------------------------
app = FastAPI(
    title="Railway Queue Service",
    description="Webhook endpoint + Redis queue worker for VideoAI",
    lifespan=lifespan,
)

# Register webhook routes
app.include_router(webhook_router)


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "railway-queue",
        "worker_running": worker is not None,
    }


# --------------------------------------------------
# Local / direct execution (Railway-safe)
# --------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))

    uvicorn.run(
        "src.main:app",   # ✅ MUST include src.
        host="0.0.0.0",
        port=port,
        log_level="info",
    )
