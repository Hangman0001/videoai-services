"""
Railway Queue Service - Unified Entry Point

This file starts both:
1. FastAPI webhook server (foreground) - Railway detects this and exposes public URL
2. Queue worker (background asyncio task) - Processes jobs from Redis

Railway only exposes public URLs when an HTTP server is running in the foreground.
"""

import os
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional

import uvicorn
from fastapi import FastAPI

from webhook import router as webhook_router
from worker import QueueWorker

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("railway-queue-service")

# --------------------------------------------------
# Global worker instance and task
# --------------------------------------------------
worker: Optional[QueueWorker] = None
worker_task: Optional[asyncio.Task] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan context manager.
    Starts the queue worker when the app starts, stops it when the app shuts down.
    """
    global worker, worker_task
    
    # Startup: Start queue worker as background task
    logger.info("🚀 Starting Railway Queue Service...")
    worker = QueueWorker()
    
    # Start worker in background
    worker_task = asyncio.create_task(worker.start())
    logger.info("✅ Queue worker started as background task")
    
    yield
    
    # Shutdown: Stop queue worker
    logger.info("🛑 Shutting down Railway Queue Service...")
    if worker_task:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
    logger.info("✅ Queue worker stopped")


# --------------------------------------------------
# Create FastAPI app with lifespan
# --------------------------------------------------
app = FastAPI(
    title="Railway Queue Service",
    description="Webhook endpoint + Queue worker for VideoAI",
    lifespan=lifespan,
)

# Include webhook routes
app.include_router(webhook_router)


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "railway-queue",
        "worker_running": worker is not None,
    }


# --------------------------------------------------
# Entry point
# --------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
    )
