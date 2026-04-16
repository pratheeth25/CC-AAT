"""
Data Quality Analyzer — FastAPI application entry point.

Startup sequence:
  1. Configure structured logging
  2. Ensure upload directory exists
  3. Open MongoDB connection
  4. Register all routers

Shutdown sequence:
  1. Close MongoDB connection
"""

import logging
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.database import connect_to_mongo, close_mongo_connection
from app.utils.file_utils import ensure_upload_dir
from app.api.routes import datasets, analysis, cleaning, alerts, versions
from app.middleware.rate_limit import RateLimitMiddleware

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Application lifespan (startup / shutdown)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    logger.info("Starting Data Quality Analyzer backend …")
    ensure_upload_dir()
    await connect_to_mongo()
    logger.info("Ready. Upload dir: %s", settings.UPLOAD_DIR)
    yield
    # --- Shutdown ---
    await close_mongo_connection()
    logger.info("Server shut down cleanly.")


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Data Quality Analyzer API",
    description=(
        "Backend for uploading datasets, profiling data quality, detecting anomalies, "
        "suggesting and applying fixes, detecting drift, and managing alerts."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------

_origins = [o.strip() for o in settings.ALLOWED_ORIGINS.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limiting (must be added AFTER CORS — Starlette applies middleware in reverse order)
app.add_middleware(RateLimitMiddleware)


# ---------------------------------------------------------------------------
# Global exception handler
# ---------------------------------------------------------------------------

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception for %s %s", request.method, request.url)
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "message": "An unexpected error occurred. Check server logs for details.",
            "detail": str(exc),
        },
    )


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(datasets.router)
app.include_router(analysis.router)
app.include_router(cleaning.router)
app.include_router(alerts.router)
app.include_router(versions.router)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health", tags=["System"], summary="Health check")
async def health():
    return {"status": "ok", "version": "1.0.0"}


# ---------------------------------------------------------------------------
# Cache stats
# ---------------------------------------------------------------------------

@app.get("/system/cache", tags=["System"], summary="Analysis cache statistics")
async def cache_stats() -> Dict[str, Any]:
    from app.services.cache_service import analysis_cache
    return {"status": "ok", "cache": analysis_cache.stats()}


@app.delete("/system/cache", tags=["System"], summary="Clear the entire analysis cache")
async def clear_cache() -> Dict[str, Any]:
    from app.services.cache_service import analysis_cache
    analysis_cache.clear()
    return {"status": "ok", "message": "Cache cleared."}


@app.delete("/system/cache/{dataset_id}", tags=["System"], summary="Invalidate cache for one dataset")
async def invalidate_dataset_cache(dataset_id: str) -> Dict[str, Any]:
    from app.services.cache_service import analysis_cache
    removed = analysis_cache.invalidate(dataset_id)
    return {"status": "ok", "message": f"Removed {removed} cache entries for {dataset_id}."}


# ---------------------------------------------------------------------------
# Background job status
# ---------------------------------------------------------------------------

@app.get("/jobs/{job_id}", tags=["System"], summary="Get background job status")
async def get_job(job_id: str) -> Dict[str, Any]:
    from app.services.job_service import job_manager
    job = job_manager.get(job_id)
    if job is None:
        return JSONResponse(status_code=404, content={"status": "error", "message": "Job not found."})
    return {"status": "ok", "job": job}


# ---------------------------------------------------------------------------
# Feature flags
# ---------------------------------------------------------------------------

@app.get("/system/features", tags=["System"], summary="Active feature flags")
async def feature_flags() -> Dict[str, Any]:
    return {
        "status": "ok",
        "features": {
            "async_analysis": settings.ASYNC_ANALYSIS_ENABLED,
            "signed_urls": settings.SIGNED_URLS_ENABLED,
            "rate_limiting": settings.RATE_LIMIT_ENABLED,
            "rate_limit": settings.RATE_LIMIT,
            "max_profile_rows": settings.MAX_PROFILE_ROWS,
            "analysis_cache_size": settings.ANALYSIS_CACHE_SIZE,
        },
    }


# ---------------------------------------------------------------------------
# Signed URL — generate download token
# ---------------------------------------------------------------------------

@app.get("/dataset/{dataset_id}/download-token", tags=["System"], summary="Generate signed download token")
async def get_download_token(dataset_id: str, version: int = 0) -> Dict[str, Any]:
    if not settings.SIGNED_URLS_ENABLED:
        return JSONResponse(
            status_code=403,
            content={"status": "error", "message": "Signed URLs are disabled on this server."},
        )
    from app.utils.signed_url import create_signed_token
    token = create_signed_token(dataset_id, version)
    return {
        "status": "ok",
        "token": token,
        "expires_in": settings.SIGNED_URL_TTL_SECONDS,
    }
