import os
from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DB: str = "data_quality"
    UPLOAD_DIR: str = "./data/uploads"
    ALLOWED_ORIGINS: str = "http://localhost:5173"
    MAX_UPLOAD_SIZE_MB: int = 100
    # ── Redis (optional) ──────────────────────────────────────────────────────
    # Leave blank to use the built-in in-memory LRU cache
    REDIS_URL: str = ""
    # TTL for cached analysis results in Redis (seconds)
    REDIS_CACHE_TTL: int = 3600
    # ── Performance ──────────────────────────────────────────────────────────
    # Max number of analysis results to keep in the in-memory LRU cache
    ANALYSIS_CACHE_SIZE: int = 128
    # Max rows to sample when profiling very large datasets (0 = no limit)
    MAX_PROFILE_ROWS: int = 50_000

    # ── Rate limiting ────────────────────────────────────────────────────────
    # Format: "<count> per <period>"  e.g. "100/minute"
    RATE_LIMIT: str = "100/minute"
    RATE_LIMIT_ENABLED: bool = True

    # ── Feature flags ────────────────────────────────────────────────────────
    # Set to False to fall back to synchronous analysis
    ASYNC_ANALYSIS_ENABLED: bool = True
    # Enable signed-URL style token for download links
    SIGNED_URLS_ENABLED: bool = False
    SIGNED_URL_SECRET: str = "change-me-in-production"
    SIGNED_URL_TTL_SECONDS: int = 3600

    # ── AWS S3 storage (optional) ────────────────────────────────────────────
    # Set S3_ENABLED=true to store uploaded files in S3 instead of local disk.
    # MongoDB still stores all metadata; only raw files go to S3.
    S3_ENABLED: bool = False
    S3_BUCKET: str = ""
    S3_REGION: str = "ap-south-1"
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
