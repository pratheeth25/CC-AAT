import os
from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DB: str = "data_quality"
    UPLOAD_DIR: str = "./data/uploads"
    ALLOWED_ORIGINS: str = "http://localhost:5173"
    MAX_UPLOAD_SIZE_MB: int = 100
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

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
