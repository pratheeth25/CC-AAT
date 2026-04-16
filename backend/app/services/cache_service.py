"""
Analysis cache — two-tier: Redis (when available) → in-memory LRU fallback.

Keys are (dataset_id, version, analysis_type) tuples.
The cache is automatically invalidated when a new version is created,
cleaned, restored, or deleted by calling ``invalidate(dataset_id)``.

Usage:
    from app.services.cache_service import analysis_cache

    result = analysis_cache.get("ds123", 2, "profile")
    if result is None:
        result = compute_expensive_thing()
        analysis_cache.set("ds123", 2, "profile", result)

Redis is used automatically when REDIS_URL is set in .env:
    REDIS_URL=redis://localhost:6379/0

Falls back to in-memory LRU if Redis is not configured or unavailable.
"""

import json
import logging
from collections import OrderedDict
from threading import Lock
from typing import Any, Optional, Tuple

from app.config import settings

logger = logging.getLogger(__name__)

_CacheKey = Tuple[str, int, str]


# ── In-process LRU ────────────────────────────────────────────────────────────

class _LRUCache:
    """Thread-safe LRU cache with a fixed capacity."""

    def __init__(self, capacity: int) -> None:
        self._capacity = max(1, capacity)
        self._store: OrderedDict[str, Any] = OrderedDict()
        self._lock = Lock()
        self._hits = 0
        self._misses = 0

    def _key(self, dataset_id: str, version: int, analysis_type: str) -> str:
        return f"{dataset_id}:{version}:{analysis_type}"

    def get(self, dataset_id: str, version: int, analysis_type: str) -> Optional[Any]:
        k = self._key(dataset_id, version, analysis_type)
        with self._lock:
            if k not in self._store:
                self._misses += 1
                return None
            self._store.move_to_end(k)
            self._hits += 1
            return self._store[k]

    def set(self, dataset_id: str, version: int, analysis_type: str, value: Any) -> None:
        k = self._key(dataset_id, version, analysis_type)
        with self._lock:
            if k in self._store:
                self._store.move_to_end(k)
            self._store[k] = value
            if len(self._store) > self._capacity:
                self._store.popitem(last=False)

    def invalidate(self, dataset_id: str) -> int:
        prefix = f"{dataset_id}:"
        with self._lock:
            keys = [k for k in self._store if k.startswith(prefix)]
            for k in keys:
                del self._store[k]
            if keys:
                logger.info("Cache INVALIDATED %d entries for %s", len(keys), dataset_id)
            return len(keys)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def stats(self) -> dict:
        with self._lock:
            total = self._hits + self._misses
            return {
                "backend": "memory",
                "size": len(self._store),
                "capacity": self._capacity,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": round(self._hits / total, 4) if total else 0.0,
            }


# ── Redis-backed cache ────────────────────────────────────────────────────────

class _RedisCache:
    """
    Redis-backed cache using JSON serialisation.

    TTL defaults to 1 hour — expired keys are evicted automatically by Redis.
    Falls back gracefully to miss on any Redis error.
    """

    def __init__(self, redis_url: str, ttl: int = 3600) -> None:
        import redis as _redis
        self._client = _redis.from_url(redis_url, decode_responses=True)
        self._ttl = ttl
        self._hits = 0
        self._misses = 0
        logger.info("RedisCache connected: %s (ttl=%ds)", redis_url, ttl)

    def _key(self, dataset_id: str, version: int, analysis_type: str) -> str:
        return f"dqa:{dataset_id}:{version}:{analysis_type}"

    def get(self, dataset_id: str, version: int, analysis_type: str) -> Optional[Any]:
        try:
            raw = self._client.get(self._key(dataset_id, version, analysis_type))
            if raw is None:
                self._misses += 1
                return None
            self._hits += 1
            return json.loads(raw)
        except Exception as exc:
            logger.debug("Redis GET error: %s", exc)
            self._misses += 1
            return None

    def set(self, dataset_id: str, version: int, analysis_type: str, value: Any) -> None:
        try:
            self._client.setex(
                self._key(dataset_id, version, analysis_type),
                self._ttl,
                json.dumps(value, default=str),
            )
        except Exception as exc:
            logger.debug("Redis SET error: %s", exc)

    def invalidate(self, dataset_id: str) -> int:
        try:
            pattern = f"dqa:{dataset_id}:*"
            keys = list(self._client.scan_iter(pattern))
            if keys:
                self._client.delete(*keys)
                logger.info("Cache INVALIDATED %d Redis keys for %s", len(keys), dataset_id)
            return len(keys)
        except Exception as exc:
            logger.debug("Redis INVALIDATE error: %s", exc)
            return 0

    def clear(self) -> None:
        try:
            keys = list(self._client.scan_iter("dqa:*"))
            if keys:
                self._client.delete(*keys)
        except Exception as exc:
            logger.debug("Redis CLEAR error: %s", exc)

    def stats(self) -> dict:
        try:
            info = self._client.info("stats")
            total = self._hits + self._misses
            return {
                "backend": "redis",
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": round(self._hits / total, 4) if total else 0.0,
                "redis_hits": info.get("keyspace_hits", 0),
                "redis_misses": info.get("keyspace_misses", 0),
            }
        except Exception:
            return {"backend": "redis", "hits": self._hits, "misses": self._misses}


# ── Factory — pick backend based on config ────────────────────────────────────

def _build_cache():
    redis_url = getattr(settings, "REDIS_URL", "")
    if redis_url:
        try:
            cache = _RedisCache(redis_url, ttl=getattr(settings, "REDIS_CACHE_TTL", 3600))
            # quick connectivity check
            cache._client.ping()
            return cache
        except Exception as exc:
            logger.warning("Redis unavailable (%s) — falling back to in-memory cache", exc)

    return _LRUCache(capacity=settings.ANALYSIS_CACHE_SIZE)


# Singleton used by all routes
analysis_cache = _build_cache()
