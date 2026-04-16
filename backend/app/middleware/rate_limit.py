"""
Simple in-process rate limiter middleware.

Uses a sliding-window counter per (client IP, endpoint) pair.
No external dependencies required.

Configuration via Settings:
    RATE_LIMIT_ENABLED = True
    RATE_LIMIT = "100/minute"   # "<count>/<period>"  (second|minute|hour)
"""

import logging
import time
from collections import defaultdict, deque
from threading import Lock
from typing import Callable

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from app.config import settings

logger = logging.getLogger(__name__)

# Map period names → seconds
_PERIOD_SECONDS = {"second": 1, "minute": 60, "hour": 3600}


def _parse_rate_limit(spec: str):
    """Parse "100/minute" into (100, 60)."""
    try:
        count_str, period = spec.split("/")
        return int(count_str), _PERIOD_SECONDS[period.lower().strip()]
    except Exception:
        logger.warning("Invalid RATE_LIMIT spec %r, defaulting to 100/minute", spec)
        return 100, 60


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Sliding-window rate limiter.

    Returns HTTP 429 if a client exceeds the configured limit.
    Health-check endpoint is always exempt.
    """

    _EXEMPT_PATHS = {"/health", "/docs", "/redoc", "/openapi.json"}

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)
        self._limit, self._window = _parse_rate_limit(settings.RATE_LIMIT)
        self._store: dict = defaultdict(deque)
        self._lock = Lock()
        logger.info(
            "RateLimitMiddleware: %d requests per %ds (enabled=%s)",
            self._limit,
            self._window,
            settings.RATE_LIMIT_ENABLED,
        )

    async def dispatch(self, request: Request, call_next: Callable):
        if not settings.RATE_LIMIT_ENABLED:
            return await call_next(request)

        path = request.url.path
        if path in self._EXEMPT_PATHS:
            return await call_next(request)

        client_ip = self._get_client_ip(request)
        key = f"{client_ip}:{path}"
        now = time.monotonic()

        with self._lock:
            window = self._store[key]
            # Evict timestamps outside the sliding window
            cutoff = now - self._window
            while window and window[0] < cutoff:
                window.popleft()

            if len(window) >= self._limit:
                retry_after = int(self._window - (now - window[0])) + 1
                logger.warning("Rate limit exceeded for %s on %s", client_ip, path)
                return JSONResponse(
                    status_code=429,
                    content={
                        "status": "error",
                        "message": f"Rate limit exceeded. Try again in {retry_after}s.",
                    },
                    headers={
                        "Retry-After": str(retry_after),
                        "X-RateLimit-Limit": str(self._limit),
                        "X-RateLimit-Window": str(self._window),
                    },
                )

            window.append(now)

        return await call_next(request)

    @staticmethod
    def _get_client_ip(request: Request) -> str:
        # Respect X-Forwarded-For from trusted proxies (first hop only)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        if request.client:
            return request.client.host
        return "unknown"
