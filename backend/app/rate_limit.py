from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from time import monotonic

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from .config import settings


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: object) -> None:
        super().__init__(app)
        self._requests: dict[str, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    def _limit_for(self, path: str) -> int:
        if path.endswith("/auth/login"):
            return settings.login_rate_limit
        if path.endswith("/backtest/run"):
            return settings.backtest_rate_limit
        if path.endswith("/notifications/test"):
            return settings.notification_rate_limit
        if path.endswith("/positions/history") or path.endswith("/backtest/trades"):
            return settings.history_rate_limit
        return settings.general_rate_limit

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if request.url.path in {"/health", "/ready"}:
            return await call_next(request)
        forwarded = request.headers.get("x-forwarded-for", "")
        ip = forwarded.split(",", 1)[0].strip() if forwarded else (request.client.host if request.client else "unknown")
        key = f"{ip}:{request.url.path}"
        limit = self._limit_for(request.url.path)
        now = monotonic()
        async with self._lock:
            bucket = self._requests[key]
            while bucket and bucket[0] <= now - 60:
                bucket.popleft()
            if len(bucket) >= limit:
                retry_after = max(1, int(60 - (now - bucket[0])))
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Too many requests"},
                    headers={"Retry-After": str(retry_after)},
                )
            bucket.append(now)
        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(limit)
        return response

