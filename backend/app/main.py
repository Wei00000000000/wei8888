from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .config import settings
from .database import create_schema
from .rate_limit import RateLimitMiddleware
from .security_headers import SecurityHeadersMiddleware
from .routers import auth, backtest, market, signals, system


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("wei.api")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    if settings.auto_create_schema:
        await create_schema()
    yield


app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    docs_url="/docs" if settings.app_env != "production" else None,
    redoc_url=None,
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

app.add_middleware(RateLimitMiddleware)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=settings.allowed_hosts)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-CSRF-Token", "X-Admin-Token"],
)


@app.exception_handler(Exception)
async def unhandled_error(_request: Request, exc: Exception) -> ORJSONResponse:
    logger.exception("Unhandled request error", exc_info=exc)
    return ORJSONResponse(status_code=500, content={"detail": "Internal server error"})


@app.get("/health", include_in_schema=False)
async def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(auth.router, prefix=settings.api_prefix)
app.include_router(signals.router, prefix=settings.api_prefix)
app.include_router(market.router, prefix=settings.api_prefix)
app.include_router(backtest.router, prefix=settings.api_prefix)
app.include_router(system.router, prefix=settings.api_prefix)
