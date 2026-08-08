from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .config import settings
from .database import create_schema
from .rate_limit import RateLimitMiddleware
from .security_headers import SecurityHeadersMiddleware
from .routers import auth, backtest, market, notifications, positions, signals, system


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("wei.api")
ROOT_DIR = Path(__file__).resolve().parents[2]
FRONTEND_FILES = {
    "api-config.js",
    "app.html",
    "binance-api-test.html",
    "scanner-api-trace.html",
    "brand-hero.png",
    "icon-192.png",
    "icon-512.png",
    "icon.svg",
    "index.html",
    "login.html",
    "manifest.webmanifest",
    "markets.json",
    "sw.js",
}


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
async def unhandled_error(_request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled request error", exc_info=exc)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


@app.get("/health", include_in_schema=False)
async def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(auth.router, prefix=settings.api_prefix)
app.include_router(signals.router, prefix=settings.api_prefix)
app.include_router(positions.router, prefix=settings.api_prefix)
app.include_router(market.router, prefix=settings.api_prefix)
app.include_router(backtest.router, prefix=settings.api_prefix)
app.include_router(notifications.router, prefix=settings.api_prefix)
app.include_router(system.router, prefix=settings.api_prefix)

try:
    from sentiment_scanner.plugins.api_trace.router import router as api_trace_router

    app.include_router(api_trace_router, prefix=settings.api_prefix)
except ImportError:
    pass


@app.get("/", include_in_schema=False)
async def frontend_root() -> FileResponse:
    return FileResponse(ROOT_DIR / "login.html")


@app.get("/app", include_in_schema=False)
async def frontend_app() -> FileResponse:
    return FileResponse(ROOT_DIR / "app.html")


@app.get("/login", include_in_schema=False)
async def frontend_login() -> FileResponse:
    return FileResponse(ROOT_DIR / "login.html")


@app.get("/{asset_path:path}", include_in_schema=False)
async def frontend_asset(asset_path: str) -> FileResponse:
    normalized = asset_path.strip("/")
    if normalized == "sentiment_scanner/app.html":
        return FileResponse(ROOT_DIR / "sentiment_scanner" / "app.html")
    if normalized in FRONTEND_FILES:
        return FileResponse(ROOT_DIR / normalized)
    return FileResponse(ROOT_DIR / "login.html")
