from __future__ import annotations

import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator

from sqlalchemy import text

from .config import settings
from .database import SessionFactory, engine
from .importer import ROOT, import_legacy_history, import_market_file
from .models import SystemLog


logger = logging.getLogger("wei.worker")
SCAN_LOCK_ID = 928_884_215
_local_scan_lock = asyncio.Lock()


@asynccontextmanager
async def scan_lock() -> AsyncIterator[bool]:
    if engine.dialect.name != "postgresql":
        if _local_scan_lock.locked():
            yield False
            return
        async with _local_scan_lock:
            yield True
        return

    async with engine.connect() as connection:
        acquired = bool(await connection.scalar(text("SELECT pg_try_advisory_lock(:id)"), {"id": SCAN_LOCK_ID}))
        try:
            yield acquired
        finally:
            if acquired:
                await connection.execute(text("SELECT pg_advisory_unlock(:id)"), {"id": SCAN_LOCK_ID})


async def write_log(level: str, event: str, message: str, details: dict | None = None) -> None:
    async with SessionFactory() as session:
        session.add(
            SystemLog(
                level=level,
                component="scanner",
                event=event,
                message=message[:2000],
                details=details or {},
            )
        )
        await session.commit()


async def import_existing_data() -> dict[str, int]:
    async with SessionFactory() as session:
        signal_result = await import_legacy_history(session)
        markets = await import_market_file(session)
    return {**signal_result, "markets": markets}


async def run_scanner_job() -> None:
    async with scan_lock() as acquired:
        if not acquired:
            logger.info("Scanner skipped because another worker owns the lock")
            return
        started = datetime.now(timezone.utc)
        if not settings.run_scanner:
            await write_log("INFO", "scan_skipped", "Scanner is disabled by configuration")
            return

        env = os.environ.copy()
        env.setdefault("MARKET_DATA_PROVIDER", "mixed")
        env.setdefault("MAX_SEED_ROWS", "0")
        env.setdefault("MAX_ACTIVE_PER_SYMBOL_SIDE", "2")
        env.setdefault("MIN_QUOTE_VOLUME_USDT", "5000000")
        command = [sys.executable, str(ROOT / "scripts" / "update_seed_signals.py")]
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(ROOT),
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=settings.scanner_timeout_seconds)
            if process.returncode != 0:
                raise RuntimeError(stderr.decode("utf-8", errors="replace")[-1500:] or "Scanner returned non-zero status")
            result = await import_existing_data()
            elapsed = round((datetime.now(timezone.utc) - started).total_seconds(), 2)
            await write_log(
                "INFO",
                "scan_succeeded",
                "Scan completed and database was updated",
                {**result, "elapsed_seconds": elapsed, "output_tail": stdout.decode("utf-8", errors="replace")[-500:]},
            )
        except asyncio.TimeoutError:
            if "process" in locals():
                process.kill()
                await process.wait()
            await write_log("ERROR", "scan_failed", "Scanner timed out; previous database data was preserved")
        except Exception as exc:
            logger.exception("Scanner job failed")
            await write_log("ERROR", "scan_failed", "Scanner failed; previous database data was preserved", {"error": str(exc)[:1000]})

