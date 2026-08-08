from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import AsyncIterator

from sqlalchemy import select, text

from .config import settings
from .database import SessionFactory, engine
from .importer import ROOT, import_legacy_history, import_market_file
from .models import Signal, SystemLog
from .positions import sync_positions_from_signals


logger = logging.getLogger("wei.worker")
SCAN_LOCK_ID = 928_884_215
_local_scan_lock = asyncio.Lock()
ACTIVE_POSITION_STATES = ("holding", "active", "tp1", "tp2", "tp3")
SCAN_HEARTBEAT_SECONDS = 60
LOG_TAIL_CHARS = 2000


def _tail(text: str, limit: int = LOG_TAIL_CHARS) -> str:
    cleaned = text.strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[-limit:]


async def _communicate_with_heartbeat(
    process: asyncio.subprocess.Process,
    timeout: float,
    *,
    heartbeat_seconds: int = SCAN_HEARTBEAT_SECONDS,
) -> tuple[bytes, bytes]:
    """Wait for scanner subprocess while logging periodic progress for long runs."""
    started = time.monotonic()
    last_heartbeat = started

    async def _pump(stream: asyncio.StreamReader | None, chunks: list[bytes]) -> None:
        if stream is None:
            return
        while True:
            chunk = await stream.read(65536)
            if not chunk:
                break
            chunks.append(chunk)

    stdout_chunks: list[bytes] = []
    stderr_chunks: list[bytes] = []
    pump_tasks = [
        asyncio.create_task(_pump(process.stdout, stdout_chunks)),
        asyncio.create_task(_pump(process.stderr, stderr_chunks)),
    ]
    try:
        while True:
            if process.returncode is not None:
                break
            elapsed = time.monotonic() - started
            if elapsed >= timeout:
                raise asyncio.TimeoutError
            wait_for = min(heartbeat_seconds, timeout - elapsed, heartbeat_seconds - (time.monotonic() - last_heartbeat))
            wait_for = max(0.5, wait_for)
            try:
                await asyncio.wait_for(process.wait(), timeout=wait_for)
                break
            except asyncio.TimeoutError:
                if time.monotonic() - last_heartbeat >= heartbeat_seconds:
                    last_heartbeat = time.monotonic()
                    logger.info(
                        "Scanner subprocess still running (elapsed=%.0fs, pid=%s)",
                        time.monotonic() - started,
                        process.pid,
                    )
        await asyncio.gather(*pump_tasks)
        return b"".join(stdout_chunks), b"".join(stderr_chunks)
    finally:
        for task in pump_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*pump_tasks, return_exceptions=True)


def _json_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if isinstance(value, Decimal) else value


async def export_active_positions_to_seed() -> int:
    """Make database-only positions visible to the file-based state replayer."""
    seed_path = ROOT / "sentiment_scanner" / "seed_signals.json"
    try:
        payload = json.loads(seed_path.read_text(encoding="utf-8"))
        seed_rows = payload if isinstance(payload, list) else payload.get("rows", [])
    except (FileNotFoundError, json.JSONDecodeError, AttributeError):
        seed_rows = []

    by_id = {
        str(row.get("signal_id") or row.get("id")): row
        for row in seed_rows
        if isinstance(row, dict) and (row.get("signal_id") or row.get("id"))
    }
    async with SessionFactory() as session:
        positions = (
            await session.scalars(
                select(Signal).where(
                    Signal.official_trade.is_(True),
                    Signal.reached_state.in_(ACTIVE_POSITION_STATES),
                )
            )
        ).all()

    for signal in positions:
        row = dict(signal.raw_payload or {})
        row.update(
            {
                "id": signal.id,
                "signal_id": signal.id,
                "symbol": signal.symbol,
                "timeframe": signal.timeframe,
                "strategy": signal.strategy,
                "strategy_version": signal.strategy_version,
                "signal_type": row.get("signal_type") or ("reversal_bullish" if signal.side == "long" else "reversal_bearish"),
                "trade_layer": signal.trade_layer,
                "official_trade": True,
                "triggered_at": signal.triggered_at.isoformat(),
                "triggered_at_ms": int(signal.triggered_at.timestamp() * 1000),
                "entry_price": _json_value(signal.entry_price),
                "trigger_price": _json_value(signal.entry_price),
                "sl_price": _json_value(signal.sl_price),
                "tp1_price": _json_value(signal.tp1_price),
                "tp2_price": _json_value(signal.tp2_price),
                "tp3_price": _json_value(signal.tp3_price),
                "ftp_price": _json_value(signal.ftp_price),
                "current_price": _json_value(signal.current_price),
                "reached_state": signal.reached_state,
                "status": "active",
                "pnl_pct": signal.pnl_pct,
                "hit_at": _json_value(signal.hit_at),
                "max_gain_pct": signal.max_gain_pct,
                "max_drawdown_pct": signal.max_drawdown_pct,
                "quality_score": signal.quality_score,
                "radar_score": signal.radar_score,
            }
        )
        by_id[signal.id] = row

    seed_path.write_text(json.dumps(list(by_id.values()), ensure_ascii=False, indent=2), encoding="utf-8")
    return len(positions)


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
            logger.info(
                "Scanner skipped: RUN_SCANNER=false (APScheduler still fires every 5m; enable RUN_SCANNER=true to run update_seed_signals.py)"
            )
            await write_log("INFO", "scan_skipped", "Scanner is disabled by configuration")
            return

        env = os.environ.copy()
        env.setdefault("MARKET_DATA_PROVIDER", "binance")
        env.setdefault("MAX_SEED_ROWS", "0")
        env.setdefault("MAX_ACTIVE_PER_SYMBOL_SIDE", "2")
        env.setdefault("MIN_QUOTE_VOLUME_USDT", "5000000")
        env["PYTHONUNBUFFERED"] = "1"
        exported_positions = await export_active_positions_to_seed()
        command = [sys.executable, str(ROOT / "scripts" / "update_seed_signals.py")]
        api_trace = env.get("SCANNER_API_TRACE", "")
        logger.info(
            "Scanner subprocess starting (timeout=%ss, provider=%s, scan_workers=%s, api_trace=%s, exported_positions=%s)",
            settings.scanner_timeout_seconds,
            env.get("MARKET_DATA_PROVIDER"),
            env.get("SCAN_WORKERS", "(default)"),
            api_trace or "off",
            exported_positions,
        )
        process: asyncio.subprocess.Process | None = None
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(ROOT),
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            logger.info("Scanner subprocess pid=%s", process.pid)
            stdout, stderr = await _communicate_with_heartbeat(
                process,
                float(settings.scanner_timeout_seconds),
            )
            stdout_text = stdout.decode("utf-8", errors="replace")
            stderr_text = stderr.decode("utf-8", errors="replace")
            if process.returncode != 0:
                logger.error(
                    "Scanner subprocess failed (returncode=%s)\n%s",
                    process.returncode,
                    _tail(stderr_text or stdout_text),
                )
                raise RuntimeError(_tail(stderr_text or stdout_text, 1500) or "Scanner returned non-zero status")
            for line in stdout_text.splitlines():
                stripped = line.strip()
                if stripped and (
                    stripped.startswith(("symbols=", "contract_rows=", "WARN ", "ERROR "))
                    or "scan_ok" in stripped
                ):
                    logger.info("Scanner: %s", stripped)
            result = await import_existing_data()
            async with SessionFactory() as session:
                position_result = await sync_positions_from_signals(session, notify_new_since=started)
            persisted = False
            try:
                from sentiment_scanner.plugins.api_trace.store import persist_trace_file

                persisted = await persist_trace_file(ROOT / "sentiment_scanner" / "scanner_api_trace.json")
                if persisted:
                    logger.info("API trace snapshot persisted to database")
                elif api_trace.lower() in {"1", "true", "yes", "on"}:
                    logger.warning(
                        "API trace enabled but snapshot was not persisted (missing trace file or DB error)"
                    )
            except ImportError:
                pass
            except Exception as exc:
                logger.warning("API trace persist skipped: %s", exc)
            elapsed = round((datetime.now(timezone.utc) - started).total_seconds(), 2)
            logger.info(
                "Scanner completed successfully in %ss (import=%s, positions=%s, trace_persisted=%s)",
                elapsed,
                result,
                position_result,
                persisted,
            )
            await write_log(
                "INFO",
                "scan_succeeded",
                "Scan completed and database was updated",
                {
                    **result,
                    **position_result,
                    "exported_positions": exported_positions,
                    "elapsed_seconds": elapsed,
                    "output_tail": _tail(stdout_text, 500),
                },
            )
        except asyncio.TimeoutError:
            logger.error(
                "Scanner subprocess timed out after %ss (pid=%s)",
                settings.scanner_timeout_seconds,
                process.pid if process else None,
            )
            if process is not None:
                process.kill()
                await process.wait()
                stdout, stderr = await process.communicate()
                if stderr:
                    logger.error("Scanner stderr before kill:\n%s", _tail(stderr.decode("utf-8", errors="replace")))
                if stdout:
                    for line in stdout.decode("utf-8", errors="replace").splitlines():
                        if line.strip().startswith("API_TRACE "):
                            logger.info("Scanner: %s", line.strip())
            try:
                from sentiment_scanner.plugins.api_trace.store import persist_trace_file

                if await persist_trace_file(ROOT / "sentiment_scanner" / "scanner_api_trace.json"):
                    logger.info("API trace snapshot persisted after scanner timeout")
            except ImportError:
                pass
            except Exception as exc:
                logger.warning("API trace persist after timeout skipped: %s", exc)
            await write_log(
                "ERROR",
                "scan_failed",
                "Scanner timed out; previous database data was preserved",
                {"timeout_seconds": settings.scanner_timeout_seconds},
            )
        except Exception as exc:
            logger.exception("Scanner job failed")
            await write_log("ERROR", "scan_failed", "Scanner failed; previous database data was preserved", {"error": str(exc)[:1000]})

