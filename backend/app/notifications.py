from __future__ import annotations

from datetime import datetime, timezone

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import settings
from .models import NotificationLog, Position


def notification_dedupe_key(position: Position, message_type: str) -> str:
    return f"{position.symbol}:{position.side}:{position.timeframe}:{position.signal_id}:{message_type}"


def position_message(position: Position, message_type: str = "NEW_POSITION") -> str:
    direction = "LONG" if position.side == "long" else "SHORT"
    lines = [
        "Wei 策略情報室",
        f"類型：{message_type}",
        f"幣種：{position.symbol}USDT",
        f"方向：{direction}",
        f"週期：{position.timeframe}",
        f"策略：{position.strategy_name}",
        f"Entry：{position.entry_price}",
        f"SL：{position.stop_loss}",
        f"TP1/TP2/TP3/FTP：{position.take_profit_1} / {position.take_profit_2} / {position.take_profit_3} / {position.take_profit_final}",
        f"Score：{position.score if position.score is not None else '-'}",
        f"時間：{position.entry_time.astimezone(timezone.utc).isoformat()}",
        settings.public_site_url,
    ]
    return "\n".join(lines)


async def create_notification_log(
    session: AsyncSession,
    position: Position,
    message_type: str,
    message_text: str | None = None,
    status: str = "pending",
    error_message: str | None = None,
) -> NotificationLog:
    dedupe_key = notification_dedupe_key(position, message_type)
    existing = await session.scalar(select(NotificationLog).where(NotificationLog.dedupe_key == dedupe_key))
    if existing is not None:
        return existing
    log = NotificationLog(
        signal_id=position.signal_id,
        position_id=position.id,
        symbol=position.symbol,
        side=position.side,
        timeframe=position.timeframe,
        message_type=message_type,
        message_text=message_text or position_message(position, message_type),
        telegram_chat_id=settings.telegram_chat_id or None,
        sent_status=status,
        error_message=error_message,
        dedupe_key=dedupe_key,
    )
    session.add(log)
    await session.flush()
    return log


async def send_telegram_for_position(session: AsyncSession, position: Position, message_type: str = "NEW_POSITION") -> NotificationLog:
    log = await create_notification_log(session, position, message_type)
    if log.sent_status == "sent":
        return log
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        log.sent_status = "skipped"
        log.error_message = "Telegram environment variables are not configured"
        await session.flush()
        return log

    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                url,
                json={
                    "chat_id": settings.telegram_chat_id,
                    "text": log.message_text,
                    "disable_web_page_preview": True,
                },
            )
        if response.status_code >= 400:
            raise RuntimeError(f"Telegram returned HTTP {response.status_code}")
        log.sent_status = "sent"
        log.sent_at = datetime.now(timezone.utc)
        log.error_message = None
    except Exception as exc:
        log.sent_status = "failed"
        log.error_message = str(exc)[:1000]
    await session.flush()
    return log
