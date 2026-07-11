from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .config import settings
from .models import NotificationLog, Position


def notification_dedupe_key(position: Position, message_type: str) -> str:
    return f"{position.symbol}:{position.side}:{position.timeframe}:{position.signal_id}:{message_type}"


def format_price(value: Decimal | None) -> str:
    if value is None:
        return "-"
    number = float(value)
    decimals = 8 if 0 < abs(number) < 0.0001 else 5
    text = f"{number:.{decimals}f}"
    whole, fraction = text.split(".")
    minimum = 5 if decimals == 8 else 4
    return f"{whole}.{fraction.rstrip('0').ljust(minimum, '0')}"


def format_taipei_time(value: datetime) -> str:
    local = value.astimezone(ZoneInfo("Asia/Taipei"))
    period = "上午" if local.hour < 12 else "下午"
    hour = local.hour % 12 or 12
    return f"{local:%Y/%m/%d} {period}{hour}:{local:%M}"


def position_message(position: Position, message_type: str = "NEW_POSITION") -> str:
    direction = "做多" if position.side == "long" else "做空"
    score = position.score if position.score is not None else "-"
    lines = [
        "Wei 策略情報室",
        f"類型：{message_type}",
        f"幣種：{position.symbol}USDT",
        f"方向：{direction}",
        f"週期：{position.timeframe}",
        f"策略：{position.strategy_name}",
        f"分數：{score}",
        f"成立時間：{format_taipei_time(position.entry_time)}",
        f"Entry：{format_price(position.entry_price)}",
        f"SL：{format_price(position.stop_loss)}",
        "TP1/TP2/TP3/FTP："
        f"{format_price(position.take_profit_1)} / {format_price(position.take_profit_2)} / "
        f"{format_price(position.take_profit_3)} / {format_price(position.take_profit_final)}",
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
