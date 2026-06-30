from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_session
from ..models import NotificationLog, Position
from ..notifications import create_notification_log, send_telegram_for_position
from ..schemas import NotificationLogResponse, NotificationTestRequest
from ..security import Admin, User, require_csrf


router = APIRouter(prefix="/notifications", tags=["notifications"])
Session = Annotated[AsyncSession, Depends(get_session)]


@router.get("/logs", response_model=list[NotificationLogResponse])
async def notification_logs(
    _user: User,
    session: Session,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> list[NotificationLogResponse]:
    rows = (
        await session.scalars(
            select(NotificationLog).order_by(NotificationLog.created_at.desc(), NotificationLog.id.desc()).limit(limit)
        )
    ).all()
    return [NotificationLogResponse.model_validate(row) for row in rows]


@router.post("/test", response_model=NotificationLogResponse, dependencies=[Depends(require_csrf)])
async def test_notification(payload: NotificationTestRequest, _admin: Admin, session: Session) -> NotificationLogResponse:
    now = datetime.now(timezone.utc)
    symbol = payload.symbol.upper().replace("USDT", "")
    synthetic = Position(
        id=f"test-{int(now.timestamp())}",
        signal_id=f"test-{int(now.timestamp())}",
        symbol=symbol,
        side=payload.side,
        timeframe=payload.timeframe.upper(),
        strategy_name="notification_test",
        status="OPEN",
        entry_time=now,
        entry_reason=payload.message,
    )
    log = await create_notification_log(
        session,
        synthetic,
        "TEST",
        message_text=f"{payload.message}\n{symbol}USDT {payload.side.upper()} {payload.timeframe.upper()}",
    )
    if log.sent_status != "sent":
        if await session.scalar(select(func.count(NotificationLog.id)).where(NotificationLog.id == log.id)):
            await send_telegram_for_position(session, synthetic, "TEST")
    await session.commit()
    await session.refresh(log)
    return NotificationLogResponse.model_validate(log)
