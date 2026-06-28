from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_session
from ..models import Signal, SystemLog
from ..security import Admin, User


router = APIRouter(prefix="/system", tags=["system"])
Session = Annotated[AsyncSession, Depends(get_session)]


@router.get("/status")
async def status(_user: User, session: Session) -> dict[str, object]:
    await session.execute(text("SELECT 1"))
    last_scan = await session.scalar(
        select(SystemLog.created_at)
        .where(SystemLog.component == "scanner", SystemLog.event == "scan_succeeded")
        .order_by(SystemLog.created_at.desc())
        .limit(1)
    )
    total = int((await session.scalar(select(func.count(Signal.id)))) or 0)
    return {
        "status": "ok",
        "database": "ok",
        "total_signals": total,
        "last_successful_scan_at": last_scan,
        "server_time": datetime.now(timezone.utc),
    }


@router.get("/logs")
async def logs(_admin: Admin, session: Session, limit: int = 100) -> list[dict[str, object]]:
    rows = (await session.scalars(select(SystemLog).order_by(SystemLog.created_at.desc()).limit(min(limit, 200)))).all()
    return [
        {
            "level": row.level,
            "component": row.component,
            "event": row.event,
            "message": row.message,
            "details": row.details,
            "created_at": row.created_at,
        }
        for row in rows
    ]

