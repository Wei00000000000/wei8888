from __future__ import annotations

from datetime import datetime
from math import ceil
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_session
from ..models import Signal
from ..schemas import PageMeta, SignalPage, SignalResponse
from ..security import User
from ..signal_scope import apply_signal_scope, signal_scope_filter


router = APIRouter(prefix="/signals", tags=["signals"])
Session = Annotated[AsyncSession, Depends(get_session)]


@router.get("/positions", response_model=list[SignalResponse])
async def active_positions(_user: User, session: Session, include_legacy: bool = False) -> list[SignalResponse]:
    active_status = func.lower(func.coalesce(Signal.raw_payload["status"].as_string(), "active")) == "active"
    query = select(Signal).where(
        Signal.official_trade.is_(True),
        or_(
            Signal.reached_state.in_(("holding", "active", "warning")),
            and_(Signal.reached_state.in_(("tp1", "tp2", "tp3")), active_status),
        ),
    )
    query = apply_signal_scope(query, include_legacy=include_legacy)
    rows = (
        await session.scalars(
            query.order_by(Signal.triggered_at.desc(), Signal.id.desc()).limit(500)
        )
    ).all()
    return [SignalResponse.model_validate(row) for row in rows]


@router.get("", response_model=SignalPage)
async def list_signals(
    _user: User,
    session: Session,
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    symbol: Annotated[str | None, Query(max_length=24)] = None,
    strategy: Annotated[str | None, Query(max_length=64)] = None,
    state: Annotated[str | None, Query(max_length=24)] = None,
    official_only: bool = False,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    include_legacy: bool = False,
) -> SignalPage:
    filters = []
    scope = signal_scope_filter(include_legacy=include_legacy)
    if scope is not None:
        filters.append(scope)
    if symbol:
        filters.append(Signal.symbol == symbol.upper().replace("USDT", ""))
    if strategy:
        if strategy == "high_quality":
            filters.append(
                or_(
                    Signal.strategy == "high_quality",
                    Signal.raw_payload["high_quality"].as_boolean().is_(True),
                )
            )
        else:
            filters.append(Signal.strategy == strategy)
    if state:
        filters.append(Signal.reached_state == state)
    if official_only:
        filters.append(Signal.official_trade.is_(True))
    if date_from:
        filters.append(Signal.triggered_at >= date_from)
    if date_to:
        filters.append(Signal.triggered_at <= date_to)

    total = int((await session.scalar(select(func.count(Signal.id)).where(*filters))) or 0)
    rows = (
        await session.scalars(
            select(Signal)
            .where(*filters)
            .order_by(Signal.triggered_at.desc(), Signal.id.desc())
            .offset((page - 1) * limit)
            .limit(limit)
        )
    ).all()
    return SignalPage(
        rows=[SignalResponse.model_validate(row) for row in rows],
        meta=PageMeta(page=page, limit=limit, total=total, pages=ceil(total / limit) if total else 0),
    )


@router.get("/{signal_id}", response_model=SignalResponse)
async def get_signal(signal_id: str, _user: User, session: Session) -> SignalResponse:
    signal = await session.get(Signal, signal_id)
    if signal is None:
        raise HTTPException(status_code=404, detail="Signal not found")
    return SignalResponse.model_validate(signal)

