from __future__ import annotations

import csv
from datetime import datetime
from io import StringIO
from math import ceil
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ..database import get_session
from ..models import Position
from ..schemas import PageMeta, PositionDetail, PositionPage, PositionResponse
from ..security import User


router = APIRouter(prefix="/positions", tags=["positions"])
Session = Annotated[AsyncSession, Depends(get_session)]


def position_filters(
    symbol: str | None = None,
    side: str | None = None,
    timeframe: str | None = None,
    status: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> list:
    filters = []
    if symbol:
        filters.append(Position.symbol == symbol.upper().replace("USDT", ""))
    if side:
        filters.append(Position.side == side.lower())
    if timeframe:
        filters.append(Position.timeframe == timeframe.upper())
    if status:
        filters.append(Position.status == status.upper())
    if date_from:
        filters.append(Position.entry_time >= date_from)
    if date_to:
        filters.append(Position.entry_time <= date_to)
    return filters


@router.get("/open", response_model=list[PositionResponse])
async def open_positions(_user: User, session: Session) -> list[PositionResponse]:
    rows = (
        await session.scalars(
            select(Position)
            .where(Position.status == "OPEN")
            .order_by(Position.entry_time.desc(), Position.id.desc())
            .limit(500)
        )
    ).all()
    return [PositionResponse.model_validate(row) for row in rows]


@router.get("/history", response_model=PositionPage)
async def position_history(
    _user: User,
    session: Session,
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    symbol: Annotated[str | None, Query(max_length=24)] = None,
    side: Annotated[str | None, Query(max_length=8)] = None,
    timeframe: Annotated[str | None, Query(max_length=12)] = None,
    status: Annotated[str | None, Query(max_length=24)] = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    sort: Annotated[str, Query(pattern="^(asc|desc)$")] = "desc",
) -> PositionPage:
    filters = position_filters(symbol, side, timeframe, status, date_from, date_to)
    total = int((await session.scalar(select(func.count(Position.id)).where(*filters))) or 0)
    order = Position.entry_time.asc() if sort == "asc" else Position.entry_time.desc()
    rows = (
        await session.scalars(
            select(Position)
            .where(*filters)
            .order_by(order, Position.id.desc())
            .offset((page - 1) * limit)
            .limit(limit)
        )
    ).all()
    return PositionPage(
        rows=[PositionResponse.model_validate(row) for row in rows],
        meta=PageMeta(page=page, limit=limit, total=total, pages=ceil(total / limit) if total else 0),
    )


@router.get("/export.csv")
async def export_positions_csv(
    _user: User,
    session: Session,
    symbol: Annotated[str | None, Query(max_length=24)] = None,
    side: Annotated[str | None, Query(max_length=8)] = None,
    timeframe: Annotated[str | None, Query(max_length=12)] = None,
    status: Annotated[str | None, Query(max_length=24)] = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> Response:
    filters = position_filters(symbol, side, timeframe, status, date_from, date_to)
    rows = (
        await session.scalars(
            select(Position).where(*filters).order_by(Position.entry_time.desc(), Position.id.desc()).limit(5000)
        )
    ).all()
    output = StringIO()
    writer = csv.writer(output)
    headers = [
        "symbol",
        "side",
        "timeframe",
        "strategy_name",
        "status",
        "entry_price",
        "stop_loss",
        "take_profit_1",
        "take_profit_2",
        "take_profit_3",
        "take_profit_final",
        "exit_price",
        "pnl_percent",
        "rr",
        "entry_time",
        "exit_time",
        "entry_reason",
        "exit_reason",
        "score",
        "signal_id",
    ]
    writer.writerow(headers)
    for row in rows:
        writer.writerow([getattr(row, key) for key in headers])
    return Response(
        output.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=positions.csv"},
    )


@router.get("/{position_id}", response_model=PositionDetail)
async def position_detail(position_id: str, _user: User, session: Session) -> PositionDetail:
    position = await session.scalar(
        select(Position)
        .where(Position.id == position_id)
        .options(selectinload(Position.events), selectinload(Position.signal))
    )
    if position is None:
        raise HTTPException(status_code=404, detail="Position not found")
    data = PositionDetail.model_validate(position).model_dump()
    data["events"] = sorted(data["events"], key=lambda item: item["event_time"])
    return PositionDetail(**data)
