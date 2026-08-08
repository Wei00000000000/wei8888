from __future__ import annotations

import csv
from datetime import datetime
from io import StringIO
from math import ceil
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ..database import get_session
from ..models import Position, Signal
from ..positions import position_from_signal
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


def signal_filters(
    symbol: str | None = None,
    side: str | None = None,
    timeframe: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> list:
    filters = [Signal.official_trade.is_(True), Signal.entry_price.is_not(None)]
    if symbol:
        filters.append(Signal.symbol == symbol.upper().replace("USDT", ""))
    if side:
        filters.append(Signal.side == side.lower())
    if timeframe:
        filters.append(Signal.timeframe == timeframe.upper())
    if date_from:
        filters.append(Signal.triggered_at >= date_from)
    if date_to:
        filters.append(Signal.triggered_at <= date_to)
    return filters


async def merged_position_rows(
    session: AsyncSession,
    *,
    symbol: str | None = None,
    side: str | None = None,
    timeframe: str | None = None,
    status: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> list[Position]:
    """Return persisted positions plus official signals projected as positions.

    The position table is the source of event timelines. When a sync/import gap leaves
    it empty or incomplete, official locked signals still represent real trade records,
    so the history page should not go blank.
    """
    persisted = (
        await session.scalars(
            select(Position).where(*position_filters(symbol, side, timeframe, status, date_from, date_to))
        )
    ).all()
    rows_by_signal = {row.signal_id: row for row in persisted}
    rows_by_id = {row.id: row for row in persisted if not row.signal_id}

    signals = (await session.scalars(select(Signal).where(*signal_filters(symbol, side, timeframe, date_from, date_to)))).all()
    wanted_status = status.upper() if status else None
    for signal in signals:
        if signal.id in rows_by_signal:
            continue
        row = position_from_signal(signal)
        row.created_at = signal.created_at
        row.updated_at = signal.updated_at
        if wanted_status and row.status != wanted_status:
            continue
        rows_by_signal[signal.id] = row

    return [*rows_by_id.values(), *rows_by_signal.values()]


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
    rows = await merged_position_rows(
        session,
        symbol=symbol,
        side=side,
        timeframe=timeframe,
        status=status,
        date_from=date_from,
        date_to=date_to,
    )
    reverse = sort != "asc"
    rows.sort(key=lambda row: (row.entry_time, row.id), reverse=reverse)
    total = len(rows)
    start = (page - 1) * limit
    rows = rows[start : start + limit]
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
    rows = await merged_position_rows(
        session,
        symbol=symbol,
        side=side,
        timeframe=timeframe,
        status=status,
        date_from=date_from,
        date_to=date_to,
    )
    rows.sort(key=lambda row: (row.entry_time, row.id), reverse=True)
    rows = rows[:5000]
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
