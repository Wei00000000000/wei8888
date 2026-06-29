from __future__ import annotations

from datetime import datetime
from math import ceil
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_session
from ..models import BacktestRun, Signal
from ..schemas import BacktestRequest, BacktestRunResponse, BacktestSummary, PageMeta, SignalPage, SignalResponse
from ..security import Admin, User, require_csrf
from ..stats import all_strategy_summaries, backtest_summary, filtered_signals


router = APIRouter(prefix="/backtest", tags=["backtest"])
Session = Annotated[AsyncSession, Depends(get_session)]


@router.get("/summary", response_model=BacktestSummary)
async def summary(
    _user: User,
    session: Session,
    strategy: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> BacktestSummary:
    return BacktestSummary(**await backtest_summary(session, strategy, date_from, date_to))


@router.get("/summaries", response_model=list[BacktestSummary])
async def summaries(
    _user: User,
    session: Session,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> list[BacktestSummary]:
    return [BacktestSummary(**row) for row in await all_strategy_summaries(session, date_from, date_to)]


@router.get("/trades", response_model=SignalPage)
async def trades(
    _user: User,
    session: Session,
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    strategy: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> SignalPage:
    base = filtered_signals(strategy, date_from, date_to)
    count_query = base.with_only_columns(func.count(Signal.id)).order_by(None)
    total = int((await session.scalar(count_query)) or 0)
    rows = (
        await session.scalars(
            base.order_by(Signal.triggered_at.desc()).offset((page - 1) * limit).limit(limit)
        )
    ).all()
    return SignalPage(
        rows=[SignalResponse.model_validate(row) for row in rows],
        meta=PageMeta(page=page, limit=limit, total=total, pages=ceil(total / limit) if total else 0),
    )


@router.post("/run", response_model=BacktestRunResponse, dependencies=[Depends(require_csrf)])
async def run_backtest(payload: BacktestRequest, admin: Admin, session: Session) -> BacktestRunResponse:
    run = BacktestRun(
        requested_by=admin.subject,
        strategy=payload.strategy_id,
        strategy_version="current",
        status="queued",
        parameters=payload.model_dump(mode="json"),
    )
    session.add(run)
    await session.commit()
    return BacktestRunResponse(run_id=run.id, status=run.status)

