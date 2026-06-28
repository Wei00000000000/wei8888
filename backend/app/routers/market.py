from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_session
from ..models import MarketSnapshot
from ..schemas import MarketResponse
from ..security import User


router = APIRouter(prefix="/market", tags=["market"])
Session = Annotated[AsyncSession, Depends(get_session)]


@router.get("", response_model=list[MarketResponse])
async def latest_market(
    _user: User,
    session: Session,
    symbols: Annotated[str | None, Query(max_length=1000)] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 200,
) -> list[MarketResponse]:
    latest = (
        select(MarketSnapshot.symbol, func.max(MarketSnapshot.observed_at).label("latest_at"))
        .group_by(MarketSnapshot.symbol)
        .subquery()
    )
    query = select(MarketSnapshot).join(
        latest,
        and_(MarketSnapshot.symbol == latest.c.symbol, MarketSnapshot.observed_at == latest.c.latest_at),
    )
    if symbols:
        requested = [item.strip().upper().replace("USDT", "") for item in symbols.split(",") if item.strip()]
        query = query.where(MarketSnapshot.symbol.in_(requested[:100]))
    rows = (await session.scalars(query.order_by(MarketSnapshot.quote_volume_24h.desc()).limit(limit))).all()
    return [MarketResponse.model_validate(row) for row in rows]


@router.get("/watchlist", response_model=list[MarketResponse])
async def watchlist(_user: User, session: Session) -> list[MarketResponse]:
    return await latest_market(_user, session, symbols=None, limit=20)
