from __future__ import annotations

import json
from pathlib import Path
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
ROOT = Path(__file__).resolve().parents[3]
CONTRACT_ANOMALIES_FILE = ROOT / "sentiment_scanner" / "contract_anomalies.json"


def read_contract_anomalies() -> dict[str, object]:
    try:
        payload = json.loads(CONTRACT_ANOMALIES_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"rows": [], "updated_at": None}
    rows = payload.get("rows", []) if isinstance(payload, dict) else []
    return {
        "rows": rows if isinstance(rows, list) else [],
        "updated_at": payload.get("updated_at") if isinstance(payload, dict) else None,
    }


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


@router.get("/contract-anomalies")
async def contract_anomalies(_user: User) -> dict[str, object]:
    return read_contract_anomalies()
