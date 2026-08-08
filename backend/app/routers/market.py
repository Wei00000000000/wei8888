from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from time import monotonic
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_session
from ..models import MarketSnapshot
from ..schemas import MarketResponse
from ..security import User
from sentiment_scanner.market_data import market_client


router = APIRouter(prefix="/market", tags=["market"])
Session = Annotated[AsyncSession, Depends(get_session)]
ROOT = Path(__file__).resolve().parents[3]
CONTRACT_ANOMALIES_FILE = ROOT / "sentiment_scanner" / "contract_anomalies.json"
SECTOR_SYMBOLS = {
    "Layer 1": {"BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "AVAX", "TON", "TRX", "DOT", "NEAR", "SUI", "APT"},
    "Layer 2": {"ARB", "OP", "STRK", "ZK", "MANTA", "METIS", "IMX"},
    "DeFi": {"AAVE", "UNI", "LDO", "CRV", "COMP", "MKR", "SNX", "RUNE", "INJ", "PENDLE", "CAKE"},
    "AI": {"FET", "TAO", "WLD", "ARKM", "VIRTUAL", "RENDER", "RNDR", "AI"},
    "Meme": {"DOGE", "SHIB", "PEPE", "BONK", "FLOKI", "WIF", "MEME", "NEIRO"},
    "Gaming": {"SAND", "MANA", "GALA", "IMX", "RON", "PIXEL", "MAGIC", "AXS", "YGG"},
}
LIVE_TICKER_TTL_SECONDS = 60.0
_live_ticker_cache: tuple[float, list[MarketResponse]] = (0.0, [])


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


def sector_name(symbol: str) -> str:
    clean = symbol.upper().replace("USDT", "")
    for name, symbols in SECTOR_SYMBOLS.items():
        if clean in symbols:
            return name
    return "其他"


def build_volume_anomalies(rows: list[MarketResponse]) -> list[dict[str, object]]:
    result = []
    for row in rows:
        volume = float(row.quote_volume_24h or 0)
        change = float(row.change_24h_pct or 0)
        if volume < 5_000_000:
            continue
        result.append({
            "symbol": row.symbol,
            "price": float(row.price),
            "price_change_pct": change,
            "quote_volume": volume,
            "anomaly_score": math.log10(max(volume, 1)) * (abs(change) + 0.5),
            "sector": sector_name(row.symbol),
            "reason": "24H 成交量與價格波動異常",
        })
    return sorted(result, key=lambda item: float(item["anomaly_score"]), reverse=True)[:40]


def build_sector_flows(rows: list[MarketResponse]) -> list[dict[str, object]]:
    groups: dict[str, dict[str, float]] = {}
    for row in rows:
        sector = sector_name(row.symbol)
        if sector == "其他":
            continue
        volume = float(row.quote_volume_24h or 0)
        change = float(row.change_24h_pct or 0)
        group = groups.setdefault(sector, {"volume": 0.0, "weighted_change": 0.0})
        group["volume"] += volume
        group["weighted_change"] += change * volume
    result = []
    for name, group in groups.items():
        volume = group["volume"]
        result.append({
            "name": name,
            "source": "Zeabur 即時行情",
            "market_cap": 0,
            "volume_24h": volume,
            "market_cap_change_24h": group["weighted_change"] / volume if volume else 0,
        })
    return sorted(result, key=lambda item: float(item["volume_24h"]), reverse=True)


async def fetch_live_tickers() -> list[MarketResponse]:
    global _live_ticker_cache
    cached_at, cached_rows = _live_ticker_cache
    if cached_rows and monotonic() - cached_at < LIVE_TICKER_TTL_SECONDS:
        return cached_rows
    async with market_client(timeout=12) as client:
        payload = await client.ticker_24hr()
    observed_at = datetime.now(timezone.utc)
    rows = []
    for item in payload if isinstance(payload, list) else []:
        try:
            price = Decimal(str(item.get("lastPrice") or item.get("price") or 0))
            if price <= 0:
                continue
            rows.append(MarketResponse(
                symbol=str(item.get("symbol") or "").upper().replace("USDT", ""),
                source="binance-live",
                price=price,
                change_24h_pct=float(item.get("priceChangePercent") or item.get("price_change_pct") or 0),
                quote_volume_24h=float(item.get("quoteVolume") or item.get("quote_volume") or 0),
                observed_at=observed_at,
            ))
        except (TypeError, ValueError, ArithmeticError):
            continue
    if rows:
        _live_ticker_cache = (monotonic(), rows)
    return rows


async def dashboard_market_rows(_user: User, session: Session) -> list[MarketResponse]:
    try:
        live_rows = await fetch_live_tickers()
        if any(float(row.quote_volume_24h or 0) > 0 for row in live_rows):
            return live_rows
    except Exception:
        pass
    return await latest_market(_user, session, symbols=None, limit=500)


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


@router.get("/volume-anomalies")
async def volume_anomalies(_user: User, session: Session) -> dict[str, object]:
    rows = await dashboard_market_rows(_user, session)
    return {"rows": build_volume_anomalies(rows), "updated_at": max((row.observed_at for row in rows), default=None)}


@router.get("/sector-flows")
async def sector_flows(_user: User, session: Session) -> dict[str, object]:
    rows = await dashboard_market_rows(_user, session)
    return {"rows": build_sector_flows(rows), "updated_at": max((row.observed_at for row in rows), default=None)}
