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
from ..quant import QuantConfig, run_quant_v1
from ..quant.research import research, parameter_sweep, walk_forward
from ..quant.agent import autonomous_research
from ..quant.universe_agent import universe_research
from sentiment_scanner.bingx import BingxFuturesClient


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



@router.get("/quant-v1")
async def quant_v1(
    _user: User,
    symbol: str = "BTCUSDT",
    timeframe: str = "15m",
    limit: Annotated[int, Query(ge=250, le=1000)] = 1000,
) -> dict:
    """Run the first deterministic quant strategy on recent public futures candles."""
    clean_symbol = symbol.upper().replace("/", "").replace("-", "")
    if not clean_symbol.isalnum() or len(clean_symbol) > 24:
        return {"detail": "Invalid symbol"}
    allowed = {"5m", "15m", "1h", "4h", "1d"}
    tf = timeframe.lower()
    if tf not in allowed:
        return {"detail": "Unsupported timeframe"}
    client = BingxFuturesClient(timeout=20.0)
    try:
        rows = client.klines(clean_symbol, interval=tf, limit=limit)
    finally:
        client.close()
    result = run_quant_v1(rows, QuantConfig())
    result["symbol"] = clean_symbol
    result["timeframe"] = tf
    result["bars"] = len(rows)
    return result


@router.get("/quant-v2/research")
async def quant_v2_research(
    _user: User,
    symbol: str = "BTCUSDT",
    timeframe: str = "15m",
    limit: Annotated[int, Query(ge=400, le=1000)] = 1000,
    optimize: bool = False,
) -> dict:
    clean_symbol = symbol.upper().replace("/", "").replace("-", "")
    if not clean_symbol.isalnum() or len(clean_symbol) > 24:
        return {"detail": "Invalid symbol"}
    tf = timeframe.lower()
    if tf not in {"5m", "15m", "1h", "4h", "1d"}:
        return {"detail": "Unsupported timeframe"}
    client = BingxFuturesClient(timeout=20.0)
    try:
        rows = client.klines(clean_symbol, interval=tf, limit=limit)
    finally:
        client.close()
    result = research(rows)
    result["symbol"], result["timeframe"], result["bars"] = clean_symbol, tf, len(rows)
    if optimize:
        result["parameter_sweep"] = parameter_sweep(rows)
        result["walk_forward"] = walk_forward(rows)
    return result


@router.get("/quant-v2/universe")
async def quant_v2_universe(
    _user: User,
    symbols: str = "",
    timeframe: str = "15m",
    limit: Annotated[int, Query(ge=400, le=1000)] = 750,
    max_symbols: Annotated[int, Query(ge=10, le=200)] = 80,
) -> dict:
    """Cross-sectional research over a liquid USDT perpetual universe.

    When symbols is empty the universe is discovered from BingX and ranked by
    24h quote volume. max_symbols bounds request cost; explicit symbols are
    still supported for reproducible research.
    """
    tf = timeframe.lower()
    if tf not in {"5m", "15m", "1h", "4h", "1d"}:
        return {"detail": "Unsupported timeframe"}
    universe = []
    client = BingxFuturesClient(timeout=20.0)
    try:
        requested = [x.strip() for x in symbols.split(",") if x.strip()]
        symbol_list = requested[:max_symbols] if requested else client.symbols_by_volume(limit=max_symbols)
        for raw in symbol_list:
            symbol = raw.upper().replace("/", "").replace("-", "")
            if not symbol.isalnum() or len(symbol) > 24:
                continue
            try:
                rows = client.klines(symbol, interval=tf, limit=limit)
                result = research(rows)
                s, m, b = result.get("summary", {}), result.get("risk_metrics", {}), result.get("benchmark", {})
                universe.append({
                    "symbol": symbol, "bars": len(rows), "trades": s.get("trades", 0),
                    "win_rate_pct": s.get("win_rate_pct", 0), "profit_factor": s.get("profit_factor", 0),
                    "expectancy_r": s.get("expectancy_r", 0), "net_r": s.get("net_r", 0),
                    "max_drawdown_r": s.get("max_drawdown_r", 0), "sharpe_r": m.get("sharpe_r", 0),
                    "sortino_r": m.get("sortino_r", 0), "calmar_r": m.get("calmar_r", 0),
                    "alpha_trade_pct": b.get("alpha_trade_pct", 0), "buy_hold_pct": b.get("buy_hold_pct", 0),
                    "avg_mfe_r": s.get("avg_mfe_r", 0), "avg_mae_r": s.get("avg_mae_r", 0),
                })
            except Exception as exc:
                universe.append({"symbol": symbol, "error": str(exc)[:160]})
    finally:
        client.close()
    valid = [x for x in universe if "error" not in x]
    valid.sort(key=lambda x: (x.get("expectancy_r", 0), x.get("sharpe_r", 0)), reverse=True)
    return {"strategy": "quant-v2-research", "timeframe": tf, "symbols": len(universe),
            "successful": len(valid), "rows": valid, "errors": [x for x in universe if "error" in x]}


@router.get("/quant-v3/agent")
async def quant_v3_agent(
    _user: User,
    symbol: str = "BTCUSDT",
    timeframe: str = "15m",
    limit: Annotated[int, Query(ge=500, le=1000)] = 1000,
) -> dict:
    """Run one bounded autonomous diagnosis -> hypothesis -> OOS experiment cycle."""
    clean_symbol = symbol.upper().replace("/", "").replace("-", "")
    if not clean_symbol.isalnum() or len(clean_symbol) > 24:
        return {"detail": "Invalid symbol"}
    tf = timeframe.lower()
    if tf not in {"5m", "15m", "1h", "4h", "1d"}:
        return {"detail": "Unsupported timeframe"}
    client = BingxFuturesClient(timeout=20.0)
    try:
        rows = client.klines(clean_symbol, interval=tf, limit=limit)
    finally:
        client.close()
    result = autonomous_research(rows)
    result["symbol"], result["timeframe"], result["bars"] = clean_symbol, tf, len(rows)
    return result


@router.get("/quant-v4/universe-agent")
async def quant_v4_universe_agent(
    _user: User,
    timeframe: str = "15m",
    limit: Annotated[int, Query(ge=500, le=1000)] = 1000,
    max_symbols: Annotated[int, Query(ge=5, le=120)] = 30,
    min_quote_volume: float = 0.0,
) -> dict:
    """Run autonomous research across a liquid USDT perpetual universe."""
    tf = timeframe.lower()
    if tf not in {"5m", "15m", "1h", "4h", "1d"}:
        return {"detail": "Unsupported timeframe"}
    client = BingxFuturesClient(timeout=20.0)
    datasets = {}
    errors = []
    try:
        symbols = client.symbols_by_volume(limit=max_symbols, min_quote_volume=min_quote_volume)
        for symbol in symbols:
            try:
                rows = client.klines(symbol, interval=tf, limit=limit)
                if len(rows) >= 500:
                    datasets[symbol] = rows
                else:
                    errors.append({"symbol": symbol, "error": "insufficient_history", "bars": len(rows)})
            except Exception as exc:
                errors.append({"symbol": symbol, "error": str(exc)[:160]})
    finally:
        client.close()
    result = universe_research(datasets)
    result["timeframe"], result["bars_per_symbol"] = tf, limit
    result["data_errors"] = errors
    return result
