from __future__ import annotations

from datetime import datetime

from sqlalchemy import Select, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Signal


WIN_STATES = ("tp1", "tp2", "tp3", "ftp")


def filtered_signals(strategy: str | None, date_from: datetime | None, date_to: datetime | None) -> Select:
    query = select(Signal).where(Signal.official_trade.is_(True))
    if strategy:
        query = query.where(Signal.strategy == strategy)
    if date_from:
        query = query.where(Signal.triggered_at >= date_from)
    if date_to:
        query = query.where(Signal.triggered_at <= date_to)
    return query


async def backtest_summary(
    session: AsyncSession,
    strategy: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> dict[str, float | int | str | None]:
    base = filtered_signals(strategy, date_from, date_to).subquery()
    query = select(
        func.count(base.c.id).label("total"),
        func.sum(case((base.c.reached_state.in_(WIN_STATES), 1), else_=0)).label("wins"),
        func.sum(case((base.c.reached_state == "sl", 1), else_=0)).label("losses"),
        func.sum(case((base.c.reached_state.in_(("holding", "active", "warning")), 1), else_=0)).label("open_positions"),
        func.coalesce(func.sum(base.c.pnl_pct), 0.0).label("total_pnl"),
        func.coalesce(func.min(base.c.max_drawdown_pct), 0.0).label("max_drawdown"),
        *[
            func.sum(case((base.c.reached_state.in_(states), 1), else_=0)).label(label)
            for label, states in (
                ("tp1", ("tp1", "tp2", "tp3", "ftp")),
                ("tp2", ("tp2", "tp3", "ftp")),
                ("tp3", ("tp3", "ftp")),
                ("ftp", ("ftp",)),
            )
        ],
    )
    row = (await session.execute(query)).one()
    total = int(row.total or 0)
    percentage = lambda value: round((int(value or 0) / total * 100), 2) if total else 0.0
    return {
        "strategy": strategy,
        "total_entries": total,
        "wins": int(row.wins or 0),
        "losses": int(row.losses or 0),
        "open_positions": int(row.open_positions or 0),
        "win_rate_pct": percentage(row.wins),
        "total_pnl_pct": round(float(row.total_pnl or 0), 4),
        "tp1_rate_pct": percentage(row.tp1),
        "tp2_rate_pct": percentage(row.tp2),
        "tp3_rate_pct": percentage(row.tp3),
        "ftp_rate_pct": percentage(row.ftp),
        "max_drawdown_pct": round(float(row.max_drawdown or 0), 4),
    }

