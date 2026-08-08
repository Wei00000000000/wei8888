from __future__ import annotations

from datetime import datetime

from math import isfinite

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Signal


WIN_STATES = ("tp1", "tp2", "tp3", "ftp")
PARTIAL_STATES = ("tp1", "tp2", "tp3")
KNOWN_STRATEGIES = ("sentiment_oi", "stable_dog", "contract_anomaly", "high_quality")


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
    rows = (await session.scalars(filtered_signals(strategy, date_from, date_to))).all()
    return summarize_signals(rows, strategy)


async def all_strategy_summaries(
    session: AsyncSession,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> list[dict[str, float | int | str | None]]:
    rows = (await session.scalars(filtered_signals(None, date_from, date_to))).all()
    groups = {
        None: rows,
        "sentiment_oi": [row for row in rows if row.strategy == "sentiment_oi"],
        "stable_dog": [row for row in rows if row.strategy == "stable_dog" or stable_score(row) >= 88],
        "contract_anomaly": [row for row in rows if row.strategy == "contract_anomaly"],
        "high_quality": [row for row in rows if row.strategy == "high_quality" or is_high_quality(row)],
    }
    return [summarize_signals(groups[key], key) for key in (None, *KNOWN_STRATEGIES)]


def signal_quality(signal: Signal) -> int:
    if signal.quality_score is not None:
        return int(signal.quality_score)
    payload = signal.raw_payload or {}
    for key in ("quality_score", "score", "signal_score"):
        try:
            return int(float(payload.get(key)))
        except (TypeError, ValueError):
            continue
    return signal_score(signal)


def metric(signal: Signal, name: str) -> object:
    payload = signal.raw_payload or {}
    if payload.get(name) is not None:
        return payload[name]
    snapshot = payload.get("snapshot_data")
    if isinstance(snapshot, dict):
        return snapshot.get(name)
    return None


def number(value: object, default: float = 0.0) -> float:
    try:
        parsed = float(value)
        return parsed if isfinite(parsed) else default
    except (TypeError, ValueError):
        return default


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(value, high))


def signal_score(signal: Signal) -> int:
    direction = 1 if signal.side == "long" else -1
    oi_percentile = clamp(number(metric(signal, "oi_percentile")), 0, 100)
    oi_move = abs(number(metric(signal, "oi_change_pct")))
    price_move = number(metric(signal, "price_change_pct")) * direction
    taker_value = metric(signal, "taker_buy_ratio")
    taker_bias = (number(taker_value, 0.5) - 0.5) * 200 * direction if taker_value is not None else 0
    risk = risk_pct(signal)
    status_boost = 6 if is_open_position(signal) else 3 if signal.reached_state in WIN_STATES else 0
    score = (
        clamp(oi_percentile / 100 * 28, 0, 28)
        + clamp(oi_move * 7, 0, 18)
        + clamp(price_move * 8, 0, 18)
        + clamp(taker_bias / 2, 0, 12)
        + (clamp(14 - abs(risk - 3) * 2.4, 0, 14) if risk > 0 else 4)
        + (8 if metric(signal, "mtf_5m_confluence") else 0)
        + status_boost
    )
    return round(clamp(score, 0, 100))


def stable_score(signal: Signal) -> int:
    direction = 1 if signal.side == "long" else -1
    oi_percentile = clamp(number(metric(signal, "oi_percentile")), 0, 100)
    oi_move = abs(number(metric(signal, "oi_change_pct")))
    price_move = number(metric(signal, "price_change_pct")) * direction
    taker_value = metric(signal, "taker_buy_ratio")
    taker_bias = (number(taker_value, 0.5) - 0.5) * 200 * direction if taker_value is not None else 0
    risk = risk_pct(signal)
    status_boost = 8 if is_open_position(signal) else 4 if signal.reached_state in WIN_STATES else 0
    score = (
        clamp(oi_percentile / 100 * 24, 0, 24)
        + clamp(oi_move * 8, 0, 22)
        + clamp(price_move * 9, 0, 18)
        + clamp(taker_bias / 2, 0, 12)
        + (clamp(16 - abs(risk - 2.5) * 2.5, 0, 16) if risk > 0 else 5)
        + status_boost
    )
    return round(clamp(score, 0, 100))


def is_high_quality(signal: Signal) -> bool:
    base = signal_score(signal)
    volume = number(metric(signal, "volume_24h") or metric(signal, "quote_volume"))
    risk = risk_pct(signal)
    price = abs(number(metric(signal, "price_change_pct")))
    score = base
    if metric(signal, "mtf_5m_confluence") or metric(signal, "mtf_5m_oi_confluence"):
        score += 8
    if 0.5 <= risk <= 8:
        score += 6
    if price >= 0.5:
        score += 3
    if volume >= 5_000_000:
        score += 3
    return score >= 70 and volume >= 5_000_000 and risk >= 0.5


def is_open_position(signal: Signal) -> bool:
    state = str(signal.reached_state or "holding").lower()
    status = str((signal.raw_payload or {}).get("status") or "active").lower()
    return state in ("holding", "active", "warning") or (state in PARTIAL_STATES and status == "active")


def risk_pct(signal: Signal) -> float:
    if signal.entry_price is None or signal.sl_price is None:
        return 0.0
    entry = float(signal.entry_price)
    return abs(entry - float(signal.sl_price)) / entry * 100 if entry > 0 else 0.0


def staged_r(stage: str, raw_r: float | None = None) -> float:
    if stage == "sl":
        return -1.0
    if stage == "tp1":
        return 1.0 if raw_r is None else 0.3 + 0.7 * max(-1.0, min(raw_r, 5.0))
    if stage == "tp2":
        return 1.7 if raw_r is None else 0.9 + 0.4 * max(0.0, min(raw_r, 5.0))
    if stage == "tp3":
        return 2.3 if raw_r is None else 1.5 + 0.2 * max(1.0, min(raw_r, 5.0))
    if stage == "ftp":
        return 3.3
    return 0.0 if raw_r is None else max(-1.0, min(raw_r, 5.0))


def signal_pnl_pct(signal: Signal) -> float:
    recorded = float(signal.pnl_pct or 0.0)
    if isfinite(recorded) and abs(recorded) > 1e-12:
        return recorded
    risk = risk_pct(signal)
    if risk <= 0:
        return 0.0
    state = str(signal.reached_state or "holding").lower()
    payload = signal.raw_payload or {}
    max_state = str(payload.get("max_reached_state") or state).lower()
    if state == "sl":
        return risk * {"tp1": -0.4, "tp2": 0.9, "tp3": 1.5}.get(max_state, -1.0)
    if is_open_position(signal) and signal.entry_price is not None and signal.current_price is not None:
        entry = float(signal.entry_price)
        current = float(signal.current_price)
        raw_pct = (current - entry) / entry * 100 if signal.side == "long" else (entry - current) / entry * 100
        return staged_r(state, raw_pct / risk) * risk
    return staged_r(state) * risk


def summarize_signals(rows: list[Signal], strategy: str | None) -> dict[str, float | int | str | None]:
    ordered = sorted(rows, key=lambda row: (row.triggered_at, row.id))
    open_rows = [row for row in ordered if is_open_position(row)]
    losses = [row for row in ordered if row.reached_state == "sl"]
    wins = [row for row in ordered if row.reached_state in WIN_STATES and not is_open_position(row)]
    closed = len(wins) + len(losses)
    realized = sum(signal_pnl_pct(row) for row in ordered if not is_open_position(row))
    unrealized = sum(signal_pnl_pct(row) for row in open_rows)
    equity = peak = max_drawdown = 0.0
    for row in ordered:
        equity += signal_pnl_pct(row)
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)
    total = len(ordered)
    percentage = lambda value: round((value / total * 100), 2) if total else 0.0
    return {
        "strategy": strategy,
        "total_entries": total,
        "closed_entries": closed,
        "wins": len(wins),
        "losses": len(losses),
        "open_positions": len(open_rows),
        "win_rate_pct": round(len(wins) / closed * 100, 2) if closed else 0.0,
        "total_pnl_pct": round(realized + unrealized, 4),
        "realized_pnl_pct": round(realized, 4),
        "unrealized_pnl_pct": round(unrealized, 4),
        "tp1_rate_pct": percentage(sum(row.reached_state in WIN_STATES for row in ordered)),
        "tp2_rate_pct": percentage(sum(row.reached_state in ("tp2", "tp3", "ftp") for row in ordered)),
        "tp3_rate_pct": percentage(sum(row.reached_state in ("tp3", "ftp") for row in ordered)),
        "ftp_rate_pct": percentage(sum(row.reached_state == "ftp" for row in ordered)),
        "max_drawdown_pct": round(max_drawdown, 4),
    }

