from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Position, PositionEvent, Signal
from .notifications import send_telegram_for_position


OPEN_STATES = {"holding", "active", "tp1", "tp2", "tp3"}
TP_EVENTS = {"tp1": "TP1_HIT", "tp2": "TP2_HIT", "tp3": "TP3_HIT", "ftp": "FTP_HIT"}


def signal_position_status(signal: Signal) -> str:
    state = (signal.reached_state or "").lower()
    raw_status = str((signal.raw_payload or {}).get("status") or "").lower()
    if state in {"invalid", "invalidated"}:
        return "INVALIDATED"
    if state in {"expired"}:
        return "EXPIRED"
    if state in {"cancelled", "canceled"}:
        return "CANCELLED"
    if state in {"sl", "ftp"}:
        return "CLOSED"
    if state in {"tp1", "tp2", "tp3"} and raw_status != "closed":
        return "OPEN"
    if state in OPEN_STATES:
        return "OPEN"
    if raw_status == "closed":
        return "CLOSED"
    return "OPEN"


def exit_price_for_signal(signal: Signal) -> Decimal | None:
    state = (signal.reached_state or "").lower()
    if state == "sl":
        return signal.sl_price
    if state == "tp1":
        return signal.tp1_price
    if state == "tp2":
        return signal.tp2_price
    if state == "tp3":
        return signal.tp3_price
    if state == "ftp":
        return signal.ftp_price
    return signal.current_price


def pnl_percent_for_signal(signal: Signal) -> float | None:
    entry = float(signal.entry_price or 0)
    exit_price = float(exit_price_for_signal(signal) or 0)
    if entry <= 0 or exit_price <= 0:
        return signal.pnl_pct
    pct = (exit_price - entry) / entry * 100
    return pct if signal.side == "long" else -pct


def rr_for_signal(signal: Signal) -> float | None:
    entry = float(signal.entry_price or 0)
    sl = float(signal.sl_price or 0)
    pnl = pnl_percent_for_signal(signal)
    if entry <= 0 or sl <= 0 or pnl is None:
        return None
    risk = abs(entry - sl) / entry * 100
    return pnl / risk if risk else None


def event_price_for_state(signal: Signal, state: str) -> Decimal | None:
    if state == "tp1":
        return signal.tp1_price
    if state == "tp2":
        return signal.tp2_price
    if state == "tp3":
        return signal.tp3_price
    if state == "ftp":
        return signal.ftp_price
    if state == "sl":
        return signal.sl_price
    return signal.current_price


async def add_position_event_once(
    session: AsyncSession,
    position: Position,
    event_type: str,
    event_time: datetime,
    event_price: Decimal | None,
    description: str,
) -> None:
    existing = await session.scalar(
        select(PositionEvent).where(
            PositionEvent.position_id == position.id,
            PositionEvent.event_type == event_type,
            PositionEvent.event_time == event_time,
        )
    )
    if existing is not None:
        return
    session.add(
        PositionEvent(
            position_id=position.id,
            event_type=event_type,
            event_price=event_price,
            event_time=event_time,
            description=description,
        )
    )


def position_from_signal(signal: Signal) -> Position:
    status = signal_position_status(signal)
    return Position(
        id=signal.id,
        signal_id=signal.id,
        symbol=signal.symbol,
        side=signal.side,
        timeframe=signal.timeframe,
        strategy_name=signal.strategy,
        status=status,
        entry_price=signal.entry_price,
        stop_loss=signal.sl_price,
        take_profit_1=signal.tp1_price,
        take_profit_2=signal.tp2_price,
        take_profit_3=signal.tp3_price,
        take_profit_final=signal.ftp_price,
        exit_price=exit_price_for_signal(signal) if status != "OPEN" else None,
        pnl=pnl_percent_for_signal(signal),
        pnl_percent=pnl_percent_for_signal(signal),
        rr=rr_for_signal(signal),
        entry_time=signal.triggered_at,
        exit_time=signal.hit_at if status != "OPEN" else None,
        entry_reason=str((signal.raw_payload or {}).get("setup_id") or signal.strategy),
        exit_reason=None if status == "OPEN" else (signal.reached_state or status),
        score=signal.quality_score or signal.radar_score,
    )


async def sync_positions_from_signals(session: AsyncSession, notify_new_since: datetime | None = None) -> dict[str, int]:
    signals = (
        await session.scalars(
            select(Signal).where(
                Signal.official_trade.is_(True),
                Signal.entry_price.is_not(None),
            )
        )
    ).all()
    signal_ids = [signal.id for signal in signals]
    existing_rows = (await session.scalars(select(Position).where(Position.signal_id.in_(signal_ids)))).all() if signal_ids else []
    existing = {row.signal_id: row for row in existing_rows}
    inserted = updated = events = notified = 0

    for signal in signals:
        position = existing.get(signal.id)
        is_new = position is None
        source = position_from_signal(signal)
        if is_new:
            position = source
            session.add(position)
            inserted += 1
            await session.flush()
            await add_position_event_once(
                session,
                position,
                "SIGNAL_CREATED",
                signal.triggered_at,
                signal.entry_price,
                "Position created from official signal",
            )
            events += 1
        else:
            for name in (
                "symbol",
                "side",
                "timeframe",
                "strategy_name",
                "status",
                "exit_price",
                "pnl",
                "pnl_percent",
                "rr",
                "exit_time",
                "exit_reason",
                "score",
            ):
                setattr(position, name, getattr(source, name))
            updated += 1

        state = (signal.reached_state or "holding").lower()
        event_type = TP_EVENTS.get(state)
        if event_type:
            await add_position_event_once(
                session,
                position,
                event_type,
                signal.hit_at or signal.triggered_at,
                event_price_for_state(signal, state),
                f"{state.upper()} reached",
            )
            events += 1
        elif state == "sl":
            await add_position_event_once(
                session,
                position,
                "SL_HIT",
                signal.hit_at or signal.triggered_at,
                signal.sl_price,
                "Stop loss reached",
            )
            events += 1
        elif position.status in {"EXPIRED", "INVALIDATED", "CANCELLED"}:
            await add_position_event_once(
                session,
                position,
                position.status,
                signal.hit_at or signal.triggered_at,
                signal.current_price,
                f"Position {position.status.lower()}",
            )
            events += 1

        if is_new and position.status == "OPEN" and notify_new_since and signal.created_at >= notify_new_since:
            await send_telegram_for_position(session, position, "NEW_POSITION")
            await add_position_event_once(
                session,
                position,
                "NOTIFIED",
                signal.created_at,
                signal.entry_price,
                "Telegram notification attempted",
            )
            notified += 1

    await session.commit()
    return {"positions_inserted": inserted, "positions_updated": updated, "position_events": events, "notifications": notified}
