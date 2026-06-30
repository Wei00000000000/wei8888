from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Index, Integer, JSON, Numeric, String, Text, UniqueConstraint, event, inspect
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


PRICE = Numeric(38, 18)


class Signal(Base):
    __tablename__ = "signals"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: str(uuid4()))
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    timeframe: Mapped[str] = mapped_column(String(12), index=True)
    strategy: Mapped[str] = mapped_column(String(64), index=True)
    strategy_version: Mapped[str] = mapped_column(String(32), default="legacy-v1", index=True)
    side: Mapped[str] = mapped_column(String(8), index=True)
    trade_layer: Mapped[str] = mapped_column(String(24), default="raw_signal", index=True)
    official_trade: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    entry_price: Mapped[Decimal | None] = mapped_column(PRICE)
    sl_price: Mapped[Decimal | None] = mapped_column(PRICE)
    tp1_price: Mapped[Decimal | None] = mapped_column(PRICE)
    tp2_price: Mapped[Decimal | None] = mapped_column(PRICE)
    tp3_price: Mapped[Decimal | None] = mapped_column(PRICE)
    ftp_price: Mapped[Decimal | None] = mapped_column(PRICE)

    current_price: Mapped[Decimal | None] = mapped_column(PRICE)
    reached_state: Mapped[str] = mapped_column(String(24), default="holding", index=True)
    pnl_pct: Mapped[float | None] = mapped_column(Float)
    hit_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    max_gain_pct: Mapped[float | None] = mapped_column(Float)
    max_drawdown_pct: Mapped[float | None] = mapped_column(Float)
    quality_score: Mapped[int | None] = mapped_column(Integer)
    radar_score: Mapped[int | None] = mapped_column(Integer)
    raw_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    events: Mapped[list["SignalEvent"]] = relationship(back_populates="signal", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_signals_strategy_triggered", "strategy", "triggered_at"),
        Index("ix_signals_symbol_side_state", "symbol", "side", "reached_state"),
    )


LOCKED_SIGNAL_FIELDS = (
    "symbol",
    "timeframe",
    "strategy",
    "strategy_version",
    "side",
    "triggered_at",
    "entry_price",
    "sl_price",
    "tp1_price",
    "tp2_price",
    "tp3_price",
    "ftp_price",
)


@event.listens_for(Signal, "before_update")
def prevent_locked_signal_mutation(_mapper: object, _connection: object, target: Signal) -> None:
    state = inspect(target)
    changed = [name for name in LOCKED_SIGNAL_FIELDS if state.attrs[name].history.has_changes()]
    if changed:
        raise ValueError(f"Immutable signal fields cannot be changed: {', '.join(changed)}")


class SignalEvent(Base):
    __tablename__ = "signal_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    signal_id: Mapped[str] = mapped_column(ForeignKey("signals.id", ondelete="CASCADE"), index=True)
    event_type: Mapped[str] = mapped_column(String(32), index=True)
    reached_state: Mapped[str | None] = mapped_column(String(24))
    price: Mapped[Decimal | None] = mapped_column(PRICE)
    happened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    signal: Mapped[Signal] = relationship(back_populates="events")

    __table_args__ = (
        UniqueConstraint("signal_id", "event_type", "happened_at", name="uq_signal_event_once"),
    )


class Position(Base):
    __tablename__ = "positions"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: str(uuid4()))
    signal_id: Mapped[str] = mapped_column(ForeignKey("signals.id", ondelete="CASCADE"), unique=True, index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    side: Mapped[str] = mapped_column(String(8), index=True)
    timeframe: Mapped[str] = mapped_column(String(12), index=True)
    strategy_name: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(24), default="OPEN", index=True)
    entry_price: Mapped[Decimal | None] = mapped_column(PRICE)
    stop_loss: Mapped[Decimal | None] = mapped_column(PRICE)
    take_profit_1: Mapped[Decimal | None] = mapped_column(PRICE)
    take_profit_2: Mapped[Decimal | None] = mapped_column(PRICE)
    take_profit_3: Mapped[Decimal | None] = mapped_column(PRICE)
    take_profit_final: Mapped[Decimal | None] = mapped_column(PRICE)
    exit_price: Mapped[Decimal | None] = mapped_column(PRICE)
    pnl: Mapped[float | None] = mapped_column(Float)
    pnl_percent: Mapped[float | None] = mapped_column(Float)
    rr: Mapped[float | None] = mapped_column(Float)
    entry_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    exit_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    entry_reason: Mapped[str | None] = mapped_column(Text)
    exit_reason: Mapped[str | None] = mapped_column(Text)
    score: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    signal: Mapped[Signal] = relationship()
    events: Mapped[list["PositionEvent"]] = relationship(back_populates="position", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_positions_symbol_status_time", "symbol", "status", "entry_time"),
        Index("ix_positions_strategy_status", "strategy_name", "status"),
    )


class PositionEvent(Base):
    __tablename__ = "position_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    position_id: Mapped[str] = mapped_column(ForeignKey("positions.id", ondelete="CASCADE"), index=True)
    event_type: Mapped[str] = mapped_column(String(32), index=True)
    event_price: Mapped[Decimal | None] = mapped_column(PRICE)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    description: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    position: Mapped[Position] = relationship(back_populates="events")

    __table_args__ = (
        UniqueConstraint("position_id", "event_type", "event_time", name="uq_position_event_once"),
    )


class NotificationLog(Base):
    __tablename__ = "notification_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    signal_id: Mapped[str | None] = mapped_column(String(64), index=True)
    position_id: Mapped[str | None] = mapped_column(String(64), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    side: Mapped[str] = mapped_column(String(8), index=True)
    timeframe: Mapped[str] = mapped_column(String(12), index=True)
    message_type: Mapped[str] = mapped_column(String(32), index=True)
    message_text: Mapped[str] = mapped_column(Text)
    telegram_chat_id: Mapped[str | None] = mapped_column(String(128))
    sent_status: Mapped[str] = mapped_column(String(24), default="pending", index=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[str | None] = mapped_column(Text)
    dedupe_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)


class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    source: Mapped[str] = mapped_column(String(24), index=True)
    price: Mapped[Decimal] = mapped_column(PRICE)
    change_24h_pct: Mapped[float | None] = mapped_column(Float)
    quote_volume_24h: Mapped[float | None] = mapped_column(Float)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    __table_args__ = (
        UniqueConstraint("symbol", "source", "observed_at", name="uq_market_observation"),
    )


class BacktestRun(Base):
    __tablename__ = "backtest_runs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: str(uuid4()))
    requested_by: Mapped[str] = mapped_column(String(64), default="user")
    strategy: Mapped[str] = mapped_column(String(64), index=True)
    strategy_version: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(24), default="queued", index=True)
    parameters: Mapped[dict] = mapped_column(JSON, default=dict)
    summary: Mapped[dict] = mapped_column(JSON, default=dict)
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class StrategyParameter(Base):
    __tablename__ = "strategy_parameters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy: Mapped[str] = mapped_column(String(64), index=True)
    version: Mapped[str] = mapped_column(String(32))
    parameters: Mapped[dict] = mapped_column(JSON)
    active: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    __table_args__ = (UniqueConstraint("strategy", "version", name="uq_strategy_version"),)


class SystemLog(Base):
    __tablename__ = "system_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    level: Mapped[str] = mapped_column(String(16), index=True)
    component: Mapped[str] = mapped_column(String(48), index=True)
    event: Mapped[str] = mapped_column(String(64), index=True)
    message: Mapped[str] = mapped_column(Text)
    details: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)

