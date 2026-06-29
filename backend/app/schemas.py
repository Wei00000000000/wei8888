from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class LoginRequest(BaseModel):
    password: str = Field(min_length=8, max_length=256)


class SessionResponse(BaseModel):
    authenticated: bool = True
    expires_at: datetime
    csrf_token: str
    access_token: str
    token_type: str = "bearer"


class SignalResponse(BaseModel):
    id: str
    symbol: str
    timeframe: str
    strategy: str
    strategy_version: str
    side: str
    trade_layer: str
    official_trade: bool
    triggered_at: datetime
    entry_price: Decimal | None
    sl_price: Decimal | None
    tp1_price: Decimal | None
    tp2_price: Decimal | None
    tp3_price: Decimal | None
    ftp_price: Decimal | None
    current_price: Decimal | None
    reached_state: str
    pnl_pct: float | None
    hit_at: datetime | None
    max_gain_pct: float | None
    max_drawdown_pct: float | None
    quality_score: int | None
    radar_score: int | None
    raw_payload: dict[str, Any]

    model_config = {"from_attributes": True}


class PageMeta(BaseModel):
    page: int
    limit: int
    total: int
    pages: int


class SignalPage(BaseModel):
    rows: list[SignalResponse]
    meta: PageMeta


class BacktestSummary(BaseModel):
    strategy: str | None
    total_entries: int
    closed_entries: int
    wins: int
    losses: int
    open_positions: int
    win_rate_pct: float
    total_pnl_pct: float
    realized_pnl_pct: float
    unrealized_pnl_pct: float
    tp1_rate_pct: float
    tp2_rate_pct: float
    tp3_rate_pct: float
    ftp_rate_pct: float
    max_drawdown_pct: float


class BacktestRequest(BaseModel):
    strategy_id: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9_.-]+$")
    symbols: list[str] = Field(default_factory=list, max_length=50)
    timeframe: Literal["5M", "15M", "1H", "4H", "1D"] = "15M"
    date_from: datetime
    date_to: datetime
    parameters: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbols")
    @classmethod
    def clean_symbols(cls, symbols: list[str]) -> list[str]:
        cleaned = []
        for symbol in symbols:
            value = symbol.upper().replace("/", "").replace("-", "")
            if not value.isalnum() or len(value) > 24:
                raise ValueError("Invalid symbol")
            cleaned.append(value)
        return list(dict.fromkeys(cleaned))

    @field_validator("date_to")
    @classmethod
    def validate_range(cls, date_to: datetime, info: Any) -> datetime:
        date_from = info.data.get("date_from")
        if date_from and date_to <= date_from:
            raise ValueError("date_to must be after date_from")
        if date_from and (date_to - date_from).days > 366:
            raise ValueError("Backtest range cannot exceed 366 days")
        return date_to


class BacktestRunResponse(BaseModel):
    run_id: str
    status: str


class MarketResponse(BaseModel):
    symbol: str
    source: str
    price: Decimal
    change_24h_pct: float | None
    quote_volume_24h: float | None
    observed_at: datetime

    model_config = {"from_attributes": True}
