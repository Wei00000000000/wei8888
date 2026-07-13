from __future__ import annotations

from datetime import datetime, timezone
from typing import TypeVar

from sqlalchemy.sql import Select

from .config import settings
from .models import Position, Signal


TSelect = TypeVar("TSelect", bound=Select)


def own_signal_start_at() -> datetime | None:
    value = settings.own_signal_start_at
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def apply_signal_scope(query: TSelect, include_legacy: bool = False) -> TSelect:
    if include_legacy:
        return query
    start = own_signal_start_at()
    if start is not None:
        query = query.where(Signal.triggered_at >= start)
    return query


def apply_position_scope(query: TSelect, include_legacy: bool = False) -> TSelect:
    if include_legacy:
        return query
    start = own_signal_start_at()
    if start is not None:
        query = query.where(Position.entry_time >= start)
    return query


def signal_scope_filter(include_legacy: bool = False):
    if include_legacy:
        return None
    start = own_signal_start_at()
    return Signal.triggered_at >= start if start is not None else None


def position_scope_filter(include_legacy: bool = False):
    if include_legacy:
        return None
    start = own_signal_start_at()
    return Position.entry_time >= start if start is not None else None
