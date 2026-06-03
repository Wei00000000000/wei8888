from __future__ import annotations

from .binance import Kline


def atr(klines: list[Kline], period: int = 14) -> list[float | None]:
    values: list[float | None] = []
    true_ranges: list[float] = []
    previous_close: float | None = None
    for kline in klines:
        if previous_close is None:
            true_range = kline.high - kline.low
        else:
            true_range = max(
                kline.high - kline.low,
                abs(kline.high - previous_close),
                abs(kline.low - previous_close),
            )
        true_ranges.append(true_range)
        previous_close = kline.close
        if len(true_ranges) < period:
            values.append(None)
        elif len(true_ranges) == period:
            values.append(sum(true_ranges[-period:]) / period)
        else:
            previous_atr = values[-1]
            if previous_atr is None:
                values.append(sum(true_ranges[-period:]) / period)
            else:
                values.append(((previous_atr * (period - 1)) + true_range) / period)
    return values


def percentile_rank(value: float, history: list[float]) -> float | None:
    if not history:
        return None
    below_or_equal = sum(1 for item in history if item <= value)
    return below_or_equal / len(history) * 100.0

