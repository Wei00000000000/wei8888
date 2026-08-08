from __future__ import annotations

from sentiment_scanner.binance import BinanceFuturesClient


def market_client(timeout: float = 20.0) -> BinanceFuturesClient:
    """Return the sole market data client (Binance USD-M futures)."""
    return BinanceFuturesClient(timeout=timeout)
