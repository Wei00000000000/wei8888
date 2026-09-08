from __future__ import annotations

from sentiment_scanner.bybit_okx import BybitOkxFuturesClient


def market_client(timeout: float = 20.0) -> BybitOkxFuturesClient:
    """Return the public Bybit client with OKX ticker validation."""
    return BybitOkxFuturesClient(timeout=timeout)
