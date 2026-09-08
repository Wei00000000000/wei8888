from __future__ import annotations

import asyncio
from typing import Any

from sentiment_scanner.okx import OkxFuturesClient


class AsyncOkxFuturesClient:
    """Async adapter for the public OKX client used by the scanner."""
    def __init__(self, timeout: float = 20.0) -> None:
        self._client = OkxFuturesClient(timeout=timeout)
        self.last_provider = "okx"

    async def __aenter__(self) -> "AsyncOkxFuturesClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        self._client.close()

    async def premium_index(self) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._client.ticker_24hr)

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._client, name)
        async def call(*args: Any, **kwargs: Any) -> Any:
            return await asyncio.to_thread(target, *args, **kwargs)
        return call


def market_client(timeout: float = 20.0) -> AsyncOkxFuturesClient:
    """Use OKX for scanning; Bybit is optional because GitHub blocks it."""
    return AsyncOkxFuturesClient(timeout=timeout)
