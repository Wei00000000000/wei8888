from __future__ import annotations

import os
from typing import Any

from .bingx import BingxFuturesClient
from .bybit import BybitFuturesClient
from .okx import OkxFuturesClient


class MixedFuturesClient:
    _disabled: set[str] = set()

    def __init__(self, timeout: float = 20.0) -> None:
        self.timeout = timeout
        self.clients: list[tuple[str, Any]] = [
            ("bingx", BingxFuturesClient(timeout=timeout)),
            ("okx", OkxFuturesClient(timeout=timeout)),
        ]
        if os.getenv("MARKET_DATA_ALLOW_BYBIT_FALLBACK", "").strip().lower() in {"1", "true", "yes"}:
            self.clients.append(("bybit", BybitFuturesClient(timeout=timeout)))
        self.last_provider = "mixed"

    def close(self) -> None:
        for _, client in self.clients:
            close = getattr(client, "close", None)
            if close:
                close()

    def __enter__(self) -> "MixedFuturesClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        errors: list[str] = []
        for name, client in self.clients:
            if name in self._disabled:
                continue
            try:
                result = getattr(client, method)(*args, **kwargs)
                self.last_provider = name
                return result
            except Exception as exc:
                text = str(exc)
                errors.append(f"{name}: {text}")
                if "403" in text or "Forbidden" in text or "10006" in text:
                    self._disabled.add(name)
        raise RuntimeError("Mixed market data failed: " + " | ".join(errors))

    def exchange_symbols(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("exchange_symbols", *args, **kwargs)

    def symbols_by_volume(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("symbols_by_volume", *args, **kwargs)

    def top_symbols_by_volume(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("top_symbols_by_volume", *args, **kwargs)

    def ticker_24hr(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("ticker_24hr", *args, **kwargs)

    def ticker_price(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("ticker_price", *args, **kwargs)

    def klines(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("klines", *args, **kwargs)

    def klines_since(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("klines_since", *args, **kwargs)

    def open_interest_hist(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("open_interest_hist", *args, **kwargs)

    def taker_buy_sell_volume(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("taker_buy_sell_volume", *args, **kwargs)

    def global_long_short_account_ratio(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("global_long_short_account_ratio", *args, **kwargs)

    def top_long_short_account_ratio(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("top_long_short_account_ratio", *args, **kwargs)

    def top_long_short_position_ratio(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("top_long_short_position_ratio", *args, **kwargs)

    def prefetch(self, *args: Any, **kwargs: Any) -> None:
        return None
