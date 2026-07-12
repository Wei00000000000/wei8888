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
        self.oi_client = BybitFuturesClient(timeout=timeout)
        self._oi_symbols: set[str] | None = None
        self.clients: list[tuple[str, Any]] = [
            ("bingx", BingxFuturesClient(timeout=timeout)),
            ("okx", OkxFuturesClient(timeout=timeout)),
        ]
        if os.getenv("MARKET_DATA_ALLOW_BYBIT_FALLBACK", "").strip().lower() in {"1", "true", "yes"}:
            self.clients.append(("bybit", BybitFuturesClient(timeout=timeout)))
        self.last_provider = "mixed"

    def close(self) -> None:
        self.oi_client.close()
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
                if any(term in text for term in ("403", "Forbidden", "10006", "100410", "429", "Too Many Requests")):
                    self._disabled.add(name)
        raise RuntimeError("Mixed market data failed: " + " | ".join(errors))

    def _bybit_symbols(self, quote_asset: str = "USDT") -> set[str]:
        if self._oi_symbols is None:
            self._oi_symbols = set(self.oi_client.exchange_symbols(quote_asset=quote_asset))
        return self._oi_symbols

    def exchange_symbols(self, *args: Any, **kwargs: Any) -> Any:
        price_symbols = set(self._call("exchange_symbols", *args, **kwargs))
        quote_asset = str(kwargs.get("quote_asset") or (args[0] if args else "USDT"))
        return sorted(price_symbols & self._bybit_symbols(quote_asset))

    def symbols_by_volume(self, *args: Any, **kwargs: Any) -> Any:
        limit = int(kwargs.get("limit") or (args[0] if args else 0))
        quote_asset = str(kwargs.get("quote_asset") or "USDT")
        min_quote_volume = float(kwargs.get("min_quote_volume") or 0.0)
        # Ask for the full price ranking first, then apply the Bybit OI universe
        # before the final limit so unsupported contracts cannot crowd out valid ones.
        ranked = self._call(
            "symbols_by_volume",
            limit=0,
            quote_asset=quote_asset,
            min_quote_volume=min_quote_volume,
        )
        filtered = [symbol for symbol in ranked if symbol in self._bybit_symbols(quote_asset)]
        return filtered[:limit] if limit > 0 else filtered

    def top_symbols_by_volume(self, *args: Any, **kwargs: Any) -> Any:
        limit = int(kwargs.get("limit") or (args[0] if args else 50))
        quote_asset = str(kwargs.get("quote_asset") or "USDT")
        return self.symbols_by_volume(limit=limit, quote_asset=quote_asset)

    def ticker_24hr(self, *args: Any, **kwargs: Any) -> Any:
        rows = self._call("ticker_24hr", *args, **kwargs)
        try:
            valid = self._bybit_symbols(str(kwargs.get("quote_asset") or "USDT"))
        except Exception:
            return rows
        return [row for row in rows if str(row.get("symbol") or "") in valid]

    def ticker_price(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("ticker_price", *args, **kwargs)

    def klines(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("klines", *args, **kwargs)

    def klines_since(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("klines_since", *args, **kwargs)

    def open_interest_hist(self, *args: Any, **kwargs: Any) -> Any:
        result = self.oi_client.open_interest_hist(*args, **kwargs)
        self.last_provider = "bybit"
        return result

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
