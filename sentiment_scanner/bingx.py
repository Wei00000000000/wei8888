from __future__ import annotations

import json
from typing import Any, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .binance import Kline, OpenInterestPoint, TakerPoint, normalize_symbols


BINGX_BASE = "https://open-api.bingx.com"


class BingxFuturesClient:
    def __init__(self, timeout: float = 20.0) -> None:
        self.timeout = timeout

    def close(self) -> None:
        return None

    def __enter__(self) -> "BingxFuturesClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        query = urlencode(params or {})
        url = f"{BINGX_BASE}{path}"
        if query:
            url = f"{url}?{query}"
        request = Request(url, headers={"User-Agent": "wei-strategy-room/0.1", "Accept": "application/json"})
        with urlopen(request, timeout=self.timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if int(payload.get("code") or 0) != 0:
            raise RuntimeError(f"BingX error {payload.get('code')}: {payload.get('msg')}")
        return payload.get("data")

    @staticmethod
    def _api_symbol(symbol: str) -> str:
        normalized = normalize_symbols([symbol])[0]
        if normalized.endswith("USDT"):
            return f"{normalized[:-4]}-USDT"
        return normalized

    @staticmethod
    def _local_symbol(symbol: str) -> str:
        return str(symbol or "").replace("-", "")

    def exchange_symbols(self, quote_asset: str = "USDT") -> list[str]:
        rows = self._get("/openApi/swap/v2/quote/contracts", {}) or []
        symbols: list[str] = []
        suffix = f"-{quote_asset}"
        for item in rows:
            if int(item.get("status") or 0) != 1:
                continue
            symbol = str(item.get("symbol") or "")
            if not symbol.endswith(suffix):
                continue
            symbols.append(self._local_symbol(symbol))
        return sorted(set(symbols))

    def symbols_by_volume(
        self,
        limit: int = 0,
        quote_asset: str = "USDT",
        min_quote_volume: float = 0.0,
    ) -> list[str]:
        valid = set(self.exchange_symbols(quote_asset=quote_asset))
        ranked = sorted(
            (
                item
                for item in self.ticker_24hr()
                if item.get("symbol") in valid and float(item.get("quoteVolume") or 0) >= min_quote_volume
            ),
            key=lambda item: float(item.get("quoteVolume") or 0),
            reverse=True,
        )
        if limit > 0:
            ranked = ranked[:limit]
        return [str(item["symbol"]) for item in ranked]

    def top_symbols_by_volume(self, limit: int = 50, quote_asset: str = "USDT") -> list[str]:
        return self.symbols_by_volume(limit=limit, quote_asset=quote_asset)

    def ticker_24hr(self) -> list[dict[str, Any]]:
        data = self._get("/openApi/swap/v2/quote/ticker", {}) or []
        if isinstance(data, dict):
            data = [data]
        rows: list[dict[str, Any]] = []
        for item in data:
            symbol = self._local_symbol(item.get("symbol"))
            if not symbol.endswith("USDT"):
                continue
            price = _float(item.get("lastPrice"))
            change = _float(item.get("priceChangePercent"))
            rows.append(
                {
                    "symbol": symbol,
                    "lastPrice": price,
                    "price": price,
                    "priceChangePercent": change,
                    "quoteVolume": _float(item.get("quoteVolume")),
                    "highPrice": _float(item.get("highPrice")),
                    "lowPrice": _float(item.get("lowPrice")),
                    "openPrice": _float(item.get("openPrice")),
                    "source": "bingx",
                }
            )
        return rows

    def ticker_price(self, symbols: Iterable[str]) -> dict[str, float]:
        wanted = set(normalize_symbols(symbols))
        return {
            str(item["symbol"]): float(item["lastPrice"])
            for item in self.ticker_24hr()
            if item.get("symbol") in wanted and _float(item.get("lastPrice")) > 0
        }

    def klines(
        self,
        symbol: str,
        interval: str = "15m",
        limit: int = 500,
        start_time: int | None = None,
    ) -> list[Kline]:
        params: dict[str, Any] = {
            "symbol": self._api_symbol(symbol),
            "interval": interval.lower(),
            "limit": min(int(limit), 1000),
        }
        if start_time is not None:
            params["startTime"] = int(start_time)
        data = self._get("/openApi/swap/v2/quote/klines", params) or []
        interval_ms = _interval_ms(interval)
        rows = [
            Kline(
                open_time=int(row["time"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row.get("volume") or 0.0),
                close_time=int(row["time"]) + interval_ms - 1,
            )
            for row in data
        ]
        return sorted(rows, key=lambda item: item.open_time)

    def klines_since(
        self,
        symbol: str,
        interval: str,
        start_time: int,
        max_pages: int = 8,
    ) -> list[Kline]:
        return [row for row in self.klines(symbol, interval=interval, limit=1000) if row.open_time >= start_time]

    def open_interest_hist(self, symbol: str, period: str = "15m", limit: int = 500) -> list[OpenInterestPoint]:
        raise RuntimeError("BingX public open-interest history is not used by this scanner")

    def taker_buy_sell_volume(self, symbol: str, period: str = "15m", limit: int = 500) -> list[TakerPoint]:
        data = self._get(
            "/openApi/swap/v2/quote/trades",
            {"symbol": self._api_symbol(symbol), "limit": min(int(limit), 1000)},
        ) or []
        interval_ms = _interval_ms(period)
        buckets: dict[int, dict[str, float]] = {}
        for row in data:
            ts = int(row.get("time") or row.get("ts") or 0)
            if ts <= 0:
                continue
            bucket = ts - (ts % interval_ms)
            values = buckets.setdefault(bucket, {"buy": 0.0, "sell": 0.0})
            qty = _float(row.get("qty"))
            if bool(row.get("isBuyerMaker")):
                values["sell"] += qty
            else:
                values["buy"] += qty
        points = [
            TakerPoint(
                timestamp=timestamp,
                buy_volume=values["buy"],
                sell_volume=values["sell"],
                buy_sell_ratio=values["buy"] / values["sell"] if values["sell"] > 0 else None,
            )
            for timestamp, values in buckets.items()
        ]
        return sorted(points, key=lambda item: item.timestamp)[-min(limit, 500):]

    def global_long_short_account_ratio(self, symbol: str, period: str = "1h", limit: int = 1) -> list[dict[str, Any]]:
        return []

    def top_long_short_account_ratio(self, symbol: str, period: str = "1h", limit: int = 1) -> list[dict[str, Any]]:
        return []

    def top_long_short_position_ratio(self, symbol: str, period: str = "1h", limit: int = 1) -> list[dict[str, Any]]:
        return []

    def prefetch(self, requests: list[tuple[str, str, dict[str, Any]]], batch_size: int = 35) -> None:
        return None


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _interval_ms(interval: str) -> int:
    value = interval.lower()
    if value.endswith("m"):
        return int(value.removesuffix("m")) * 60 * 1000
    if value.endswith("h"):
        return int(value.removesuffix("h")) * 60 * 60 * 1000
    return int(value) * 60 * 1000
