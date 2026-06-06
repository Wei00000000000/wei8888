from __future__ import annotations

import json
import time
from typing import Any, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .binance import Kline, OpenInterestPoint, TakerPoint, normalize_symbols


BYBIT_BASE = "https://api.bybit.com"


class BybitFuturesClient:
    def __init__(self, timeout: float = 20.0) -> None:
        self.timeout = timeout

    def close(self) -> None:
        return None

    def __enter__(self) -> "BybitFuturesClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        query = urlencode(params)
        url = f"{BYBIT_BASE}{path}"
        if query:
            url = f"{url}?{query}"
        request = Request(url, headers={"User-Agent": "wei-strategy-room/0.1", "Accept": "application/json"})
        with urlopen(request, timeout=self.timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if str(payload.get("retCode")) != "0":
            raise RuntimeError(f"Bybit error {payload.get('retCode')}: {payload.get('retMsg')}")
        return payload.get("result") or {}

    def exchange_symbols(self, quote_asset: str = "USDT") -> list[str]:
        symbols: list[str] = []
        cursor = ""
        while True:
            params: dict[str, Any] = {"category": "linear", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            data = self._get("/v5/market/instruments-info", params)
            for item in data.get("list") or []:
                if (
                    item.get("status") == "Trading"
                    and item.get("quoteCoin") == quote_asset
                    and item.get("contractType") == "LinearPerpetual"
                ):
                    symbols.append(str(item.get("symbol")))
            cursor = str(data.get("nextPageCursor") or "")
            if not cursor:
                break
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
        data = self._get("/v5/market/tickers", {"category": "linear"})
        rows: list[dict[str, Any]] = []
        for item in data.get("list") or []:
            price = _float(item.get("lastPrice"))
            change = _float(item.get("price24hPcnt")) * 100.0
            rows.append(
                {
                    "symbol": item.get("symbol"),
                    "lastPrice": price,
                    "price": price,
                    "priceChangePercent": change,
                    "quoteVolume": _float(item.get("turnover24h")),
                    "highPrice": _float(item.get("highPrice24h")),
                    "lowPrice": _float(item.get("lowPrice24h")),
                    "openPrice": price / (1 + change / 100.0) if price and change != -100 else price,
                    "openInterestValue": _float(item.get("openInterestValue")),
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
        interval_code = _interval_code(interval)
        params: dict[str, Any] = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval_code,
            "limit": min(int(limit), 1000),
        }
        if start_time is not None:
            params["start"] = int(start_time)
        data = self._get("/v5/market/kline", params)
        interval_ms = _interval_ms(interval)
        rows = []
        for row in data.get("list") or []:
            open_time = int(row[0])
            rows.append(
                Kline(
                    open_time=open_time,
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5]),
                    close_time=open_time + interval_ms - 1,
                )
            )
        return sorted(rows, key=lambda item: item.open_time)

    def klines_since(
        self,
        symbol: str,
        interval: str,
        start_time: int,
        max_pages: int = 8,
    ) -> list[Kline]:
        # Bybit returns the newest rows for wide ranges; for replay we only need
        # the latest available candles after the trigger in current retention.
        return [row for row in self.klines(symbol, interval=interval, limit=1000) if row.open_time >= start_time]

    def open_interest_hist(self, symbol: str, period: str = "15m", limit: int = 500) -> list[OpenInterestPoint]:
        data = self._get(
            "/v5/market/open-interest",
            {
                "category": "linear",
                "symbol": symbol,
                "intervalTime": _oi_interval(period),
                "limit": min(int(limit), 200),
            },
        )
        rows = [
            OpenInterestPoint(
                timestamp=int(row["timestamp"]),
                open_interest=float(row.get("openInterest") or 0.0),
                open_interest_value=None,
            )
            for row in data.get("list") or []
        ]
        return sorted(rows, key=lambda item: item.timestamp)

    def taker_buy_sell_volume(self, symbol: str, period: str = "15m", limit: int = 500) -> list[TakerPoint]:
        data = self._get("/v5/market/recent-trade", {"category": "linear", "symbol": symbol, "limit": 1000})
        interval_ms = _interval_ms(period)
        buckets: dict[int, dict[str, float]] = {}
        for row in data.get("list") or []:
            ts = int(row.get("time") or row.get("T") or 0)
            if ts <= 0:
                continue
            bucket = ts - (ts % interval_ms)
            side = str(row.get("side") or row.get("S") or "")
            size = float(row.get("size") or row.get("v") or 0.0)
            values = buckets.setdefault(bucket, {"buy": 0.0, "sell": 0.0})
            if side.lower() == "buy":
                values["buy"] += size
            elif side.lower() == "sell":
                values["sell"] += size
        points = []
        for timestamp, values in buckets.items():
            sell = values["sell"]
            buy = values["buy"]
            points.append(
                TakerPoint(
                    timestamp=timestamp,
                    buy_volume=buy,
                    sell_volume=sell,
                    buy_sell_ratio=buy / sell if sell > 0 else None,
                )
            )
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


def _interval_code(interval: str) -> str:
    value = interval.lower()
    if value.endswith("m"):
        return value.removesuffix("m")
    if value.endswith("h"):
        return str(int(value.removesuffix("h")) * 60)
    return value


def _oi_interval(interval: str) -> str:
    value = interval.lower()
    if value.endswith("m"):
        return f"{value.removesuffix('m')}min"
    if value.endswith("h"):
        return f"{value.removesuffix('h')}h"
    return value


def _interval_ms(interval: str) -> int:
    value = interval.lower()
    if value.endswith("m"):
        return int(value.removesuffix("m")) * 60 * 1000
    if value.endswith("h"):
        return int(value.removesuffix("h")) * 60 * 60 * 1000
    return int(value) * 60 * 1000
