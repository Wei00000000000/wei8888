from __future__ import annotations

from dataclasses import dataclass
import json
import os
from threading import Lock
from typing import Any, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BINANCE_FAPI = "https://fapi.binance.com"
BINANCE_FDATA = "https://fapi.binance.com/futures/data"


@dataclass(frozen=True)
class Kline:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int


@dataclass(frozen=True)
class OpenInterestPoint:
    timestamp: int
    open_interest: float
    open_interest_value: float | None


@dataclass(frozen=True)
class TakerPoint:
    timestamp: int
    buy_volume: float
    sell_volume: float
    buy_sell_ratio: float | None


class BinanceFuturesClient:
    _cache: dict[str, Any] = {}
    _cache_lock = Lock()

    def __init__(self, timeout: float = 15.0) -> None:
        self.timeout = timeout
        self.proxy_url = os.getenv("BINANCE_PROXY_URL", "").rstrip("/")
        self.proxy_token = os.getenv("BINANCE_PROXY_TOKEN", "")

    def close(self) -> None:
        return None

    def __enter__(self) -> "BinanceFuturesClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _get(self, base_url: str, path: str, params: dict[str, Any]) -> Any:
        cache_key = self._cache_key(base_url, path, params)
        with self._cache_lock:
            if cache_key in self._cache:
                return self._cache[cache_key]
        if self.proxy_url:
            results = self._proxy_batch([{"base": self._base_name(base_url), "path": path, "params": params}])
            if not results:
                raise RuntimeError("Binance proxy returned no results; deploy the latest Worker")
            result = results[0]
            if not result.get("ok"):
                raise RuntimeError(f"Binance proxy error {result.get('status')}: {result.get('error')}")
            data = result.get("data")
            with self._cache_lock:
                self._cache[cache_key] = data
            return data
        query = urlencode(params)
        url = f"{base_url}{path}"
        if query:
            url = f"{url}?{query}"
        request = Request(url, headers={"User-Agent": "sentiment-scanner/0.1"})
        with urlopen(request, timeout=self.timeout) as response:
            payload = response.read().decode("utf-8")
        return json.loads(payload)

    @staticmethod
    def _base_name(base_url: str) -> str:
        return "fdata" if base_url == BINANCE_FDATA else "fapi"

    @classmethod
    def _cache_key(cls, base_url: str, path: str, params: dict[str, Any]) -> str:
        return json.dumps([cls._base_name(base_url), path, sorted(params.items())], separators=(",", ":"), default=str)

    def _proxy_batch(self, operations: list[dict[str, Any]]) -> list[dict[str, Any]]:
        body = json.dumps({"operations": operations}).encode("utf-8")
        headers = {"Content-Type": "application/json", "User-Agent": "sentiment-scanner/0.1"}
        if self.proxy_token:
            headers["X-Wei-Proxy-Key"] = self.proxy_token
        request = Request(f"{self.proxy_url}/binance/batch", data=body, headers=headers, method="POST")
        with urlopen(request, timeout=max(self.timeout, 30)) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return payload.get("results", [])

    def prefetch(self, requests: list[tuple[str, str, dict[str, Any]]], batch_size: int = 35) -> None:
        if not self.proxy_url:
            return
        missing: list[tuple[str, str, dict[str, Any]]] = []
        with self._cache_lock:
            for base_url, path, params in requests:
                if self._cache_key(base_url, path, params) not in self._cache:
                    missing.append((base_url, path, params))
        for start in range(0, len(missing), batch_size):
            chunk = missing[start:start + batch_size]
            operations = [{"base": self._base_name(base), "path": path, "params": params} for base, path, params in chunk]
            results = self._proxy_batch(operations)
            for request_info, result in zip(chunk, results):
                if not result.get("ok"):
                    continue
                base_url, path, params = request_info
                with self._cache_lock:
                    self._cache[self._cache_key(base_url, path, params)] = result.get("data")

    def exchange_symbols(self, quote_asset: str = "USDT") -> list[str]:
        data = self._get(BINANCE_FAPI, "/fapi/v1/exchangeInfo", {})
        symbols: list[str] = []
        for item in data.get("symbols", []):
            if (
                item.get("status") == "TRADING"
                and item.get("contractType") == "PERPETUAL"
                and item.get("quoteAsset") == quote_asset
            ):
                symbols.append(item["symbol"])
        return sorted(symbols)

    def symbols_by_volume(
        self,
        limit: int = 0,
        quote_asset: str = "USDT",
        min_quote_volume: float = 0.0,
    ) -> list[str]:
        valid = set(self.exchange_symbols(quote_asset=quote_asset))
        tickers = self._get(BINANCE_FAPI, "/fapi/v1/ticker/24hr", {})
        ranked = sorted(
            (
                item
                for item in tickers
                if item.get("symbol") in valid and float(item.get("quoteVolume") or 0.0) >= min_quote_volume
            ),
            key=lambda item: float(item.get("quoteVolume") or 0.0),
            reverse=True,
        )
        if limit > 0:
            ranked = ranked[:limit]
        return [item["symbol"] for item in ranked]

    def top_symbols_by_volume(self, limit: int = 50, quote_asset: str = "USDT") -> list[str]:
        return self.symbols_by_volume(limit=limit, quote_asset=quote_asset)

    def ticker_24hr(self) -> list[dict[str, Any]]:
        data = self._get(BINANCE_FAPI, "/fapi/v1/ticker/24hr", {})
        return [row for row in data if isinstance(row, dict)]

    def ticker_price(self, symbols: Iterable[str]) -> dict[str, float]:
        wanted = set(normalize_symbols(symbols))
        if not wanted:
            return {}
        data = self._get(BINANCE_FAPI, "/fapi/v1/ticker/price", {})
        prices: dict[str, float] = {}
        for item in data:
            symbol = item.get("symbol")
            if symbol in wanted:
                prices[symbol] = float(item["price"])
        return prices

    def klines(
        self,
        symbol: str,
        interval: str = "15m",
        limit: int = 500,
        start_time: int | None = None,
    ) -> list[Kline]:
        params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        data = self._get(
            BINANCE_FAPI,
            "/fapi/v1/klines",
            params,
        )
        return [
            Kline(
                open_time=int(row[0]),
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume=float(row[5]),
                close_time=int(row[6]),
            )
            for row in data
        ]

    def klines_since(
        self,
        symbol: str,
        interval: str,
        start_time: int,
        max_pages: int = 8,
    ) -> list[Kline]:
        rows: list[Kline] = []
        cursor = max(0, int(start_time))
        for _ in range(max_pages):
            page = self.klines(symbol, interval=interval, limit=1500, start_time=cursor)
            if not page:
                break
            rows.extend(page)
            next_cursor = page[-1].close_time + 1
            if len(page) < 1500 or next_cursor <= cursor:
                break
            cursor = next_cursor
        return rows

    def open_interest_hist(self, symbol: str, period: str = "15m", limit: int = 500) -> list[OpenInterestPoint]:
        data = self._get(
            BINANCE_FDATA,
            "/openInterestHist",
            {"symbol": symbol, "period": period, "limit": limit},
        )
        return [
            OpenInterestPoint(
                timestamp=int(row["timestamp"]),
                open_interest=float(row["sumOpenInterest"]),
                open_interest_value=_optional_float(row.get("sumOpenInterestValue")),
            )
            for row in data
        ]

    def taker_buy_sell_volume(self, symbol: str, period: str = "15m", limit: int = 500) -> list[TakerPoint]:
        data = self._get(
            BINANCE_FDATA,
            "/takerlongshortRatio",
            {"symbol": symbol, "period": period, "limit": limit},
        )
        return [
            TakerPoint(
                timestamp=int(row["timestamp"]),
                buy_volume=float(row.get("buyVol") or 0.0),
                sell_volume=float(row.get("sellVol") or 0.0),
                buy_sell_ratio=_optional_float(row.get("buySellRatio")),
            )
            for row in data
        ]

    def global_long_short_account_ratio(
        self, symbol: str, period: str = "1h", limit: int = 1
    ) -> list[dict[str, Any]]:
        return self._ratio_history("/globalLongShortAccountRatio", symbol, period, limit)

    def top_long_short_account_ratio(
        self, symbol: str, period: str = "1h", limit: int = 1
    ) -> list[dict[str, Any]]:
        return self._ratio_history("/topLongShortAccountRatio", symbol, period, limit)

    def top_long_short_position_ratio(
        self, symbol: str, period: str = "1h", limit: int = 1
    ) -> list[dict[str, Any]]:
        return self._ratio_history("/topLongShortPositionRatio", symbol, period, limit)

    def _ratio_history(
        self, path: str, symbol: str, period: str, limit: int
    ) -> list[dict[str, Any]]:
        data = self._get(
            BINANCE_FDATA,
            path,
            {"symbol": symbol, "period": period, "limit": limit},
        )
        return [row for row in data if isinstance(row, dict)]


def normalize_symbols(symbols: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    for symbol in symbols:
        value = symbol.strip().upper()
        if not value:
            continue
        value = value.replace("/", "").replace(":USDT", "").replace(".P", "")
        if not value.endswith("USDT"):
            value = f"{value}USDT"
        normalized.append(value)
    return normalized


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)
