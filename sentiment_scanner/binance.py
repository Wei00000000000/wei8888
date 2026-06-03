from __future__ import annotations

from dataclasses import dataclass
import json
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
    def __init__(self, timeout: float = 15.0) -> None:
        self.timeout = timeout

    def close(self) -> None:
        return None

    def __enter__(self) -> "BinanceFuturesClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _get(self, base_url: str, path: str, params: dict[str, Any]) -> Any:
        query = urlencode(params)
        url = f"{base_url}{path}"
        if query:
            url = f"{url}?{query}"
        request = Request(url, headers={"User-Agent": "sentiment-scanner/0.1"})
        with urlopen(request, timeout=self.timeout) as response:
            payload = response.read().decode("utf-8")
        return json.loads(payload)

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

    def top_symbols_by_volume(self, limit: int = 50, quote_asset: str = "USDT") -> list[str]:
        valid = set(self.exchange_symbols(quote_asset=quote_asset))
        tickers = self._get(BINANCE_FAPI, "/fapi/v1/ticker/24hr", {})
        ranked = sorted(
            (item for item in tickers if item.get("symbol") in valid),
            key=lambda item: float(item.get("quoteVolume") or 0.0),
            reverse=True,
        )
        return [item["symbol"] for item in ranked[:limit]]

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

    def klines(self, symbol: str, interval: str = "15m", limit: int = 500) -> list[Kline]:
        data = self._get(
            BINANCE_FAPI,
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
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
