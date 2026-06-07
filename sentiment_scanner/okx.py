from __future__ import annotations

import json
from typing import Any, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .binance import Kline, OpenInterestPoint, TakerPoint, normalize_symbols


OKX_BASE = "https://www.okx.com"


class OkxFuturesClient:
    def __init__(self, timeout: float = 20.0) -> None:
        self.timeout = timeout
        self._symbol_to_inst: dict[str, str] = {}

    def close(self) -> None:
        return None

    def __enter__(self) -> "OkxFuturesClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        query = urlencode(params)
        url = f"{OKX_BASE}{path}"
        if query:
            url = f"{url}?{query}"
        request = Request(url, headers={"User-Agent": "wei-strategy-room/0.1", "Accept": "application/json"})
        with urlopen(request, timeout=self.timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if str(payload.get("code")) != "0":
            raise RuntimeError(f"OKX error {payload.get('code')}: {payload.get('msg')}")
        return payload.get("data") or []

    def exchange_symbols(self, quote_asset: str = "USDT") -> list[str]:
        rows = self._get("/api/v5/public/instruments", {"instType": "SWAP"})
        symbols: list[str] = []
        mapping: dict[str, str] = {}
        suffix = f"-{quote_asset}-SWAP"
        for item in rows:
            inst_id = str(item.get("instId") or "")
            if not inst_id.endswith(suffix):
                continue
            if str(item.get("state") or "").lower() != "live":
                continue
            base = inst_id.removesuffix(suffix).replace("-", "")
            symbol = f"{base}{quote_asset}"
            symbols.append(symbol)
            mapping[symbol] = inst_id
        self._symbol_to_inst.update(mapping)
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
        data = self._get("/api/v5/market/tickers", {"instType": "SWAP"})
        rows: list[dict[str, Any]] = []
        for item in data:
            inst_id = str(item.get("instId") or "")
            if not inst_id.endswith("-USDT-SWAP"):
                continue
            symbol = _symbol_from_inst(inst_id)
            last = _float(item.get("last"))
            open_price = _float(item.get("open24h"))
            change = (last - open_price) / open_price * 100.0 if open_price > 0 else 0.0
            quote_volume = _float(item.get("volCcy24h")) * last
            rows.append(
                {
                    "symbol": symbol,
                    "lastPrice": last,
                    "price": last,
                    "priceChangePercent": change,
                    "quoteVolume": quote_volume,
                    "highPrice": _float(item.get("high24h")),
                    "lowPrice": _float(item.get("low24h")),
                    "openPrice": open_price,
                    "instId": inst_id,
                }
            )
            self._symbol_to_inst[symbol] = inst_id
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
        params: dict[str, Any] = {"instId": self._inst_id(symbol), "bar": _bar(interval), "limit": min(int(limit), 300)}
        if start_time is not None:
            params["after"] = int(start_time)
        data = self._get("/api/v5/market/candles", params)
        rows = [
            Kline(
                open_time=int(row[0]),
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume=float(row[5]),
                close_time=int(row[0]) + _interval_ms(interval) - 1,
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
        return [row for row in self.klines(symbol, interval=interval, limit=300) if row.open_time >= start_time]

    def open_interest_hist(self, symbol: str, period: str = "15m", limit: int = 500) -> list[OpenInterestPoint]:
        data = self._get(
            "/api/v5/rubik/stat/contracts/open-interest-history",
            {"instId": self._inst_id(symbol), "period": _bar(period), "limit": min(int(limit), 100)},
        )
        rows = [
            OpenInterestPoint(
                timestamp=int(row[0]),
                open_interest=float(row[1]),
                open_interest_value=_optional_float(row[3] if len(row) > 3 else None),
            )
            for row in data
        ]
        return sorted(rows, key=lambda item: item.timestamp)

    def taker_buy_sell_volume(self, symbol: str, period: str = "15m", limit: int = 500) -> list[TakerPoint]:
        # OKX public trade history is short-retention; use recent trades to keep
        # CVD divergence alive while CoinGlass remains the low-frequency fallback.
        data = self._get("/api/v5/market/trades", {"instId": self._inst_id(symbol), "limit": 500})
        interval_ms = _interval_ms(period)
        buckets: dict[int, dict[str, float]] = {}
        for row in data:
            ts = int(row.get("ts") or 0)
            bucket = ts - (ts % interval_ms)
            values = buckets.setdefault(bucket, {"buy": 0.0, "sell": 0.0})
            size = _float(row.get("sz"))
            if str(row.get("side") or "").lower() == "buy":
                values["buy"] += size
            else:
                values["sell"] += size
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

    def _inst_id(self, symbol: str) -> str:
        normalized = normalize_symbols([symbol])[0]
        if normalized not in self._symbol_to_inst:
            base = normalized.removesuffix("USDT")
            self._symbol_to_inst[normalized] = f"{base}-USDT-SWAP"
        return self._symbol_to_inst[normalized]


def _symbol_from_inst(inst_id: str) -> str:
    return inst_id.removesuffix("-USDT-SWAP").replace("-", "") + "USDT"


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return _float(value)


def _bar(interval: str) -> str:
    return interval.lower()


def _interval_ms(interval: str) -> int:
    value = interval.lower()
    if value.endswith("m"):
        return int(value.removesuffix("m")) * 60 * 1000
    if value.endswith("h"):
        return int(value.removesuffix("h")) * 60 * 60 * 1000
    return int(value) * 60 * 1000
