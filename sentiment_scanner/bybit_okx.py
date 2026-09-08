"""Public perpetual-futures market data from Bybit, validated with OKX.

The scanner keeps its Binance-shaped internal model so historical signals remain
readable, but all newly fetched market data comes from public Bybit endpoints.
OKX is queried alongside the ticker feed as an independent price check.
"""
from __future__ import annotations

import asyncio
from typing import Any, Iterable

import httpx

from .binance import Kline, OpenInterestPoint, TakerPoint, normalize_symbols


BYBIT_BASE = "https://api.bybit.com"
OKX_BASE = "https://www.okx.com"


class BybitOkxFuturesClient:
    def __init__(self, timeout: float = 20.0) -> None:
        self.timeout = timeout
        self.last_provider = "bybit+okx"
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers={"User-Agent": "wei-strategy-room/1.0", "Accept": "application/json"},
            limits=httpx.Limits(max_connections=40, max_keepalive_connections=15),
        )
        self._ticker_cache: dict[str, dict[str, Any]] = {}

    async def __aenter__(self) -> "BybitOkxFuturesClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    async def _bybit(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        response = await self._client.get(f"{BYBIT_BASE}{path}", params=params)
        response.raise_for_status()
        payload = response.json()
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit error {payload.get('retCode')}: {payload.get('retMsg')}")
        result = payload.get("result") or {}
        return result.get("list") or []

    async def _okx_tickers(self) -> dict[str, float]:
        response = await self._client.get(f"{OKX_BASE}/api/v5/market/tickers", params={"instType": "SWAP"})
        response.raise_for_status()
        payload = response.json()
        if str(payload.get("code")) != "0":
            raise RuntimeError(f"OKX error {payload.get('code')}: {payload.get('msg')}")
        prices: dict[str, float] = {}
        for row in payload.get("data") or []:
            inst = str(row.get("instId") or "")
            if not inst.endswith("-USDT-SWAP"):
                continue
            symbol = inst.removesuffix("-USDT-SWAP").replace("-", "") + "USDT"
            price = _number(row.get("last"))
            if price > 0:
                prices[symbol] = price
        return prices

    async def exchange_symbols(self, quote_asset: str = "USDT") -> list[str]:
        rows = await self._bybit("/v5/market/instruments-info", {"category": "linear", "limit": 1000})
        return sorted(
            str(row.get("symbol"))
            for row in rows
            if row.get("status") == "Trading" and str(row.get("symbol") or "").endswith(quote_asset)
        )

    async def ticker_24hr(self) -> list[dict[str, Any]]:
        bybit_rows, okx_prices = await asyncio.gather(
            self._bybit("/v5/market/tickers", {"category": "linear"}), self._okx_tickers()
        )
        rows: list[dict[str, Any]] = []
        for item in bybit_rows:
            symbol = str(item.get("symbol") or "")
            if not symbol.endswith("USDT"):
                continue
            last = _number(item.get("lastPrice"))
            if last <= 0:
                continue
            change = _number(item.get("price24hPcnt")) * 100.0
            row = {
                "symbol": symbol,
                "lastPrice": last,
                "price": last,
                "priceChangePercent": change,
                "quoteVolume": _number(item.get("turnover24h")),
                "volume": _number(item.get("volume24h")),
                "highPrice": _number(item.get("highPrice24h")),
                "lowPrice": _number(item.get("lowPrice24h")),
                "openPrice": last / (1 + change / 100) if change > -100 else last,
                "lastFundingRate": item.get("fundingRate") or item.get("lastFundingRate"),
                "markPrice": item.get("markPrice"),
                "source": "bybit",
                "validation_source": "okx",
                "okxLastPrice": okx_prices.get(symbol),
            }
            okx_price = okx_prices.get(symbol)
            if okx_price:
                row["price_validation_diff_pct"] = abs(last - okx_price) / last * 100.0
            rows.append(row)
        self._ticker_cache = {str(row["symbol"]): row for row in rows}
        return rows

    async def symbols_by_volume(self, limit: int = 0, quote_asset: str = "USDT", min_quote_volume: float = 0.0) -> list[str]:
        rows = await self.ticker_24hr()
        ranked = sorted(
            (row for row in rows if str(row["symbol"]).endswith(quote_asset) and _number(row.get("quoteVolume")) >= min_quote_volume),
            key=lambda row: _number(row.get("quoteVolume")), reverse=True,
        )
        if limit > 0:
            ranked = ranked[:limit]
        return [str(row["symbol"]) for row in ranked]

    async def top_symbols_by_volume(self, limit: int = 50, quote_asset: str = "USDT") -> list[str]:
        return await self.symbols_by_volume(limit=limit, quote_asset=quote_asset)

    async def ticker_price(self, symbols: Iterable[str]) -> dict[str, float]:
        wanted = set(normalize_symbols(symbols))
        rows = await self.ticker_24hr()
        return {str(row["symbol"]): _number(row.get("lastPrice")) for row in rows if str(row["symbol"]) in wanted}

    async def premium_index(self) -> list[dict[str, Any]]:
        return await self.ticker_24hr()

    async def klines(self, symbol: str, interval: str = "15m", limit: int = 500, start_time: int | None = None) -> list[Kline]:
        params: dict[str, Any] = {"category": "linear", "symbol": _symbol(symbol), "interval": _bybit_interval(interval), "limit": min(max(1, int(limit)), 1000)}
        if start_time is not None:
            params["start"] = int(start_time)
        data = await self._bybit("/v5/market/kline", params)
        span = _interval_ms(interval)
        rows = [Kline(int(row[0]), _number(row[1]), _number(row[2]), _number(row[3]), _number(row[4]), _number(row[5]), int(row[0]) + span - 1) for row in data]
        return sorted(rows, key=lambda row: row.open_time)

    async def klines_since(self, symbol: str, interval: str, start_time: int, max_pages: int = 8) -> list[Kline]:
        rows = await self.klines(symbol, interval=interval, limit=min(1000, max_pages * 300), start_time=start_time)
        return [row for row in rows if row.open_time >= start_time]

    async def open_interest_hist(self, symbol: str, period: str = "15m", limit: int = 500) -> list[OpenInterestPoint]:
        normalized = _symbol(symbol)
        data = await self._bybit("/v5/market/open-interest", {"category": "linear", "symbol": normalized, "intervalTime": _bybit_oi_interval(period), "limit": min(max(1, int(limit)), 200)})
        price = _number((self._ticker_cache.get(normalized) or {}).get("lastPrice"))
        if price <= 0:
            price = (await self.ticker_price([normalized])).get(normalized, 0.0)
        rows = [OpenInterestPoint(int(row.get("timestamp") or 0), _number(row.get("openInterest")), _number(row.get("openInterest")) * price if price > 0 else None) for row in data]
        return sorted((row for row in rows if row.timestamp > 0), key=lambda row: row.timestamp)

    async def taker_buy_sell_volume(self, symbol: str, period: str = "15m", limit: int = 500) -> list[TakerPoint]:
        data = await self._bybit("/v5/market/recent-trade", {"category": "linear", "symbol": _symbol(symbol), "limit": min(max(1, int(limit)), 1000)})
        span = _interval_ms(period)
        buckets: dict[int, dict[str, float]] = {}
        for row in data:
            timestamp = int(row.get("time") or 0)
            if timestamp <= 0:
                continue
            bucket = timestamp - timestamp % span
            values = buckets.setdefault(bucket, {"buy": 0.0, "sell": 0.0})
            values["buy" if str(row.get("side")).lower() == "buy" else "sell"] += _number(row.get("size"))
        return [TakerPoint(timestamp, values["buy"], values["sell"], values["buy"] / values["sell"] if values["sell"] else None) for timestamp, values in sorted(buckets.items())][-limit:]

    async def global_long_short_account_ratio(self, symbol: str, period: str = "1h", limit: int = 1) -> list[dict[str, Any]]:
        data = await self._bybit("/v5/market/account-ratio", {"category": "linear", "symbol": _symbol(symbol), "period": _bybit_ratio_period(period), "limit": min(max(1, int(limit)), 500)})
        return [{"timestamp": row.get("timestamp"), "longShortRatio": row.get("buyRatio"), "longAccount": row.get("buyRatio"), "shortAccount": row.get("sellRatio")} for row in data]

    async def top_long_short_account_ratio(self, symbol: str, period: str = "1h", limit: int = 1) -> list[dict[str, Any]]:
        return await self.global_long_short_account_ratio(symbol, period, limit)

    async def top_long_short_position_ratio(self, symbol: str, period: str = "1h", limit: int = 1) -> list[dict[str, Any]]:
        return await self.global_long_short_account_ratio(symbol, period, limit)

    async def prefetch(self, requests: list[tuple[str, str, dict[str, Any]]], batch_size: int = 35) -> None:
        return None


def _number(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _symbol(symbol: str) -> str:
    return normalize_symbols([symbol])[0]


def _interval_ms(interval: str) -> int:
    value = interval.lower()
    return int(value.removesuffix("h")) * 3_600_000 if value.endswith("h") else int(value.removesuffix("m")) * 60_000


def _bybit_interval(interval: str) -> str:
    return interval.removesuffix("m") if interval.endswith("m") else interval.upper()


def _bybit_oi_interval(interval: str) -> str:
    return {"5m": "5min", "15m": "15min", "30m": "30min", "1h": "1h", "4h": "4h", "1d": "1d"}.get(interval.lower(), "15min")


def _bybit_ratio_period(interval: str) -> str:
    return {"5m": "5min", "15m": "15min", "30m": "30min", "1h": "1h", "4h": "4h", "1d": "1d"}.get(interval.lower(), "1h")
