from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

import httpx


BINANCE_FAPI = "https://fapi.binance.com"
BINANCE_FDATA = "https://fapi.binance.com/futures/data"

HTTP_MAX_CONNECTIONS = 200
HTTP_MAX_KEEPALIVE = 50
SYMBOL_CONCURRENCY = 3
COOLDOWN_SECONDS_429 = 60
COOLDOWN_SECONDS_418 = 600
DEFAULT_HTTP_CONCURRENCY = 6


def _cooldown_state_path() -> Path:
    override = os.getenv("BINANCE_COOLDOWN_STATE_PATH", "").strip()
    if override:
        return Path(override)
    return Path(__file__).resolve().parent / ".binance_cooldown.json"


def _read_persisted_global_until(path: Path) -> float:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return float(payload.get("global_blocked_until_mono") or 0.0)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return 0.0


def _write_persisted_global_until(path: Path, until_mono: float) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"global_blocked_until_mono": until_mono}, indent=2),
            encoding="utf-8",
        )
    except OSError:
        pass


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
        self.last_provider = "binance"
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers={"User-Agent": "sentiment-scanner/0.1"},
            limits=httpx.Limits(
                max_connections=HTTP_MAX_CONNECTIONS,
                max_keepalive_connections=HTTP_MAX_KEEPALIVE,
            ),
        )
        self._endpoint_blocked_until: dict[str, float] = {}
        self._global_blocked_until = 0.0
        self._cooldown_state_path = _cooldown_state_path()
        self._refresh_global_cooldown_from_disk()
        concurrency = int(os.getenv("BINANCE_HTTP_CONCURRENCY", str(DEFAULT_HTTP_CONCURRENCY)))
        self._http_sem = asyncio.Semaphore(max(1, concurrency))

    def _refresh_global_cooldown_from_disk(self) -> None:
        persisted = _read_persisted_global_until(self._cooldown_state_path)
        self._global_blocked_until = max(self._global_blocked_until, persisted)

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "BinanceFuturesClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def _wait_cooldowns(self, endpoint: str) -> None:
        while True:
            self._refresh_global_cooldown_from_disk()
            now = time.monotonic()
            delay = max(
                self._global_blocked_until - now,
                self._endpoint_blocked_until.get(endpoint, 0.0) - now,
                0.0,
            )
            if delay <= 0:
                return
            await asyncio.sleep(delay)

    def _set_endpoint_cooldown(self, endpoint: str, seconds: float) -> None:
        until = time.monotonic() + seconds
        self._endpoint_blocked_until[endpoint] = max(
            self._endpoint_blocked_until.get(endpoint, 0.0),
            until,
        )

    def _apply_rate_limit_penalty(self, endpoint: str, status: int) -> None:
        seconds = COOLDOWN_SECONDS_418 if status == 418 else COOLDOWN_SECONDS_429
        self._set_endpoint_cooldown(endpoint, seconds)
        until = time.monotonic() + seconds
        self._global_blocked_until = max(self._global_blocked_until, until)
        _write_persisted_global_until(self._cooldown_state_path, self._global_blocked_until)

    async def _get(self, base_url: str, path: str, params: dict[str, Any]) -> Any:
        endpoint = f"{base_url}{path}"
        while True:
            await self._wait_cooldowns(endpoint)
            async with self._http_sem:
                response = await self._client.get(f"{base_url}{path}", params=params or None)
            if response.status_code == 429:
                self._apply_rate_limit_penalty(endpoint, 429)
                continue
            if response.status_code == 418:
                self._apply_rate_limit_penalty(endpoint, 418)
                continue
            response.raise_for_status()
            return response.json()

    async def prefetch(self, requests: list[tuple[str, str, dict[str, Any]]], batch_size: int = 35) -> None:
        return None

    async def exchange_symbols(self, quote_asset: str = "USDT") -> list[str]:
        data = await self._get(BINANCE_FAPI, "/fapi/v1/exchangeInfo", {})
        symbols: list[str] = []
        for item in data.get("symbols", []):
            if (
                item.get("status") == "TRADING"
                and item.get("contractType") == "PERPETUAL"
                and item.get("quoteAsset") == quote_asset
            ):
                symbols.append(item["symbol"])
        return sorted(symbols)

    async def symbols_by_volume(
        self,
        limit: int = 0,
        quote_asset: str = "USDT",
        min_quote_volume: float = 5000000,
    ) -> list[str]:
        valid = set(await self.exchange_symbols(quote_asset=quote_asset))
        tickers = await self._get(BINANCE_FAPI, "/fapi/v1/ticker/24hr", {})
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

    async def top_symbols_by_volume(self, limit: int = 300, quote_asset: str = "USDT") -> list[str]:
        return await self.symbols_by_volume(limit=limit, quote_asset=quote_asset)

    async def ticker_24hr(self) -> list[dict[str, Any]]:
        data = await self._get(BINANCE_FAPI, "/fapi/v1/ticker/24hr", {})
        return [row for row in data if isinstance(row, dict)]

    async def ticker_price(self, symbols: Iterable[str]) -> dict[str, float]:
        wanted = set(normalize_symbols(symbols))
        if not wanted:
            return {}
        data = await self._get(BINANCE_FAPI, "/fapi/v1/ticker/price", {})
        prices: dict[str, float] = {}
        for item in data:
            symbol = item.get("symbol")
            if symbol in wanted:
                prices[symbol] = float(item["price"])
        return prices

    async def premium_index(self) -> list[dict[str, Any]]:
        data = await self._get(BINANCE_FAPI, "/fapi/v1/premiumIndex", {})
        return [row for row in data if isinstance(row, dict)]

    async def klines(
        self,
        symbol: str,
        interval: str = "15m",
        limit: int = 500,
        start_time: int | None = None,
    ) -> list[Kline]:
        params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        data = await self._get(BINANCE_FAPI, "/fapi/v1/klines", params)
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

    async def klines_since(
        self,
        symbol: str,
        interval: str,
        start_time: int,
        max_pages: int = 8,
    ) -> list[Kline]:
        rows: list[Kline] = []
        cursor = max(0, int(start_time))
        for _ in range(max_pages):
            page = await self.klines(symbol, interval=interval, limit=1500, start_time=cursor)
            if not page:
                break
            rows.extend(page)
            next_cursor = page[-1].close_time + 1
            if len(page) < 1500 or next_cursor <= cursor:
                break
            cursor = next_cursor
        return rows

    async def open_interest_hist(self, symbol: str, period: str = "15m", limit: int = 500) -> list[OpenInterestPoint]:
        data = await self._get(
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

    async def taker_buy_sell_volume(self, symbol: str, period: str = "15m", limit: int = 500) -> list[TakerPoint]:
        data = await self._get(
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

    async def global_long_short_account_ratio(
        self, symbol: str, period: str = "1h", limit: int = 500
    ) -> list[dict[str, Any]]:
        return await self._ratio_history("/globalLongShortAccountRatio", symbol, period, limit)

    async def top_long_short_account_ratio(
        self, symbol: str, period: str = "1h", limit: int = 500
    ) -> list[dict[str, Any]]:
        return await self._ratio_history("/topLongShortAccountRatio", symbol, period, limit)

    async def top_long_short_position_ratio(
        self, symbol: str, period: str = "1h", limit: int = 500
    ) -> list[dict[str, Any]]:
        return await self._ratio_history("/topLongShortPositionRatio", symbol, period, limit)

    async def _ratio_history(
        self, path: str, symbol: str, period: str, limit: int
    ) -> list[dict[str, Any]]:
        data = await self._get(
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


def _serialize_klines(klines: list[Kline]) -> list[dict[str, Any]]:
    return [asdict(row) for row in klines]


def _serialize_open_interest(rows: list[OpenInterestPoint]) -> list[dict[str, Any]]:
    return [asdict(row) for row in rows]


def _serialize_taker(rows: list[TakerPoint]) -> list[dict[str, Any]]:
    return [asdict(row) for row in rows]


def oi_change_pct(points: list[OpenInterestPoint]) -> float | None:
    if len(points) < 2 or points[-2].open_interest <= 0:
        return None
    return (points[-1].open_interest - points[-2].open_interest) / points[-2].open_interest * 100.0


def funding_rate_map(rows: list[dict[str, Any]]) -> dict[str, float]:
    book: dict[str, float] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol.endswith("USDT"):
            continue
        rate = _optional_float(row.get("lastFundingRate"))
        if rate is not None:
            book[symbol.removesuffix("USDT")] = rate
    return book


async def fetch_symbol_data(client: BinanceFuturesClient, symbol: str) -> dict[str, Any]:
    (
        klines_5m,
        klines_15m,
        klines_1h,
        open_interest_5m,
        open_interest_15m,
        open_interest_1h,
        global_ratio_5m,
        global_ratio_15m,
        global_ratio_1h,
        top_account_ratio_5m,
        top_account_ratio_15m,
        top_account_ratio_1h,
        top_position_ratio_5m,
        top_position_ratio_15m,
        top_position_ratio_1h,
        taker_5m,
        taker_15m,
        taker_1h,
    ) = await asyncio.gather(
        client.klines(symbol, interval="5m", limit=500),
        client.klines(symbol, interval="15m", limit=500),
        client.klines(symbol, interval="1h", limit=500),
        client.open_interest_hist(symbol, period="5m", limit=500),
        client.open_interest_hist(symbol, period="15m", limit=500),
        client.open_interest_hist(symbol, period="1h", limit=500),
        client.global_long_short_account_ratio(symbol, period="5m", limit=500),
        client.global_long_short_account_ratio(symbol, period="15m", limit=500),
        client.global_long_short_account_ratio(symbol, period="1h", limit=500),
        client.top_long_short_account_ratio(symbol, period="5m", limit=500),
        client.top_long_short_account_ratio(symbol, period="15m", limit=500),
        client.top_long_short_account_ratio(symbol, period="1h", limit=500),
        client.top_long_short_position_ratio(symbol, period="5m", limit=500),
        client.top_long_short_position_ratio(symbol, period="15m", limit=500),
        client.top_long_short_position_ratio(symbol, period="1h", limit=500),
        client.taker_buy_sell_volume(symbol, period="5m", limit=500),
        client.taker_buy_sell_volume(symbol, period="15m", limit=500),
        client.taker_buy_sell_volume(symbol, period="1h", limit=500),
    )
    return {
        "symbol": symbol,
        "klines": {
            "5m": _serialize_klines(klines_5m),
            "15m": _serialize_klines(klines_15m),
            "1h": _serialize_klines(klines_1h),
        },
        "open_interest_hist": {
            "5m": _serialize_open_interest(open_interest_5m),
            "15m": _serialize_open_interest(open_interest_15m),
            "1h": _serialize_open_interest(open_interest_1h),
        },
        "global_long_short_account_ratio": {
            "5m": global_ratio_5m,
            "15m": global_ratio_15m,
            "1h": global_ratio_1h,
        },
        "top_long_short_account_ratio": {
            "5m": top_account_ratio_5m,
            "15m": top_account_ratio_15m,
            "1h": top_account_ratio_1h,
        },
        "top_long_short_position_ratio": {
            "5m": top_position_ratio_5m,
            "15m": top_position_ratio_15m,
            "1h": top_position_ratio_1h,
        },
        "taker_buy_sell_volume": {
            "5m": _serialize_taker(taker_5m),
            "15m": _serialize_taker(taker_15m),
            "1h": _serialize_taker(taker_1h),
        },
    }


async def scan_market(client: BinanceFuturesClient) -> dict[str, Any]:
    top_symbols = await client.top_symbols_by_volume()
    sem = asyncio.Semaphore(SYMBOL_CONCURRENCY)
    data: dict[str, dict[str, Any]] = {}

    async def process_symbol(symbol: str) -> None:
        async with sem:
            data[symbol] = await fetch_symbol_data(client, symbol)

    await asyncio.gather(*(process_symbol(symbol) for symbol in top_symbols))
    return {
        "symbols": top_symbols,
        "data": data,
        "fetched_at": time.time(),
    }


async def main() -> None:
    async with BinanceFuturesClient() as binance_exchange:
        start_time = time.time()
        snapshot = await scan_market(binance_exchange)
        end_time = time.time()
        print(f"Fetched {len(snapshot['symbols'])} symbols in {end_time - start_time:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
