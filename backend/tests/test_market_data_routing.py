from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from sentiment_scanner.binance import BinanceFuturesClient


class BinanceMarketDataTest(unittest.IsolatedAsyncioTestCase):
    async def test_ticker_price_filters_symbols(self) -> None:
        async with BinanceFuturesClient() as client:
            with patch.object(
                client,
                "_get",
                new=AsyncMock(
                    return_value=[
                        {"symbol": "BTCUSDT", "price": "100.0"},
                        {"symbol": "ETHUSDT", "price": "50.0"},
                    ]
                ),
            ):
                prices = await client.ticker_price(["BTCUSDT"])

        self.assertEqual(prices, {"BTCUSDT": 100.0})


if __name__ == "__main__":
    unittest.main()
