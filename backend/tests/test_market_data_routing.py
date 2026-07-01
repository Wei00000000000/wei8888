from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from sentiment_scanner.market_data import MixedFuturesClient


class MarketDataRoutingTest(unittest.TestCase):
    @patch("sentiment_scanner.market_data.BybitFuturesClient")
    @patch("sentiment_scanner.market_data.OkxFuturesClient")
    @patch("sentiment_scanner.market_data.BingxFuturesClient")
    def test_oi_uses_bybit_while_price_uses_bingx(self, bingx_cls: Mock, okx_cls: Mock, bybit_cls: Mock) -> None:
        bingx_cls.return_value.ticker_price.return_value = {"BTCUSDT": 100.0}
        bybit_cls.return_value.open_interest_hist.return_value = ["oi"]
        client = MixedFuturesClient()

        self.assertEqual(client.ticker_price(["BTCUSDT"]), {"BTCUSDT": 100.0})
        bingx_cls.return_value.ticker_price.assert_called_once()
        self.assertEqual(client.open_interest_hist("BTCUSDT"), ["oi"])
        bybit_cls.return_value.open_interest_hist.assert_called_once_with("BTCUSDT")
        okx_cls.return_value.open_interest_hist.assert_not_called()


if __name__ == "__main__":
    unittest.main()
