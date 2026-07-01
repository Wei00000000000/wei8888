from __future__ import annotations

import unittest

from scripts.update_seed_signals import classify_high_quality, classify_trade_layer, lock_signal_to_latest_price, trade_score


class TradeLayerScoreTest(unittest.TestCase):
    def test_top_level_oi_percentile_can_promote_official_trade(self) -> None:
        row = {
            "entry_price": 100,
            "sl_price": 98,
            "oi_percentile": 92,
            "setup_id": "oi_15m_long_buildup",
        }

        self.assertEqual(trade_score(row), 92)
        self.assertEqual(classify_trade_layer(row), ("official_trade", []))

    def test_negative_contract_radar_uses_confidence_magnitude(self) -> None:
        row = {
            "entry_price": 100,
            "sl_price": 103,
            "setup_id": "coinglass_contract_short_price_24h",
            "snapshot_data": {"radar_score": -88},
        }

        self.assertEqual(trade_score(row), 88)
        self.assertEqual(classify_trade_layer(row), ("official_trade", []))

    def test_low_score_remains_warning(self) -> None:
        row = {
            "entry_price": 100,
            "sl_price": 98,
            "oi_percentile": 55,
            "setup_id": "oi_15m_long_buildup",
        }

        layer, reasons = classify_trade_layer(row)
        self.assertEqual(layer, "warning")
        self.assertIn("score_below_60_warning_only", reasons)

    def test_high_quality_is_independent_from_strategy(self) -> None:
        row = {
            "official_trade": True,
            "strategy": "sentiment_oi",
            "entry_price": 100,
            "sl_price": 98,
            "oi_percentile": 92,
            "volume_24h": 10_000_000,
        }

        self.assertTrue(classify_high_quality(row))

    def test_new_long_trade_is_locked_to_latest_ticker(self) -> None:
        row = {
            "official_trade": True,
            "signal_type": "reversal_bullish",
            "entry_price": 100,
            "trigger_price": 100,
            "sl_price": 98,
        }

        lock_signal_to_latest_price(row, 105)

        self.assertEqual(row["entry_price"], 105)
        self.assertAlmostEqual(row["sl_price"], 102.9)
        self.assertAlmostEqual(row["tp1_price"], 107.1)
        self.assertEqual(row["snapshot_data"]["entry_price_source"], "latest_ticker_at_formalization")


if __name__ == "__main__":
    unittest.main()
