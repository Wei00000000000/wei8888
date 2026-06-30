from __future__ import annotations

import unittest

from scripts.update_seed_signals import classify_trade_layer, trade_score


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


if __name__ == "__main__":
    unittest.main()
