from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from backend.app.models import Position
from backend.app.notifications import format_price, format_taipei_time, position_message


class NotificationFormatTest(unittest.TestCase):
    def test_price_and_taipei_time_are_readable(self) -> None:
        self.assertEqual(format_price(Decimal("1.7817000000000000")), "1.7817")
        self.assertEqual(format_price(Decimal("0.00000123")), "0.00000123")
        self.assertEqual(format_taipei_time(datetime(2026, 6, 30, 12, 54, tzinfo=timezone.utc)), "2026/06/30 下午8:54")

    def test_notification_omits_site_url(self) -> None:
        position = Position(
            id="notification-format",
            signal_id="notification-format",
            symbol="NEAR",
            side="short",
            timeframe="15M",
            strategy_name="contract_anomaly",
            status="OPEN",
            entry_price=Decimal("1.7817"),
            stop_loss=Decimal("1.8315876"),
            take_profit_1=Decimal("1.7318124"),
            take_profit_2=Decimal("1.6819248"),
            take_profit_3=Decimal("1.6320372"),
            take_profit_final=Decimal("1.532262"),
            entry_time=datetime(2026, 6, 30, 12, 54, tzinfo=timezone.utc),
        )

        message = position_message(position)

        self.assertIn("Entry：1.7817", message)
        self.assertIn("成立時間：2026/06/30 下午8:54", message)
        self.assertNotIn("http", message)


if __name__ == "__main__":
    unittest.main()
