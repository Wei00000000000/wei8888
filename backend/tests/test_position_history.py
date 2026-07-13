from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from backend.app.database import SessionFactory, create_schema
from backend.app.models import Signal
from backend.app.routers.positions import merged_position_rows


class PositionHistoryTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await create_schema()
        self.signal_id = "position-history-fallback"
        async with SessionFactory() as session:
            existing = await session.get(Signal, self.signal_id)
            if existing is None:
                session.add(
                    Signal(
                        id=self.signal_id,
                        symbol="ICNT",
                        timeframe="15M",
                        strategy="sentiment_oi",
                        strategy_version="test",
                        side="short",
                        trade_layer="official_trade",
                        official_trade=True,
                        triggered_at=datetime(2026, 6, 29, 4, 0, tzinfo=timezone.utc),
                        entry_price=Decimal("0.5"),
                        sl_price=Decimal("0.515"),
                        tp1_price=Decimal("0.485"),
                        tp2_price=Decimal("0.47"),
                        tp3_price=Decimal("0.455"),
                        ftp_price=Decimal("0.43"),
                        current_price=Decimal("0.49"),
                        reached_state="holding",
                    )
                )
                await session.commit()

    async def test_legacy_signal_is_hidden_by_default(self) -> None:
        async with SessionFactory() as session:
            rows = await merged_position_rows(session, symbol="ICNT")

        row = next((item for item in rows if item.signal_id == self.signal_id), None)
        self.assertIsNone(row)

    async def test_legacy_signal_can_be_requested_for_audit(self) -> None:
        async with SessionFactory() as session:
            rows = await merged_position_rows(session, symbol="ICNT", include_legacy=True)

        row = next((item for item in rows if item.signal_id == self.signal_id), None)
        self.assertIsNotNone(row)
        self.assertEqual(row.status, "OPEN")
        self.assertEqual(row.entry_price, Decimal("0.5"))


if __name__ == "__main__":
    unittest.main()
