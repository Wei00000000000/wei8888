from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select

from backend.app.database import SessionFactory, create_schema
from backend.app.models import Signal


class SignalIntegrityTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await create_schema()
        signal = Signal(
            id="immutability-test",
            symbol="BTC",
            timeframe="15M",
            strategy="test",
            strategy_version="v1",
            side="long",
            official_trade=True,
            triggered_at=datetime.now(timezone.utc),
            entry_price=Decimal("100"),
            sl_price=Decimal("99"),
            tp1_price=Decimal("101"),
            tp2_price=Decimal("102"),
            tp3_price=Decimal("103"),
            ftp_price=Decimal("105"),
        )
        async with SessionFactory() as session:
            existing = await session.get(Signal, signal.id)
            if existing is None:
                session.add(signal)
                await session.commit()

    async def test_trade_prices_are_immutable_after_insert(self) -> None:

        async with SessionFactory() as session:
            stored = (await session.scalars(select(Signal).where(Signal.id == "immutability-test"))).one()
            stored.entry_price = Decimal("200")
            with self.assertRaisesRegex(ValueError, "Immutable signal fields"):
                await session.commit()
            await session.rollback()

        async with SessionFactory() as session:
            stored = await session.get(Signal, "immutability-test")
            self.assertEqual(stored.entry_price, Decimal("100"))

    async def test_mutable_trade_state_can_advance(self) -> None:
        async with SessionFactory() as session:
            stored = await session.get(Signal, "immutability-test")
            if stored is None:
                self.skipTest("immutable test fixture is unavailable")
            stored.current_price = Decimal("101")
            stored.reached_state = "tp1"
            stored.pnl_pct = 1.0
            await session.commit()

        async with SessionFactory() as session:
            stored = await session.get(Signal, "immutability-test")
            self.assertEqual(stored.reached_state, "tp1")
            self.assertEqual(stored.entry_price, Decimal("100"))


if __name__ == "__main__":
    unittest.main()
