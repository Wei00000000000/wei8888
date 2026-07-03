from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from backend.app.routers.market import build_sector_flows, build_volume_anomalies
from backend.app.schemas import MarketResponse


def market(symbol: str, change: float, volume: float) -> MarketResponse:
    return MarketResponse(
        symbol=symbol,
        source="test",
        price=Decimal("1"),
        change_24h_pct=change,
        quote_volume_24h=volume,
        observed_at=datetime.now(timezone.utc),
    )


def test_volume_anomalies_filter_low_volume_and_rank() -> None:
    rows = [market("BTC", 2, 100_000_000), market("ETH", 8, 20_000_000), market("TINY", 99, 1_000_000)]
    result = build_volume_anomalies(rows)
    assert [row["symbol"] for row in result] == ["ETH", "BTC"]


def test_sector_flows_use_volume_weighted_change() -> None:
    rows = [market("BTC", 2, 100), market("ETH", -2, 300), market("DOGE", 5, 50)]
    result = {row["name"]: row for row in build_sector_flows(rows)}
    assert result["Layer 1"]["market_cap_change_24h"] == -1
    assert result["Meme"]["market_cap_change_24h"] == 5
