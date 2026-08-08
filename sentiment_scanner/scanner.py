from __future__ import annotations

from dataclasses import asdict, dataclass
from time import time

from .binance import BinanceFuturesClient, Kline, OpenInterestPoint, TakerPoint
from .indicators import atr, percentile_rank


@dataclass(frozen=True)
class ScannerConfig:
    interval: str = "15m"
    lookback_limit: int = 500
    atr_period: int = 14
    oi_percentile_threshold: float = 99.0
    oi_change_min_pct: float = 3.0
    oi_change_strong_pct: float = 5.0
    atr_risk_multiple: float = 2.5
    max_risk_pct: float = 0.10
    eval_window_hours: float = 6.0
    cooldown_minutes: int = 15


@dataclass(frozen=True)
class MarketSnapshot:
    symbol: str
    timestamp: int
    price: float
    atr: float
    oi_prev: float
    oi_value: float
    oi_value_usdt: float | None
    oi_change_pct: float
    oi_percentile: float
    price_change_pct: float
    taker_buy_ratio: float | None
    taker_buy_volume: float | None
    taker_sell_volume: float | None


@dataclass(frozen=True)
class Signal:
    symbol: str
    timeframe: str
    signal_type: str
    setup_id: str
    triggered_at_ms: int
    trigger_price: float
    atr_at_trigger: float
    sl_price: float
    tp1_price: float
    tp2_price: float
    tp3_price: float
    ftp_price: float
    risk: float
    sl_source: str
    oi_percentile: float
    oi_change_pct: float
    price_change_pct: float
    taker_buy_ratio: float | None
    oi_value: float
    oi_value_usdt: float | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class EvaluatedSignal:
    signal: Signal
    reached_state: str
    hit_at_ms: int | None
    hit_price: float | None
    max_gain_pct: float
    max_drawdown_pct: float

    def to_dict(self) -> dict[str, object]:
        data = self.signal.to_dict()
        data.update(
            {
                "reached_state": self.reached_state,
                "hit_at_ms": self.hit_at_ms,
                "hit_price": self.hit_price,
                "max_gain_pct": self.max_gain_pct,
                "max_drawdown_pct": self.max_drawdown_pct,
            }
        )
        return data


class SentimentScanner:
    def __init__(self, client: BinanceFuturesClient, config: ScannerConfig | None = None) -> None:
        self.client = client
        self.config = config or ScannerConfig()

    async def latest_signal(self, symbol: str) -> Signal | None:
        klines, oi_points, taker_points = await self._load(symbol)
        snapshots = self._snapshots(symbol, klines, oi_points, taker_points)
        if not snapshots:
            return None
        return self._signal_from_snapshot(snapshots[-1])

    async def backtest(self, symbol: str) -> list[EvaluatedSignal]:
        klines, oi_points, taker_points = await self._load(symbol)
        snapshots = self._snapshots(symbol, klines, oi_points, taker_points)
        kline_by_time = {kline.open_time: kline for kline in klines}
        signals: list[EvaluatedSignal] = []
        last_by_setup: dict[str, int] = {}
        for snapshot in snapshots:
            signal = self._signal_from_snapshot(snapshot)
            if signal is None:
                continue
            cooldown_key = f"{signal.symbol}|{signal.setup_id}"
            last_time = last_by_setup.get(cooldown_key)
            cooldown_ms = self.config.cooldown_minutes * 60 * 1000
            if last_time is not None and signal.triggered_at_ms - last_time < cooldown_ms:
                continue
            last_by_setup[cooldown_key] = signal.triggered_at_ms
            signals.append(self._evaluate_signal(signal, kline_by_time))
        return signals

    async def _load(
        self, symbol: str
    ) -> tuple[list[Kline], list[OpenInterestPoint], list[TakerPoint]]:
        klines = await self.client.klines(symbol, interval=self.config.interval, limit=self.config.lookback_limit)
        oi_points = await self.client.open_interest_hist(
            symbol, period=self.config.interval, limit=min(self.config.lookback_limit, 500)
        )
        try:
            taker_points = await self.client.taker_buy_sell_volume(
                symbol, period=self.config.interval, limit=min(self.config.lookback_limit, 500)
            )
        except Exception:
            taker_points = []
        return klines, oi_points, taker_points

    def _snapshots(
        self,
        symbol: str,
        klines: list[Kline],
        oi_points: list[OpenInterestPoint],
        taker_points: list[TakerPoint],
    ) -> list[MarketSnapshot]:
        atr_values = atr(klines, period=self.config.atr_period)
        klines_by_open = {kline.open_time: (index, kline) for index, kline in enumerate(klines)}
        taker_by_time = {point.timestamp: point for point in taker_points}
        oi_changes_abs: list[float] = []
        snapshots: list[MarketSnapshot] = []

        for index in range(1, len(oi_points)):
            previous_oi = oi_points[index - 1]
            current_oi = oi_points[index]
            matched = klines_by_open.get(current_oi.timestamp)
            previous_matched = klines_by_open.get(previous_oi.timestamp)
            if matched is None or previous_matched is None:
                continue
            kline_index, kline = matched
            _, previous_kline = previous_matched
            if kline.close_time > int(time() * 1000):
                continue
            if previous_oi.open_interest == 0 or previous_kline.close == 0:
                continue

            oi_change_pct = (current_oi.open_interest - previous_oi.open_interest) / previous_oi.open_interest * 100.0
            price_change_pct = (kline.close - previous_kline.close) / previous_kline.close * 100.0
            current_atr = atr_values[kline_index]
            oi_percentile = percentile_rank(abs(oi_change_pct), oi_changes_abs)
            oi_changes_abs.append(abs(oi_change_pct))
            if current_atr is None or oi_percentile is None:
                continue

            taker = taker_by_time.get(current_oi.timestamp)
            taker_buy_ratio: float | None = None
            taker_buy_volume: float | None = None
            taker_sell_volume: float | None = None
            if taker is not None:
                taker_buy_volume = taker.buy_volume
                taker_sell_volume = taker.sell_volume
                total = taker.buy_volume + taker.sell_volume
                if total > 0:
                    taker_buy_ratio = taker.buy_volume / total

            snapshots.append(
                MarketSnapshot(
                    symbol=symbol,
                    # The setup only becomes knowable after the matched candle
                    # closes, so this is the actual signal establishment time.
                    timestamp=kline.close_time + 1,
                    price=kline.close,
                    atr=current_atr,
                    oi_prev=previous_oi.open_interest,
                    oi_value=current_oi.open_interest,
                    oi_value_usdt=current_oi.open_interest_value,
                    oi_change_pct=oi_change_pct,
                    oi_percentile=oi_percentile,
                    price_change_pct=price_change_pct,
                    taker_buy_ratio=taker_buy_ratio,
                    taker_buy_volume=taker_buy_volume,
                    taker_sell_volume=taker_sell_volume,
                )
            )
        return snapshots

    def _signal_from_snapshot(self, snapshot: MarketSnapshot) -> Signal | None:
        if abs(snapshot.oi_change_pct) < self.config.oi_change_min_pct:
            return None

        if snapshot.oi_change_pct > 0 and snapshot.price_change_pct > 0:
            signal_type = "reversal_bullish"
            setup_id = "oi_15m_long_buildup"
            direction = 1
        elif snapshot.oi_change_pct < 0 and snapshot.price_change_pct <= 0:
            signal_type = "reversal_bearish"
            setup_id = "oi_15m_long_unwind"
            direction = -1
        else:
            return None

        raw_risk = self.config.atr_risk_multiple * snapshot.atr
        capped_risk = snapshot.price * self.config.max_risk_pct
        risk = min(raw_risk, capped_risk)
        sl_source = "atr" if risk == raw_risk else "capped_10pct"

        if direction == 1:
            sl = snapshot.price - risk
            tp1 = snapshot.price + risk
            tp2 = snapshot.price + risk * 2
            tp3 = snapshot.price + risk * 3
            ftp = snapshot.price + risk * 5
        else:
            sl = snapshot.price + risk
            tp1 = snapshot.price - risk
            tp2 = snapshot.price - risk * 2
            tp3 = snapshot.price - risk * 3
            ftp = snapshot.price - risk * 5

        return Signal(
            symbol=snapshot.symbol,
            timeframe=self.config.interval.upper(),
            signal_type=signal_type,
            setup_id=setup_id,
            triggered_at_ms=snapshot.timestamp,
            trigger_price=snapshot.price,
            atr_at_trigger=snapshot.atr,
            sl_price=sl,
            tp1_price=tp1,
            tp2_price=tp2,
            tp3_price=tp3,
            ftp_price=ftp,
            risk=risk,
            sl_source=sl_source,
            oi_percentile=snapshot.oi_percentile,
            oi_change_pct=snapshot.oi_change_pct,
            price_change_pct=snapshot.price_change_pct,
            taker_buy_ratio=snapshot.taker_buy_ratio,
            oi_value=snapshot.oi_value,
            oi_value_usdt=snapshot.oi_value_usdt,
        )

    def _evaluate_signal(self, signal: Signal, kline_by_time: dict[int, Kline]) -> EvaluatedSignal:
        interval_minutes = int(self.config.interval.removesuffix("m"))
        bars = int(self.config.eval_window_hours * 60 / interval_minutes)
        is_bullish = signal.signal_type == "reversal_bullish"
        reached_state = "holding"
        hit_at_ms: int | None = None
        hit_price: float | None = None
        max_gain_pct = 0.0
        max_drawdown_pct = 0.0

        targets = [
            ("ftp", signal.ftp_price),
            ("tp3", signal.tp3_price),
            ("tp2", signal.tp2_price),
            ("tp1", signal.tp1_price),
        ]

        future_klines = sorted(
            (kline for kline in kline_by_time.values() if kline.open_time >= signal.triggered_at_ms),
            key=lambda kline: kline.open_time,
        )[:bars]
        for kline in future_klines:

            if is_bullish:
                gain = (kline.high - signal.trigger_price) / signal.trigger_price * 100.0
                drawdown = (signal.trigger_price - kline.low) / signal.trigger_price * 100.0
                max_gain_pct = max(max_gain_pct, gain)
                max_drawdown_pct = max(max_drawdown_pct, drawdown)
                if kline.low <= signal.sl_price:
                    reached_state = "sl"
                    hit_price = signal.sl_price
                else:
                    for name, price in targets:
                        if kline.high >= price:
                            reached_state = name
                            hit_price = price
                            break
            else:
                gain = (signal.trigger_price - kline.low) / signal.trigger_price * 100.0
                drawdown = (kline.high - signal.trigger_price) / signal.trigger_price * 100.0
                max_gain_pct = max(max_gain_pct, gain)
                max_drawdown_pct = max(max_drawdown_pct, drawdown)
                if kline.high >= signal.sl_price:
                    reached_state = "sl"
                    hit_price = signal.sl_price
                else:
                    for name, price in targets:
                        if kline.low <= price:
                            reached_state = name
                            hit_price = price
                            break

            if reached_state != "holding":
                hit_at_ms = kline.open_time
                break

        return EvaluatedSignal(
            signal=signal,
            reached_state=reached_state,
            hit_at_ms=hit_at_ms,
            hit_price=hit_price,
            max_gain_pct=max_gain_pct,
            max_drawdown_pct=max_drawdown_pct,
        )
