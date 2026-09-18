from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Sequence

from sentiment_scanner.binance import Kline


@dataclass(frozen=True)
class QuantConfig:
    ema_fast: int = 50
    ema_slow: int = 200
    atr_period: int = 14
    volume_period: int = 20
    volume_multiplier: float = 1.2
    breakout_lookback: int = 20
    acceptance_bars: int = 2
    stop_atr: float = 0.5
    risk_fraction: float = 0.005


def _ema(values: Sequence[float], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(values)
    if period <= 0 or len(values) < period:
        return out
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    alpha = 2.0 / (period + 1)
    prev = seed
    for i in range(period, len(values)):
        prev = values[i] * alpha + prev * (1.0 - alpha)
        out[i] = prev
    return out


def _atr(rows: Sequence[Kline], period: int) -> list[float | None]:
    trs: list[float] = []
    out: list[float | None] = []
    prev_close: float | None = None
    prev_atr: float | None = None
    for row in rows:
        tr = row.high - row.low if prev_close is None else max(
            row.high - row.low, abs(row.high - prev_close), abs(row.low - prev_close)
        )
        trs.append(tr)
        prev_close = row.close
        if len(trs) < period:
            out.append(None)
        elif len(trs) == period:
            prev_atr = sum(trs[-period:]) / period
            out.append(prev_atr)
        else:
            prev_atr = ((prev_atr or tr) * (period - 1) + tr) / period
            out.append(prev_atr)
    return out


def run_quant_v1(rows: Sequence[Kline], config: QuantConfig | None = None) -> dict:
    cfg = config or QuantConfig()
    if len(rows) < max(cfg.ema_slow + 3, cfg.breakout_lookback + 3):
        return {"strategy": "quant-tpo-breakout-v1", "trades": [], "summary": _summary([])}

    closes = [r.close for r in rows]
    volumes = [r.volume for r in rows]
    fast = _ema(closes, cfg.ema_fast)
    slow = _ema(closes, cfg.ema_slow)
    atrs = _atr(rows, cfg.atr_period)
    trades: list[dict] = []
    position: dict | None = None

    for i in range(max(cfg.ema_slow, cfg.breakout_lookback), len(rows)):
        row = rows[i]
        if position:
            side = position["side"]
            hit_sl = row.low <= position["sl"] if side == "long" else row.high >= position["sl"]
            hit_tp = row.high >= position["tp3"] if side == "long" else row.low <= position["tp3"]
            if hit_sl or hit_tp:
                exit_price = position["sl"] if hit_sl else position["tp3"]
                risk = abs(position["entry"] - position["sl"])
                pnl_r = ((exit_price - position["entry"]) / risk) * (1 if side == "long" else -1)
                position.update(exit_time=row.close_time, exit=exit_price, exit_reason="SL" if hit_sl else "TP3", pnl_r=pnl_r)
                trades.append(position)
                position = None
            continue

        f, s, a = fast[i], slow[i], atrs[i]
        if f is None or s is None or a is None or a <= 0:
            continue
        prior = rows[i-cfg.breakout_lookback:i]
        vah = max(r.high for r in prior)
        val = min(r.low for r in prior)
        avg_vol = sum(volumes[i-cfg.volume_period:i]) / cfg.volume_period
        volume_ok = row.volume > avg_vol * cfg.volume_multiplier
        slope_up = fast[i-1] is not None and f > fast[i-1]
        slope_down = fast[i-1] is not None and f < fast[i-1]
        accepted_long = all(rows[j].close > max(r.high for r in rows[j-cfg.breakout_lookback:j]) for j in range(i-cfg.acceptance_bars+1, i+1))
        accepted_short = all(rows[j].close < min(r.low for r in rows[j-cfg.breakout_lookback:j]) for j in range(i-cfg.acceptance_bars+1, i+1))
        side = None
        if row.close > vah and f > s and slope_up and volume_ok and accepted_long:
            side = "long"
        elif row.close < val and f < s and slope_down and volume_ok and accepted_short:
            side = "short"
        if not side:
            continue
        entry = row.close
        sl = entry - cfg.stop_atr * a if side == "long" else entry + cfg.stop_atr * a
        risk = abs(entry - sl)
        direction = 1 if side == "long" else -1
        position = {
            "side": side, "entry_time": row.close_time, "entry": entry, "sl": sl,
            "tp1": entry + direction*risk, "tp2": entry + direction*2*risk, "tp3": entry + direction*3*risk,
            "vah": vah, "val": val, "ema50": f, "ema200": s, "atr": a,
            "volume_ratio": row.volume / avg_vol if avg_vol else 0.0,
        }

    return {"strategy": "quant-tpo-breakout-v1", "trades": trades, "summary": _summary(trades)}


def _summary(trades: Sequence[dict]) -> dict:
    rs = [float(t.get("pnl_r", 0.0)) for t in trades]
    wins = [r for r in rs if r > 0]
    losses = [r for r in rs if r <= 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    equity = peak = drawdown = 0.0
    for r in rs:
        equity += r
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return {
        "trades": len(rs), "wins": len(wins), "losses": len(losses),
        "win_rate_pct": round(len(wins)/len(rs)*100, 2) if rs else 0.0,
        "expectancy_r": round(sum(rs)/len(rs), 4) if rs else 0.0,
        "profit_factor": round(gross_win/gross_loss, 3) if gross_loss else (999.0 if gross_win else 0.0),
        "max_drawdown_r": round(drawdown, 3), "net_r": round(sum(rs), 3),
    }
