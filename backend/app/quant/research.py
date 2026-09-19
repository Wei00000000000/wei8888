from __future__ import annotations

from dataclasses import asdict
from math import sqrt
from statistics import mean, pstdev
from typing import Sequence

from sentiment_scanner.binance import Kline
from .engine import QuantConfig, run_quant_v1


def _metrics(trades: Sequence[dict]) -> dict:
    rs = [float(t.get("pnl_r", 0.0)) for t in trades]
    if not rs:
        return {"trades": 0, "net_r": 0.0, "expectancy_r": 0.0, "sharpe_r": 0.0,
                "sortino_r": 0.0, "max_drawdown_r": 0.0, "calmar_r": 0.0}
    mu = mean(rs)
    sd = pstdev(rs) if len(rs) > 1 else 0.0
    downside = [min(0.0, r) for r in rs]
    dsd = sqrt(mean([r*r for r in downside])) if downside else 0.0
    equity = peak = dd = 0.0
    for r in rs:
        equity += r
        peak = max(peak, equity)
        dd = max(dd, peak-equity)
    return {
        "trades": len(rs), "net_r": round(sum(rs), 4), "expectancy_r": round(mu, 4),
        "sharpe_r": round(mu/sd*sqrt(len(rs)), 3) if sd else 0.0,
        "sortino_r": round(mu/dsd*sqrt(len(rs)), 3) if dsd else 0.0,
        "max_drawdown_r": round(dd, 4),
        "calmar_r": round(sum(rs)/dd, 3) if dd else 0.0,
    }


def _curve(trades: Sequence[dict]) -> list[dict]:
    eq = peak = 0.0
    out = []
    for i, t in enumerate(trades, 1):
        eq += float(t.get("pnl_r", 0.0))
        peak = max(peak, eq)
        out.append({"n": i, "time": t.get("exit_time"), "equity_r": round(eq, 4),
                    "drawdown_r": round(eq-peak, 4)})
    return out


def research(rows: Sequence[Kline], config: QuantConfig | None = None) -> dict:
    cfg = config or QuantConfig()
    base = run_quant_v1(rows, cfg)
    trades = base.get("trades", [])
    long_t = [t for t in trades if t.get("side") == "long"]
    short_t = [t for t in trades if t.get("side") == "short"]
    trend_t = [t for t in trades if t.get("regime") == "trend"]
    return {
        **base,
        "risk_metrics": _metrics(trades),
        "equity_curve": _curve(trades),
        "segments": {"long": _metrics(long_t), "short": _metrics(short_t), "trend": _metrics(trend_t)},
        "config": asdict(cfg),
    }


def parameter_sweep(rows: Sequence[Kline]) -> list[dict]:
    results = []
    for stop in (0.25, 0.5, 0.75, 1.0, 1.25, 1.5):
        for volume in (1.0, 1.1, 1.2, 1.3, 1.5):
            for acceptance in (1, 2, 3):
                cfg = QuantConfig(stop_atr=stop, volume_multiplier=volume, acceptance_bars=acceptance)
                result = run_quant_v1(rows, cfg)
                m = _metrics(result.get("trades", []))
                results.append({"stop_atr": stop, "volume_multiplier": volume,
                                "acceptance_bars": acceptance, **m})
    return results


def walk_forward(rows: Sequence[Kline], folds: int = 4) -> list[dict]:
    if len(rows) < 400:
        return []
    chunk = len(rows)//(folds+1)
    out = []
    for fold in range(folds):
        train_end = chunk*(fold+1)
        test_end = min(len(rows), train_end+chunk)
        train = rows[:train_end]
        test = rows[train_end:test_end]
        sweep = parameter_sweep(train)
        viable = [x for x in sweep if x["trades"] >= 3]
        best = max(viable or sweep, key=lambda x: (x["expectancy_r"], -x["max_drawdown_r"]))
        cfg = QuantConfig(stop_atr=best["stop_atr"], volume_multiplier=best["volume_multiplier"],
                          acceptance_bars=best["acceptance_bars"])
        oos = run_quant_v1(test, cfg)
        out.append({"fold": fold+1, "train_bars": len(train), "test_bars": len(test),
                    "params": {k: best[k] for k in ("stop_atr","volume_multiplier","acceptance_bars")},
                    "is_metrics": {k: best[k] for k in ("trades","net_r","expectancy_r","sharpe_r","max_drawdown_r")},
                    "oos_metrics": _metrics(oos.get("trades", []))})
    return out
