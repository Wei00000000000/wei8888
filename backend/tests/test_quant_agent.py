from backend.app.quant.agent import _score, diagnose, propose
from backend.app.quant.engine import QuantConfig


def test_score_penalizes_tiny_samples():
    assert _score({"trades": 1, "expectancy_r": 3, "sharpe_r": 5, "max_drawdown_r": 0}) == -999.0


def test_diagnose_returns_interpretable_findings():
    trades = [
        {"side": "long", "pnl_r": -1, "exit_reason": "SL", "sl_reason": "high_va_overlap", "va_overlap": .8, "volume_ratio": 1.1, "mfe_r": .2, "mae_r": 1},
        {"side": "long", "pnl_r": -1, "exit_reason": "SL", "sl_reason": "high_va_overlap", "va_overlap": .7, "volume_ratio": 1.1, "mfe_r": .3, "mae_r": 1},
        {"side": "short", "pnl_r": -1, "exit_reason": "SL", "sl_reason": "high_va_overlap", "va_overlap": .6, "volume_ratio": 1.2, "mfe_r": .2, "mae_r": 1},
        {"side": "long", "pnl_r": 3, "exit_reason": "TP3", "va_overlap": .2, "volume_ratio": 1.4, "mfe_r": 3, "mae_r": .2},
        {"side": "short", "pnl_r": 3, "exit_reason": "TP3", "va_overlap": .2, "volume_ratio": 1.5, "mfe_r": 3, "mae_r": .2},
        {"side": "short", "pnl_r": 3, "exit_reason": "TP3", "va_overlap": .2, "volume_ratio": 1.4, "mfe_r": 3, "mae_r": .2},
    ]
    d = diagnose(trades)
    assert d["sl_reasons"]["high_va_overlap"] == 3
    assert any("overlap" in x.lower() for x in d["findings"])


def test_proposals_are_bounded_and_explainable():
    p = propose(QuantConfig(), {})
    assert len(p) >= 5
    assert all(x["id"] and x["hypothesis"] for x in p)
    assert all(.25 <= x["config"]["stop_atr"] <= 1.5 for x in p)
