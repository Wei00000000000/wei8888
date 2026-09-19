from backend.app.quant.research import _metrics, _curve


def test_research_metrics_and_curve():
    trades = [
        {"pnl_r": 1.0, "exit_time": 1},
        {"pnl_r": -0.5, "exit_time": 2},
        {"pnl_r": 2.0, "exit_time": 3},
    ]
    metrics = _metrics(trades)
    assert metrics["trades"] == 3
    assert metrics["net_r"] == 2.5
    assert metrics["max_drawdown_r"] == 0.5
    curve = _curve(trades)
    assert len(curve) == 3
    assert curve[-1]["equity_r"] == 2.5
    assert curve[1]["drawdown_r"] == -0.5


def test_empty_metrics_are_safe():
    metrics = _metrics([])
    assert metrics["trades"] == 0
    assert metrics["expectancy_r"] == 0.0
