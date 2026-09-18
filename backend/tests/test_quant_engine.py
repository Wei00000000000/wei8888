from sentiment_scanner.binance import Kline
from backend.app.quant.engine import QuantConfig, run_quant_v1

def test_quant_v1_returns_metrics():
    rows=[]
    price=100.0
    for i in range(260):
        price *= 1.001
        rows.append(Kline(open_time=i*900000, open=price-0.1, high=price+0.2, low=price-0.2, close=price, volume=1000+i, close_time=(i+1)*900000-1))
    result=run_quant_v1(rows, QuantConfig(volume_multiplier=0.5))
    assert result["strategy"] == "quant-tpo-breakout-v1"
    assert "expectancy_r" in result["summary"]
    assert "max_drawdown_r" in result["summary"]

    assert "avg_mfe_r" in result["summary"]
    assert "avg_mae_r" in result["summary"]
    assert "sl_reasons" in result["summary"]
