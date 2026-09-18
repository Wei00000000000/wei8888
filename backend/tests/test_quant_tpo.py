from sentiment_scanner.binance import Kline
from backend.app.quant.tpo import tpo_profile

def test_tpo_profile_orders_levels():
    rows=[Kline(open_time=i,open=100,high=102+i*.01,low=99,close=101,volume=10,close_time=i+1) for i in range(50)]
    p=tpo_profile(rows)
    assert p is not None
    assert p.val <= p.poc <= p.vah
    assert p.total_tpo > 0
