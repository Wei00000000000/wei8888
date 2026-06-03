from __future__ import annotations

import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sentiment_scanner.binance import BinanceFuturesClient
from sentiment_scanner.scanner import ScannerConfig, SentimentScanner
APP_HTML = ROOT / "sentiment_scanner" / "app.html"
SEED = ROOT / "sentiment_scanner" / "seed_signals.json"
OUT = ROOT / "site"
MAIN_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "TONUSDT",
    "TRXUSDT", "BCHUSDT", "LTCUSDT", "DOTUSDT", "NEARUSDT",
    "UNIUSDT", "APTUSDT", "SUIUSDT", "OPUSDT", "ARBUSDT",
]


def load_seed_rows() -> list[dict[str, object]]:
    if not SEED.exists():
        return []
    rows = json.loads(SEED.read_text(encoding="utf-8"))
    return [row for row in rows if isinstance(row, dict)]


def fallback_market(symbol: str, rows: list[dict[str, object]]) -> dict[str, object]:
    clean = symbol.replace("USDT", "")
    signal = next((row for row in rows if str(row.get("symbol", "")).replace("USDT", "") == clean), None)
    if not signal:
        return {"symbol": symbol, "error": "no fallback"}
    return {
        "symbol": symbol,
        "price": signal.get("trigger_price"),
        "oi_value_usdt": signal.get("oi_value_usdt"),
        "oi_change_pct": signal.get("oi_change_pct"),
        "oi_percentile": signal.get("oi_percentile"),
        "price_change_pct": signal.get("price_change_pct"),
        "taker_buy_ratio": signal.get("taker_buy_ratio"),
        "signal_type": signal.get("signal_type"),
        "fallback": True,
    }


def live_market(symbol: str) -> dict[str, object]:
    config = ScannerConfig(lookback_limit=500, oi_percentile_threshold=99)
    with BinanceFuturesClient() as client:
        scanner = SentimentScanner(client, config)
        klines, oi_points, taker_points = scanner._load(symbol)
        snapshots = scanner._snapshots(symbol, klines, oi_points, taker_points)
        if not snapshots:
            return {"symbol": symbol, "error": "no snapshot"}
        snapshot = snapshots[-1]
        signal = scanner._signal_from_snapshot(snapshot)
        return {
            "symbol": symbol,
            "timestamp_ms": snapshot.timestamp,
            "price": snapshot.price,
            "atr": snapshot.atr,
            "oi_value": snapshot.oi_value,
            "oi_value_usdt": snapshot.oi_value_usdt,
            "oi_change_pct": snapshot.oi_change_pct,
            "oi_percentile": snapshot.oi_percentile,
            "price_change_pct": snapshot.price_change_pct,
            "taker_buy_ratio": snapshot.taker_buy_ratio,
            "signal_type": signal.signal_type if signal else None,
            "setup_id": signal.setup_id if signal else None,
        }


def build_markets(seed_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    markets: dict[str, dict[str, object]] = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(live_market, symbol): symbol for symbol in MAIN_SYMBOLS}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                row = future.result()
            except Exception as exc:
                row = {"symbol": symbol, "error": str(exc)}
            if row.get("error"):
                row = fallback_market(symbol, seed_rows)
            markets[symbol] = row
    return [markets[symbol] for symbol in MAIN_SYMBOLS if symbol in markets]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "data").mkdir(parents=True, exist_ok=True)
    seed_rows = load_seed_rows()
    markets = build_markets(seed_rows)
    (OUT / "index.html").write_text(APP_HTML.read_text(encoding="utf-8"), encoding="utf-8")
    (OUT / "data" / "book.json").write_text(
        json.dumps({"rows": seed_rows, "markets": markets}, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    chunk_size = 100
    chunks = []
    for index in range(0, len(seed_rows), chunk_size):
        name = f"signals-{index // chunk_size:03d}.json"
        chunk_rows = seed_rows[index : index + chunk_size]
        (OUT / "data" / name).write_text(
            json.dumps({"rows": chunk_rows}, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        chunks.append(name)
    (OUT / "data" / "markets.json").write_text(
        json.dumps({"rows": markets}, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    (OUT / "data" / "manifest.json").write_text(
        json.dumps({"signal_chunks": chunks, "markets": "markets.json"}, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"Exported GitHub Pages site to {OUT}")
    print(f"Signals: {len(seed_rows)} Markets: {len(markets)}")


if __name__ == "__main__":
    main()
