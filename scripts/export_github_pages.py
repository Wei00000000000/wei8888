from __future__ import annotations

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from shutil import copyfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sentiment_scanner.binance import BinanceFuturesClient
from sentiment_scanner.bybit import BybitFuturesClient
from sentiment_scanner.market_data import MixedFuturesClient
from sentiment_scanner.okx import OkxFuturesClient
from sentiment_scanner.scanner import ScannerConfig, SentimentScanner
APP_HTML = ROOT / "sentiment_scanner" / "app.html"
SEED = ROOT / "sentiment_scanner" / "seed_signals.json"
CONTRACT_RADAR = ROOT / "sentiment_scanner" / "contract_anomalies.json"
SCANNER_STATUS = ROOT / "sentiment_scanner" / "scanner_status.json"
BRAND_IMAGE = ROOT / "sentiment_scanner" / "brand-hero.png"
OUT = ROOT / "site"
MAIN_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "TONUSDT",
    "TRXUSDT", "BCHUSDT", "LTCUSDT", "DOTUSDT", "NEARUSDT",
    "UNIUSDT", "APTUSDT", "SUIUSDT", "OPUSDT", "ARBUSDT",
]


def provider_name() -> str:
    return os.getenv("MARKET_DATA_PROVIDER", "mixed").strip().lower()


def market_client(timeout: float = 20.0):
    provider = provider_name()
    if provider == "binance":
        return BinanceFuturesClient(timeout=timeout)
    if provider == "bybit":
        return BybitFuturesClient(timeout=timeout)
    if provider == "okx":
        return OkxFuturesClient(timeout=timeout)
    if provider == "mixed":
        return MixedFuturesClient(timeout=timeout)
    return BybitFuturesClient(timeout=timeout)
SECTOR_KEYWORDS = [
    ("Layer 1", ("layer-1", "smart-contract-platform")),
    ("DeFi", ("decentralized-finance", "defi")),
    ("AI", ("artificial-intelligence", "ai")),
    ("Meme", ("meme",)),
    ("Gaming", ("gaming", "gamefi")),
    ("RWA", ("real-world-assets", "rwa")),
    ("Exchange", ("exchange-based", "centralized-exchange")),
    ("Privacy", ("privacy",)),
]
SECTOR_SYMBOLS = {
    "BTC": "比特幣生態", "ETH": "Layer 1", "BNB": "交易所平台", "SOL": "Layer 1", "XRP": "支付網路",
    "DOGE": "Meme", "ADA": "Layer 1", "AVAX": "Layer 1", "LINK": "Oracle", "TON": "Layer 1",
    "TRX": "Layer 1", "BCH": "比特幣生態", "LTC": "支付網路", "DOT": "Layer 1", "NEAR": "Layer 1",
    "UNI": "DeFi", "APT": "Layer 1", "SUI": "Layer 1", "OP": "Layer 2", "ARB": "Layer 2",
    "AAVE": "DeFi", "LDO": "DeFi", "INJ": "DeFi", "RUNE": "DeFi", "FET": "AI", "TAO": "AI",
    "WLD": "AI", "MANA": "Gaming", "SAND": "Gaming", "GALA": "Gaming", "IMX": "Gaming",
}


def load_seed_rows() -> list[dict[str, object]]:
    if not SEED.exists():
        return []
    rows = json.loads(SEED.read_text(encoding="utf-8"))
    return [row for row in rows if isinstance(row, dict)]


def signal_identity(row: dict[str, object]) -> str:
    return str(row.get("id") or row.get("signal_id") or "|".join(
        [
            str(row.get("symbol") or ""),
            str(row.get("setup_id") or ""),
            str(row.get("triggered_at") or row.get("triggered_at_ms") or ""),
        ]
    ))


def is_visible_live_signal(row: dict[str, object]) -> bool:
    state = str(row.get("reached_state") or "holding")
    status = str(row.get("status") or "active")
    return status == "active" and state in {"holding", "tp1", "tp2", "tp3"}


def build_quick_rows(seed_rows: list[dict[str, object]], recent_limit: int = 800) -> list[dict[str, object]]:
    seen: set[str] = set()
    rows: list[dict[str, object]] = []
    for row in [*seed_rows[:recent_limit], *[item for item in seed_rows if is_visible_live_signal(item)]]:
        key = signal_identity(row)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
    return rows


def load_contract_radar() -> dict[str, object]:
    if not CONTRACT_RADAR.exists():
        return {"rows": [], "updated_at": None}
    try:
        data = json.loads(CONTRACT_RADAR.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {"rows": [], "updated_at": None}


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
    config = ScannerConfig(lookback_limit=500, oi_percentile_threshold=99, oi_change_min_pct=3)
    with market_client() as client:
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


def build_sector_flows() -> list[dict[str, object]]:
    try:
        request = Request(
            "https://api.coingecko.com/api/v3/coins/categories",
            headers={"User-Agent": "wei-strategy-room/0.1"},
        )
        with urlopen(request, timeout=20) as response:
            categories = json.loads(response.read().decode("utf-8"))
    except Exception:
        return []

    rows: list[dict[str, object]] = []
    used: set[str] = set()
    for display_name, keywords in SECTOR_KEYWORDS:
        match = next(
            (
                item for item in categories
                if item.get("id") not in used
                and any(keyword in str(item.get("id") or "").lower() or keyword in str(item.get("name") or "").lower() for keyword in keywords)
            ),
            None,
        )
        if not match:
            continue
        used.add(str(match.get("id")))
        rows.append(
            {
                "name": display_name,
                "source": "CoinGecko",
                "category_id": match.get("id"),
                "market_cap": match.get("market_cap") or 0,
                "volume_24h": match.get("volume_24h") or 0,
                "market_cap_change_24h": match.get("market_cap_change_24h") or 0,
                "top_coins": match.get("top_3_coins") or [],
            }
        )
    return rows


def clean_symbol(symbol: object) -> str:
    return str(symbol or "").upper().replace("USDT", "")


def sector_for_symbol(symbol: object) -> str:
    clean = clean_symbol(symbol)
    if clean in SECTOR_SYMBOLS:
        return SECTOR_SYMBOLS[clean]
    if any(key in clean for key in ("PEPE", "BONK", "FLOKI", "SHIB", "DOGE", "WIF", "MEME", "NEIRO", "FART")):
        return "Meme"
    if any(key in clean for key in ("AI", "FET", "TAO", "WLD", "ARKM", "VIRTUAL", "RENDER", "RNDR")):
        return "AI"
    if any(key in clean for key in ("AAVE", "UNI", "LDO", "CRV", "COMP", "MKR", "SNX", "RUNE", "INJ", "PENDLE", "CAKE")):
        return "DeFi"
    if any(key in clean for key in ("SAND", "MANA", "GALA", "IMX", "RON", "PIXEL", "MAGIC", "AXS", "YGG")):
        return "Gaming"
    if any(key in clean for key in ("ARB", "OP", "STRK", "ZK", "MANTA", "METIS")):
        return "Layer 2"
    return "其他板塊"


def signal_tags(symbol: str, seed_rows: list[dict[str, object]]) -> list[str]:
    clean = clean_symbol(symbol)
    rows = [row for row in seed_rows if clean_symbol(row.get("symbol")) == clean][:12]
    tags: list[str] = []
    if any("oi_5m" not in str(row.get("setup_id") or "") and "divergence" not in str(row.get("setup_id") or "") for row in rows):
        tags.append("嘎空/嘎多")
    if rows:
        tags.append("穩如老狗")
    if any("oi_5m" in str(row.get("setup_id") or "") or "divergence" in str(row.get("setup_id") or "") for row in rows):
        tags.append("5M背離")
    return tags


def build_volume_anomalies(seed_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    try:
        with market_client(timeout=25) as client:
            tickers = client.ticker_24hr()
    except Exception:
        return []

    rows: list[dict[str, object]] = []
    for item in tickers:
        symbol = str(item.get("symbol") or "")
        if not symbol.endswith("USDT"):
            continue
        quote_volume = float(item.get("quoteVolume") or 0)
        change = float(item.get("priceChangePercent") or 0)
        trades = float(item.get("count") or item.get("trade_count") or 0)
        if quote_volume <= 0:
            continue
        score = (quote_volume ** 0.5) * (abs(change) + 1) * (1 + min(trades, 500000) / 500000)
        rows.append(
            {
                "symbol": symbol,
                "price": float(item.get("lastPrice") or 0),
                "price_change_pct": change,
                "quote_volume": quote_volume,
                "trade_count": trades,
                "anomaly_score": score,
                "sector": sector_for_symbol(symbol),
                "strategy_tags": signal_tags(symbol, seed_rows),
                "reason": "24H 成交量與波動同步放大",
            }
        )
    return sorted(rows, key=lambda row: row["anomaly_score"], reverse=True)[:40]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "data").mkdir(parents=True, exist_ok=True)
    (OUT / "data" / "history").mkdir(parents=True, exist_ok=True)
    seed_rows = load_seed_rows()
    quick_rows = build_quick_rows(seed_rows)
    contract_radar = load_contract_radar()
    markets = build_markets(seed_rows)
    sector_flows = build_sector_flows()
    volume_anomalies = build_volume_anomalies(seed_rows)
    (OUT / "index.html").write_text(APP_HTML.read_text(encoding="utf-8"), encoding="utf-8")
    if BRAND_IMAGE.exists():
        copyfile(BRAND_IMAGE, OUT / "brand-hero.png")
    (OUT / "data" / "book.json").write_text(
        json.dumps({"rows": quick_rows, "markets": markets}, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    (OUT / "data" / "active_signals.json").write_text(
        json.dumps({"rows": quick_rows, "total": len(seed_rows), "quick": len(quick_rows)}, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    chunk_size = 500
    history_chunks = []
    for index in range(0, len(seed_rows), chunk_size):
        name = f"history/signals-{index // chunk_size:04d}.json"
        chunk_rows = seed_rows[index : index + chunk_size]
        (OUT / "data" / name).write_text(
            json.dumps({"rows": chunk_rows}, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        history_chunks.append(name)
    (OUT / "data" / "markets.json").write_text(
        json.dumps({"rows": markets}, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    (OUT / "data" / "sector_flows.json").write_text(
        json.dumps({"rows": sector_flows}, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    (OUT / "data" / "volume_anomalies.json").write_text(
        json.dumps({"rows": volume_anomalies}, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    (OUT / "data" / "contract_anomalies.json").write_text(
        json.dumps(contract_radar, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    scanner_status = json.loads(SCANNER_STATUS.read_text(encoding="utf-8")) if SCANNER_STATUS.exists() else {}
    (OUT / "data" / "scanner_status.json").write_text(
        json.dumps(scanner_status, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    (OUT / "data" / "manifest.json").write_text(
        json.dumps(
            {
                "active_signals": "active_signals.json",
                "history_chunks": history_chunks,
                "signal_chunks": history_chunks,
                "total_signals": len(seed_rows),
                "quick_signals": len(quick_rows),
                "markets": "markets.json",
                "sector_flows": "sector_flows.json",
                "volume_anomalies": "volume_anomalies.json",
                "contract_anomalies": "contract_anomalies.json",
                "scanner_status": "scanner_status.json",
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    print(f"Exported GitHub Pages site to {OUT}")
    print(f"Signals: {len(seed_rows)} Quick: {len(quick_rows)} Markets: {len(markets)} Sectors: {len(sector_flows)} Volume anomalies: {len(volume_anomalies)} Contract radar: {len(contract_radar.get('rows') or [])}")


if __name__ == "__main__":
    main()
