from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from shutil import copyfile
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sentiment_scanner.bybit_okx import BybitOkxFuturesClient
from sentiment_scanner.market_data import market_client
from sentiment_scanner.scanner import ScannerConfig, SentimentScanner
APP_HTML = ROOT / "sentiment_scanner" / "app.html"
SEED = ROOT / "sentiment_scanner" / "seed_signals.json"
CONTRACT_RADAR = ROOT / "sentiment_scanner" / "contract_anomalies.json"
SCANNER_STATUS = ROOT / "sentiment_scanner" / "scanner_status.json"
SCANNER_API_TRACE = ROOT / "sentiment_scanner" / "scanner_api_trace.json"
BRAND_IMAGE = ROOT / "sentiment_scanner" / "brand-hero.png"
OUT = ROOT / "site"
PROTECTED_HISTORY_START_AT = "2026-09-09T00:00:00+08:00"

MAIN_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "TONUSDT",
    "TRXUSDT", "BCHUSDT", "LTCUSDT", "DOTUSDT", "NEARUSDT",
    "UNIUSDT", "APTUSDT", "SUIUSDT", "OPUSDT", "ARBUSDT",
]


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


def parse_time(value: object) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def protected_history_cutoff() -> datetime:
    return datetime.fromisoformat(PROTECTED_HISTORY_START_AT.replace("Z", "+00:00"))


def signal_time(row: dict[str, object]) -> datetime | None:
    return parse_time(row.get("triggered_at") or row.get("established_at") or row.get("detected_at"))


def keep_protected_history(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    cutoff = protected_history_cutoff()
    return [row for row in rows if (started := signal_time(row)) and started >= cutoff]


def load_seed_rows() -> list[dict[str, object]]:
    if SEED.exists():
        rows = json.loads(SEED.read_text(encoding="utf-8") or "[]")
        if rows:
            return keep_protected_history([row for row in rows if isinstance(row, dict)])
    return keep_protected_history(load_exported_history_rows())


def load_exported_history_rows() -> list[dict[str, object]]:
    data_root = ROOT / "data"
    manifest_path = data_root / "manifest.json"
    chunk_names: list[str] = []
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            chunk_names = [str(name) for name in (manifest.get("history_chunks") or manifest.get("signal_chunks") or [])]
        except Exception:
            chunk_names = []
    if not chunk_names:
        chunk_names = [str(path.relative_to(data_root)) for path in sorted((data_root / "history").glob("signals-*.json"))]
    rows: list[dict[str, object]] = []
    for name in chunk_names:
        try:
            payload = json.loads((data_root / name).read_text(encoding="utf-8"))
            chunk_rows = payload.get("rows") if isinstance(payload, dict) else payload
            rows.extend(row for row in chunk_rows if isinstance(row, dict))
        except Exception:
            continue
    if rows:
        print(f"Recovered {len(rows)} signals from exported history")
    return rows


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


def as_float(value: object, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def signal_side(row: dict[str, object]) -> str:
    return "long" if row.get("signal_type") == "reversal_bullish" else "short"


def signal_position_status(row: dict[str, object]) -> str:
    state = str(row.get("reached_state") or "holding")
    status = str(row.get("status") or "active")
    if state in {"sl", "ftp"} or status == "closed":
        return "CLOSED"
    return "OPEN"


def signal_pnl_pct(row: dict[str, object]) -> float:
    entry = as_float(row.get("entry_price") or row.get("trigger_price"))
    sl = as_float(row.get("sl_price"))
    if entry <= 0 or sl <= 0:
        return 0.0
    risk_pct = abs(entry - sl) / entry * 100
    state = str(row.get("reached_state") or "holding")
    if state == "sl":
        return -risk_pct
    if state == "tp1":
        return risk_pct
    if state == "tp2":
        return risk_pct * 1.7
    if state == "tp3":
        return risk_pct * 2.3
    if state == "ftp":
        return risk_pct * 3.3
    return 0.0


def build_positions(seed_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in seed_rows:
        entry = as_float(row.get("entry_price") or row.get("trigger_price"))
        if not row.get("symbol") or entry <= 0:
            continue
        state = str(row.get("reached_state") or "holding")
        exit_price = ""
        if state == "sl":
            exit_price = row.get("sl_price")
        elif state == "ftp":
            exit_price = row.get("ftp_price")
        elif state == "tp3":
            exit_price = row.get("tp3_price")
        elif state == "tp2":
            exit_price = row.get("tp2_price")
        elif state == "tp1":
            exit_price = row.get("tp1_price")
        rows.append(
            {
                "id": row.get("id") or row.get("signal_id") or signal_identity(row),
                "signal_id": row.get("signal_id") or row.get("id") or signal_identity(row),
                "symbol": str(row.get("symbol") or "").replace("USDT", ""),
                "side": signal_side(row),
                "timeframe": row.get("timeframe") or row.get("interval") or "-",
                "strategy_name": row.get("setup_id") or row.get("strategy_name") or "signal",
                "status": signal_position_status(row),
                "entry_price": entry,
                "stop_loss": row.get("active_sl_price") or row.get("sl_price"),
                "take_profit_1": row.get("tp1_price"),
                "take_profit_2": row.get("tp2_price"),
                "take_profit_3": row.get("tp3_price"),
                "take_profit_final": row.get("ftp_price"),
                "exit_price": exit_price if signal_position_status(row) == "CLOSED" else "",
                "pnl_percent": signal_pnl_pct(row),
                "entry_time": row.get("triggered_at") or row.get("established_at") or row.get("detected_at"),
                "exit_time": row.get("hit_at") if signal_position_status(row) == "CLOSED" else "",
                "reached_state": state,
            }
        )
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


async def live_market(client: BybitOkxFuturesClient, symbol: str) -> dict[str, object]:
    config = ScannerConfig(lookback_limit=500, oi_percentile_threshold=99, oi_change_min_pct=3)
    scanner = SentimentScanner(client, config)
    klines, oi_points, taker_points = await scanner._load(symbol)
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


async def build_markets(seed_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    markets: dict[str, dict[str, object]] = {}
    async with market_client(timeout=30) as client:
        results = await asyncio.gather(
            *(live_market(client, symbol) for symbol in MAIN_SYMBOLS),
            return_exceptions=True,
        )
    for symbol, result in zip(MAIN_SYMBOLS, results):
        if isinstance(result, Exception):
            row = {"symbol": symbol, "error": str(result)}
        else:
            row = result
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


async def build_volume_anomalies(seed_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    try:
        async with market_client(timeout=25) as client:
            tickers = await client.ticker_24hr()
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


async def main_async() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "data").mkdir(parents=True, exist_ok=True)
    (OUT / "data" / "history").mkdir(parents=True, exist_ok=True)
    seed_rows = load_seed_rows()
    quick_rows = build_quick_rows(seed_rows)
    positions = build_positions(seed_rows)
    contract_radar = load_contract_radar()
    markets = await build_markets(seed_rows)
    sector_flows = build_sector_flows()
    volume_anomalies = await build_volume_anomalies(seed_rows)
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
    (OUT / "data" / "positions.json").write_text(
        json.dumps({"rows": positions, "total": len(positions), "history_start_at": PROTECTED_HISTORY_START_AT}, ensure_ascii=False, separators=(",", ":")),
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
    manifest_extra: dict[str, str] = {}
    if SCANNER_API_TRACE.exists():
        trace_payload = json.loads(SCANNER_API_TRACE.read_text(encoding="utf-8"))
        (OUT / "data" / "scanner_api_trace.json").write_text(
            json.dumps(trace_payload, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        manifest_extra["scanner_api_trace"] = "scanner_api_trace.json"
    (OUT / "data" / "manifest.json").write_text(
        json.dumps(
            {
                "active_signals": "active_signals.json",
                "positions": "positions.json",
                "history_start_at": PROTECTED_HISTORY_START_AT,
                "history_chunks": history_chunks,
                "signal_chunks": history_chunks,
                "total_signals": len(seed_rows),
                "quick_signals": len(quick_rows),
                "markets": "markets.json",
                "sector_flows": "sector_flows.json",
                "volume_anomalies": "volume_anomalies.json",
                "contract_anomalies": "contract_anomalies.json",
                "scanner_status": "scanner_status.json",
                **manifest_extra,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    print(f"Exported GitHub Pages site to {OUT}")
    print(f"Signals: {len(seed_rows)} Quick: {len(quick_rows)} Markets: {len(markets)} Sectors: {len(sector_flows)} Volume anomalies: {len(volume_anomalies)} Contract radar: {len(contract_radar.get('rows') or [])}")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
