from __future__ import annotations

import json
import os
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sentiment_scanner.binance import BinanceFuturesClient
from sentiment_scanner.cli import format_signal
from sentiment_scanner.coinglass import CoinGlassClient
from sentiment_scanner.scanner import ScannerConfig, SentimentScanner


SEED = ROOT / "sentiment_scanner" / "seed_signals.json"
CONTRACT_RADAR = ROOT / "sentiment_scanner" / "contract_anomalies.json"
TARGET_ORDER = {"holding": 0, "tp1": 1, "tp2": 2, "tp3": 3, "ftp": 4, "sl": -1}


def load_rows() -> list[dict[str, object]]:
    if not SEED.exists():
        return []
    data = json.loads(SEED.read_text(encoding="utf-8"))
    return [row for row in data if isinstance(row, dict)]


def signal_id(row: dict[str, object]) -> str:
    raw = "|".join(
        [
            str(row.get("symbol") or ""),
            str(row.get("setup_id") or ""),
            str(row.get("triggered_at") or row.get("triggered_at_ms") or ""),
        ]
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, raw))


def normalize_row(row: dict[str, object]) -> dict[str, object]:
    row = dict(row)
    row.setdefault("id", signal_id(row))
    row.setdefault("status", "active")
    row.setdefault("reached_state", "holding")
    row.setdefault("detected_at", datetime.now(timezone.utc).isoformat())
    setup = str(row.get("setup_id") or "")
    if "oi_5m" in setup or "divergence" in setup:
        snapshot = row.get("snapshot_data")
        if not isinstance(snapshot, dict):
            snapshot = {}
        if snapshot.get("divergence_label") not in {"top_divergence", "bottom_divergence"}:
            snapshot["divergence_label"] = "top_divergence" if "bearish" in setup else "bottom_divergence"
        row["snapshot_data"] = snapshot
    return row


def clean_symbol(symbol: object) -> str:
    value = str(symbol or "").strip().upper()
    if value.endswith("USDT"):
        return value
    return f"{value}USDT"


def iso_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def as_float(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def first_float(row: dict[str, object], names: tuple[str, ...], default: float = 0.0) -> float:
    for name in names:
        value = as_float(row.get(name))
        if value is not None:
            return value
    return default


def coinglass_symbol(row: dict[str, object]) -> str:
    raw = str(row.get("symbol") or row.get("coin") or row.get("base_asset") or "").strip().upper()
    raw = raw.replace("-", "").replace("_", "").replace("/", "")
    if raw.endswith("USDT"):
        return raw
    if raw:
        return f"{raw}USDT"
    return ""


def quote_volume_24h(row: dict[str, object]) -> float:
    return first_float(
        row,
        (
            "volume_usd_24h",
            "volume_24h_usd",
            "volume_usd",
            "quote_volume",
            "quoteVolume",
            "turnover_usd_24h",
            "volume",
        ),
    )


def cvd_ratio(row: dict[str, object], suffix: str = "1h") -> float:
    long_vol = first_float(row, (f"long_volume_usd_{suffix}", f"long_vol_usd_{suffix}", f"long_volume_{suffix}"))
    short_vol = first_float(row, (f"short_volume_usd_{suffix}", f"short_vol_usd_{suffix}", f"short_volume_{suffix}"))
    total = long_vol + short_vol
    if total <= 0:
        return 0.0
    return (long_vol - short_vol) / total * 100.0


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def score_contract_market(row: dict[str, object]) -> tuple[int, str, str, str, list[str]]:
    oi = first_float(row, ("open_interest_change_percent_1h", "open_interest_change_percent_15m", "open_interest_change_percent_4h"))
    price = first_float(row, ("price_change_percent_1h", "price_change_percent_15m", "price_change_percent_24h"))
    price24 = first_float(row, ("price_change_percent_24h",))
    funding = first_float(row, ("avg_funding_rate_by_oi", "avg_funding_rate_by_vol"))
    ls_ratio = first_float(row, ("long_short_ratio_1h", "long_short_ratio_15m", "long_short_ratio_4h"))
    long_liq = first_float(row, ("long_liquidation_usd_1h", "long_liq_usd_1h", "long_liquidation_usd_4h", "long_liq_usd_24h"))
    short_liq = first_float(row, ("short_liquidation_usd_1h", "short_liq_usd_1h", "short_liquidation_usd_4h", "short_liq_usd_24h"))
    cvd = cvd_ratio(row, "1h") or cvd_ratio(row, "15m")

    oi_strong = abs(oi) > 3
    reasons: list[str] = []
    if oi > 0 and cvd > 0:
        mkt_label = "多頭建倉"
        score = 40 if oi_strong else 24
    elif oi > 0 and cvd < 0:
        mkt_label = "空頭建倉"
        score = -40 if oi_strong else -24
    elif oi < 0 and cvd > 0:
        mkt_label = "空頭回補"
        score = 16 if oi_strong else 8
    elif oi < 0 and cvd < 0:
        mkt_label = "多頭出場"
        score = -16 if oi_strong else -8
    elif oi > 3 and price > 0:
        mkt_label = "多頭建倉"
        score = 24
    elif oi > 3 and price < 0:
        mkt_label = "空頭建倉"
        score = -24
    elif oi < -3 and price > 0:
        mkt_label = "空頭回補"
        score = 8
    elif oi < -3 and price < 0:
        mkt_label = "多頭出場"
        score = -8
    else:
        mkt_label = "觀察"
        score = 0

    score += round(clamp(price * 0.8, -8, 8))
    score += round(clamp(price24 * 0.48, -4, 4))
    if funding < -0.05:
        score += 12
        reasons.append("冷資金費率")
    elif funding < -0.02:
        score += 6
        reasons.append("資金偏冷")
    elif funding > 0.05:
        score -= 12
        reasons.append("熱資金費率")
    elif funding > 0.02:
        score -= 6
        reasons.append("資金偏熱")

    if ls_ratio > 0:
        long_pct = ls_ratio / (1 + ls_ratio) * 100.0
        if long_pct > 60:
            score -= 4
            reasons.append("多頭擁擠")
        elif long_pct > 55:
            score -= 2
        elif long_pct < 40:
            score += 4
            reasons.append("空頭擁擠")
        elif long_pct < 45:
            score += 2

    if short_liq > long_liq * 2 and short_liq > 0:
        score += 3
        reasons.append("空單清算")
    elif short_liq > long_liq and short_liq > 0:
        score += 2
    elif long_liq > short_liq * 2 and long_liq > 0:
        score -= 3
        reasons.append("多單清算")
    elif long_liq > short_liq and long_liq > 0:
        score -= 2

    score = int(clamp(round(score), -100, 100))
    if score >= 12:
        bias = "long"
        kind = "bull"
    elif score <= -10:
        bias = "short"
        kind = "bear"
    else:
        bias = "neutral"
        kind = "neutral"
    return score, bias, kind, mkt_label, reasons


def contract_quality_score(row: dict[str, object], trigger: str, radar_score: int, min_volume: float) -> tuple[int, list[str]]:
    checks: list[str] = []
    if abs(first_float(row, ("open_interest_change_percent_1h", "open_interest_change_percent_15m", "open_interest_change_percent_4h", "oi_change_1h"))) >= 8:
        checks.append("OI強")
    if abs(cvd_ratio(row, "1h") or first_float(row, ("cvd_ratio_1h",))) >= 5:
        checks.append("CVD明確")
    if abs(first_float(row, ("price_change_percent_5m", "price_change_5m"))) >= 3 or abs(first_float(row, ("price_change_percent_15m", "price_change_15m"))) >= 3:
        checks.append("價格配合")
    if quote_volume_24h(row) >= max(min_volume, 5_000_000):
        checks.append("高流動性")
    if abs(first_float(row, ("avg_funding_rate_by_oi", "avg_funding_rate_by_vol", "funding_rate"))) >= 0.02:
        checks.append("費率極端")
    if abs(radar_score) >= 40 or trigger != "watch":
        checks.append("觸發明確")
    return min(6, len(checks)), checks


def build_contract_radar(coinglass_rows: list[dict[str, object]], min_volume: float) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in coinglass_rows:
        symbol = coinglass_symbol(item)
        if not symbol.endswith("USDT"):
            continue
        volume = quote_volume_24h(item)
        if volume < min_volume:
            continue
        oi_1h = first_float(item, ("open_interest_change_percent_1h", "open_interest_change_percent_15m", "open_interest_change_percent_4h"))
        price_5m = first_float(item, ("price_change_percent_5m",))
        price_15m = first_float(item, ("price_change_percent_15m",))
        if max(abs(oi_1h), abs(price_5m), abs(price_15m)) <= 0:
            continue
        score, bias, kind, mkt_label, reasons = score_contract_market(item)
        trigger = "oi_cross" if abs(oi_1h) >= 8 else "price_5m" if abs(price_5m) >= 3 else "price_15m" if abs(price_15m) >= 3 else "watch"
        quality, quality_checks = contract_quality_score(item, trigger, score, min_volume)
        rows.append(
            {
                "symbol": symbol,
                "score": score,
                "radar_score": score,
                "quality_score": quality,
                "quality_label": "高品質" if quality >= 4 else "一般",
                "quality_checks": quality_checks,
                "bias": bias,
                "kind": kind,
                "trigger": trigger,
                "market_label": mkt_label,
                "price": first_float(item, ("current_price", "price", "last_price")),
                "price_change_5m": price_5m,
                "price_change_15m": price_15m,
                "price_change_1h": first_float(item, ("price_change_percent_1h",)),
                "price_change_24h": first_float(item, ("price_change_percent_24h",)),
                "oi_change_1h": oi_1h,
                "oi_change_15m": first_float(item, ("open_interest_change_percent_15m",)),
                "oi_usd": first_float(item, ("open_interest_usd", "open_interest_value_usd")),
                "volume_24h": volume,
                "cvd_ratio_1h": cvd_ratio(item, "1h"),
                "funding_rate": first_float(item, ("avg_funding_rate_by_oi", "avg_funding_rate_by_vol")),
                "long_short_ratio_1h": first_float(item, ("long_short_ratio_1h", "long_short_ratio_15m")),
                "long_liquidation_1h": first_float(item, ("long_liquidation_usd_1h", "long_liq_usd_1h")),
                "short_liquidation_1h": first_float(item, ("short_liquidation_usd_1h", "short_liq_usd_1h")),
                "reasons": reasons,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return sorted(rows, key=lambda row: (float(row["quality_score"]), abs(float(row["radar_score"])), abs(float(row["oi_change_1h"])), float(row["volume_24h"])), reverse=True)[:160]


def funding_map(rows: list[dict[str, object]]) -> dict[str, float]:
    book: dict[str, float] = {}
    for row in rows:
        symbol = clean_symbol(row.get("symbol"))
        rates: list[float] = []
        for group in ("stablecoin_margin_list", "token_margin_list"):
            values = row.get(group)
            if not isinstance(values, list):
                continue
            for item in values:
                if not isinstance(item, dict):
                    continue
                rate = as_float(item.get("funding_rate"))
                if rate is not None:
                    rates.append(rate)
        if rates:
            book[symbol] = sum(rates) / len(rates)
    return book


def build_contract_radar_from_binance_tickers(
    tickers: list[dict[str, object]],
    funding_rows: list[dict[str, object]],
    min_volume: float,
    short_changes: dict[str, dict[str, float]] | None = None,
) -> list[dict[str, object]]:
    funding = funding_map(funding_rows)
    short_changes = short_changes or {}
    rows: list[dict[str, object]] = []
    now = datetime.now(timezone.utc).isoformat()
    for item in tickers:
        symbol = str(item.get("symbol") or "").upper()
        if not symbol.endswith("USDT"):
            continue
        volume = first_float(item, ("quoteVolume", "quote_volume"))
        if volume < min_volume:
            continue
        price = first_float(item, ("lastPrice", "price"))
        change = first_float(item, ("priceChangePercent", "price_change_percent_24h"))
        short = short_changes.get(symbol, {})
        change_5m = short.get("price_change_5m")
        change_15m = short.get("price_change_15m")
        change_1h = short.get("price_change_1h")
        high = first_float(item, ("highPrice",))
        low = first_float(item, ("lowPrice",))
        open_price = first_float(item, ("openPrice",))
        range_pct = (high - low) / open_price * 100 if open_price > 0 else abs(change)
        fr = funding.get(clean_symbol(symbol), 0.0)
        intraday = change_15m if change_15m is not None else change_1h if change_1h is not None else 0.0
        score = round(clamp(change * 1.15 + intraday * 4.5 + range_pct * (1 if change >= 0 else -1) + (-fr * 450), -100, 100))
        if score >= 12:
            bias, kind = "long", "bull"
        elif score <= -10:
            bias, kind = "short", "bear"
        else:
            bias, kind = "neutral", "neutral"
        trigger = "price_5m" if change_5m is not None and abs(change_5m) >= 3 else "price_15m" if change_15m is not None and abs(change_15m) >= 3 else "price_24h" if abs(change) >= 3 else "funding" if abs(fr) >= 0.02 else "watch"
        temp_row = {
            "price_change_percent_5m": change_5m if change_5m is not None else 0,
            "price_change_percent_15m": change_15m if change_15m is not None else 0,
            "funding_rate": fr,
            "quote_volume": volume,
            "cvd_ratio_1h": 0,
            "oi_change_1h": 0,
        }
        quality, quality_checks = contract_quality_score(temp_row, trigger, score, min_volume)
        rows.append(
            {
                "symbol": symbol,
                "score": score,
                "radar_score": score,
                "quality_score": quality,
                "quality_label": "高品質" if quality >= 4 else "一般",
                "quality_checks": quality_checks,
                "bias": bias,
                "kind": kind,
                "trigger": trigger,
                "market_label": "價格動能" if trigger == "price_24h" else "資金費率擁擠" if trigger == "funding" else "觀察",
                "price": price,
                "price_change_5m": change_5m,
                "price_change_15m": change_15m,
                "price_change_1h": change_1h,
                "price_change_24h": change,
                "oi_change_1h": 0,
                "oi_change_15m": 0,
                "oi_usd": 0,
                "volume_24h": volume,
                "cvd_ratio_1h": 0,
                "funding_rate": fr,
                "long_short_ratio_1h": 0,
                "long_liquidation_1h": 0,
                "short_liquidation_1h": 0,
                "reasons": ["Binance高成交量", "CoinGlass資金費率"] if fr else ["Binance高成交量"],
                "updated_at": now,
            }
        )
    return sorted(rows, key=lambda row: (float(row["quality_score"]), abs(float(row["radar_score"])), abs(float(row["price_change_24h"])), float(row["volume_24h"])), reverse=True)[:160]


def candidate_symbols_from_tickers(tickers: list[dict[str, object]], min_volume: float, limit: int = 180) -> list[str]:
    ranked = sorted(
        (
            item
            for item in tickers
            if str(item.get("symbol") or "").endswith("USDT") and first_float(item, ("quoteVolume",)) >= min_volume
        ),
        key=lambda item: first_float(item, ("quoteVolume",)),
        reverse=True,
    )
    return [str(item.get("symbol")) for item in ranked[:limit]]


def short_kline_changes(symbols: list[str], workers: int = 8) -> dict[str, dict[str, float]]:
    def load(symbol: str) -> tuple[str, dict[str, float]]:
        with BinanceFuturesClient(timeout=12) as client:
            rows = client.klines(symbol, interval="5m", limit=14)
        if len(rows) < 4:
            return symbol, {}
        last = rows[-1].close
        changes: dict[str, float] = {}
        if rows[-2].close:
            changes["price_change_5m"] = (last - rows[-2].close) / rows[-2].close * 100.0
        if len(rows) >= 4 and rows[-4].close:
            changes["price_change_15m"] = (last - rows[-4].close) / rows[-4].close * 100.0
        if len(rows) >= 13 and rows[-13].close:
            changes["price_change_1h"] = (last - rows[-13].close) / rows[-13].close * 100.0
        return symbol, changes

    book: dict[str, dict[str, float]] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(load, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            try:
                symbol, changes = future.result()
            except Exception:
                continue
            if changes:
                book[symbol] = changes
    return book


def recent_active_contract_key(row: dict[str, object]) -> tuple[str, str] | None:
    setup = str(row.get("setup_id") or "")
    if "coinglass_contract" not in setup:
        return None
    if str(row.get("status") or "active") != "active":
        return None
    triggered = row.get("triggered_at") or row.get("detected_at")
    try:
        age = datetime.now(timezone.utc) - datetime.fromisoformat(str(triggered).replace("Z", "+00:00"))
    except Exception:
        return None
    if age.total_seconds() > int(os.getenv("CONTRACT_SIGNAL_COOLDOWN_MINUTES", "60")) * 60:
        return None
    return clean_symbol(row.get("symbol")), str(row.get("signal_type") or "")


def signals_from_contract_radar(radar_rows: list[dict[str, object]], existing: list[dict[str, object]], config: ScannerConfig) -> list[dict[str, object]]:
    active_keys = {key for row in existing if (key := recent_active_contract_key(row)) is not None}
    rows: list[dict[str, object]] = []
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    now_iso = datetime.now(timezone.utc).isoformat()
    max_new = int(os.getenv("CONTRACT_SIGNAL_MAX_NEW", "30"))
    for row in radar_rows:
        bias = str(row.get("bias") or "neutral")
        trigger = str(row.get("trigger") or "watch")
        if bias not in {"long", "short"} or trigger == "watch":
            continue
        signal_type = "reversal_bullish" if bias == "long" else "reversal_bearish"
        symbol = clean_symbol(row.get("symbol"))
        if (symbol, signal_type) in active_keys:
            continue
        price = as_float(row.get("price"))
        if price is None or price <= 0:
            continue
        risk = price * float(os.getenv("CONTRACT_RISK_PCT", "0.028"))
        if signal_type == "reversal_bullish":
            sl, tp1, tp2, tp3, ftp = price - risk, price + risk, price + risk * 2, price + risk * 3, price + risk * 5
        else:
            sl, tp1, tp2, tp3, ftp = price + risk, price - risk, price - risk * 2, price - risk * 3, price - risk * 5
        quality = 0
        if abs(float(row.get("oi_change_1h") or 0)) >= 8:
            quality += 1
        if abs(float(row.get("cvd_ratio_1h") or 0)) >= 5:
            quality += 1
        if abs(float(row.get("price_change_5m") or 0)) >= 3 or abs(float(row.get("price_change_15m") or 0)) >= 3:
            quality += 1
        if float(row.get("volume_24h") or 0) >= 5_000_000:
            quality += 1
        if abs(float(row.get("funding_rate") or 0)) >= 0.02:
            quality += 1
        if abs(float(row.get("score") or 0)) >= 40:
            quality += 1
        rows.append(
            normalize_row(
                {
                    "symbol": symbol,
                    "timeframe": "15M",
                    "signal_type": signal_type,
                    "setup_id": f"coinglass_contract_{bias}_{trigger}",
                    "triggered_at_ms": now_ms,
                    "triggered_at": now_iso,
                    "trigger_price": price,
                    "atr_at_trigger": risk / max(config.atr_risk_multiple, 0.1),
                    "sl_price": sl,
                    "tp1_price": tp1,
                    "tp2_price": tp2,
                    "tp3_price": tp3,
                    "ftp_price": ftp,
                    "risk": risk,
                    "sl_source": "coinglass_risk_pct",
                    "oi_percentile": min(100.0, max(0.0, 88.0 + abs(float(row.get("score") or 0)) / 8)),
                    "oi_change_pct": row.get("oi_change_1h"),
                    "price_change_pct": row.get("price_change_15m") or row.get("price_change_1h"),
                    "taker_buy_ratio": None,
                    "oi_value": 0,
                    "oi_value_usdt": row.get("oi_usd"),
                    "source": "coinglass_contract_scan",
                    "snapshot_data": {
                        "contract_radar": True,
                        "radar_score": row.get("radar_score", row.get("score")),
                        "bias": bias,
                        "trigger": trigger,
                        "market_label": row.get("market_label"),
                        "cvd_ratio_1h": row.get("cvd_ratio_1h"),
                        "funding_rate": row.get("funding_rate"),
                        "long_short_ratio_1h": row.get("long_short_ratio_1h"),
                        "quality_score": quality,
                        "quality_text": "高品質" if quality >= 4 else "一般",
                    },
                }
            )
        )
        active_keys.add((symbol, signal_type))
        if len(rows) >= max_new:
            break
    return rows


def target_order(value: object) -> int:
    return TARGET_ORDER.get(str(value or "holding"), 0)


def hit_price_for_state(row: dict[str, object], state: str) -> float | None:
    field = {
        "sl": "sl_price",
        "tp1": "tp1_price",
        "tp2": "tp2_price",
        "tp3": "tp3_price",
        "ftp": "ftp_price",
    }.get(state)
    return as_float(row.get(field)) if field else None


def state_from_price(row: dict[str, object], price: float) -> str:
    bullish = row.get("signal_type") == "reversal_bullish"

    def hit(value: object) -> bool:
        target = as_float(value)
        return target is not None and (price >= target if bullish else price <= target)

    stop = as_float(row.get("sl_price"))
    if stop is not None and (price <= stop if bullish else price >= stop):
        return "sl"
    for name in ("ftp", "tp3", "tp2", "tp1"):
        if hit(row.get(f"{name}_price")):
            return name
    return str(row.get("reached_state") or "holding")


def update_existing_states(rows: list[dict[str, object]]) -> None:
    active_rows = [
        row
        for row in rows
        if str(row.get("status") or "active") == "active"
        and str(row.get("reached_state") or "holding") in {"holding", "tp1", "tp2", "tp3"}
    ]
    symbols = sorted({clean_symbol(row.get("symbol")) for row in active_rows})
    if not symbols:
        return
    try:
        with BinanceFuturesClient(timeout=20) as client:
            prices = client.ticker_price(symbols)
    except Exception as exc:
        print(f"WARN price state update skipped: {exc}")
        return
    now = datetime.now(timezone.utc).isoformat()
    for row in active_rows:
        price = prices.get(clean_symbol(row.get("symbol")))
        if price is None:
            continue
        prev = str(row.get("reached_state") or "holding")
        next_state = state_from_price(row, price)
        if next_state == prev or next_state == "holding":
            continue
        if next_state in {"tp1", "tp2", "tp3", "ftp"} and target_order(next_state) < target_order(prev):
            continue
        if next_state == "sl" and target_order(prev) > 0:
            continue
        row["reached_state"] = next_state
        row["hit_price"] = hit_price_for_state(row, next_state)
        row["hit_at"] = now
        if next_state in {"sl", "ftp"}:
            row["status"] = "closed"


def format_divergence_signal(symbol: str, snapshot: object, config: ScannerConfig) -> dict[str, object] | None:
    oi_change = float(getattr(snapshot, "oi_change_pct"))
    price_change = float(getattr(snapshot, "price_change_pct"))
    oi_percentile = float(getattr(snapshot, "oi_percentile"))
    if oi_percentile < config.oi_percentile_threshold:
        return None
    if oi_change == 0 or price_change == 0 or (oi_change > 0) == (price_change > 0):
        return None

    price = float(getattr(snapshot, "price"))
    atr_value = float(getattr(snapshot, "atr"))
    raw_risk = config.atr_risk_multiple * atr_value
    capped_risk = price * config.max_risk_pct
    risk = min(raw_risk, capped_risk)
    sl_source = "atr" if risk == raw_risk else "capped_10pct"

    if price_change < 0 and oi_change > 0:
        signal_type = "reversal_bullish"
        setup_id = "oi_5m_bullish_divergence"
        divergence_name = "bottom_divergence"
        sl = price - risk
        tp1 = price + risk
        tp2 = price + risk * 2
        tp3 = price + risk * 3
        ftp = price + risk * 5
    else:
        signal_type = "reversal_bearish"
        setup_id = "oi_5m_bearish_divergence"
        divergence_name = "top_divergence"
        sl = price + risk
        tp1 = price - risk
        tp2 = price - risk * 2
        tp3 = price - risk * 3
        ftp = price - risk * 5

    row = {
        "symbol": symbol,
        "timeframe": "5M",
        "signal_type": signal_type,
        "setup_id": setup_id,
        "triggered_at_ms": int(getattr(snapshot, "timestamp")),
        "triggered_at": iso_ms(int(getattr(snapshot, "timestamp"))),
        "trigger_price": price,
        "atr_at_trigger": atr_value,
        "sl_price": sl,
        "tp1_price": tp1,
        "tp2_price": tp2,
        "tp3_price": tp3,
        "ftp_price": ftp,
        "risk": risk,
        "sl_source": sl_source,
        "oi_percentile": oi_percentile,
        "oi_change_pct": oi_change,
        "price_change_pct": price_change,
        "taker_buy_ratio": getattr(snapshot, "taker_buy_ratio"),
        "oi_value": float(getattr(snapshot, "oi_value")),
        "oi_value_usdt": getattr(snapshot, "oi_value_usdt"),
        "snapshot_data": {
            "divergence": "price_down_oi_up" if price_change < 0 and oi_change > 0 else "price_up_oi_down",
            "divergence_label": divergence_name,
            "interval": "5m",
        },
    }
    return normalize_row(row)


def apply_5m_confluence(row: dict[str, object], snapshot: object) -> dict[str, object]:
    signal_type = str(row.get("signal_type") or "")
    oi_change = float(getattr(snapshot, "oi_change_pct"))
    price_change = float(getattr(snapshot, "price_change_pct"))
    oi_percentile = float(getattr(snapshot, "oi_percentile"))
    bullish_5m = oi_change > 0 and price_change > 0
    bearish_5m = oi_change < 0 and price_change <= 0
    confluence = (
        (signal_type == "reversal_bullish" and bullish_5m)
        or (signal_type == "reversal_bearish" and bearish_5m)
    )
    snapshot_data = row.get("snapshot_data")
    if not isinstance(snapshot_data, dict):
        snapshot_data = {}
    snapshot_data.update(
        {
            "mtf_5m_confluence": confluence,
            "mtf_5m_oi_change_pct": oi_change,
            "mtf_5m_price_change_pct": price_change,
            "mtf_5m_oi_percentile": oi_percentile,
        }
    )
    row["mtf_5m_confluence"] = confluence
    row["mtf_5m_oi_change_pct"] = oi_change
    row["mtf_5m_price_change_pct"] = price_change
    row["mtf_5m_oi_percentile"] = oi_percentile
    row["snapshot_data"] = snapshot_data
    return row


def scan_symbol(symbol: str, config: ScannerConfig, divergence_config: ScannerConfig) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with BinanceFuturesClient(timeout=20) as client:
        scanner = SentimentScanner(client, config)
        signal = scanner.latest_signal(symbol)

        divergence_scanner = SentimentScanner(client, divergence_config)
        klines, oi_points, taker_points = divergence_scanner._load(symbol)
        snapshots = divergence_scanner._snapshots(symbol, klines, oi_points, taker_points)
        if signal is not None:
            row = normalize_row(format_signal(signal))
            if snapshots:
                row = apply_5m_confluence(row, snapshots[-1])
            rows.append(row)
        if snapshots:
            row = format_divergence_signal(symbol, snapshots[-1], divergence_config)
            if row is not None:
                rows.append(row)
    return rows


def resolve_symbols() -> list[str]:
    top = int(os.getenv("SCAN_TOP", "0") or "0")
    min_volume = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "5000000"))
    try:
        with BinanceFuturesClient(timeout=20) as client:
            return client.symbols_by_volume(limit=top, min_quote_volume=min_volume)
    except Exception as exc:
        existing_symbols = sorted({clean_symbol(row.get("symbol")) for row in load_rows() if row.get("symbol")})
        fallback_limit = int(os.getenv("SCAN_FALLBACK_LIMIT", "120"))
        if existing_symbols:
            print(f"WARN symbol discovery failed, using {min(len(existing_symbols), fallback_limit)} seed symbols: {exc}")
            return existing_symbols[:fallback_limit]
        raise


def main() -> None:
    existing = [normalize_row(row) for row in load_rows()]
    min_volume = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "5000000"))
    config = ScannerConfig(
        lookback_limit=int(os.getenv("LOOKBACK_LIMIT", "500")),
        oi_percentile_threshold=float(os.getenv("OI_PERCENTILE", "99")),
        atr_risk_multiple=float(os.getenv("ATR_MULTIPLE", "2.5")),
        eval_window_hours=float(os.getenv("EVAL_HOURS", "6")),
    )
    divergence_config = ScannerConfig(
        interval="5m",
        lookback_limit=int(os.getenv("DIVERGENCE_LOOKBACK_LIMIT", "500")),
        oi_percentile_threshold=float(os.getenv("DIVERGENCE_OI_PERCENTILE", "95")),
        atr_risk_multiple=float(os.getenv("DIVERGENCE_ATR_MULTIPLE", os.getenv("ATR_MULTIPLE", "2.5"))),
        eval_window_hours=float(os.getenv("EVAL_HOURS", "6")),
    )
    workers = int(os.getenv("SCAN_WORKERS", "8"))
    contract_radar: list[dict[str, object]] = []
    try:
        contract_radar = build_contract_radar(CoinGlassClient(timeout=20).coins_markets(), min_volume)
        if not contract_radar:
            with BinanceFuturesClient(timeout=20) as client:
                tickers = client.ticker_24hr()
                changes = short_kline_changes(candidate_symbols_from_tickers(tickers, min_volume), workers=8)
                contract_radar = build_contract_radar_from_binance_tickers(
                    tickers,
                    CoinGlassClient(timeout=20).funding_rates(),
                    min_volume,
                    changes,
                )
        CONTRACT_RADAR.write_text(
            json.dumps({"rows": contract_radar, "updated_at": datetime.now(timezone.utc).isoformat(), "min_volume_usdt": min_volume}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"coinglass_contract_rows={len(contract_radar)}")
    except Exception as exc:
        print(f"WARN coinglass contract radar skipped: {exc}")
        try:
            with BinanceFuturesClient(timeout=20) as client:
                tickers = client.ticker_24hr()
                changes = short_kline_changes(candidate_symbols_from_tickers(tickers, min_volume), workers=8)
                contract_radar = build_contract_radar_from_binance_tickers(tickers, [], min_volume, changes)
                CONTRACT_RADAR.write_text(
                    json.dumps({"rows": contract_radar, "updated_at": datetime.now(timezone.utc).isoformat(), "warning": str(exc), "min_volume_usdt": min_volume}, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                print(f"fallback_contract_rows={len(contract_radar)}")
        except Exception as fallback_exc:
            print(f"WARN fallback contract radar skipped: {fallback_exc}")
        if not CONTRACT_RADAR.exists():
            CONTRACT_RADAR.write_text(
                json.dumps({"rows": [], "updated_at": datetime.now(timezone.utc).isoformat(), "error": str(exc), "min_volume_usdt": min_volume}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    symbols = resolve_symbols()
    found: list[dict[str, object]] = []
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scan_symbol, symbol, config, divergence_config): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                rows = future.result()
            except Exception as exc:
                errors.append(f"{symbol}: {exc}")
                continue
            found.extend(rows)
    if contract_radar:
        contract_signals = signals_from_contract_radar(contract_radar, existing + found, config)
        found.extend(contract_signals)
        print(f"coinglass_contract_signals={len(contract_signals)}")

    by_id = {str(row.get("id") or signal_id(row)): row for row in existing}
    new_count = 0
    for row in found:
        row_id = str(row.get("id") or signal_id(row))
        if row_id not in by_id:
            new_count += 1
        by_id[row_id] = row
    rows_for_state = list(by_id.values())
    update_existing_states(rows_for_state)

    rows = sorted(
        rows_for_state,
        key=lambda row: str(row.get("triggered_at") or row.get("detected_at") or ""),
        reverse=True,
    )
    max_rows = int(os.getenv("MAX_SEED_ROWS", "3000"))
    SEED.write_text(json.dumps(rows[:max_rows], ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"symbols={len(symbols)} found={len(found)} new={new_count} saved={min(len(rows), max_rows)} errors={len(errors)}")
    for error in errors[:20]:
        print(f"ERROR {error}")


if __name__ == "__main__":
    main()
