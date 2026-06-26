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
from sentiment_scanner.binance import BINANCE_FAPI, BINANCE_FDATA
from sentiment_scanner.bybit import BybitFuturesClient
from sentiment_scanner.cli import format_signal
from sentiment_scanner.coinglass import CoinGlassClient
from sentiment_scanner.market_data import MixedFuturesClient
from sentiment_scanner.okx import OkxFuturesClient
from sentiment_scanner.scanner import ScannerConfig, SentimentScanner


SEED = ROOT / "sentiment_scanner" / "seed_signals.json"
CONTRACT_RADAR = ROOT / "sentiment_scanner" / "contract_anomalies.json"
SCANNER_STATUS = ROOT / "sentiment_scanner" / "scanner_status.json"
TARGET_ORDER = {"holding": 0, "tp1": 1, "tp2": 2, "tp3": 3, "ftp": 4, "sl": -1}


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


def previous_success_at() -> str | None:
    try:
        data = json.loads(SCANNER_STATUS.read_text(encoding="utf-8"))
        value = data.get("last_success_at") or data.get("updated_at")
        return str(value) if value else None
    except Exception:
        return None


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


def raw_number(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def metric_value(row: dict[str, object], name: str, default: object = None) -> object:
    if row.get(name) is not None:
        return row.get(name)
    snapshot = row.get("snapshot_data")
    if isinstance(snapshot, dict) and snapshot.get(name) is not None:
        return snapshot.get(name)
    return default


def classify_trade_layer(row: dict[str, object]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    blocks: list[str] = []
    snapshot = row.get("snapshot_data")
    if not isinstance(snapshot, dict):
        snapshot = {}
    score = raw_number(row.get("score"), raw_number(snapshot.get("score"), raw_number(snapshot.get("oi_percentile"), 0)))
    entry = raw_number(row.get("entry_price") or row.get("trigger_price"), 0)
    sl = raw_number(row.get("sl_price"), 0)
    risk = abs(entry - sl) / entry * 100 if entry > 0 and sl > 0 else 0
    volume = raw_number(metric_value(row, "volume_24h", metric_value(row, "quote_volume", 0)), 0)
    price_move = abs(raw_number(metric_value(row, "price_change_pct", 0), 0))
    setup = str(row.get("setup_id") or "")
    if score < 60:
        reasons.append("score_below_60_warning_only")
    if 60 <= score < 70 and not any(metric_value(row, key) in (True, "true", 1, "1") for key in ("mtf_5m_confluence", "mtf_15m_confluence", "mtf_5m_oi_confluence", "mtf_15m_oi_confluence")):
        reasons.append("score_60_69_requires_5m_15m_alignment")
    if score < 70:
        reasons.append("score_below_official_threshold")
    if entry <= 0 or sl <= 0:
        blocks.append("missing_entry_or_sl")
    elif risk < 0.5 or risk > 5:
        reasons.append("risk_distance_outside_0_5_to_5_pct")
    if volume > 0 and volume < 5_000_000:
        reasons.append("volume_below_5m_warning_only")
    if price_move > 5:
        reasons.append("chasing_price_move_too_large")
    if "cvd_5m" in setup or "divergence" in setup:
        engine = metric_value(row, "entry_engine", {})
        confirmed = isinstance(engine, dict) and (engine.get("formal") or raw_number(engine.get("timing_score"), 0) >= 65)
        if not confirmed:
            reasons.append("cvd_divergence_needs_price_vwap_confirmation")
    if blocks:
        return "filtered_out", blocks + reasons
    if reasons:
        return "warning", reasons
    return "official_trade", []


def apply_trade_layer(row: dict[str, object]) -> None:
    layer, reasons = classify_trade_layer(row)
    row["raw_signal"] = True
    row["trade_layer"] = layer
    row["official_trade"] = layer == "official_trade"
    row["warning"] = layer == "warning"
    row["filtered_out"] = layer == "filtered_out"
    row["trade_filter_reason"] = "; ".join(reasons)


def normalize_row(row: dict[str, object]) -> dict[str, object]:
    row = dict(row)
    triggered_at_ms = row.get("triggered_at_ms")
    triggered_at = row.get("triggered_at")
    if not triggered_at_ms and triggered_at:
        try:
            parsed = datetime.fromisoformat(str(triggered_at).replace("Z", "+00:00"))
            triggered_at_ms = int(parsed.timestamp() * 1000)
            row["triggered_at_ms"] = triggered_at_ms
        except (TypeError, ValueError):
            pass
    if triggered_at_ms:
        row["triggered_at"] = iso_ms(int(triggered_at_ms))
        row.setdefault("established_at", row["triggered_at"])
    row.setdefault("id", signal_id(row))
    row.setdefault("signal_id", row["id"])
    entry = as_float(row.get("entry_price"))
    if entry is None:
        entry = as_float(row.get("trigger_price"))
        if entry is not None:
            row["entry_price"] = entry
    elif row.get("trigger_price") is None:
        row["trigger_price"] = entry
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
    apply_trade_layer(row)
    return row


def valid_signal_time(row: dict[str, object]) -> bool:
    triggered = row.get("triggered_at")
    detected = row.get("detected_at")
    if not triggered or not detected:
        return True
    try:
        triggered_at = datetime.fromisoformat(str(triggered).replace("Z", "+00:00"))
        detected_at = datetime.fromisoformat(str(detected).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return True
    return triggered_at <= detected_at


def is_legacy_oi_divergence(row: dict[str, object]) -> bool:
    return str(row.get("setup_id") or "").startswith("oi_5m_")


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
    market_details: dict[str, dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    funding = funding_map(funding_rows)
    short_changes = short_changes or {}
    market_details = market_details or {}
    rows: list[dict[str, object]] = []
    observed_at_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    now = iso_ms(observed_at_ms)
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
        detail = market_details.get(symbol, {})
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
        oi_1h = as_float(detail.get("open_interest_change_percent_1h"))
        cvd_1h = as_float(detail.get("cvd_ratio_1h"))
        retail_ratio = as_float(detail.get("long_short_ratio_1h"))
        top_position_ratio = as_float(detail.get("top_position_long_short_ratio_1h"))
        if oi_1h is not None:
            score += round(clamp(oi_1h * 2.0, -18, 18))
        if cvd_1h is not None:
            score += round(clamp(cvd_1h * 0.7, -15, 15))
        # Crowded ratios are contrarian inputs; top-position ratio has a smaller trend-following weight.
        if retail_ratio is not None:
            score += -6 if retail_ratio >= 1.5 else 6 if retail_ratio <= 0.67 else 0
        if top_position_ratio is not None:
            score += 3 if top_position_ratio >= 1.3 else -3 if top_position_ratio <= 0.77 else 0
        score = round(clamp(score, -100, 100))
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
            "cvd_ratio_1h": detail.get("cvd_ratio_1h"),
            "oi_change_1h": detail.get("open_interest_change_percent_1h"),
            "long_short_ratio_1h": detail.get("long_short_ratio_1h"),
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
                "trigger_price": short.get("last_completed_price")
                if trigger in {"price_5m", "price_15m"}
                else price,
                "price_change_5m": change_5m,
                "price_change_15m": change_15m,
                "price_change_1h": change_1h,
                "price_change_24h": change,
                "oi_change_1h": detail.get("open_interest_change_percent_1h"),
                "oi_change_15m": detail.get("open_interest_change_percent_15m"),
                "oi_usd": detail.get("open_interest_usd"),
                "volume_24h": volume,
                "cvd_ratio_1h": detail.get("cvd_ratio_1h"),
                "funding_rate": fr,
                "long_short_ratio_1h": detail.get("long_short_ratio_1h"),
                "top_account_long_short_ratio_1h": detail.get("top_account_long_short_ratio_1h"),
                "top_position_long_short_ratio_1h": detail.get("top_position_long_short_ratio_1h"),
                "long_liquidation_1h": detail.get("long_liquidation_usd_1h"),
                "short_liquidation_1h": detail.get("short_liquidation_usd_1h"),
                "reasons": ["Binance高成交量", "CoinGlass資金費率"] if fr else ["Binance高成交量"],
                "updated_at": now,
                "triggered_at_ms": int(short.get("last_completed_at_ms") or observed_at_ms)
                if trigger in {"price_5m", "price_15m"}
                else observed_at_ms,
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
        with market_client(timeout=12) as client:
            rows = client.klines(symbol, interval="5m", limit=14)
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        rows = [row for row in rows if row.close_time <= now_ms]
        if len(rows) < 13:
            return symbol, {}
        last = rows[-1].close
        changes: dict[str, float] = {
            "last_completed_at_ms": float(rows[-1].close_time + 1),
            "last_completed_price": last,
        }
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


def _kline_value(kline: object, name: str, default: float = 0.0) -> float:
    value = getattr(kline, name, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _swing_points(klines: list[object], field: str, lookback: int = 2) -> list[tuple[int, float]]:
    points: list[tuple[int, float]] = []
    if len(klines) < lookback * 2 + 3:
        return points
    for index in range(lookback, len(klines) - lookback):
        value = _kline_value(klines[index], field)
        left = [_kline_value(klines[i], field) for i in range(index - lookback, index)]
        right = [_kline_value(klines[i], field) for i in range(index + 1, index + lookback + 1)]
        if field == "high" and all(value > item for item in left + right):
            points.append((index, value))
        if field == "low" and all(value < item for item in left + right):
            points.append((index, value))
    return points


def _rolling_vwap(klines: list[object], window: int) -> float | None:
    sample = klines[-window:]
    numerator = 0.0
    denominator = 0.0
    for item in sample:
        high = _kline_value(item, "high")
        low = _kline_value(item, "low")
        close = _kline_value(item, "close")
        volume = _kline_value(item, "volume")
        typical = (high + low + close) / 3.0
        numerator += typical * volume
        denominator += volume
    return numerator / denominator if denominator > 0 else None


def _structure_snapshot(klines: list[object], side: str) -> dict[str, object]:
    if len(klines) < 24:
        return {"ready": False, "reason": "kline_insufficient"}
    last = klines[-1]
    close = _kline_value(last, "close")
    high = _kline_value(last, "high")
    low = _kline_value(last, "low")
    highs = _swing_points(klines[:-1], "high")
    lows = _swing_points(klines[:-1], "low")
    prev_high = highs[-1][1] if highs else max(_kline_value(k, "high") for k in klines[-21:-1])
    prev_low = lows[-1][1] if lows else min(_kline_value(k, "low") for k in klines[-21:-1])
    recent = klines[-21:-1]
    bsl = max(_kline_value(k, "high") for k in recent)
    ssl = min(_kline_value(k, "low") for k in recent)
    atr_proxy = sum(_kline_value(k, "high") - _kline_value(k, "low") for k in klines[-14:]) / 14.0
    rolling_vwap = _rolling_vwap(klines, min(48, len(klines)))
    daily_vwap = _rolling_vwap(klines, min(288, len(klines)))
    bullish_bos = close > prev_high
    bearish_bos = close < prev_low
    sweep_bsl = high > bsl and close < bsl
    sweep_ssl = low < ssl and close > ssl
    range_mode = not bullish_bos and not bearish_bos
    eqh = len(highs) >= 2 and abs(highs[-1][1] - highs[-2][1]) <= atr_proxy * 0.1
    eql = len(lows) >= 2 and abs(lows[-1][1] - lows[-2][1]) <= atr_proxy * 0.1
    vwap = rolling_vwap or daily_vwap
    vwap_state = "unknown"
    if vwap:
        if close > vwap:
            vwap_state = "above"
        elif close < vwap:
            vwap_state = "below"
        else:
            vwap_state = "at"
    if side == "long":
        structure_ok = bullish_bos or (range_mode and close > prev_low)
        liquidity_ok = sweep_ssl or close > ssl
        vwap_ok = vwap_state == "above"
    else:
        structure_ok = bearish_bos or (range_mode and close < prev_high)
        liquidity_ok = sweep_bsl or close < bsl
        vwap_ok = vwap_state == "below"
    timing = 0
    timing += 30 if structure_ok else 0
    timing += 25 if vwap_ok else 0
    timing += 20 if liquidity_ok else 0
    timing += 15 if (side == "long" and sweep_ssl) or (side == "short" and sweep_bsl) else 0
    timing += 10 if (side == "long" and close >= prev_low) or (side == "short" and close <= prev_high) else 0
    return {
        "ready": True,
        "close": close,
        "structure": "Bullish BOS" if bullish_bos else "Bearish BOS" if bearish_bos else "Range",
        "structure_ok": structure_ok,
        "bullish_bos": bullish_bos,
        "bearish_bos": bearish_bos,
        "bsl": bsl,
        "ssl": ssl,
        "eqh": eqh,
        "eql": eql,
        "sweep_bsl": sweep_bsl,
        "sweep_ssl": sweep_ssl,
        "rolling_vwap": rolling_vwap,
        "daily_vwap": daily_vwap,
        "vwap_state": vwap_state,
        "vwap_ok": vwap_ok,
        "liquidity_ok": liquidity_ok,
        "timing_score": min(100, timing),
    }


def load_entry_engine_book(symbols: list[str], workers: int = 8, limit: int = 80) -> dict[str, dict[str, list[object]]]:
    def load(symbol: str) -> tuple[str, dict[str, list[object]]]:
        with market_client(timeout=14) as client:
            return symbol, {
                "5m": client.klines(symbol, interval="5m", limit=limit),
                "15m": client.klines(symbol, interval="15m", limit=max(40, limit // 2)),
            }

    book: dict[str, dict[str, list[object]]] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(load, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            try:
                symbol, data = future.result()
            except Exception:
                continue
            book[symbol] = data
    return book


def contract_entry_engine(row: dict[str, object], kline_book: dict[str, dict[str, list[object]]]) -> dict[str, object]:
    symbol = clean_symbol(row.get("symbol"))
    side = str(row.get("bias") or "")
    data = kline_book.get(symbol) or {}
    snap_5m = _structure_snapshot(data.get("5m") or [], side)
    snap_15m = _structure_snapshot(data.get("15m") or [], side)
    ready = bool(snap_5m.get("ready")) and bool(snap_15m.get("ready"))
    if not ready:
        return {"ready": False, "grade": "watch", "reason": "entry_engine_kline_insufficient"}
    timing = int(round(float(snap_5m.get("timing_score") or 0) * 0.65 + float(snap_15m.get("timing_score") or 0) * 0.35))
    side_ok = (side == "long" and snap_5m.get("vwap_state") == "above") or (side == "short" and snap_5m.get("vwap_state") == "below")
    formal = timing >= int(os.getenv("ENTRY_ENGINE_MIN_TIMING", "65")) and side_ok
    entry = as_float(snap_5m.get("rolling_vwap")) or as_float(row.get("trigger_price")) or as_float(row.get("price"))
    close = as_float(snap_5m.get("close")) or entry
    if entry and close:
        # If price already pulled away, keep the locked entry near current executable price.
        if abs(close - entry) / max(entry, 1e-12) > float(os.getenv("ENTRY_ENGINE_MAX_VWAP_DISTANCE", "0.018")):
            entry = close
    return {
        "ready": True,
        "formal": formal,
        "grade": "formal" if formal else "watch",
        "timing_score": timing,
        "entry_price": entry,
        "structure_5m": snap_5m.get("structure"),
        "structure_15m": snap_15m.get("structure"),
        "vwap_state": snap_5m.get("vwap_state"),
        "rolling_vwap": snap_5m.get("rolling_vwap"),
        "daily_vwap": snap_5m.get("daily_vwap"),
        "bsl": snap_5m.get("bsl"),
        "ssl": snap_5m.get("ssl"),
        "eqh": snap_5m.get("eqh"),
        "eql": snap_5m.get("eql"),
        "sweep_bsl": snap_5m.get("sweep_bsl"),
        "sweep_ssl": snap_5m.get("sweep_ssl"),
        "trigger": (
            "VWAP reclaim + bullish structure" if side == "long" and formal else
            "VWAP rejection + bearish structure" if side == "short" and formal else
            "waiting for BOS/VWAP confirmation"
        ),
    }


def prefetch_scan_data(symbols: list[str], lookback: int, divergence_lookback: int) -> None:
    if provider_name() != "binance":
        return
    requests: list[tuple[str, str, dict[str, object]]] = []
    for symbol in symbols:
        for interval, limit in (("15m", lookback), ("5m", divergence_lookback)):
            requests.extend(
                [
                    (BINANCE_FAPI, "/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit}),
                    (BINANCE_FDATA, "/openInterestHist", {"symbol": symbol, "period": interval, "limit": min(limit, 500)}),
                    (BINANCE_FDATA, "/takerlongshortRatio", {"symbol": symbol, "period": interval, "limit": min(limit, 500)}),
                ]
            )
    with market_client(timeout=60) as client:
        client.prefetch(requests)


def latest_ratio(rows: list[dict[str, object]]) -> float | None:
    if not rows:
        return None
    return as_float(rows[-1].get("longShortRatio"))


def binance_positioning_details(symbols: list[str], workers: int = 8) -> dict[str, dict[str, object]]:
    def load(symbol: str) -> tuple[str, dict[str, object]]:
        with market_client(timeout=12) as client:
            taker = client.taker_buy_sell_volume(symbol, period="1h", limit=1)
            retail = client.global_long_short_account_ratio(symbol, period="1h", limit=1)
            top_account = client.top_long_short_account_ratio(symbol, period="1h", limit=1)
            top_position = client.top_long_short_position_ratio(symbol, period="1h", limit=1)
        cvd = None
        if taker:
            total = taker[-1].buy_volume + taker[-1].sell_volume
            if total > 0:
                cvd = (taker[-1].buy_volume - taker[-1].sell_volume) / total * 100.0
        return symbol, {
            "cvd_ratio_1h": cvd,
            "long_short_ratio_1h": latest_ratio(retail),
            "top_account_long_short_ratio_1h": latest_ratio(top_account),
            "top_position_long_short_ratio_1h": latest_ratio(top_position),
        }

    book: dict[str, dict[str, object]] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(load, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            try:
                symbol, detail = future.result()
            except Exception:
                continue
            book[symbol] = detail
    return book


def enrich_coinglass_details(
    client: CoinGlassClient,
    symbols: list[str],
    details: dict[str, dict[str, object]],
) -> None:
    try:
        liquidations = client.liquidation_coin_list("Binance")
    except Exception as exc:
        print(f"WARN CoinGlass liquidation skipped: {exc}")
        liquidations = []
    for row in liquidations:
        symbol = clean_symbol(row.get("symbol"))
        if symbol in details:
            details[symbol].update(row)

    for symbol in symbols:
        coin = symbol.removesuffix("USDT")
        try:
            rows = client.open_interest_exchange(coin)
        except Exception as exc:
            print(f"WARN CoinGlass OI skipped for {symbol}: {exc}")
            continue
        aggregate = next((row for row in rows if str(row.get("exchange")) == "All"), None)
        if aggregate is None and rows:
            aggregate = rows[0]
        if aggregate:
            details.setdefault(symbol, {}).update(aggregate)


def build_live_contract_radar(min_volume: float, workers: int) -> tuple[list[dict[str, object]], dict[str, object]]:
    with market_client(timeout=20) as client:
        tickers = client.ticker_24hr()
    kline_limit = int(os.getenv("CONTRACT_KLINE_CANDIDATES", "120"))
    detail_limit = int(os.getenv("CONTRACT_DETAIL_CANDIDATES", "20"))
    candidates = candidate_symbols_from_tickers(tickers, min_volume, limit=kline_limit)
    detail_symbols = candidates[:detail_limit]
    prefetch: list[tuple[str, str, dict[str, object]]] = [
        (BINANCE_FAPI, "/fapi/v1/klines", {"symbol": symbol, "interval": "5m", "limit": 14})
        for symbol in candidates
    ]
    for symbol in detail_symbols:
        prefetch.extend(
            (BINANCE_FDATA, path, {"symbol": symbol, "period": "1h", "limit": 1})
            for path in (
                "/takerlongshortRatio",
                "/globalLongShortAccountRatio",
                "/topLongShortAccountRatio",
                "/topLongShortPositionRatio",
            )
        )
    with market_client(timeout=60) as client:
        client.prefetch(prefetch)
    changes = short_kline_changes(candidates, workers=workers)
    details = binance_positioning_details(detail_symbols, workers=workers)

    funding_rows: list[dict[str, object]] = []
    cg_calls = 0
    try:
        cg = CoinGlassClient(timeout=20, max_requests=int(os.getenv("COINGLASS_REQUEST_BUDGET", "27")))
        try:
            funding_rows = cg.funding_rates()
        except Exception as exc:
            print(f"WARN CoinGlass funding skipped: {exc}")
        enrich_coinglass_details(cg, detail_symbols, details)
        cg_calls = cg.request_count
    except Exception as exc:
        print(f"WARN CoinGlass enrichment skipped: {exc}")

    rows = build_contract_radar_from_binance_tickers(
        tickers,
        funding_rows,
        min_volume,
        changes,
        details,
    )
    return rows, {
        "coinglass_calls": cg_calls,
        "detail_symbols": len(detail_symbols),
        "kline_symbols": len(candidates),
        "market_data_provider": provider_name(),
        "positioning_symbols": len(details),
    }


def preserve_previous_detail(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    if not CONTRACT_RADAR.exists():
        return rows
    try:
        previous_data = json.loads(CONTRACT_RADAR.read_text(encoding="utf-8"))
        previous_rows = previous_data.get("rows", []) if isinstance(previous_data, dict) else []
    except Exception:
        return rows
    previous = {clean_symbol(row.get("symbol")): row for row in previous_rows if isinstance(row, dict)}
    detail_fields = (
        "oi_change_1h",
        "oi_change_15m",
        "oi_usd",
        "cvd_ratio_1h",
        "long_short_ratio_1h",
        "top_account_long_short_ratio_1h",
        "top_position_long_short_ratio_1h",
        "long_liquidation_1h",
        "short_liquidation_1h",
    )
    for row in rows:
        old = previous.get(clean_symbol(row.get("symbol")))
        if not old:
            continue
        for field in detail_fields:
            if row.get(field) is None:
                row[field] = old.get(field)
    return rows


def is_active_position(row: dict[str, object]) -> bool:
    return (
        str(row.get("status") or "active") == "active"
        and str(row.get("reached_state") or "holding") in {"holding", "tp1", "tp2", "tp3"}
    )


def position_key(row: dict[str, object]) -> tuple[str, str]:
    return clean_symbol(row.get("symbol")), str(row.get("signal_type") or "")


def active_position_counts(rows: list[dict[str, object]]) -> dict[tuple[str, str], int]:
    counts: dict[tuple[str, str], int] = {}
    for row in rows:
        if is_active_position(row):
            key = position_key(row)
            counts[key] = counts.get(key, 0) + 1
    return counts


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
    active_counts = active_position_counts(existing)
    max_same_side = int(os.getenv("MAX_ACTIVE_PER_SYMBOL_SIDE", "2"))
    rows: list[dict[str, object]] = []
    max_new = int(os.getenv("CONTRACT_SIGNAL_MAX_NEW", "30"))
    entry_candidates = [
        clean_symbol(row.get("symbol"))
        for row in radar_rows
        if str(row.get("bias") or "neutral") in {"long", "short"} and str(row.get("trigger") or "watch") != "watch"
    ]
    entry_limit = int(os.getenv("ENTRY_ENGINE_CANDIDATES", "60"))
    entry_book = load_entry_engine_book(entry_candidates[:entry_limit], workers=int(os.getenv("ENTRY_ENGINE_WORKERS", "6"))) if entry_candidates else {}
    for row in radar_rows:
        bias = str(row.get("bias") or "neutral")
        trigger = str(row.get("trigger") or "watch")
        if bias not in {"long", "short"} or trigger == "watch":
            continue
        signal_type = "reversal_bullish" if bias == "long" else "reversal_bearish"
        symbol = clean_symbol(row.get("symbol"))
        key = (symbol, signal_type)
        if active_counts.get(key, 0) >= max_same_side:
            continue
        engine = contract_entry_engine(row, entry_book)
        if not engine.get("formal"):
            continue
        price = as_float(engine.get("entry_price")) or as_float(row.get("trigger_price")) or as_float(row.get("price"))
        if price is None or price <= 0:
            continue
        triggered_at_ms = int(row.get("triggered_at_ms") or datetime.now(timezone.utc).timestamp() * 1000)
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
                    "triggered_at_ms": triggered_at_ms,
                    "triggered_at": iso_ms(triggered_at_ms),
                    "trigger_price": price,
                    "entry_price": price,
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
                        "entry_engine": engine,
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
        active_counts[key] = active_counts.get(key, 0) + 1
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


def replay_signal_state(row: dict[str, object], klines: list[object]) -> tuple[str, float | None, str | None, str]:
    bullish = row.get("signal_type") == "reversal_bullish"
    trigger_ms = int(row.get("triggered_at_ms") or 0)
    entry = as_float(row.get("entry_price")) or as_float(row.get("trigger_price"))
    original_stop = as_float(row.get("sl_price"))
    # Always rebuild chronologically from the trigger. Starting from the stored TP
    # would incorrectly apply a moved stop before that TP was actually reached.
    current = "holding"
    max_reached = "holding"
    hit_price = None
    hit_at = None

    for kline in klines:
        if int(getattr(kline, "open_time")) <= trigger_ms:
            continue
        high = float(getattr(kline, "high"))
        low = float(getattr(kline, "low"))
        timestamp = iso_ms(int(getattr(kline, "open_time")))

        stop = entry if target_order(max_reached) >= target_order("tp2") else original_stop
        if stop is not None and (low <= stop if bullish else high >= stop):
            return "sl", stop, timestamp, max_reached

        for name in ("ftp", "tp3", "tp2", "tp1"):
            if target_order(name) <= target_order(max_reached):
                continue
            target = as_float(row.get(f"{name}_price"))
            if target is None:
                continue
            reached = high >= target if bullish else low <= target
            if reached:
                current = name
                max_reached = name
                hit_price = target
                hit_at = timestamp
                break
        if current == "ftp":
            return current, hit_price, hit_at, max_reached
    return current, hit_price, hit_at, max_reached


def update_existing_states(rows: list[dict[str, object]]) -> dict[str, int]:
    active_rows = [
        row
        for row in rows
        if str(row.get("status") or "active") == "active"
        and str(row.get("reached_state") or "holding") in {"holding", "tp1", "tp2", "tp3"}
    ]
    earliest_by_pair: dict[tuple[str, str], int] = {}
    for row in active_rows:
        pair = (clean_symbol(row.get("symbol")), str(row.get("timeframe") or "15M").lower())
        triggered_at_ms = int(row.get("triggered_at_ms") or 0)
        if triggered_at_ms > 0:
            earliest_by_pair[pair] = min(earliest_by_pair.get(pair, triggered_at_ms), triggered_at_ms)
    pairs = sorted(earliest_by_pair)
    if not pairs:
        return {"checked": 0, "changed": 0, "closed": 0}
    if provider_name() == "binance":
        with market_client(timeout=60) as client:
            client.prefetch(
                [
                    (
                        BINANCE_FAPI,
                        "/fapi/v1/klines",
                        {"symbol": symbol, "interval": interval, "limit": 1500, "startTime": earliest_by_pair[(symbol, interval)]},
                    )
                    for symbol, interval in pairs
                ]
            )

    def load(pair: tuple[str, str]) -> tuple[tuple[str, str], list[object]]:
        symbol, interval = pair
        with market_client(timeout=15) as client:
            return pair, client.klines_since(symbol, interval=interval, start_time=earliest_by_pair[pair])

    histories: dict[tuple[str, str], list[object]] = {}
    replay_workers = int(os.getenv("STATE_REPLAY_WORKERS", "3" if provider_name() in {"okx", "mixed"} else "8"))
    with ThreadPoolExecutor(max_workers=min(replay_workers, max(1, len(pairs)))) as executor:
        futures = {executor.submit(load, pair): pair for pair in pairs}
        for future in as_completed(futures):
            pair = futures[future]
            try:
                key, klines = future.result()
            except Exception as exc:
                message = f"WARN state replay skipped for {pair[0]} {pair[1]}: {exc}"
                print(message.encode("ascii", "backslashreplace").decode("ascii"))
                continue
            histories[key] = klines

    changed = 0
    closed = 0
    for row in active_rows:
        key = (clean_symbol(row.get("symbol")), str(row.get("timeframe") or "15M").lower())
        klines = histories.get(key)
        if not klines:
            continue
        prev = str(row.get("reached_state") or "holding")
        next_state, hit_price, hit_at, max_reached = replay_signal_state(row, klines)
        if next_state == prev and max_reached == str(row.get("max_reached_state") or prev):
            continue
        changed += 1
        row["reached_state"] = next_state
        row["max_reached_state"] = max_reached
        row["hit_price"] = hit_price
        row["hit_at"] = hit_at
        row["state_resolution"] = "chronological_ohlc_stop_first"
        if target_order(max_reached) >= target_order("tp2"):
            row["active_sl_price"] = row.get("entry_price") or row.get("trigger_price")
        if next_state in {"sl", "ftp"}:
            row["status"] = "closed"
            closed += 1
    return {"checked": len(active_rows), "changed": changed, "closed": closed}


LOCKED_SIGNAL_FIELDS = {
    "entry_price",
    "trigger_price",
    "sl_price",
    "tp1_price",
    "tp2_price",
    "tp3_price",
    "ftp_price",
    "risk",
    "atr_at_trigger",
    "triggered_at",
    "triggered_at_ms",
    "signal_id",
    "id",
}


LIVE_UPDATE_FIELDS = {
    "current_price",
    "price",
    "price_change_pct",
    "oi_change_pct",
    "oi_value",
    "oi_value_usdt",
    "taker_buy_ratio",
    "detected_at",
    "snapshot_data",
    "mtf_5m_confluence",
    "mtf_5m_oi_confluence",
    "mtf_5m_oi_change_pct",
    "mtf_5m_price_change_pct",
    "mtf_5m_oi_percentile",
}


def merge_locked_signal(old: dict[str, object], new: dict[str, object]) -> dict[str, object]:
    merged = dict(old)
    for field in LIVE_UPDATE_FIELDS:
        if field in new and field not in LOCKED_SIGNAL_FIELDS:
            merged[field] = new[field]
    return normalize_row(merged)


def validate_state_consistency(rows: list[dict[str, object]]) -> dict[str, int]:
    changed = 0
    closed = 0
    for row in rows:
        state = str(row.get("reached_state") or "holding")
        status = str(row.get("status") or "active")
        if state in {"sl", "ftp"} and status == "active":
            row["status"] = "closed"
            row["state_resolution"] = "terminal_state_consistency"
            changed += 1
            closed += 1
        if target_order(row.get("max_reached_state")) >= target_order("tp2"):
            entry = row.get("entry_price") or row.get("trigger_price")
            if row.get("active_sl_price") != entry:
                row["active_sl_price"] = entry
                changed += 1
    return {"checked": len(rows), "changed": changed, "closed": closed}


def format_cvd_divergence_signal(symbol: str, snapshots: list[object], config: ScannerConfig) -> dict[str, object] | None:
    window = int(os.getenv("CVD_DIVERGENCE_WINDOW", "3"))
    if len(snapshots) <= window:
        return None
    snapshot = snapshots[-1]
    previous = snapshots[-window - 1]
    previous_price = float(getattr(previous, "price"))
    price = float(getattr(snapshot, "price"))
    if previous_price <= 0:
        return None
    price_change = (price - previous_price) / previous_price * 100.0

    def cvd_ratio(group: list[object]) -> float:
        buy = sum(float(getattr(item, "taker_buy_volume") or 0) for item in group)
        sell = sum(float(getattr(item, "taker_sell_volume") or 0) for item in group)
        total = buy + sell
        return (buy - sell) / total * 100.0 if total > 0 else 0.0

    history: list[float] = []
    for end in range(window, len(snapshots) - 1):
        history.append(abs(cvd_ratio(snapshots[end - window:end])))
    cvd_change = cvd_ratio(snapshots[-window:])
    from sentiment_scanner.indicators import percentile_rank
    cvd_percentile = percentile_rank(abs(cvd_change), history)
    if cvd_percentile is None or cvd_percentile < config.oi_percentile_threshold:
        return None
    if price_change == 0 or cvd_change == 0 or (price_change > 0) == (cvd_change > 0):
        return None

    atr_value = float(getattr(snapshot, "atr"))
    raw_risk = config.atr_risk_multiple * atr_value
    capped_risk = price * config.max_risk_pct
    risk = min(raw_risk, capped_risk)
    sl_source = "atr" if risk == raw_risk else "capped_10pct"

    if price_change < 0 and cvd_change > 0:
        signal_type = "reversal_bullish"
        setup_id = "cvd_5m_bullish_divergence"
        divergence_name = "bottom_divergence"
        sl = price - risk
        tp1 = price + risk
        tp2 = price + risk * 2
        tp3 = price + risk * 3
        ftp = price + risk * 5
    else:
        signal_type = "reversal_bearish"
        setup_id = "cvd_5m_bearish_divergence"
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
        "entry_price": price,
        "atr_at_trigger": atr_value,
        "sl_price": sl,
        "tp1_price": tp1,
        "tp2_price": tp2,
        "tp3_price": tp3,
        "ftp_price": ftp,
        "risk": risk,
        "sl_source": sl_source,
        "oi_percentile": float(getattr(snapshot, "oi_percentile")),
        "oi_change_pct": float(getattr(snapshot, "oi_change_pct")),
        "price_change_pct": price_change,
        "taker_buy_ratio": getattr(snapshot, "taker_buy_ratio"),
        "oi_value": float(getattr(snapshot, "oi_value")),
        "oi_value_usdt": getattr(snapshot, "oi_value_usdt"),
        "snapshot_data": {
            "divergence": "price_down_cvd_up" if price_change < 0 and cvd_change > 0 else "price_up_cvd_down",
            "divergence_label": divergence_name,
            "interval": "5m",
            "cvd_change_pct": cvd_change,
            "cvd_percentile": cvd_percentile,
            "cvd_window_bars": window,
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
    oi_min = float(os.getenv("OI_CHANGE_MIN_PCT", "3"))
    oi_strong = float(os.getenv("OI_CHANGE_STRONG_PCT", "5"))
    oi_confluence = confluence and abs(oi_change) >= oi_min
    snapshot_data = row.get("snapshot_data")
    if not isinstance(snapshot_data, dict):
        snapshot_data = {}
    snapshot_data.update(
        {
            "mtf_5m_confluence": confluence,
            "mtf_5m_oi_confluence": oi_confluence,
            "mtf_5m_oi_strength": "strong" if abs(oi_change) >= oi_strong else "normal" if abs(oi_change) >= oi_min else "weak",
            "mtf_5m_oi_change_pct": oi_change,
            "mtf_5m_price_change_pct": price_change,
            "mtf_5m_oi_percentile": oi_percentile,
        }
    )
    row["mtf_5m_confluence"] = confluence
    row["mtf_5m_oi_confluence"] = oi_confluence
    row["mtf_5m_oi_change_pct"] = oi_change
    row["mtf_5m_price_change_pct"] = price_change
    row["mtf_5m_oi_percentile"] = oi_percentile
    row["snapshot_data"] = snapshot_data
    return row


def scan_symbol(symbol: str, config: ScannerConfig, divergence_config: ScannerConfig) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with market_client(timeout=20) as client:
        scanner = SentimentScanner(client, config)
        signal = scanner.latest_signal(symbol)

        divergence_scanner = SentimentScanner(client, divergence_config)
        klines, oi_points, taker_points = divergence_scanner._load(symbol)
        snapshots = divergence_scanner._snapshots(symbol, klines, oi_points, taker_points)
        if signal is not None:
            row = normalize_row(format_signal(signal))
            if snapshots:
                row = apply_5m_confluence(row, snapshots[-1])
            if os.getenv("REQUIRE_5M_OI_CONFLUENCE", "true").lower() == "true" and not row.get("mtf_5m_oi_confluence"):
                row = None
            if row is not None:
                rows.append(row)
        if snapshots:
            row = format_cvd_divergence_signal(symbol, snapshots, divergence_config)
            if row is not None:
                rows.append(row)
    return rows


def resolve_symbols() -> list[str]:
    top = int(os.getenv("SCAN_TOP", "0") or "0")
    min_volume = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "5000000"))
    try:
        with market_client(timeout=20) as client:
            return client.symbols_by_volume(limit=top, min_quote_volume=min_volume)
    except Exception as exc:
        existing_symbols = sorted({clean_symbol(row.get("symbol")) for row in load_rows() if row.get("symbol")})
        fallback_limit = int(os.getenv("SCAN_FALLBACK_LIMIT", "120"))
        if existing_symbols:
            print(f"WARN symbol discovery failed, using {min(len(existing_symbols), fallback_limit)} seed symbols: {exc}")
            return existing_symbols[:fallback_limit]
        raise


def main() -> None:
    existing = [
        row
        for raw in load_rows()
        if valid_signal_time(row := normalize_row(raw)) and not is_legacy_oi_divergence(row)
    ]
    min_volume = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "5000000"))
    config = ScannerConfig(
        lookback_limit=int(os.getenv("LOOKBACK_LIMIT", "500")),
        oi_percentile_threshold=float(os.getenv("OI_PERCENTILE", "99")),
        oi_change_min_pct=float(os.getenv("OI_CHANGE_MIN_PCT", "3")),
        oi_change_strong_pct=float(os.getenv("OI_CHANGE_STRONG_PCT", "5")),
        atr_risk_multiple=float(os.getenv("ATR_MULTIPLE", "2.5")),
        eval_window_hours=float(os.getenv("EVAL_HOURS", "6")),
    )
    divergence_config = ScannerConfig(
        interval="5m",
        lookback_limit=int(os.getenv("DIVERGENCE_LOOKBACK_LIMIT", "500")),
        oi_percentile_threshold=float(os.getenv("DIVERGENCE_CVD_PERCENTILE", "95")),
        atr_risk_multiple=float(os.getenv("DIVERGENCE_ATR_MULTIPLE", os.getenv("ATR_MULTIPLE", "2.5"))),
        eval_window_hours=float(os.getenv("EVAL_HOURS", "6")),
    )
    workers = int(os.getenv("SCAN_WORKERS", "8"))
    contract_radar: list[dict[str, object]] = []
    radar_meta: dict[str, object] = {}
    try:
        contract_radar, radar_meta = build_live_contract_radar(min_volume, workers)
        contract_radar = preserve_previous_detail(contract_radar)
        CONTRACT_RADAR.write_text(
            json.dumps({"rows": contract_radar, "updated_at": datetime.now(timezone.utc).isoformat(), "min_volume_usdt": min_volume, **radar_meta}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"contract_rows={len(contract_radar)} coinglass_calls={radar_meta.get('coinglass_calls', 0)}")
    except Exception as exc:
        print(f"WARN contract radar skipped; keeping previous valid data: {exc}")

    symbols = resolve_symbols()
    prefetch_scan_data(symbols, config.lookback_limit, divergence_config.lookback_limit)
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
    max_same_side = int(os.getenv("MAX_ACTIVE_PER_SYMBOL_SIDE", "2"))
    active_counts = active_position_counts(existing)
    new_count = 0
    for row in found:
        row_id = str(row.get("id") or signal_id(row))
        if row_id in by_id:
            by_id[row_id] = merge_locked_signal(by_id[row_id], row)
            continue
        key = position_key(row)
        if active_counts.get(key, 0) >= max_same_side:
            continue
        new_count += 1
        by_id[row_id] = normalize_row(row)
        if is_active_position(row):
            active_counts[key] = active_counts.get(key, 0) + 1
    rows_for_state = list(by_id.values())
    state_audit = update_existing_states(rows_for_state)
    print(f"state_audit checked={state_audit['checked']} changed={state_audit['changed']} closed={state_audit['closed']}")
    state_consistency = validate_state_consistency(rows_for_state)
    print(
        f"state_consistency checked={state_consistency['checked']} "
        f"changed={state_consistency['changed']} closed={state_consistency['closed']}"
    )

    rows = sorted(
        rows_for_state,
        key=lambda row: str(row.get("triggered_at") or row.get("detected_at") or ""),
        reverse=True,
    )
    max_rows = int(os.getenv("MAX_SEED_ROWS", "0"))
    allowed_errors = int(os.getenv("MAX_SCAN_ERRORS", str(max(5, int(len(symbols) * 0.05)))))
    scan_ok = bool(found) and len(errors) <= allowed_errors
    if scan_ok:
        rows_to_save = rows[:max_rows] if max_rows > 0 else rows
        SEED.write_text(json.dumps(rows_to_save, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        print("WARN scan failed; keeping previous signal data")
    status_time = datetime.now(timezone.utc).isoformat()
    SCANNER_STATUS.write_text(
        json.dumps(
            {
                "ok": scan_ok,
                "updated_at": status_time,
                "last_attempt_at": status_time,
                "last_success_at": status_time if scan_ok else previous_success_at(),
                "market_data_provider": provider_name(),
                "symbols_scanned": len(symbols),
                "signals_found": len(found),
                "new_signals": new_count,
                "errors": errors[:20],
                "error_count": len(errors),
                "allowed_errors": allowed_errors,
                "state_audit": state_audit,
                "state_consistency": state_consistency,
                "min_volume_usdt": min_volume,
                **radar_meta,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    saved_count = min(len(rows), max_rows) if max_rows > 0 else len(rows)
    print(f"symbols={len(symbols)} found={len(found)} new={new_count} saved={saved_count} errors={len(errors)}")
    for error in errors[:20]:
        print(f"ERROR {error}")


if __name__ == "__main__":
    main()
