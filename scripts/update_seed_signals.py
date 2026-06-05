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
from sentiment_scanner.scanner import ScannerConfig, SentimentScanner


SEED = ROOT / "sentiment_scanner" / "seed_signals.json"
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
    try:
        with BinanceFuturesClient(timeout=20) as client:
            if top > 0:
                return client.top_symbols_by_volume(limit=top)
            return client.exchange_symbols()
    except Exception as exc:
        existing_symbols = sorted({clean_symbol(row.get("symbol")) for row in load_rows() if row.get("symbol")})
        fallback_limit = int(os.getenv("SCAN_FALLBACK_LIMIT", "120"))
        if existing_symbols:
            print(f"WARN symbol discovery failed, using {min(len(existing_symbols), fallback_limit)} seed symbols: {exc}")
            return existing_symbols[:fallback_limit]
        raise


def main() -> None:
    existing = [normalize_row(row) for row in load_rows()]
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
