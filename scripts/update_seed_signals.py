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
    return row


def scan_symbol(symbol: str, config: ScannerConfig) -> dict[str, object] | None:
    with BinanceFuturesClient(timeout=20) as client:
        scanner = SentimentScanner(client, config)
        signal = scanner.latest_signal(symbol)
    if signal is None:
        return None
    return normalize_row(format_signal(signal))


def resolve_symbols() -> list[str]:
    top = int(os.getenv("SCAN_TOP", "0") or "0")
    with BinanceFuturesClient(timeout=20) as client:
        if top > 0:
            return client.top_symbols_by_volume(limit=top)
        return client.exchange_symbols()


def main() -> None:
    existing = [normalize_row(row) for row in load_rows()]
    config = ScannerConfig(
        lookback_limit=int(os.getenv("LOOKBACK_LIMIT", "500")),
        oi_percentile_threshold=float(os.getenv("OI_PERCENTILE", "99")),
        atr_risk_multiple=float(os.getenv("ATR_MULTIPLE", "2.5")),
        eval_window_hours=float(os.getenv("EVAL_HOURS", "6")),
    )
    workers = int(os.getenv("SCAN_WORKERS", "8"))
    symbols = resolve_symbols()
    found: list[dict[str, object]] = []
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
      futures = {executor.submit(scan_symbol, symbol, config): symbol for symbol in symbols}
      for future in as_completed(futures):
          symbol = futures[future]
          try:
              row = future.result()
          except Exception as exc:
              errors.append(f"{symbol}: {exc}")
              continue
          if row is not None:
              found.append(row)

    by_id = {str(row.get("id") or signal_id(row)): row for row in existing}
    new_count = 0
    for row in found:
        row_id = str(row.get("id") or signal_id(row))
        if row_id not in by_id:
            new_count += 1
        by_id[row_id] = row

    rows = sorted(
        by_id.values(),
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
