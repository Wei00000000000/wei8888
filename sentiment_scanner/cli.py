from __future__ import annotations

import argparse
import asyncio
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .binance import BinanceFuturesClient, normalize_symbols
from .scanner import EvaluatedSignal, ScannerConfig, SentimentScanner, Signal


def main() -> None:
    asyncio.run(run_cli())


async def run_cli() -> None:
    parser = argparse.ArgumentParser(description="15M OI percentile sentiment scanner for Binance USD-M futures.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser("scan", help="Scan latest 15M bar for signals.")
    add_common_args(scan_parser)
    scan_parser.add_argument("--top", type=int, default=0, help="Use top N USDT perpetual symbols by 24h volume.")

    backtest_parser = subparsers.add_parser("backtest", help="Backtest recent history.")
    add_common_args(backtest_parser)
    backtest_parser.add_argument("--top", type=int, default=0, help="Use top N USDT perpetual symbols by 24h volume.")

    args = parser.parse_args()
    config = ScannerConfig(
        lookback_limit=args.limit,
        oi_percentile_threshold=args.oi_percentile,
        oi_change_min_pct=args.oi_change_min_pct,
        oi_change_strong_pct=args.oi_change_strong_pct,
        atr_risk_multiple=args.atr_multiple,
        max_risk_pct=args.max_risk_pct,
        eval_window_hours=args.eval_hours,
    )

    async with BinanceFuturesClient() as client:
        symbols = await resolve_symbols(client, args.symbols, args.top)
        scanner = SentimentScanner(client, config)
        if args.command == "scan":
            results = await run_scan(scanner, symbols)
        else:
            results = await run_backtest(scanner, symbols)

    write_outputs(results, args.output_json, args.output_csv)
    print_summary(results)


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT", help="Comma separated symbols, e.g. BTCUSDT,ETHUSDT.")
    parser.add_argument("--limit", type=int, default=500, help="Lookback bars, max 500 for OI history.")
    parser.add_argument("--oi-percentile", type=float, default=99.0, help="OI change percentile trigger threshold.")
    parser.add_argument("--oi-change-min-pct", type=float, default=3.0, help="Minimum absolute OI change percentage.")
    parser.add_argument("--oi-change-strong-pct", type=float, default=5.0, help="Strong absolute OI change percentage.")
    parser.add_argument("--atr-multiple", type=float, default=2.5, help="ATR stop risk multiple.")
    parser.add_argument("--max-risk-pct", type=float, default=0.10, help="Maximum stop distance as price percentage.")
    parser.add_argument("--eval-hours", type=float, default=6.0, help="Backtest evaluation window in hours.")
    parser.add_argument("--output-json", default="", help="Optional JSON output path.")
    parser.add_argument("--output-csv", default="", help="Optional CSV output path.")


async def resolve_symbols(client: BinanceFuturesClient, symbols_arg: str, top: int) -> list[str]:
    if top > 0:
        return await client.top_symbols_by_volume(limit=top)
    return normalize_symbols(symbols_arg.split(","))


async def run_scan(scanner: SentimentScanner, symbols: Iterable[str]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for symbol in symbols:
        try:
            signal = await scanner.latest_signal(symbol)
        except Exception as exc:
            rows.append({"symbol": symbol, "error": str(exc)})
            continue
        if signal is not None:
            rows.append(format_signal(signal))
    return rows


async def run_backtest(scanner: SentimentScanner, symbols: Iterable[str]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for symbol in symbols:
        try:
            evaluated = await scanner.backtest(symbol)
        except Exception as exc:
            rows.append({"symbol": symbol, "error": str(exc)})
            continue
        rows.extend(format_evaluated(item) for item in evaluated)
    return rows


def format_signal(signal: Signal) -> dict[str, object]:
    data = signal.to_dict()
    data["triggered_at"] = iso_ms(signal.triggered_at_ms)
    return data


def format_evaluated(evaluated: EvaluatedSignal) -> dict[str, object]:
    data = evaluated.to_dict()
    data["triggered_at"] = iso_ms(evaluated.signal.triggered_at_ms)
    data["hit_at"] = iso_ms(evaluated.hit_at_ms) if evaluated.hit_at_ms else None
    return data


def write_outputs(rows: list[dict[str, object]], output_json: str, output_csv: str) -> None:
    if output_json:
        path = Path(output_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    if output_csv:
        path = Path(output_csv)
        path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = sorted({key for row in rows for key in row.keys()})
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)


def print_summary(rows: list[dict[str, object]]) -> None:
    print(f"rows={len(rows)}")
    errors = [row for row in rows if "error" in row]
    if errors:
        print(f"errors={len(errors)}")
        for row in errors[:10]:
            print(f"ERROR {row.get('symbol')}: {row.get('error')}")
    states: dict[str, int] = {}
    for row in rows:
        state = str(row.get("reached_state") or row.get("signal_type") or "signal")
        states[state] = states.get(state, 0) + 1
    for state, count in sorted(states.items(), key=lambda item: item[1], reverse=True):
        print(f"{state}={count}")
    for row in rows[:20]:
        if "error" in row:
            continue
        print(
            f"{row.get('triggered_at')} {row.get('symbol')} {row.get('signal_type')} "
            f"oi_pct={float(row.get('oi_percentile') or 0):.2f} "
            f"oi_chg={float(row.get('oi_change_pct') or 0):.3f}% "
            f"px_chg={float(row.get('price_change_pct') or 0):.3f}% "
            f"state={row.get('reached_state', '-')}"
        )


def iso_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


if __name__ == "__main__":
    main()
