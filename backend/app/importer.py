from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import MarketSnapshot, Signal


ROOT = Path(__file__).resolve().parents[2]
MUTABLE_SIGNAL_FIELDS = (
    "current_price",
    "reached_state",
    "pnl_pct",
    "hit_at",
    "max_gain_pct",
    "max_drawdown_pct",
    "quality_score",
    "radar_score",
    "trade_layer",
    "official_trade",
    "raw_payload",
)


def _decimal(value: object) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _float(value: object) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (ValueError, TypeError):
        return None


def _datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        stamp = float(value) / (1000 if float(value) > 10_000_000_000 else 1)
        return datetime.fromtimestamp(stamp, timezone.utc)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _side(row: dict[str, Any]) -> str:
    text = " ".join(str(row.get(key, "")) for key in ("side", "direction", "signal_type", "setup_id", "narrative")).lower()
    if any(term in text for term in ("bear", "short", "嘎空", "頂背離", "空頭")):
        return "short"
    return "long"


def _strategy(row: dict[str, Any]) -> str:
    text = " ".join(str(row.get(key, "")) for key in ("strategy", "setup_id", "signal_type", "narrative")).lower()
    if any(term in text for term in ("stable", "cvd_divergence", "頂背離", "底背離", "穩如老狗")):
        return "stable_dog"
    if any(term in text for term in ("contract", "anomaly", "合約異常")):
        return "contract_anomaly"
    if row.get("high_quality") or row.get("trade_layer") == "high_quality":
        return "high_quality"
    return "sentiment_oi"


def _state(row: dict[str, Any]) -> str:
    value = str(row.get("reached_state") or row.get("state") or row.get("status") or "holding").lower()
    aliases = {
        "active": "holding",
        "open": "holding",
        "evaluated": "holding",
        "stop_loss": "sl",
        "stopped": "sl",
    }
    return aliases.get(value, value)


def normalize_signal(row: dict[str, Any]) -> dict[str, Any]:
    signal_id = str(row.get("signal_id") or row.get("id") or "").strip()
    if not signal_id:
        raise ValueError("Signal has no stable id")
    triggered_at = _datetime(row.get("triggered_at") or row.get("established_at") or row.get("triggered_at_ms"))
    entry = _decimal(row.get("entry_price") or row.get("trigger_price"))
    current = _decimal(row.get("current_price") or row.get("latest_price") or row.get("price"))
    return {
        "id": signal_id,
        "symbol": str(row.get("symbol", "UNKNOWN")).upper().replace("USDT", ""),
        "timeframe": str(row.get("timeframe", "15M")).upper(),
        "strategy": _strategy(row),
        "strategy_version": str(row.get("strategy_version") or "legacy-v1"),
        "side": _side(row),
        "trade_layer": str(row.get("trade_layer") or ("official_trade" if row.get("official_trade") else "raw_signal")),
        "official_trade": bool(row.get("official_trade", row.get("trade_layer") == "official_trade")),
        "triggered_at": triggered_at,
        "entry_price": entry,
        "sl_price": _decimal(row.get("sl_price")),
        "tp1_price": _decimal(row.get("tp1_price")),
        "tp2_price": _decimal(row.get("tp2_price")),
        "tp3_price": _decimal(row.get("tp3_price")),
        "ftp_price": _decimal(row.get("ftp_price")),
        "current_price": current or entry,
        "reached_state": _state(row),
        "pnl_pct": _float(row.get("pnl_pct") or row.get("cumulative_pnl_pct")),
        "hit_at": _datetime(row["hit_at"]) if row.get("hit_at") else None,
        "max_gain_pct": _float(row.get("max_gain_pct") or row.get("max_excursion_pct")),
        "max_drawdown_pct": _float(row.get("max_drawdown_pct")),
        "quality_score": int(row["quality_score"]) if row.get("quality_score") is not None else None,
        "radar_score": int(row["radar_score"]) if row.get("radar_score") is not None else None,
        "raw_payload": row,
    }


def iter_legacy_rows(root: Path = ROOT) -> Iterable[dict[str, Any]]:
    manifest_path = root / "data" / "manifest.json"
    files: list[Path] = []
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        files.extend(root / "data" / name for name in manifest.get("history_chunks", []))
    seed = root / "sentiment_scanner" / "seed_signals.json"
    if seed.exists():
        files.append(seed)
    seen: set[str] = set()
    for path in files:
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = payload.get("rows", []) if isinstance(payload, dict) else payload
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            signal_id = str(row.get("signal_id") or row.get("id") or "")
            if signal_id and signal_id not in seen:
                seen.add(signal_id)
                yield row


async def import_signals(session: AsyncSession, rows: Iterable[dict[str, Any]], batch_size: int = 500) -> dict[str, int]:
    inserted = updated = skipped = 0
    batch: list[dict[str, Any]] = []

    async def flush() -> None:
        nonlocal inserted, updated, skipped
        if not batch:
            return
        normalized: dict[str, dict[str, Any]] = {}
        for row in batch:
            try:
                item = normalize_signal(row)
                normalized[item["id"]] = item
            except (ValueError, TypeError):
                skipped += 1
        existing_rows = (await session.scalars(select(Signal).where(Signal.id.in_(normalized)))).all()
        existing = {row.id: row for row in existing_rows}
        for signal_id, values in normalized.items():
            target = existing.get(signal_id)
            if target is None:
                session.add(Signal(**values))
                inserted += 1
            else:
                for name in MUTABLE_SIGNAL_FIELDS:
                    setattr(target, name, values[name])
                updated += 1
        await session.commit()
        batch.clear()

    for source in rows:
        batch.append(source)
        if len(batch) >= batch_size:
            await flush()
    await flush()
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


async def import_legacy_history(session: AsyncSession) -> dict[str, int]:
    return await import_signals(session, iter_legacy_rows())


async def import_market_file(session: AsyncSession, root: Path = ROOT) -> int:
    path = root / "data" / "markets.json"
    if not path.exists():
        return 0
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows", payload) if isinstance(payload, dict) else payload
    observed_at = datetime.now(timezone.utc)
    count = 0
    for row in rows if isinstance(rows, list) else []:
        price = _decimal(row.get("price"))
        if price is None:
            continue
        session.add(
            MarketSnapshot(
                symbol=str(row.get("symbol", "")).upper().replace("USDT", ""),
                source=str(row.get("source") or "legacy-json"),
                price=price,
                change_24h_pct=_float(row.get("price_change_pct") or row.get("change_24h_pct")),
                quote_volume_24h=_float(row.get("quote_volume") or row.get("quote_volume_24h")),
                payload=row,
                observed_at=observed_at,
            )
        )
        count += 1
    await session.commit()
    return count

