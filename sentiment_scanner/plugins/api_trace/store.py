"""Persist API trace snapshots to PostgreSQL so the API service can read them."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger("wei.api_trace")


def compact_for_storage(payload: dict[str, Any]) -> dict[str, Any]:
    compact = dict(payload)
    rows: list[dict[str, Any]] = []
    for row in payload.get("requests") or []:
        if not isinstance(row, dict):
            continue
        item = {key: value for key, value in row.items() if key != "data_summary"}
        summary = row.get("data_summary")
        if isinstance(summary, dict):
            item["data_summary"] = {
                "type": summary.get("type"),
                "count": summary.get("count"),
                "fields": summary.get("fields"),
                "keys": summary.get("keys"),
            }
        rows.append(item)
    compact["requests"] = rows
    return compact


async def persist_trace_file(trace_file: Path) -> bool:
    if not trace_file.exists():
        return False
    try:
        payload = json.loads(trace_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("API trace file unreadable: %s", exc)
        return False
    return await persist_trace_payload(payload)


async def persist_trace_payload(payload: dict[str, Any]) -> bool:
    try:
        from backend.app.database import SessionFactory
        from backend.app.models import SystemLog
    except ImportError:
        return False

    compact = compact_for_storage(payload)
    partial = bool(compact.get("partial"))
    try:
        async with SessionFactory() as session:
            session.add(
                SystemLog(
                    level="INFO",
                    component="api_trace",
                    event="snapshot",
                    message=(
                        f"API trace partial {compact.get('run_id', 'unknown')}"
                        if partial
                        else f"API trace {compact.get('run_id', 'unknown')}"
                    ),
                    details=compact,
                )
            )
            await session.commit()
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("API trace persist failed: %s", exc)
        return False
