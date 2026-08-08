"""Optional FastAPI route for cloud mode. Safe to remove with the plugin."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.database import get_session
from backend.app.models import SystemLog

from .ban_status import build_ban_status

router = APIRouter(prefix="/market", tags=["market"])
Session = Annotated[AsyncSession, Depends(get_session)]
TRACE_FILE = Path(__file__).resolve().parents[2] / "scanner_api_trace.json"
TRACE_HISTORY_LIMIT = 32


def _snapshot_request_count(details: dict[str, object]) -> int:
    summary = details.get("summary")
    if isinstance(summary, dict) and summary.get("total_requests") is not None:
        return int(summary.get("total_requests") or 0)
    requests = details.get("requests")
    if isinstance(requests, list):
        return len(requests)
    return 0


def _pick_trace_rows(rows: list[SystemLog]) -> tuple[SystemLog | None, SystemLog | None]:
    """Return (latest, richest_by_request_count). Rows must be newest-first."""
    if not rows:
        return None, None
    latest = rows[0]

    def count_row(row: SystemLog) -> int:
        details = row.details if isinstance(row.details, dict) else {}
        return _snapshot_request_count(details)

    richest = max(rows, key=count_row)
    return latest, richest


@router.get("/api-trace")
async def latest_api_trace(session: Session, mode: str = "latest") -> dict[str, object]:
    """Debug-only route. No auth so the standalone trace page works without login."""
    rows = list(
        await session.scalars(
            select(SystemLog)
            .where(SystemLog.component == "api_trace", SystemLog.event == "snapshot")
            .order_by(SystemLog.created_at.desc())
            .limit(TRACE_HISTORY_LIMIT)
        )
    )
    latest_row, richest_row = _pick_trace_rows(rows)
    row = richest_row if mode == "richest" else latest_row
    if row and isinstance(row.details, dict) and row.details:
        details = dict(row.details)
        details["ban_status"] = build_ban_status(
            details.get("requests") or [],
            TRACE_FILE.parent / ".binance_cooldown.json",
        )
        latest_details = latest_row.details if latest_row and isinstance(latest_row.details, dict) else {}
        richest_details = richest_row.details if richest_row and isinstance(richest_row.details, dict) else {}
        return {
            "ok": True,
            **details,
            "source": "database",
            "stored_at": row.created_at.isoformat(),
            "snapshot_rank": {
                "candidates": len(rows),
                "selection": mode if mode in {"latest", "richest"} else "latest",
                "picked_requests": _snapshot_request_count(details),
                "latest_requests": _snapshot_request_count(latest_details),
                "latest_stored_at": latest_row.created_at.isoformat() if latest_row else None,
                "richest_requests": _snapshot_request_count(richest_details),
                "richest_stored_at": richest_row.created_at.isoformat() if richest_row else None,
            },
        }

    if TRACE_FILE.exists():
        try:
            payload = json.loads(TRACE_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return {"ok": False, "error": str(exc)}
        payload["ban_status"] = build_ban_status(
            payload.get("requests") or [],
            TRACE_FILE.parent / ".binance_cooldown.json",
        )
        return {"ok": True, **payload, "source": "local_file"}

    return {
        "ok": False,
        "error": (
            "No API trace data yet. Set SCANNER_API_TRACE=1 on the worker service, "
            "wait for the next 5-minute scan, then refresh."
        ),
    }
