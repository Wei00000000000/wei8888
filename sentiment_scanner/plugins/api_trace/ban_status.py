"""Derive 418 ban / scanner pause metadata for the API trace test page."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Keep in sync with sentiment_scanner.binance.COOLDOWN_SECONDS_418
SCANNER_PAUSE_AFTER_418_SECONDS = 600


def _parse_at_iso(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def build_ban_status(
    requests: list[dict[str, Any]],
    cooldown_path: Path,
) -> dict[str, Any]:
    count_418 = sum(1 for row in requests if row.get("status") == 418)
    count_429 = sum(1 for row in requests if row.get("status") == 429)
    last_418_at: str | None = None
    for row in reversed(requests):
        if row.get("status") == 418:
            last_418_at = str(row.get("at") or "") or None
            break

    until_unix = 0.0
    until_utc: str | None = None
    source = "none"

    if cooldown_path.exists():
        try:
            payload = json.loads(cooldown_path.read_text(encoding="utf-8"))
            until_unix = float(payload.get("global_blocked_until_unix") or 0.0)
            until_utc = payload.get("global_blocked_until_utc")
            if isinstance(until_utc, str):
                source = "cooldown_file"
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass

    if until_unix <= time.time() and last_418_at:
        parsed = _parse_at_iso(last_418_at)
        if parsed is not None:
            until_unix = parsed.timestamp() + SCANNER_PAUSE_AFTER_418_SECONDS
            until_utc = datetime.fromtimestamp(until_unix, tz=timezone.utc).isoformat()
            source = "estimated_from_last_418"

    now = time.time()
    remaining = max(0, int(until_unix - now)) if until_unix > now else 0
    pause_active = remaining > 0 and count_418 > 0

    return {
        "has_418": count_418 > 0,
        "status_418_count": count_418,
        "status_429_count": count_429,
        "last_418_at": last_418_at,
        "scanner_pause_until_utc": until_utc if pause_active else (until_utc if count_418 > 0 and remaining == 0 else None),
        "scanner_pause_remaining_seconds": remaining if count_418 > 0 else 0,
        "scanner_pause_active": pause_active,
        "pause_seconds_after_418": SCANNER_PAUSE_AFTER_418_SECONDS,
        "source": source,
        "note": (
            "scanner_pause_until 為本程式收到 418 後主動暫停發送請求的截止時間；"
            "Binance 實際 IP 解禁時間可能更晚，請以 probe 為準。"
        ),
    }
