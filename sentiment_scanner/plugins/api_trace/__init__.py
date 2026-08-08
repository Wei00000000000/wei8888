"""API trace plugin — records Binance calls during scanner runs.

Enable with environment variable:
    SCANNER_API_TRACE=1

Optional (partial snapshots while scan runs — visible on test page even if worker times out):
    API_TRACE_PARTIAL_PERSIST_SECONDS=90
    API_TRACE_PARTIAL_EVERY_REQUESTS=40

Remove for production:
    1. Delete sentiment_scanner/plugins/api_trace/
    2. Remove hook block in scripts/update_seed_signals.py
    3. Remove try/except router block in backend/app/main.py
    4. Delete scanner-api-trace.html
"""

from __future__ import annotations

import os
from pathlib import Path

from .session import ApiTraceSession

__all__ = ["ApiTraceSession", "enabled", "maybe_start"]


def enabled() -> bool:
    return os.getenv("SCANNER_API_TRACE", "").lower() in {"1", "true", "yes", "on"}


def maybe_start(root: Path) -> ApiTraceSession | None:
    if not enabled():
        return None
    return ApiTraceSession(root)
