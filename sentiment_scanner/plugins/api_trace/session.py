"""Capture Binance HTTP calls and scanner phase timings."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

MAX_REQUESTS = 800
MAX_SAMPLE_ITEMS = 2
MAX_STRING_LEN = 240
PARTIAL_PERSIST_SECONDS = 90
PARTIAL_PERSIST_EVERY_REQUESTS = 40

from .ban_status import build_ban_status

logger = logging.getLogger("wei.api_trace")


class ApiTraceSession:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.run_id = str(uuid.uuid4())
        self.started_at = datetime.now(timezone.utc)
        self.started_mono = time.perf_counter()
        self.phase = "init"
        self.phases: list[dict[str, Any]] = []
        self.requests: list[dict[str, Any]] = []
        self._phase_started = time.perf_counter()
        self.trace_file = root / "sentiment_scanner" / "scanner_api_trace.json"
        self._last_partial_persist_mono = 0.0
        self._persist_lock = asyncio.Lock()

    def _partial_interval_seconds(self) -> float:
        raw = os.getenv("API_TRACE_PARTIAL_PERSIST_SECONDS", str(PARTIAL_PERSIST_SECONDS))
        try:
            return max(30.0, float(raw))
        except ValueError:
            return float(PARTIAL_PERSIST_SECONDS)

    def set_phase(self, name: str) -> None:
        now = time.perf_counter()
        if self.phases:
            self.phases[-1]["duration_ms"] = int((now - self._phase_started) * 1000)
        self.phases.append({
            "name": name,
            "started_at": datetime.now(timezone.utc).isoformat(),
        })
        self._phase_started = now
        self.phase = name
        self._schedule_partial_persist(force=True)

    def attach(self, client: Any) -> None:
        http_client = getattr(client, "_client", None)
        if http_client is not None and callable(getattr(http_client, "get", None)):
            original_http_get = http_client.get

            async def instrumented_http_get(url: str, *args: Any, **kwargs: Any) -> Any:
                return await self._record_http(original_http_get, url, *args, **kwargs)

            http_client.get = instrumented_http_get  # type: ignore[method-assign]
        else:
            original_get = client._get

            async def instrumented_get(base_url: str, path: str, params: dict[str, Any]) -> Any:
                return await self._record_request(original_get, base_url, path, params or {})

            client._get = instrumented_get  # type: ignore[method-assign]

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._partial_heartbeat())
        except RuntimeError:
            pass

    async def _record_http(
        self,
        original_http_get: Any,
        url: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Record each raw HTTP round-trip (including 418/429 retries inside Binance _get)."""
        parsed = urlparse(url)
        path = parsed.path or "/"
        raw_params = kwargs.get("params")
        query: dict[str, Any] = {}
        if isinstance(raw_params, dict):
            query = {k: v for k, v in raw_params.items() if v is not None and v != ""}
        display_url = url if not query else f"{url}?{urlencode(query)}"

        started = time.perf_counter()
        error: str | None = None
        status = 0
        try:
            response = await original_http_get(url, *args, **kwargs)
            status = int(response.status_code)
            if status >= 400:
                error = f"HTTP {status}"
            return response
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            response = getattr(exc, "response", None)
            if response is not None and getattr(response, "status_code", None):
                status = int(response.status_code)
            raise
        finally:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            self._append_request_row(
                path=path,
                url=display_url,
                query=query,
                status=status,
                duration_ms=elapsed_ms,
                error=error,
            )

    async def _partial_heartbeat(self) -> None:
        while True:
            await asyncio.sleep(self._partial_interval_seconds())
            await self._maybe_persist_partial(force=True)

    async def _record_request(
        self,
        original_get: Any,
        base_url: str,
        path: str,
        params: dict[str, Any],
    ) -> Any:
        query = {k: v for k, v in params.items() if v is not None and v != ""}
        url = f"{base_url}{path}"
        if query:
            url = f"{url}?{urlencode(query)}"

        started = time.perf_counter()
        error: str | None = None
        status = 200
        data: Any = None
        try:
            data = await original_get(base_url, path, params)
            return data
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            response = getattr(exc, "response", None)
            if response is not None and getattr(response, "status_code", None):
                status = int(response.status_code)
            else:
                status = 0
            raise
        finally:
            if len(self.requests) < MAX_REQUESTS:
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                self._append_request_row(
                    path=path,
                    url=url,
                    query=query,
                    status=status,
                    duration_ms=elapsed_ms,
                    error=error,
                    data=data if error is None else None,
                )

    def _append_request_row(
        self,
        *,
        path: str,
        url: str,
        query: dict[str, Any],
        status: int,
        duration_ms: int,
        error: str | None,
        data: Any = None,
    ) -> None:
        if len(self.requests) >= MAX_REQUESTS:
            return
        self.requests.append({
            "phase": self.phase,
            "url": url,
            "path": path,
            "params": query,
            "status": status,
            "duration_ms": duration_ms,
            "record_count": count_records(data) if error is None and data is not None else 0,
            "data_summary": summarize_data(data) if error is None and data is not None else None,
            "error": error,
            "at": datetime.now(timezone.utc).isoformat(),
        })
        every = int(os.getenv("API_TRACE_PARTIAL_EVERY_REQUESTS", str(PARTIAL_PERSIST_EVERY_REQUESTS)))
        if every > 0 and len(self.requests) % every == 0:
            self._schedule_partial_persist(force=True)

    def _schedule_partial_persist(self, *, force: bool) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._maybe_persist_partial(force=force))

    def _build_summary(self) -> dict[str, Any]:
        ok_requests = [row for row in self.requests if not row.get("error")]
        failed_requests = [row for row in self.requests if row.get("error")]
        endpoint_stats: dict[str, dict[str, Any]] = {}
        for row in ok_requests:
            path = str(row.get("path") or "")
            bucket = endpoint_stats.setdefault(path, {
                "count": 0,
                "total_ms": 0,
                "records": 0,
            })
            bucket["count"] += 1
            bucket["total_ms"] += int(row.get("duration_ms") or 0)
            bucket["records"] += int(row.get("record_count") or 0)

        for bucket in endpoint_stats.values():
            count = bucket["count"] or 1
            bucket["avg_ms"] = round(bucket["total_ms"] / count, 1)

        return {
            "total_requests": len(self.requests),
            "success_count": len(ok_requests),
            "error_count": len(failed_requests),
            "total_api_ms": sum(int(row.get("duration_ms") or 0) for row in self.requests),
            "avg_latency_ms": round(
                sum(int(row.get("duration_ms") or 0) for row in ok_requests) / max(len(ok_requests), 1),
                1,
            ),
            "total_records": sum(int(row.get("record_count") or 0) for row in ok_requests),
            "endpoints": endpoint_stats,
            "truncated": len(self.requests) >= MAX_REQUESTS,
        }

    def _build_payload(self, *, complete: bool, **meta: Any) -> dict[str, Any]:
        now = time.perf_counter()
        phases = [dict(row) for row in self.phases]
        if complete and phases:
            phases[-1] = dict(phases[-1])
            phases[-1]["duration_ms"] = int((now - self._phase_started) * 1000)

        total_ms = int((now - self.started_mono) * 1000)
        payload: dict[str, Any] = {
            "plugin": "api_trace",
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "duration_ms": total_ms,
            "complete": complete,
            "partial": not complete,
            "current_phase": self.phase,
            "phases": phases,
            "requests": self.requests,
            "summary": self._build_summary(),
            "scan": meta,
        }
        if complete:
            payload["finished_at"] = datetime.now(timezone.utc).isoformat()
        else:
            payload["snapshot_at"] = datetime.now(timezone.utc).isoformat()
        cooldown_path = self.trace_file.parent / ".binance_cooldown.json"
        payload["ban_status"] = build_ban_status(self.requests, cooldown_path)
        return payload

    def _write_trace_file(self, payload: dict[str, Any]) -> None:
        self.trace_file.parent.mkdir(parents=True, exist_ok=True)
        self.trace_file.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    async def _maybe_persist_partial(self, *, force: bool = False) -> None:
        if not self.requests:
            return
        interval = self._partial_interval_seconds()
        now = time.monotonic()
        if not force and now - self._last_partial_persist_mono < interval:
            return

        async with self._persist_lock:
            now = time.monotonic()
            if not force and now - self._last_partial_persist_mono < interval:
                return
            payload = self._build_payload(complete=False, note="partial_snapshot")
            self._write_trace_file(payload)
            self._last_partial_persist_mono = now
            try:
                from .store import persist_trace_payload

                ok = await persist_trace_payload(payload)
                if ok:
                    logger.info(
                        "API trace partial snapshot persisted (requests=%s, phase=%s)",
                        len(self.requests),
                        self.phase,
                    )
                    print(
                        f"API_TRACE partial ok requests={len(self.requests)} phase={self.phase}",
                        flush=True,
                    )
                else:
                    print(
                        f"API_TRACE partial file_only requests={len(self.requests)} phase={self.phase}",
                        flush=True,
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning("API trace partial persist failed: %s", exc)
                print(f"API_TRACE partial failed: {exc}", flush=True)

    def finish(self, **meta: Any) -> None:
        payload = self._build_payload(complete=True, **meta)
        self._write_trace_file(payload)


def count_records(data: Any) -> int:
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        if "symbols" in data and isinstance(data["symbols"], list):
            return len(data["symbols"])
        return len(data)
    return 1 if data is not None else 0


def summarize_data(data: Any) -> dict[str, Any]:
    if isinstance(data, list):
        summary: dict[str, Any] = {"type": "array", "count": len(data)}
        if not data:
            return summary
        if isinstance(data[0], list):
            summary["sample"] = data[:MAX_SAMPLE_ITEMS]
            if len(data) > MAX_SAMPLE_ITEMS:
                summary["sample"].append(data[-1])
            return summary
        if isinstance(data[0], dict):
            summary["fields"] = list(data[0].keys())[:16]
            summary["sample"] = [_truncate_dict(item) for item in data[:MAX_SAMPLE_ITEMS]]
            if len(data) > MAX_SAMPLE_ITEMS:
                summary["sample"].append(_truncate_dict(data[-1]))
            return summary
        summary["sample"] = data[:MAX_SAMPLE_ITEMS]
        return summary
    if isinstance(data, dict):
        keys = list(data.keys())
        return {
            "type": "object",
            "keys": keys[:24],
            "sample": {key: _truncate_value(data[key]) for key in keys[:8]},
        }
    return {"type": type(data).__name__, "value": _truncate_value(data)}


def _truncate_dict(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _truncate_value(value) for key, value in list(row.items())[:12]}


def _truncate_value(value: Any) -> Any:
    if isinstance(value, str) and len(value) > MAX_STRING_LEN:
        return value[:MAX_STRING_LEN] + "…"
    if isinstance(value, list):
        return value[:3]
    return value
