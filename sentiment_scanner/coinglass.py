from __future__ import annotations

from dataclasses import dataclass
import json
import os
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


COINGLASS_BASE = os.getenv("COINGLASS_BASE_URL", "https://open-api-v4.coinglass.com")


@dataclass
class CoinGlassClient:
    api_key: str | None = None
    timeout: float = 20.0

    def __post_init__(self) -> None:
        if self.api_key is None:
            self.api_key = os.getenv("COINGLASS_API_KEY")

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        if not self.api_key:
            raise RuntimeError("COINGLASS_API_KEY is not configured")
        query = urlencode(params or {})
        url = f"{COINGLASS_BASE}{path}"
        if query:
            url = f"{url}?{query}"
        request = Request(
            url,
            headers={
                "accept": "application/json",
                "User-Agent": "wei-strategy-room/0.1",
                "CG-API-KEY": self.api_key,
            },
        )
        with urlopen(request, timeout=self.timeout) as response:
            payload = response.read().decode("utf-8")
        return json.loads(payload)

    def coins_markets(self) -> list[dict[str, Any]]:
        payload = self._get("/api/futures/coins-markets")
        data = payload.get("data") if isinstance(payload, dict) else payload
        if isinstance(data, dict):
            for key in ("list", "rows", "data"):
                value = data.get(key)
                if isinstance(value, list):
                    return [row for row in value if isinstance(row, dict)]
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        return []

    def funding_rates(self) -> list[dict[str, Any]]:
        payload = self._get("/api/futures/funding-rate/exchange-list")
        data = payload.get("data") if isinstance(payload, dict) else payload
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        return []
