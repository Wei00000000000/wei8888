#!/usr/bin/env python3
"""Serve binance-api-test.html and proxy Binance public API with CORS headers.

Usage:
    python scripts/serve_binance_test.py
    python scripts/serve_binance_test.py --port 8765

Then open http://127.0.0.1:8765/binance-api-test.html
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEST_PAGE = ROOT / "binance-api-test.html"
ALLOWED_HOSTS = (
    "fapi.binance.com",
)
USER_AGENT = "wei-binance-test/1.0"


class Handler(BaseHTTPRequestHandler):
    server_version = "WeiBinanceTest/1.0"

    def log_message(self, fmt: str, *args) -> None:
        print(f"[{self.log_date_time_string()}] {self.address_string()} {fmt % args}")

    def _cors(self) -> None:
        origin = self.headers.get("Origin", "*")
        self.send_header("Access-Control-Allow-Origin", origin or "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Accept, Content-Type")
        self.send_header("Vary", "Origin")

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path in {"/", "/binance-api-test.html"}:
            return self._serve_test_page()
        if parsed.path == "/health":
            return self._json(200, {"ok": True, "service": "wei-binance-test"})
        if parsed.path == "/proxy":
            return self._proxy(parsed)
        self._text(404, "Not found")

    def _serve_test_page(self) -> None:
        if not TEST_PAGE.exists():
            self._text(500, f"Missing test page: {TEST_PAGE}")
            return
        body = TEST_PAGE.read_bytes()
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _proxy(self, parsed: urllib.parse.ParseResult) -> None:
        query = urllib.parse.parse_qs(parsed.query)
        target = (query.get("url") or [""])[0]
        if not target:
            return self._json(400, {"ok": False, "error": "Missing url query parameter"})
        target_url = urllib.parse.urlparse(target)
        if target_url.scheme != "https" or target_url.netloc not in ALLOWED_HOSTS:
            return self._json(400, {"ok": False, "error": "Only https://fapi.binance.com is allowed"})

        started = time.perf_counter()
        request = urllib.request.Request(
            target,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                raw = response.read()
                status = response.status
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            status = exc.code
        except Exception as exc:  # noqa: BLE001
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return self._json(502, {
                "ok": False,
                "request_url": target,
                "elapsed_ms": elapsed_ms,
                "error": str(exc),
            })

        elapsed_ms = int((time.perf_counter() - started) * 1000)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return self._json(502, {
                "ok": False,
                "request_url": target,
                "status": status,
                "elapsed_ms": elapsed_ms,
                "error": "Invalid JSON from Binance",
            })

        if status >= 400:
            return self._json(status, {
                "ok": False,
                "request_url": target,
                "status": status,
                "elapsed_ms": elapsed_ms,
                "error": json.dumps(data)[:500],
                "data": data,
            })

        self._json(200, {
            "ok": True,
            "request_url": target,
            "status": status,
            "elapsed_ms": elapsed_ms,
            "data": data,
        })

    def _json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _text(self, status: int, message: str) -> None:
        body = message.encode("utf-8")
        self.send_response(status)
        self._cors()
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve Binance API browser test page")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    url = f"http://{args.host}:{args.port}/binance-api-test.html"
    print(f"Serving Binance API test page at {url}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
