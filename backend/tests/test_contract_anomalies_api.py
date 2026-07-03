from __future__ import annotations

import json
from unittest.mock import patch

from backend.app.routers.market import read_contract_anomalies


def test_read_contract_anomalies_returns_rows(tmp_path) -> None:
    source = tmp_path / "contract_anomalies.json"
    source.write_text(json.dumps({"rows": [{"symbol": "BTCUSDT"}], "updated_at": "2026-07-03T00:00:00Z"}), encoding="utf-8")

    with patch("backend.app.routers.market.CONTRACT_ANOMALIES_FILE", source):
        payload = read_contract_anomalies()

    assert payload["rows"] == [{"symbol": "BTCUSDT"}]
    assert payload["updated_at"] == "2026-07-03T00:00:00Z"


def test_read_contract_anomalies_handles_invalid_json(tmp_path) -> None:
    source = tmp_path / "contract_anomalies.json"
    source.write_text("not-json", encoding="utf-8")

    with patch("backend.app.routers.market.CONTRACT_ANOMALIES_FILE", source):
        assert read_contract_anomalies() == {"rows": [], "updated_at": None}
