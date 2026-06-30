from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from backend.app.importer import iter_legacy_rows


class ImporterPrecedenceTest(unittest.TestCase):
    def test_live_seed_overrides_history_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "data"
            scanner = root / "sentiment_scanner"
            data.mkdir()
            scanner.mkdir()
            (data / "manifest.json").write_text(
                json.dumps({"history_chunks": ["history.json"]}), encoding="utf-8"
            )
            (data / "history.json").write_text(
                json.dumps([{"id": "same", "reached_state": "holding"}]), encoding="utf-8"
            )
            (scanner / "seed_signals.json").write_text(
                json.dumps([{"id": "same", "reached_state": "invalid", "status": "closed"}]),
                encoding="utf-8",
            )

            rows = list(iter_legacy_rows(root))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["reached_state"], "invalid")
        self.assertEqual(rows[0]["status"], "closed")


if __name__ == "__main__":
    unittest.main()
