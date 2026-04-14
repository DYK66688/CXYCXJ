from __future__ import annotations

import json
import sys
import unittest
from http import HTTPStatus
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[1]
if str(WORKSPACE / "src") not in sys.path:
    sys.path.insert(0, str(WORKSPACE / "src"))

from financial_qa_assistant.web import RebuildMutex, _guard_rebuild_request, _rebuild_busy_payload


class WebMutexTests(unittest.TestCase):
    def test_rebuild_mutex_rejects_duplicate_begin(self) -> None:
        mutex = RebuildMutex()

        self.assertTrue(mutex.begin())
        self.assertTrue(mutex.is_busy())
        self.assertFalse(mutex.begin())

        mutex.finish()
        self.assertFalse(mutex.is_busy())
        self.assertTrue(mutex.begin())
        mutex.finish()

    def test_ask_and_upload_are_blocked_during_rebuild(self) -> None:
        mutex = RebuildMutex()
        self.assertTrue(mutex.begin())

        ask_result = _guard_rebuild_request("/api/ask", mutex)
        upload_result = _guard_rebuild_request("/api/upload", mutex)
        rebuild_result = _guard_rebuild_request("/api/rebuild-database", mutex)

        self.assertIsNotNone(ask_result)
        self.assertIsNotNone(upload_result)
        self.assertIsNotNone(rebuild_result)
        ask_payload, ask_status = ask_result
        upload_payload, upload_status = upload_result
        rebuild_payload, rebuild_status = rebuild_result
        self.assertEqual(HTTPStatus.CONFLICT, ask_status)
        self.assertEqual(HTTPStatus.CONFLICT, upload_status)
        self.assertEqual(HTTPStatus.CONFLICT, rebuild_status)
        self.assertIn("提问", ask_payload["message"])
        self.assertIn("上传", upload_payload["message"])
        self.assertIn("重复", rebuild_payload["message"])

        mutex.finish()

    def test_read_only_or_export_routes_are_not_blocked(self) -> None:
        mutex = RebuildMutex()
        self.assertTrue(mutex.begin())

        self.assertIsNone(_guard_rebuild_request("/api/overview", mutex))
        self.assertIsNone(_guard_rebuild_request("/api/export-results", mutex))

        mutex.finish()

    def test_busy_payload_is_friendly_and_has_no_traceback(self) -> None:
        payload = _rebuild_busy_payload("/api/ask")
        text = json.dumps(payload, ensure_ascii=False)

        self.assertEqual("rebuild_in_progress", payload["code"])
        self.assertIn("请稍候", payload["message"])
        self.assertIn("error", payload)
        self.assertNotIn("Traceback", text)


if __name__ == "__main__":
    unittest.main()
