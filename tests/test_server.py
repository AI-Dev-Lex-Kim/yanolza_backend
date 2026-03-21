from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

from web import server

DAYUSE_HTML = """
<div id="PLACE_SECTION_DAYUSE">
  <div>
    <div>
      <div>
        <div>
          <div>
            <div>
              <h2>Deluxe 21</h2>
              <div>
                <div>대실</div>
                <div>상세보기</div>
                <div>최대 5시간</div>
                <div>(운영시간 12:00 ~ 21:00)</div>
                <div>예약하기</div>
              </div>
              <div>
                <div>숙박</div>
                <div>예약마감</div>
              </div>
            </div>
            <div>
              <h2>Deluxe 22</h2>
              <div>
                <div>대실</div>
                <div>상세보기</div>
                <div>최대 5시간</div>
                <div>(운영시간 12:00 ~ 22:00)</div>
                <div>예약하기</div>
              </div>
              <div>
                <div>숙박</div>
                <div>예약마감</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""


class QueueCheck:
    def __init__(self) -> None:
        self.items: list[object] = []

    def push_result(self, result: dict[str, object]) -> None:
        self.items.append(result)

    def push_error(self, error: Exception) -> None:
        self.items.append(error)

    def __call__(
        self,
        url: str,
        room_name: str | None,
        stay_type: str | None,
        check_in: str | None,
        check_out: str | None,
        dayuse_end_time: str | None,
        scan_all: bool,
    ) -> dict[str, object]:
        if self.items:
            item = self.items.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        return {
            "available": False,
            "status": "closed",
            "matches": [],
            "url": url,
        }


class TimedCheck:
    def __init__(self) -> None:
        self.items: list[tuple[float, dict[str, object]]] = []
        self.start_at: float | None = None

    def push_result(self, at: float, result: dict[str, object]) -> None:
        self.items.append((at, result))
        self.items.sort(key=lambda item: item[0])

    def __call__(
        self,
        url: str,
        room_name: str | None,
        stay_type: str | None,
        check_in: str | None,
        check_out: str | None,
        dayuse_end_time: str | None,
        scan_all: bool,
    ) -> dict[str, object]:
        if self.start_at is None:
            self.start_at = time.monotonic()
        elapsed = time.monotonic() - self.start_at
        current = None
        for at, result in self.items:
            if elapsed >= at:
                current = result
            else:
                break
        if current is not None:
            return current
        return {
            "available": False,
            "status": "closed",
            "matches": [],
            "url": url,
        }


def wait_for_count(items: list[object], count: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if len(items) >= count:
            return True
        time.sleep(0.05)
    return len(items) >= count


class NtfyHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8")
        self.server.calls.append(
            {
                "path": self.path,
                "body": body,
                "priority": self.headers.get("Priority"),
            }
        )
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format: str, *args: object) -> None:
        return


def make_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "user_id": "user-a",
        "name": "watch-one",
        "url": "https://example.com/room",
        "room_name": "Deluxe",
        "scan_all": False,
        "stay_type": None,
        "check_in": None,
        "check_out": None,
        "dayuse_end_time": None,
        "interval_seconds": 30,
        "start_notify": False,
        "ntfy_enabled": True,
    }
    payload.update(overrides)
    return payload


def make_spec(**overrides: object) -> server.MonitorSpec:
    payload = make_payload(**overrides)
    spec, error = server.parse_monitor_start(payload, "topic")
    if error:
        raise AssertionError(error)
    return spec


def force_due(manager: server.MonitorManager, monitor_id: str) -> None:
    with manager._lock:
        record = manager._records[monitor_id]
        record.next_run_mono = time.monotonic() - 1.0
        record.state.next_run_at = "due"


class CheckRoomTests(unittest.TestCase):
    def test_check_room_allows_dayuse_end_time_range(self) -> None:
        with mock.patch.object(server, "fetch_html", return_value=DAYUSE_HTML):
            result = server.check_room(
                "https://example.com/room",
                "Deluxe",
                "대실",
                None,
                None,
                "오후 9~10",
                False,
            )

        self.assertTrue(result["available"])
        self.assertEqual("available", result["status"])
        self.assertTrue(result["dayuse_end_time_found"])
        self.assertEqual(2, len(result["matches"]))

    def test_check_room_filters_dayuse_end_time(self) -> None:
        with mock.patch.object(server, "fetch_html", return_value=DAYUSE_HTML):
            result = server.check_room(
                "https://example.com/room",
                "Deluxe",
                "대실",
                None,
                None,
                "22:00",
                False,
            )

        self.assertTrue(result["available"])
        self.assertEqual("available", result["status"])
        self.assertTrue(result["dayuse_end_time_found"])
        self.assertEqual(1, len(result["matches"]))
        self.assertEqual("Deluxe 22", result["matches"][0]["h2_text"])
        self.assertEqual(["22:00"], result["matches"][0]["dayuse_end_times"])

    def test_check_room_returns_not_found_for_missing_dayuse_end_time(self) -> None:
        with mock.patch.object(server, "fetch_html", return_value=DAYUSE_HTML):
            result = server.check_room(
                "https://example.com/room",
                "Deluxe",
                "대실",
                None,
                None,
                "23:00",
                False,
            )

        self.assertFalse(result["available"])
        self.assertEqual("dayuse_end_time_not_found", result["status"])
        self.assertFalse(result["dayuse_end_time_found"])
        self.assertEqual([], result["matches"])

    def test_parse_monitor_start_treats_any_dayuse_end_time_as_none(self) -> None:
        spec = make_spec(stay_type="대실", dayuse_end_time="상관없음")

        self.assertIsNone(spec.dayuse_end_time)


class MonitorManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.logs = server.MonitorLogStore(Path(self.tmpdir.name))
        self.check = QueueCheck()
        self.notify_calls: list[tuple[str, str]] = []
        self.manager = server.MonitorManager(
            monitor_logs=self.logs,
            check_fn=self.check,
            notify_fn=self.fake_notify,
            topic_getter=lambda: "topic",
        )

    def tearDown(self) -> None:
        self.manager.stop()
        self.tmpdir.cleanup()

    def fake_notify(self, topic: str, message: str) -> dict[str, object]:
        self.notify_calls.append((topic, message))
        return {"ok": True}

    def test_upsert_reuses_monitor_id_for_same_target(self) -> None:
        first, created = self.manager.upsert(make_spec())
        second, created_again = self.manager.upsert(
            make_spec(name="watch-two", interval_seconds=10, start_notify=True)
        )

        self.assertTrue(created)
        self.assertFalse(created_again)
        self.assertEqual(first["monitor_id"], second["monitor_id"])
        monitors = self.manager.list_monitors()
        self.assertEqual(1, len(monitors))
        self.assertEqual("watch-two", monitors[0]["name"])
        self.assertEqual(10, monitors[0]["interval_seconds"])

    def test_upsert_creates_distinct_monitor_for_different_target(self) -> None:
        first, _ = self.manager.upsert(make_spec())
        second, _ = self.manager.upsert(make_spec(url="https://example.com/other"))

        self.assertNotEqual(first["monitor_id"], second["monitor_id"])
        self.assertEqual(2, self.manager.active_count())

    def test_upsert_creates_distinct_monitor_for_different_user(self) -> None:
        first, _ = self.manager.upsert(make_spec())
        second, _ = self.manager.upsert(make_spec(user_id="user-b"))

        self.assertNotEqual(first["monitor_id"], second["monitor_id"])
        self.assertEqual(2, self.manager.active_count())

    def test_upsert_creates_distinct_monitor_for_different_dayuse_end_time(self) -> None:
        first, _ = self.manager.upsert(make_spec(stay_type="대실", dayuse_end_time="21:00"))
        second, _ = self.manager.upsert(make_spec(stay_type="대실", dayuse_end_time="22:00"))

        self.assertNotEqual(first["monitor_id"], second["monitor_id"])
        self.assertEqual(2, self.manager.active_count())

    def test_stop_monitor_removes_record(self) -> None:
        monitor, _ = self.manager.upsert(make_spec())

        removed = self.manager.stop_monitor(monitor["monitor_id"], user_id="user-a")

        self.assertTrue(removed)
        self.assertEqual(0, self.manager.active_count())
        self.assertEqual([], self.manager.list_monitors())

    def test_single_monitor_notifies_on_closed_to_available_transition(self) -> None:
        self.check.push_result(
            {
                "available": False,
                "status": "closed",
                "matches": [],
                "url": "https://example.com/room",
            }
        )
        self.check.push_result(
            {
                "available": True,
                "status": "available",
                "matches": [{"h2_text": "Deluxe", "has_book": True}],
                "url": "https://example.com/room",
            }
        )
        self.check.push_result(
            {
                "available": True,
                "status": "available",
                "matches": [{"h2_text": "Deluxe", "has_book": True}],
                "url": "https://example.com/room",
            }
        )
        monitor, _ = self.manager.upsert(make_spec())

        self.assertEqual(1, self.manager.run_all_due())
        force_due(self.manager, monitor["monitor_id"])
        self.assertEqual(1, self.manager.run_all_due())
        force_due(self.manager, monitor["monitor_id"])
        self.assertEqual(1, self.manager.run_all_due())

        self.assertEqual(1, len(self.notify_calls))
        self.assertIn("예약가능", self.notify_calls[0][1])

    def test_single_monitor_notifies_when_initial_check_is_available(self) -> None:
        self.check.push_result(
            {
                "available": True,
                "status": "available",
                "matches": [{"h2_text": "Deluxe", "has_book": True}],
                "url": "https://example.com/room",
            }
        )

        self.manager.upsert(make_spec())
        self.assertEqual(1, self.manager.run_all_due())

        self.assertEqual(1, len(self.notify_calls))
        self.assertIn("예약가능", self.notify_calls[0][1])

    def test_scan_all_notifies_on_available_room_changes(self) -> None:
        self.check.push_result(
            {
                "available": True,
                "status": "available",
                "matches": [{"h2_text": "A", "has_book": True}],
                "url": "https://example.com/room",
            }
        )
        self.check.push_result(
            {
                "available": True,
                "status": "available",
                "matches": [{"h2_text": "A", "has_book": True}],
                "url": "https://example.com/room",
            }
        )
        self.check.push_result(
            {
                "available": True,
                "status": "available",
                "matches": [
                    {"h2_text": "A", "has_book": True},
                    {"h2_text": "B", "has_book": True},
                ],
                "url": "https://example.com/room",
            }
        )
        self.check.push_result(
            {
                "available": False,
                "status": "closed",
                "matches": [],
                "url": "https://example.com/room",
            }
        )
        monitor, _ = self.manager.upsert(make_spec(scan_all=True, room_name=None))

        self.manager.run_all_due()
        force_due(self.manager, monitor["monitor_id"])
        self.manager.run_all_due()
        force_due(self.manager, monitor["monitor_id"])
        self.manager.run_all_due()
        force_due(self.manager, monitor["monitor_id"])
        self.manager.run_all_due()

        self.assertEqual(3, len(self.notify_calls))
        self.assertIn("예약가능", self.notify_calls[0][1])
        self.assertIn("예약 가능 객실 변경", self.notify_calls[1][1])
        self.assertIn("B", self.notify_calls[1][1])
        self.assertIn("없음", self.notify_calls[2][1])

    def test_failed_check_updates_error_and_keeps_monitor(self) -> None:
        self.check.push_error(RuntimeError("boom"))
        monitor, _ = self.manager.upsert(make_spec())

        self.manager.run_all_due()

        monitors = self.manager.list_monitors()
        self.assertEqual(1, self.manager.active_count())
        self.assertEqual(monitor["monitor_id"], monitors[0]["monitor_id"])
        self.assertEqual("error", monitors[0]["last_status"])
        self.assertEqual("boom", monitors[0]["last_error"])

    def test_should_hold_shutdown_when_monitor_active(self) -> None:
        monitor, _ = self.manager.upsert(make_spec())

        self.assertTrue(server.should_hold_shutdown(self.manager))

        self.manager.stop_monitor(monitor["monitor_id"], user_id="user-a")
        self.assertFalse(server.should_hold_shutdown(self.manager))


class MonitorLogStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.logs = server.MonitorLogStore(Path(self.tmpdir.name))

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_log_store_keeps_up_to_180_entries(self) -> None:
        for idx in range(server.MONITOR_KEEP_LIMIT + 5):
            self.logs.append_check(
                {
                    "timestamp": f"2026-03-21T00:00:{idx:02d}+00:00",
                    "ok": True,
                    "status": "available",
                    "url": f"https://example.com/{idx}",
                }
            )

        events = self.logs.recent(limit=server.MAX_LOG_LIMIT)

        self.assertEqual(180, len(events))
        self.assertEqual("https://example.com/184", events[0]["url"])
        self.assertEqual("https://example.com/5", events[-1]["url"])


class RealtimeMonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.logs = server.MonitorLogStore(Path(self.tmpdir.name))
        self.ntfy = ThreadingHTTPServer(("127.0.0.1", 0), NtfyHandler)
        self.ntfy.calls = []
        self.ntfy_thread = threading.Thread(target=self.ntfy.serve_forever, daemon=True)
        self.ntfy_thread.start()
        self.prev_base_url = server.NTFY_BASE_URL
        server.NTFY_BASE_URL = f"http://127.0.0.1:{self.ntfy.server_address[1]}"

    def tearDown(self) -> None:
        server.NTFY_BASE_URL = self.prev_base_url
        self.ntfy.shutdown()
        self.ntfy_thread.join(timeout=2.0)
        self.ntfy.server_close()
        self.tmpdir.cleanup()

    def test_realtime_scan_all_sends_ntfy_for_first_and_later_dayuse_rooms(self) -> None:
        check = TimedCheck()
        check.push_result(
            0.0,
            {
                "available": False,
                "status": "closed",
                "matches": [],
                "url": "https://example.com/room",
            },
        )
        check.push_result(
            0.4,
            {
                "available": True,
                "status": "available",
                "matches": [{"h2_text": "A", "has_book": True}],
                "url": "https://example.com/room",
            },
        )
        check.push_result(
            1.4,
            {
                "available": True,
                "status": "available",
                "matches": [
                    {"h2_text": "A", "has_book": True},
                    {"h2_text": "B", "has_book": True},
                ],
                "url": "https://example.com/room",
            },
        )
        manager = server.MonitorManager(
            monitor_logs=self.logs,
            check_fn=check,
            notify_fn=server.send_ntfy_message,
            topic_getter=lambda: "topic",
        )
        manager.start()
        self.addCleanup(manager.stop)

        manager.upsert(
            make_spec(
                name="realtime-dayuse-scan",
                scan_all=True,
                room_name=None,
                stay_type="대실",
                interval_seconds=1,
            )
        )

        self.assertTrue(wait_for_count(self.ntfy.calls, 2, timeout=3.5))
        time.sleep(0.3)

        self.assertEqual(2, len(self.ntfy.calls))
        self.assertEqual("/topic", self.ntfy.calls[0]["path"])
        self.assertEqual("high", self.ntfy.calls[0]["priority"])
        self.assertIn("예약가능 [대실] realtime-dayuse-scan", self.ntfy.calls[0]["body"])
        self.assertEqual("/topic", self.ntfy.calls[1]["path"])
        self.assertEqual("high", self.ntfy.calls[1]["priority"])
        self.assertIn("예약 가능 객실 변경 [대실] realtime-dayuse-scan", self.ntfy.calls[1]["body"])
        self.assertIn("A, B", self.ntfy.calls[1]["body"])

    def test_realtime_single_room_sends_ntfy_when_dayuse_appears_later(self) -> None:
        check = TimedCheck()
        check.push_result(
            0.0,
            {
                "available": False,
                "status": "closed",
                "matches": [],
                "url": "https://example.com/room",
            },
        )
        check.push_result(
            0.4,
            {
                "available": True,
                "status": "available",
                "matches": [{"h2_text": "Deluxe", "has_book": True}],
                "url": "https://example.com/room",
            },
        )
        manager = server.MonitorManager(
            monitor_logs=self.logs,
            check_fn=check,
            notify_fn=server.send_ntfy_message,
            topic_getter=lambda: "topic",
        )
        manager.start()
        self.addCleanup(manager.stop)

        manager.upsert(
            make_spec(
                name="realtime-dayuse-room",
                room_name="Deluxe",
                stay_type="대실",
                interval_seconds=1,
            )
        )

        self.assertTrue(wait_for_count(self.ntfy.calls, 1, timeout=2.5))
        time.sleep(0.3)

        self.assertEqual(1, len(self.ntfy.calls))
        self.assertEqual("/topic", self.ntfy.calls[0]["path"])
        self.assertEqual("high", self.ntfy.calls[0]["priority"])
        self.assertIn("예약가능 [대실] realtime-dayuse-room", self.ntfy.calls[0]["body"])


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.logs = server.MonitorLogStore(Path(self.tmpdir.name))
        self.check = QueueCheck()
        self.manager = server.MonitorManager(
            monitor_logs=self.logs,
            check_fn=self.check,
            notify_fn=lambda topic, message: {"ok": True},
            topic_getter=lambda: "topic",
        )
        self.httpd = server.create_server(
            "127.0.0.1",
            0,
            session_tracker=server.SessionTracker(enabled=False),
            monitor_logs=self.logs,
            monitor_manager=self.manager,
        )
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        self.prev_topic = os.environ.get("NTFY_TOPIC")
        os.environ["NTFY_TOPIC"] = "topic"

    def tearDown(self) -> None:
        if self.prev_topic is None:
            os.environ.pop("NTFY_TOPIC", None)
        else:
            os.environ["NTFY_TOPIC"] = self.prev_topic
        self.httpd.shutdown()
        self.thread.join(timeout=2.0)
        self.httpd.server_close()
        self.tmpdir.cleanup()

    def post_json(self, path: str, payload: dict[str, object]) -> tuple[int, dict[str, object]]:
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=3.0) as response:
                body = json.loads(response.read().decode("utf-8"))
                return response.status, body
        except urllib.error.HTTPError as err:
            body = json.loads(err.read().decode("utf-8"))
            status = err.code
            err.close()
            return status, body

    def get_json(self, path: str) -> tuple[int, dict[str, object]]:
        request = urllib.request.Request(f"{self.base_url}{path}", method="GET")
        with urllib.request.urlopen(request, timeout=3.0) as response:
            body = json.loads(response.read().decode("utf-8"))
            return response.status, body

    def test_start_monitor_accepts_valid_request(self) -> None:
        status, body = self.post_json("/monitors/start", make_payload())

        self.assertEqual(200, status)
        self.assertTrue(body["ok"])
        self.assertTrue(body["created"])
        self.assertIsNotNone(body["monitor_id"])
        self.assertEqual("user-a", body["monitor"]["user_id"])

    def test_start_monitor_accepts_dayuse_end_time_range(self) -> None:
        status, body = self.post_json(
            "/monitors/start",
            make_payload(stay_type="대실", dayuse_end_time="오후 9~10"),
        )

        self.assertEqual(200, status)
        self.assertTrue(body["ok"])
        self.assertEqual("오후 9~10", body["monitor"]["dayuse_end_time"])

    def test_start_monitor_rejects_ntfy_disabled(self) -> None:
        status, body = self.post_json("/monitors/start", make_payload(ntfy_enabled=False))

        self.assertEqual(400, status)
        self.assertFalse(body["ok"])
        self.assertIn("ntfy_enabled", body["error"])

    def test_start_monitor_rejects_invalid_requests(self) -> None:
        cases = [
            make_payload(user_id=""),
            make_payload(url=""),
            make_payload(interval_seconds=0),
            make_payload(room_name=None, scan_all=False),
            make_payload(stay_type="대실", dayuse_end_time="bad"),
            make_payload(stay_type="대실", dayuse_end_time="오후 15~16"),
            make_payload(dayuse_end_time="22:00"),
        ]

        for payload in cases:
            status, body = self.post_json("/monitors/start", payload)
            self.assertEqual(400, status)
            self.assertFalse(body["ok"])

    def test_check_rejects_dayuse_end_time_without_dayuse_stay_type(self) -> None:
        status, body = self.post_json(
            "/check",
            {
                "url": "https://example.com/room",
                "room_name": "Deluxe",
                "scan_all": False,
                "stay_type": "숙박",
                "dayuse_end_time": "22:00",
            },
        )

        self.assertEqual(400, status)
        self.assertFalse(body["ok"])
        self.assertIn("dayuse_end_time", body["error"])

    def test_monitors_list_and_stop(self) -> None:
        _, start_body = self.post_json("/monitors/start", make_payload())
        self.post_json("/monitors/start", make_payload(user_id="user-b", url="https://example.com/other"))

        status, list_body = self.get_json("/monitors?user_id=user-a")
        self.assertEqual(200, status)
        self.assertEqual(1, len(list_body["monitors"]))
        self.assertEqual("user-a", list_body["monitors"][0]["user_id"])

        stop_status, stop_body = self.post_json(
            "/monitors/stop",
            {"monitor_id": start_body["monitor_id"], "user_id": "user-a"},
        )
        self.assertEqual(200, stop_status)
        self.assertTrue(stop_body["ok"])

        _, after_body = self.get_json("/monitors?user_id=user-a")
        self.assertEqual([], after_body["monitors"])

        _, other_body = self.get_json("/monitors?user_id=user-b")
        self.assertEqual(1, len(other_body["monitors"]))

    def test_stop_missing_monitor_returns_404(self) -> None:
        status, body = self.post_json("/monitors/stop", {"monitor_id": "missing", "user_id": "user-a"})

        self.assertEqual(404, status)
        self.assertFalse(body["ok"])
        self.assertEqual("Monitor not found", body["error"])

    def test_stop_with_other_user_returns_404(self) -> None:
        _, start_body = self.post_json("/monitors/start", make_payload())

        status, body = self.post_json(
            "/monitors/stop",
            {"monitor_id": start_body["monitor_id"], "user_id": "user-b"},
        )

        self.assertEqual(404, status)
        self.assertFalse(body["ok"])
        self.assertEqual("Monitor not found", body["error"])

    def test_health_exposes_active_monitors_and_scheduler(self) -> None:
        self.post_json("/monitors/start", make_payload())

        status, body = self.get_json("/health")

        self.assertEqual(200, status)
        self.assertTrue(body["ok"])
        self.assertEqual(1, body["active_monitors"])
        self.assertIn("scheduler", body)
        self.assertIn("running", body["scheduler"])
        self.assertIn("thread_alive", body["scheduler"])


if __name__ == "__main__":
    unittest.main()
