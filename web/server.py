#!/usr/bin/env python3
"""Minimal web UI server and HTML checker with ntfy notifications."""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


def _get_runtime_root() -> Path:
    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            return Path(meipass)
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


RUNTIME_ROOT = _get_runtime_root()
sys.path.insert(0, str(RUNTIME_ROOT))

from app import RoomAvailabilityDetector


WEB_DIR = RUNTIME_ROOT / "web"
PROJECT_ENV_PATH = RUNTIME_ROOT / ".env"
NTFY_BASE_URL = "https://ntfy.sh"
SERVICE_NAME = "yanolja-refund-alert"
APP_DATA_DIR_NAME = "YanoljaRefundAlert"
SESSION_TIMEOUT_SECONDS = 45.0
SHUTDOWN_GRACE_SECONDS = 3.0
MONITOR_LOG_FILE_NAME = "monitor_history.jsonl"
MONITOR_STATE_FILE_NAME = "monitor_history_state.json"
MONITOR_KEEP_LIMIT = 10
DEFAULT_LOG_LIMIT = 60
MAX_LOG_LIMIT = 500


def normalize_client_type(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    if normalized in {"web", "cli"}:
        return normalized
    return "web"


@dataclass
class SessionRecord:
    session_id: str
    client_type: str
    client_label: str
    last_seen: float


@dataclass(frozen=True)
class SessionEvent:
    session_id: str
    client_type: str
    client_label: str
    reason: str
    active_sessions: int


class SessionTracker:
    def __init__(
        self,
        enabled: bool,
        auto_shutdown_enabled: bool = False,
        session_timeout: float = SESSION_TIMEOUT_SECONDS,
        shutdown_grace: float = SHUTDOWN_GRACE_SECONDS,
    ) -> None:
        self.enabled = enabled
        self.auto_shutdown_enabled = auto_shutdown_enabled
        self.session_timeout = max(10.0, float(session_timeout))
        self.shutdown_grace = max(1.0, float(shutdown_grace))
        self._lock = threading.Lock()
        self._sessions: dict[str, SessionRecord] = {}
        self._no_sessions_since: float | None = time.monotonic() if enabled and auto_shutdown_enabled else None

    @staticmethod
    def _resolve_label(session_id: str, client_label: str | None, current_label: str | None = None) -> str:
        candidate = ""
        if isinstance(client_label, str):
            candidate = client_label.strip()
        if candidate:
            return candidate
        if current_label:
            return current_label
        return session_id

    def _update_no_sessions_since_locked(self, now: float) -> None:
        if not self.auto_shutdown_enabled:
            return
        if self._sessions:
            self._no_sessions_since = None
            return
        if self._no_sessions_since is None:
            self._no_sessions_since = now

    def _prune_timeouts_locked(self, now: float) -> list[SessionEvent]:
        events: list[SessionEvent] = []
        stale_session_ids = [
            sid
            for sid, record in self._sessions.items()
            if (now - record.last_seen) > self.session_timeout
        ]
        for session_id in stale_session_ids:
            record = self._sessions.pop(session_id, None)
            if record is None:
                continue
            events.append(
                SessionEvent(
                    session_id=record.session_id,
                    client_type=record.client_type,
                    client_label=record.client_label,
                    reason="timeout",
                    active_sessions=len(self._sessions),
                )
            )
        self._update_no_sessions_since_locked(now)
        return events

    def start(
        self,
        session_id: str,
        client_type: str | None = None,
        client_label: str | None = None,
    ) -> tuple[int, list[SessionEvent]]:
        if not self.enabled:
            return 0, []
        now = time.monotonic()
        with self._lock:
            events = self._prune_timeouts_locked(now)
            resolved_type = normalize_client_type(client_type)
            resolved_label = self._resolve_label(session_id, client_label)
            self._sessions[session_id] = SessionRecord(
                session_id=session_id,
                client_type=resolved_type,
                client_label=resolved_label,
                last_seen=now,
            )
            self._update_no_sessions_since_locked(now)
            return len(self._sessions), events

    def heartbeat(
        self,
        session_id: str,
        client_type: str | None = None,
        client_label: str | None = None,
    ) -> tuple[int, list[SessionEvent]]:
        if not self.enabled:
            return 0, []
        now = time.monotonic()
        with self._lock:
            events = self._prune_timeouts_locked(now)
            previous = self._sessions.get(session_id)
            resolved_type = normalize_client_type(client_type or (previous.client_type if previous else None))
            resolved_label = self._resolve_label(
                session_id,
                client_label,
                current_label=(previous.client_label if previous else None),
            )
            self._sessions[session_id] = SessionRecord(
                session_id=session_id,
                client_type=resolved_type,
                client_label=resolved_label,
                last_seen=now,
            )
            self._update_no_sessions_since_locked(now)
            return len(self._sessions), events

    def end(self, session_id: str) -> tuple[int, list[SessionEvent]]:
        if not self.enabled:
            return 0, []
        now = time.monotonic()
        with self._lock:
            events = self._prune_timeouts_locked(now)
            record = self._sessions.pop(session_id, None)
            if record is not None:
                events.append(
                    SessionEvent(
                        session_id=record.session_id,
                        client_type=record.client_type,
                        client_label=record.client_label,
                        reason="end",
                        active_sessions=len(self._sessions),
                    )
                )
            self._update_no_sessions_since_locked(now)
            return len(self._sessions), events

    def collect_timeout_events(self) -> list[SessionEvent]:
        if not self.enabled:
            return []
        now = time.monotonic()
        with self._lock:
            return self._prune_timeouts_locked(now)

    def active_count(self) -> int:
        if not self.enabled:
            return 0
        with self._lock:
            return len(self._sessions)

    def should_shutdown(self) -> bool:
        if not self.enabled or not self.auto_shutdown_enabled:
            return False
        now = time.monotonic()
        with self._lock:
            if self._sessions:
                return False
            if self._no_sessions_since is None:
                self._no_sessions_since = now
                return False
            return (now - self._no_sessions_since) >= self.shutdown_grace


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def _find_app_bundle_path() -> Path | None:
    executable_path = Path(sys.executable).resolve()
    if executable_path.suffix == ".app":
        return executable_path
    for parent in executable_path.parents:
        if parent.suffix == ".app":
            return parent
    return None


def resolve_env_path(env_path: str | Path | None = None) -> Path:
    if env_path:
        return Path(env_path).expanduser()
    if getattr(sys, "frozen", False):
        app_bundle = _find_app_bundle_path()
        if app_bundle is not None:
            internal_env = app_bundle / "Contents" / "Resources" / ".env"
            if internal_env.exists():
                return internal_env
            return app_bundle.parent / ".env"
        executable_dir = Path(sys.executable).resolve().parent
        internal_fallback = executable_dir.parent / "Resources" / ".env"
        if internal_fallback.exists():
            return internal_fallback
        return executable_dir / ".env"
    return PROJECT_ENV_PATH


def resolve_data_dir() -> Path:
    candidates: list[Path] = []
    if getattr(sys, "frozen", False):
        home = Path.home()
        if sys.platform == "darwin":
            candidates.append(home / "Library" / "Application Support" / APP_DATA_DIR_NAME / "data")
        candidates.append(home / f".{SERVICE_NAME}" / "data")
    candidates.append(RUNTIME_ROOT / "data")
    candidates.append(Path.cwd() / "data")
    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        except OSError:
            continue
    raise OSError("Unable to resolve writable data directory for monitor logs")


class MonitorLogStore:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.log_path = data_dir / MONITOR_LOG_FILE_NAME
        self.state_path = data_dir / MONITOR_STATE_FILE_NAME
        self._lock = threading.Lock()
        self._state = self._load_or_rebuild_state()

    @staticmethod
    def _empty_state() -> dict[str, Any]:
        return {
            "total_checks": 0,
            "total_success": 0,
            "total_failures": 0,
            "consecutive_failures": 0,
            "first_event_at": None,
            "last_event_at": None,
            "last_success_at": None,
            "last_failure_at": None,
        }

    def _normalize_state(self, raw: dict[str, Any]) -> dict[str, Any]:
        state = self._empty_state()
        for key in state:
            if key in raw:
                state[key] = raw[key]
        for key in ("total_checks", "total_success", "total_failures", "consecutive_failures"):
            try:
                state[key] = max(0, int(state[key]))
            except (TypeError, ValueError):
                state[key] = 0
        for key in ("first_event_at", "last_event_at", "last_success_at", "last_failure_at"):
            value = state.get(key)
            state[key] = value if isinstance(value, str) or value is None else None
        return state

    def _load_or_rebuild_state(self) -> dict[str, Any]:
        if self.state_path.exists():
            try:
                raw = json.loads(self.state_path.read_text(encoding="utf-8"))
                return self._normalize_state(raw if isinstance(raw, dict) else {})
            except (OSError, json.JSONDecodeError):
                pass
        state = self._rebuild_state_from_log()
        self._persist_state(state)
        return state

    def _rebuild_state_from_log(self) -> dict[str, Any]:
        state = self._empty_state()
        if not self.log_path.exists():
            return state
        try:
            lines = self.log_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return state
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue
            self._update_state_inplace(state, event)
        return state

    def _persist_state(self, state: dict[str, Any]) -> None:
        self.state_path.write_text(
            json.dumps(state, indent=2, sort_keys=True, ensure_ascii=False),
            encoding="utf-8",
        )

    def _event_timestamp(self, event: dict[str, Any]) -> str:
        timestamp = event.get("timestamp")
        if isinstance(timestamp, str) and timestamp.strip():
            return timestamp
        return datetime.now(timezone.utc).isoformat()

    def _update_state_inplace(self, state: dict[str, Any], event: dict[str, Any]) -> None:
        timestamp = self._event_timestamp(event)
        if not state.get("first_event_at"):
            state["first_event_at"] = timestamp
        state["last_event_at"] = timestamp
        state["total_checks"] = int(state.get("total_checks", 0)) + 1
        if bool(event.get("ok")):
            state["total_success"] = int(state.get("total_success", 0)) + 1
            state["consecutive_failures"] = 0
            state["last_success_at"] = timestamp
            return
        state["total_failures"] = int(state.get("total_failures", 0)) + 1
        state["consecutive_failures"] = int(state.get("consecutive_failures", 0)) + 1
        state["last_failure_at"] = timestamp

    def append_check(self, event: dict[str, Any]) -> None:
        with self._lock:
            events: list[dict[str, Any]] = []
            if self.log_path.exists():
                try:
                    lines = self.log_path.read_text(encoding="utf-8").splitlines()
                except OSError:
                    lines = []
                for line in lines:
                    text = line.strip()
                    if not text:
                        continue
                    try:
                        payload = json.loads(text)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(payload, dict):
                        events.append(payload)
            events.append(event)
            if len(events) > MONITOR_KEEP_LIMIT:
                events = events[-MONITOR_KEEP_LIMIT:]
            with self.log_path.open("w", encoding="utf-8") as handle:
                for payload in events:
                    line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
                    handle.write(line)
                    handle.write("\n")
            self._state = self._empty_state()
            for payload in events:
                self._update_state_inplace(self._state, payload)
            self._persist_state(self._state)

    def recent(self, limit: int = DEFAULT_LOG_LIMIT) -> list[dict[str, Any]]:
        safe_limit = max(1, min(MAX_LOG_LIMIT, int(limit)))
        with self._lock:
            if not self.log_path.exists():
                return []
            try:
                lines = self.log_path.read_text(encoding="utf-8").splitlines()
            except OSError:
                return []
        events: list[dict[str, Any]] = []
        for line in reversed(lines[-safe_limit:]):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                events.append(payload)
        return events

    def summary(self) -> dict[str, Any]:
        with self._lock:
            state = dict(self._state)
            file_size_bytes = 0
            if self.log_path.exists():
                try:
                    file_size_bytes = self.log_path.stat().st_size
                except OSError:
                    file_size_bytes = 0
        total_checks = int(state.get("total_checks", 0) or 0)
        avg_bytes_per_check = round(file_size_bytes / total_checks, 1) if total_checks else 0.0
        state["log_path"] = str(self.log_path)
        state["state_path"] = str(self.state_path)
        state["file_size_bytes"] = file_size_bytes
        state["avg_bytes_per_check"] = avg_bytes_per_check
        return state


def fetch_html(url: str, timeout: float = 10.0) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
    except Exception:
        ctx = ssl._create_unverified_context()
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            data = resp.read()
    return data.decode("utf-8", errors="ignore")


def build_url_with_dates(url: str, check_in: str | None, check_out: str | None) -> str:
    if not check_in or not check_out:
        return url
    parsed = urllib.parse.urlparse(url)
    query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    query["checkInDate"] = check_in
    query["checkOutDate"] = check_out
    new_query = urllib.parse.urlencode(query)
    return urllib.parse.urlunparse(parsed._replace(query=new_query))


def check_room(
    url: str,
    room_name: str | None,
    stay_type: str | None,
    check_in: str | None,
    check_out: str | None,
    scan_all: bool,
) -> dict[str, Any]:
    url_with_dates = build_url_with_dates(url, check_in, check_out)
    html = fetch_html(url_with_dates)
    detector = RoomAvailabilityDetector(room_name)
    detector.feed(html)
    available, details = detector.evaluate(stay_type=stay_type, scan_all=scan_all)
    display_details = details
    if stay_type:
        display_details = [d for d in details if d.get("stay_type_match")]
    room_found = bool(details)
    stay_type_found = None
    if stay_type:
        stay_type_found = any(d.get("stay_type_match") for d in details)
    any_closed = any(d.get("stay_type_match") and d.get("has_closed") for d in details)
    if not room_found:
        status = "room_not_found"
    elif stay_type and not stay_type_found:
        status = "stay_type_not_found"
    else:
        status = "available" if available else "closed" if any_closed else "unknown"
    return {
        "available": available,
        "status": status,
        "matches": display_details,
        "url": url_with_dates,
        "room_found": room_found,
        "stay_type_found": stay_type_found,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


def send_ntfy_message(topic: str, text: str) -> dict[str, Any]:
    try:
        import requests
    except ModuleNotFoundError:
        return {"ok": False, "error": "requests package is not installed"}
    safe_text = (text or "").strip()
    if not topic:
        return {"ok": False, "error": "NTFY topic missing"}
    if not safe_text:
        return {"ok": False, "error": "message missing"}
    try:
        response = requests.post(
            f"{NTFY_BASE_URL}/{topic}",
            headers={"Priority": "high"},
            data=safe_text,
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as err:
        error_text = ""
        if err.response is not None:
            error_text = (err.response.text or "").strip()
        if not error_text:
            error_text = str(err)
        return {"ok": False, "error": error_text}
    return {"ok": True, "response": (response.text or "").strip()}


def build_disconnect_alert_message(event: SessionEvent) -> str:
    client_tag = normalize_client_type(event.client_type).upper()
    client_label = event.client_label or event.session_id
    return (
        f"연결 끊김 [{client_tag}] {client_label} "
        f"reason={event.reason} active_sessions={event.active_sessions}"
    )


def dispatch_disconnect_events(events: list[SessionEvent]) -> None:
    if not events:
        return
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic:
        print("Disconnect alert skipped: NTFY_TOPIC missing", flush=True)
        return
    for event in events:
        message = build_disconnect_alert_message(event)
        result = send_ntfy_message(topic, message)
        if result.get("ok"):
            continue
        print(f"Disconnect alert send failed: {result.get('error')}", flush=True)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, directory=str(WEB_DIR), **kwargs)

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def _get_tracker(self) -> SessionTracker | None:
        tracker = getattr(self.server, "session_tracker", None)
        if isinstance(tracker, SessionTracker):
            return tracker
        return None

    def _get_monitor_logs(self) -> MonitorLogStore | None:
        monitor_logs = getattr(self.server, "monitor_logs", None)
        if isinstance(monitor_logs, MonitorLogStore):
            return monitor_logs
        return None

    def _is_shutdown_allowed(self) -> bool:
        return bool(getattr(self.server, "allow_shutdown_api", False))

    def do_POST(self) -> None:
        if self.path == "/server/shutdown":
            if not self._is_shutdown_allowed():
                self._send_json({"ok": False, "error": "Server shutdown API disabled"}, status=403)
                return
            action = (self.headers.get("X-YRA-Action") or "").strip().lower()
            if action != "shutdown":
                self._send_json({"ok": False, "error": "Missing or invalid shutdown action header"}, status=403)
                return
            self._send_json({"ok": True})

            def _shutdown_server() -> None:
                time.sleep(0.1)
                self.server.shutdown()

            threading.Thread(target=_shutdown_server, name="server-shutdown", daemon=True).start()
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            self.send_error(400, "Invalid JSON")
            return

        if self.path in {"/session/start", "/session/heartbeat", "/session/end"}:
            tracker = self._get_tracker()
            if tracker is None or not tracker.enabled:
                self.send_error(503, "Session tracking disabled")
                return
            session_id = payload.get("session_id")
            if not isinstance(session_id, str) or not session_id.strip():
                self.send_error(400, "Missing session_id")
                return
            safe_session_id = session_id.strip()
            client_type = payload.get("client_type")
            if not isinstance(client_type, str):
                client_type = None
            client_label = payload.get("client_label")
            if not isinstance(client_label, str):
                client_label = None
            if self.path == "/session/start":
                active_sessions, events = tracker.start(safe_session_id, client_type=client_type, client_label=client_label)
            elif self.path == "/session/heartbeat":
                active_sessions, events = tracker.heartbeat(
                    safe_session_id,
                    client_type=client_type,
                    client_label=client_label,
                )
            else:
                active_sessions, events = tracker.end(safe_session_id)
            dispatch_disconnect_events(events)
            self._send_json({"ok": True, "active_sessions": active_sessions})
            return

        if self.path == "/check":
            url = payload.get("url", "").strip()
            room_name = payload.get("room_name")
            if isinstance(room_name, str):
                room_name = room_name.strip() or None
            scan_all = bool(payload.get("scan_all"))
            stay_type = payload.get("stay_type")
            stay_type = stay_type.strip() if isinstance(stay_type, str) else None
            stay_type = stay_type or None
            check_in = payload.get("check_in")
            check_in = check_in.strip() if isinstance(check_in, str) else None
            check_in = check_in or None
            check_out = payload.get("check_out")
            check_out = check_out.strip() if isinstance(check_out, str) else None
            check_out = check_out or None
            monitor_logs = self._get_monitor_logs()
            started_at = time.perf_counter()

            def _record_log(
                *,
                ok: bool,
                status: str,
                available: bool | None = None,
                match_count: int | None = None,
                checked_url: str | None = None,
                error_text: str | None = None,
            ) -> None:
                if monitor_logs is None:
                    return
                duration_ms = round((time.perf_counter() - started_at) * 1000, 1)
                monitor_logs.append_check(
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "ok": ok,
                        "status": status,
                        "available": bool(available) if available is not None else None,
                        "match_count": int(match_count) if match_count is not None else None,
                        "duration_ms": duration_ms,
                        "url": checked_url or url,
                        "room_name": room_name,
                        "scan_all": scan_all,
                        "stay_type": stay_type,
                        "check_in": check_in,
                        "check_out": check_out,
                        "error": error_text,
                    }
                )

            if not url or (not room_name and not scan_all):
                _record_log(ok=False, status="bad_request", error_text="Missing url or room_name")
                self._send_json({"ok": False, "error": "Missing url or room_name"}, status=400)
                return
            try:
                result = check_room(url, room_name, stay_type, check_in, check_out, scan_all)
            except Exception as err:
                _record_log(ok=False, status="error", error_text=str(err))
                self._send_json({"ok": False, "error": str(err)}, status=500)
                return
            _record_log(
                ok=True,
                status=str(result.get("status") or "unknown"),
                available=bool(result.get("available")),
                match_count=len(result.get("matches", [])) if isinstance(result.get("matches"), list) else None,
                checked_url=str(result.get("url") or url),
            )
            self._send_json(result)
            return

        if self.path == "/notify":
            message = (payload.get("message") or "").strip()
            _ = payload.get("link")
            _ = payload.get("access_token")
            if not message:
                self.send_error(400, "Missing message")
                return
            topic = os.environ.get("NTFY_TOPIC", "").strip()
            if not topic:
                self.send_error(400, "Missing NTFY_TOPIC")
                return
            result = send_ntfy_message(topic, message)
            self._send_json(result)
            return

        self.send_error(404, "Not Found")

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/health":
            tracker = self._get_tracker()
            active_sessions = tracker.active_count() if tracker else 0
            monitor_logs = self._get_monitor_logs()
            monitor_summary = monitor_logs.summary() if monitor_logs else None
            self._send_json(
                {
                    "ok": True,
                    "service": SERVICE_NAME,
                    "active_sessions": active_sessions,
                    "shutdown_supported": self._is_shutdown_allowed(),
                    "monitoring": monitor_summary,
                }
            )
            return
        if parsed.path == "/monitor/logs":
            monitor_logs = self._get_monitor_logs()
            if monitor_logs is None:
                self._send_json({"ok": False, "error": "Monitor log store unavailable"}, status=503)
                return
            query = urllib.parse.parse_qs(parsed.query)
            limit = DEFAULT_LOG_LIMIT
            raw_limit = query.get("limit", [str(DEFAULT_LOG_LIMIT)])[0]
            try:
                limit = int(raw_limit)
            except (TypeError, ValueError):
                limit = DEFAULT_LOG_LIMIT
            self._send_json(
                {
                    "ok": True,
                    "logs": monitor_logs.recent(limit=limit),
                    "summary": monitor_logs.summary(),
                }
            )
            return
        super().do_GET()


def create_server(
    host: str,
    port: int,
    session_tracker: SessionTracker | None = None,
    monitor_logs: MonitorLogStore | None = None,
    allow_shutdown_api: bool = False,
) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), Handler)
    server.session_tracker = session_tracker
    server.monitor_logs = monitor_logs
    server.allow_shutdown_api = bool(allow_shutdown_api)
    return server


def _start_shutdown_watcher(server: ThreadingHTTPServer, tracker: SessionTracker) -> None:
    if not tracker.enabled:
        return

    def _watch() -> None:
        while True:
            time.sleep(1.0)
            events = tracker.collect_timeout_events()
            dispatch_disconnect_events(events)
            if tracker.should_shutdown():
                print("No active browser sessions detected. Shutting down server.", flush=True)
                server.shutdown()
                return

    threading.Thread(target=_watch, name="shutdown-watcher", daemon=True).start()


def run_server(
    host: str = "127.0.0.1",
    port: int = 8787,
    env_path: str | Path | None = None,
    auto_shutdown: bool = False,
    disconnect_alerts: bool = False,
    session_timeout: float = SESSION_TIMEOUT_SECONDS,
    shutdown_grace: float = SHUTDOWN_GRACE_SECONDS,
    allow_shutdown_api: bool = False,
) -> None:
    resolved_env_path = resolve_env_path(env_path)
    load_env(resolved_env_path)
    tracking_enabled = auto_shutdown or disconnect_alerts
    tracker = SessionTracker(
        enabled=tracking_enabled,
        auto_shutdown_enabled=auto_shutdown,
        session_timeout=session_timeout,
        shutdown_grace=shutdown_grace,
    )
    data_dir = resolve_data_dir()
    monitor_logs = MonitorLogStore(data_dir=data_dir)
    server = create_server(
        host,
        port,
        session_tracker=tracker,
        monitor_logs=monitor_logs,
        allow_shutdown_api=allow_shutdown_api,
    )
    print(f"Serving on http://{host}:{port}", flush=True)
    print(f"Env path: {resolved_env_path}", flush=True)
    print(f"Monitor logs: {monitor_logs.log_path}", flush=True)
    if auto_shutdown:
        print(
            f"Auto-shutdown enabled (session-timeout={tracker.session_timeout}s, grace={tracker.shutdown_grace}s)",
            flush=True,
        )
    if disconnect_alerts:
        print(f"Disconnect alerts enabled (session-timeout={tracker.session_timeout}s)", flush=True)
    if allow_shutdown_api:
        print("Shutdown API enabled (POST /server/shutdown)", flush=True)
    _start_shutdown_watcher(server, tracker)
    server.serve_forever()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Yanolja refund alert web server")
    parser.add_argument("--host", default="127.0.0.1", help="bind host")
    parser.add_argument("--port", type=int, default=8787, help="bind port")
    parser.add_argument("--env-path", default=None, help="path to .env file")
    parser.add_argument("--auto-shutdown", action="store_true", help="shutdown server when browser session ends")
    parser.add_argument(
        "--session-timeout",
        type=float,
        default=SESSION_TIMEOUT_SECONDS,
        help="seconds to keep a browser session alive without heartbeat",
    )
    parser.add_argument(
        "--shutdown-grace",
        type=float,
        default=SHUTDOWN_GRACE_SECONDS,
        help="seconds to wait after last session ends before shutdown",
    )
    parser.add_argument(
        "--allow-shutdown-api",
        action="store_true",
        help="allow POST /server/shutdown with X-YRA-Action: shutdown",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_server(
        host=args.host,
        port=args.port,
        env_path=args.env_path,
        auto_shutdown=args.auto_shutdown,
        session_timeout=args.session_timeout,
        shutdown_grace=args.shutdown_grace,
        allow_shutdown_api=args.allow_shutdown_api,
    )


if __name__ == "__main__":
    main()
