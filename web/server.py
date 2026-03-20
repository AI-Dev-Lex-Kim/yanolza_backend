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
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable


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
MONITOR_IDLE_WAIT_SECONDS = 1.0
MONITOR_START_FIELDS = {
    "user_id",
    "name",
    "url",
    "room_name",
    "scan_all",
    "stay_type",
    "check_in",
    "check_out",
    "interval_seconds",
    "start_notify",
    "ntfy_enabled",
}
MONITOR_STOP_FIELDS = {"monitor_id", "user_id"}


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


@dataclass(frozen=True)
class MonitorSpec:
    user_id: str
    name: str
    url: str
    room_name: str | None
    scan_all: bool
    stay_type: str | None
    check_in: str | None
    check_out: str | None
    interval_seconds: int
    start_notify: bool
    target_key: str


@dataclass
class MonitorState:
    last_status: str | None = None
    last_checked_at: str | None = None
    last_error: str | None = None
    next_run_at: str | None = None
    last_available_rooms: list[str] = field(default_factory=list)
    last_result: dict[str, Any] | None = None


@dataclass
class MonitorRecord:
    monitor_id: str
    spec: MonitorSpec
    state: MonitorState
    created_at: str
    updated_at: str
    running: bool = False
    start_notify_pending: bool = False
    next_run_mono: float = 0.0

    def to_public(self) -> dict[str, Any]:
        rooms = None
        if self.spec.scan_all:
            rooms = list(self.state.last_available_rooms)
        return {
            "monitor_id": self.monitor_id,
            "user_id": self.spec.user_id,
            "name": self.spec.name,
            "url": self.spec.url,
            "room_name": self.spec.room_name,
            "scan_all": self.spec.scan_all,
            "stay_type": self.spec.stay_type,
            "check_in": self.spec.check_in,
            "check_out": self.spec.check_out,
            "interval_seconds": self.spec.interval_seconds,
            "start_notify": self.spec.start_notify,
            "last_checked_at": self.state.last_checked_at,
            "last_status": self.state.last_status,
            "last_error": self.state.last_error,
            "next_run_at": self.state.next_run_at,
            "last_available_rooms": rooms,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class MonitorJob:
    monitor_id: str
    spec: MonitorSpec
    start_notify: bool


def normalize_monitor_text(value: Any, field_name: str) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    if not isinstance(value, str):
        return None, f"Invalid {field_name}"
    text = value.strip()
    if not text:
        return None, None
    return text, None


def build_monitor_key(
    user_id: str,
    url: str,
    room_name: str | None,
    scan_all: bool,
    stay_type: str | None,
    check_in: str | None,
    check_out: str | None,
) -> str:
    return json.dumps(
        {
            "user_id": user_id,
            "url": url,
            "room_name": room_name,
            "scan_all": scan_all,
            "stay_type": stay_type,
            "check_in": check_in,
            "check_out": check_out,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def parse_monitor_start(payload: dict[str, Any], ntfy_topic: str | None) -> tuple[MonitorSpec | None, str | None]:
    if not isinstance(payload, dict):
        return None, "Invalid payload"
    missing = []
    for key in MONITOR_START_FIELDS:
        if key not in payload:
            missing.append(key)
    if missing:
        missing.sort()
        return None, f"Missing fields: {', '.join(missing)}"
    extra = []
    for key in payload:
        if key not in MONITOR_START_FIELDS:
            extra.append(key)
    if extra:
        extra.sort()
        return None, f"Unexpected fields: {', '.join(extra)}"

    name, error = normalize_monitor_text(payload.get("name"), "name")
    if error:
        return None, error
    if not name:
        return None, "Missing name"
    user_id, error = normalize_monitor_text(payload.get("user_id"), "user_id")
    if error:
        return None, error
    if not user_id:
        return None, "Missing user_id"

    url, error = normalize_monitor_text(payload.get("url"), "url")
    if error:
        return None, error
    if not url:
        return None, "Missing url"

    room_name, error = normalize_monitor_text(payload.get("room_name"), "room_name")
    if error:
        return None, error
    stay_type, error = normalize_monitor_text(payload.get("stay_type"), "stay_type")
    if error:
        return None, error
    check_in, error = normalize_monitor_text(payload.get("check_in"), "check_in")
    if error:
        return None, error
    check_out, error = normalize_monitor_text(payload.get("check_out"), "check_out")
    if error:
        return None, error

    scan_all = payload.get("scan_all")
    if not isinstance(scan_all, bool):
        return None, "Invalid scan_all"
    start_notify = payload.get("start_notify")
    if not isinstance(start_notify, bool):
        return None, "Invalid start_notify"
    ntfy_enabled = payload.get("ntfy_enabled")
    if not isinstance(ntfy_enabled, bool):
        return None, "Invalid ntfy_enabled"
    if not ntfy_enabled:
        return None, "ntfy_enabled must be true"
    if not (ntfy_topic or "").strip():
        return None, "Missing NTFY_TOPIC"

    interval_seconds = payload.get("interval_seconds")
    if isinstance(interval_seconds, bool) or not isinstance(interval_seconds, int):
        return None, "Invalid interval_seconds"
    if interval_seconds <= 0:
        return None, "interval_seconds must be positive"

    if scan_all:
        room_name = None
    if not scan_all and not room_name:
        return None, "Missing room_name"

    target_key = build_monitor_key(user_id, url, room_name, scan_all, stay_type, check_in, check_out)
    return (
        MonitorSpec(
            user_id=user_id,
            name=name,
            url=url,
            room_name=room_name,
            scan_all=scan_all,
            stay_type=stay_type,
            check_in=check_in,
            check_out=check_out,
            interval_seconds=interval_seconds,
            start_notify=start_notify,
            target_key=target_key,
        ),
        None,
    )


def parse_monitor_stop(payload: dict[str, Any]) -> tuple[tuple[str, str] | None, str | None]:
    if not isinstance(payload, dict):
        return None, "Invalid payload"
    missing = []
    for key in MONITOR_STOP_FIELDS:
        if key not in payload:
            missing.append(key)
    if missing:
        return None, "Missing monitor_id"
    extra = []
    for key in payload:
        if key not in MONITOR_STOP_FIELDS:
            extra.append(key)
    if extra:
        extra.sort()
        return None, f"Unexpected fields: {', '.join(extra)}"
    monitor_id, error = normalize_monitor_text(payload.get("monitor_id"), "monitor_id")
    if error:
        return None, error
    if not monitor_id:
        return None, "Missing monitor_id"
    user_id, error = normalize_monitor_text(payload.get("user_id"), "user_id")
    if error:
        return None, error
    if not user_id:
        return None, "Missing user_id"
    return (monitor_id, user_id), None


def extract_available_rooms(result: dict[str, Any]) -> list[str]:
    matches = result.get("matches")
    if not isinstance(matches, list):
        return []
    rooms: list[str] = []
    seen: set[str] = set()
    for match in matches:
        if not isinstance(match, dict):
            continue
        if not bool(match.get("has_book")):
            continue
        room_name = str(match.get("h2_text") or "").strip()
        if not room_name or room_name in seen:
            continue
        seen.add(room_name)
        rooms.append(room_name)
    rooms.sort()
    return rooms


def build_monitor_message(
    spec: MonitorSpec,
    status: str,
    available_rooms: list[str],
    error_text: str | None,
    reason: str,
) -> str:
    stay_label = str(spec.stay_type or "").strip()
    prefix = f"[{stay_label}] " if stay_label else ""
    if reason == "start":
        if spec.scan_all:
            rooms_text = ", ".join(available_rooms) if available_rooms else "없음"
            return f"감시 시작 {prefix}{spec.name} status={status} rooms={rooms_text}".strip()
        if error_text:
            return f"감시 시작 {prefix}{spec.name} status=error error={error_text}".strip()
        return f"감시 시작 {prefix}{spec.name} status={status}".strip()
    if reason == "rooms_changed":
        rooms_text = ", ".join(available_rooms) if available_rooms else "없음"
        return f"가용 객실 변경 {prefix}{spec.name} rooms={rooms_text}".strip()
    return f"예약가능 {prefix}{spec.name}".strip()


def should_hold_shutdown(monitor_manager: "MonitorManager | None") -> bool:
    if monitor_manager is None:
        return False
    return monitor_manager.active_count() > 0


class MonitorManager:
    def __init__(
        self,
        monitor_logs: MonitorLogStore | None = None,
        check_fn: Callable[..., dict[str, Any]] | None = None,
        notify_fn: Callable[[str, str], dict[str, Any]] | None = None,
        topic_getter: Callable[[], str] | None = None,
    ) -> None:
        self._monitor_logs = monitor_logs
        if check_fn is None:
            check_fn = check_room
        if notify_fn is None:
            notify_fn = send_ntfy_message
        self._check_fn = check_fn
        self._notify_fn = notify_fn
        if topic_getter is None:
            topic_getter = lambda: os.environ.get("NTFY_TOPIC", "").strip()
        self._topic_getter = topic_getter
        self._lock = threading.Lock()
        self._records: dict[str, MonitorRecord] = {}
        self._monitor_ids: dict[str, str] = {}
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._running = False

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._running = True
            self._thread = threading.Thread(target=self._run, name="monitor-scheduler", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            thread = self._thread
            self._running = False
        self._stop.set()
        self._wake.set()
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)

    def scheduler_status(self) -> dict[str, bool]:
        with self._lock:
            thread = self._thread
            return {
                "running": self._running,
                "thread_alive": bool(thread and thread.is_alive()),
            }

    def active_count(self) -> int:
        with self._lock:
            return len(self._records)

    def list_monitors(self) -> list[dict[str, Any]]:
        with self._lock:
            items = []
            for record in self._records.values():
                items.append(record.to_public())
        items.sort(key=lambda item: ((item.get("name") or "").lower(), item.get("monitor_id") or ""))
        return items

    def list_user_monitors(self, user_id: str | None = None) -> list[dict[str, Any]]:
        items = self.list_monitors()
        if user_id is None:
            return items
        filtered = []
        for item in items:
            if item.get("user_id") == user_id:
                filtered.append(item)
        return filtered

    def upsert(self, spec: MonitorSpec) -> tuple[dict[str, Any], bool]:
        now_iso = datetime.now(timezone.utc).isoformat()
        next_run_iso = now_iso
        next_run_mono = time.monotonic()
        with self._lock:
            monitor_id = self._monitor_ids.get(spec.target_key)
            created = False
            if monitor_id is None:
                monitor_id = uuid.uuid4().hex
                state = MonitorState(next_run_at=next_run_iso)
                record = MonitorRecord(
                    monitor_id=monitor_id,
                    spec=spec,
                    state=state,
                    created_at=now_iso,
                    updated_at=now_iso,
                    start_notify_pending=spec.start_notify,
                    next_run_mono=next_run_mono,
                )
                self._records[monitor_id] = record
                self._monitor_ids[spec.target_key] = monitor_id
                created = True
            else:
                record = self._records[monitor_id]
                if record.spec.target_key != spec.target_key:
                    self._monitor_ids.pop(record.spec.target_key, None)
                    self._monitor_ids[spec.target_key] = monitor_id
                record.spec = spec
                record.updated_at = now_iso
                record.start_notify_pending = record.start_notify_pending or spec.start_notify
                record.next_run_mono = next_run_mono
                record.state.next_run_at = next_run_iso
            public = record.to_public()
        self._wake.set()
        return public, created

    def stop_monitor(self, monitor_id: str, user_id: str | None = None) -> bool:
        removed = False
        with self._lock:
            record = self._records.get(monitor_id)
            if record is None:
                return False
            if user_id is not None and record.spec.user_id != user_id:
                return False
            self._records.pop(monitor_id, None)
            if record is not None:
                self._monitor_ids.pop(record.spec.target_key, None)
                removed = True
        if removed:
            self._wake.set()
        return removed

    def run_due_once(self) -> bool:
        job = self._claim_due()
        if job is None:
            return False
        self._run_job(job)
        return True

    def run_all_due(self) -> int:
        count = 0
        while self.run_due_once():
            count += 1
        return count

    def _run(self) -> None:
        while not self._stop.is_set():
            if self.run_due_once():
                continue
            timeout = self._next_wait()
            self._wake.wait(timeout=timeout)
            self._wake.clear()
        with self._lock:
            self._running = False

    def _next_wait(self) -> float:
        now = time.monotonic()
        wait_seconds = MONITOR_IDLE_WAIT_SECONDS
        with self._lock:
            has_pending = False
            for record in self._records.values():
                if record.running:
                    continue
                has_pending = True
                delay = record.next_run_mono - now
                if delay <= 0:
                    return 0.0
                if delay < wait_seconds:
                    wait_seconds = delay
        if not has_pending:
            return MONITOR_IDLE_WAIT_SECONDS
        if wait_seconds < 0:
            return 0.0
        return wait_seconds

    def _claim_due(self) -> MonitorJob | None:
        now = time.monotonic()
        with self._lock:
            chosen: MonitorRecord | None = None
            for record in self._records.values():
                if record.running:
                    continue
                if record.next_run_mono > now:
                    continue
                if chosen is None or record.next_run_mono < chosen.next_run_mono:
                    chosen = record
            if chosen is None:
                return None
            chosen.running = True
            start_notify = chosen.start_notify_pending
            chosen.start_notify_pending = False
            return MonitorJob(
                monitor_id=chosen.monitor_id,
                spec=chosen.spec,
                start_notify=start_notify,
            )

    def _run_job(self, job: MonitorJob) -> None:
        started_at = time.perf_counter()
        checked_at = datetime.now(timezone.utc).isoformat()
        try:
            result = self._check_fn(
                job.spec.url,
                job.spec.room_name,
                job.spec.stay_type,
                job.spec.check_in,
                job.spec.check_out,
                job.spec.scan_all,
            )
            status = str(result.get("status") or "unknown")
            checked_url = str(result.get("url") or job.spec.url)
            available_rooms = extract_available_rooms(result)
            error_text = None
            ok = True
        except Exception as err:
            result = None
            status = "error"
            checked_url = job.spec.url
            available_rooms = []
            error_text = str(err)
            ok = False
        self._append_log(
            job=job,
            ok=ok,
            status=status,
            checked_url=checked_url,
            result=result,
            error_text=error_text,
            started_at=started_at,
        )
        self._finish_job(
            job=job,
            result=result,
            status=status,
            checked_at=checked_at,
            available_rooms=available_rooms,
            error_text=error_text,
        )

    def _append_log(
        self,
        job: MonitorJob,
        ok: bool,
        status: str,
        checked_url: str,
        result: dict[str, Any] | None,
        error_text: str | None,
        started_at: float,
    ) -> None:
        if self._monitor_logs is None:
            return
        match_count = None
        available = None
        if result is not None:
            matches = result.get("matches")
            if isinstance(matches, list):
                match_count = len(matches)
            available = bool(result.get("available"))
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "ok": ok,
            "status": status,
            "available": available,
            "match_count": match_count,
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 1),
            "url": checked_url,
            "room_name": job.spec.room_name,
            "scan_all": job.spec.scan_all,
            "stay_type": job.spec.stay_type,
            "check_in": job.spec.check_in,
            "check_out": job.spec.check_out,
            "user_id": job.spec.user_id,
            "error": error_text,
            "monitor_id": job.monitor_id,
            "monitor_name": job.spec.name,
            "mode": "background",
        }
        self._monitor_logs.append_check(event)

    def _finish_job(
        self,
        job: MonitorJob,
        result: dict[str, Any] | None,
        status: str,
        checked_at: str,
        available_rooms: list[str],
        error_text: str | None,
    ) -> None:
        with self._lock:
            record = self._records.get(job.monitor_id)
            if record is None:
                return
            previous_status = record.state.last_status
            previous_rooms = list(record.state.last_available_rooms)
            current_spec = record.spec
            record.state.last_result = result
            record.state.last_status = status
            record.state.last_checked_at = checked_at
            record.state.last_error = error_text
            record.state.last_available_rooms = list(available_rooms)
            record.updated_at = checked_at
            record.running = False
            record.next_run_mono = time.monotonic() + current_spec.interval_seconds
            record.state.next_run_at = (
                datetime.now(timezone.utc) + timedelta(seconds=current_spec.interval_seconds)
            ).isoformat()
            notify_reason = None
            if job.start_notify:
                notify_reason = "start"
            elif current_spec.scan_all:
                if previous_status is not None and previous_rooms != available_rooms:
                    notify_reason = "rooms_changed"
            elif status == "available" and previous_status not in {None, "available"}:
                notify_reason = "available"
            notify_spec = current_spec
        if notify_reason:
            self._notify(notify_spec, status, available_rooms, error_text, notify_reason)

    def _notify(
        self,
        spec: MonitorSpec,
        status: str,
        available_rooms: list[str],
        error_text: str | None,
        reason: str,
    ) -> None:
        topic = self._topic_getter()
        if not topic:
            return
        message = build_monitor_message(spec, status, available_rooms, error_text, reason)
        result = self._notify_fn(topic, message)
        if result.get("ok"):
            return
        print(f"Monitor notification failed: {result.get('error')}", flush=True)


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

    def _get_monitor_manager(self) -> MonitorManager | None:
        monitor_manager = getattr(self.server, "monitor_manager", None)
        if isinstance(monitor_manager, MonitorManager):
            return monitor_manager
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

        if self.path == "/monitors/start":
            monitor_manager = self._get_monitor_manager()
            if monitor_manager is None:
                self._send_json({"ok": False, "error": "Monitor manager unavailable"}, status=503)
                return
            topic = os.environ.get("NTFY_TOPIC", "").strip()
            spec, error = parse_monitor_start(payload, topic)
            if error:
                self._send_json({"ok": False, "error": error}, status=400)
                return
            monitor, created = monitor_manager.upsert(spec)
            self._send_json({"ok": True, "created": created, "monitor": monitor, "monitor_id": monitor["monitor_id"]})
            return

        if self.path == "/monitors/stop":
            monitor_manager = self._get_monitor_manager()
            if monitor_manager is None:
                self._send_json({"ok": False, "error": "Monitor manager unavailable"}, status=503)
                return
            stop_args, error = parse_monitor_stop(payload)
            if error:
                self._send_json({"ok": False, "error": error}, status=400)
                return
            monitor_id, user_id = stop_args
            removed = monitor_manager.stop_monitor(monitor_id, user_id=user_id)
            if not removed:
                self._send_json({"ok": False, "error": "Monitor not found"}, status=404)
                return
            self._send_json({"ok": True, "monitor_id": monitor_id})
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
            monitor_manager = self._get_monitor_manager()
            active_monitors = monitor_manager.active_count() if monitor_manager else 0
            scheduler = monitor_manager.scheduler_status() if monitor_manager else {
                "running": False,
                "thread_alive": False,
            }
            self._send_json(
                {
                    "ok": True,
                    "service": SERVICE_NAME,
                    "active_sessions": active_sessions,
                    "active_monitors": active_monitors,
                    "scheduler": scheduler,
                    "shutdown_supported": self._is_shutdown_allowed(),
                    "monitoring": monitor_summary,
                }
            )
            return
        if parsed.path == "/monitors":
            monitor_manager = self._get_monitor_manager()
            if monitor_manager is None:
                self._send_json({"ok": False, "error": "Monitor manager unavailable"}, status=503)
                return
            query = urllib.parse.parse_qs(parsed.query)
            raw_user_id = query.get("user_id", [None])[0]
            user_id = None
            if raw_user_id is not None:
                user_id, error = normalize_monitor_text(raw_user_id, "user_id")
                if error or not user_id:
                    self._send_json({"ok": False, "error": "Missing user_id"}, status=400)
                    return
            self._send_json({"ok": True, "monitors": monitor_manager.list_user_monitors(user_id=user_id)})
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
    monitor_manager: MonitorManager | None = None,
    allow_shutdown_api: bool = False,
) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), Handler)
    server.session_tracker = session_tracker
    server.monitor_logs = monitor_logs
    server.monitor_manager = monitor_manager
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
            monitor_manager = getattr(server, "monitor_manager", None)
            if should_hold_shutdown(monitor_manager):
                continue
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
    monitor_manager = MonitorManager(monitor_logs=monitor_logs)
    server = create_server(
        host,
        port,
        session_tracker=tracker,
        monitor_logs=monitor_logs,
        monitor_manager=monitor_manager,
        allow_shutdown_api=allow_shutdown_api,
    )
    monitor_manager.start()
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
    try:
        server.serve_forever()
    finally:
        monitor_manager.stop()


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
