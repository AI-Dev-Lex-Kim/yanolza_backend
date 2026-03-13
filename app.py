#!/usr/bin/env python3
"""Refund availability alert prototype (mock provider)."""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.request
import ssl
import urllib.error
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

NTFY_BASE_URL = "https://ntfy.sh"


@dataclass(frozen=True)
class WatchItem:
    item_id: str
    item_type: str
    name: str
    date: str
    criteria: dict[str, Any]


@dataclass(frozen=True)
class Availability:
    available: bool
    price: int | None
    source: str


class MockProvider:
    def __init__(self, timeline_path: Path, start_monotonic: float) -> None:
        self._timeline = self._load_timeline(timeline_path)
        self._start = start_monotonic

    @staticmethod
    def _load_timeline(path: Path) -> dict[str, list[dict[str, Any]]]:
        if not path.exists():
            return {}
        data = json.loads(path.read_text())
        # Normalize and sort by time
        for item_id, events in data.items():
            events.sort(key=lambda e: e.get("t", 0))
        return data

    def get_availability(self, item: WatchItem) -> Availability:
        events = self._timeline.get(item.item_id, [])
        elapsed = time.monotonic() - self._start
        current = None
        for event in events:
            if elapsed >= float(event.get("t", 0)):
                current = event
            else:
                break
        if current is None:
            return Availability(available=False, price=None, source="mock")
        return Availability(
            available=bool(current.get("available", False)),
            price=(int(current["price"]) if "price" in current else None),
            source="mock",
        )


class YanoljaHtmlProvider:
    def __init__(self, timeout: float = 10.0, debug: bool = False) -> None:
        self._timeout = timeout
        self._debug = debug

    def get_availability(self, item: WatchItem) -> Availability:
        url = item.criteria.get("url")
        room_name = item.criteria.get("room_name")
        stay_type = item.criteria.get("stay_type")
        scan_all = bool(item.criteria.get("scan_all"))
        if not url:
            return Availability(available=False, price=None, source="yanolja-html")
        html = self._fetch_html(url)
        detector = RoomAvailabilityDetector(room_name)
        detector.feed(html)
        available, details = detector.evaluate(stay_type=stay_type, scan_all=scan_all)
        if self._debug:
            self._print_debug(item, details)
        return Availability(available=available, price=None, source="yanolja-html")

    def _fetch_html(self, url: str) -> str:
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
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                data = resp.read()
        except urllib.error.URLError:
            # Prototype fallback for local SSL trust issues
            ctx = ssl._create_unverified_context()
            with urllib.request.urlopen(req, timeout=self._timeout, context=ctx) as resp:
                data = resp.read()
        return data.decode("utf-8", errors="ignore")

    def _print_debug(self, item: WatchItem, details: list[dict[str, Any]]) -> None:
        print("\n[DEBUG] Room scan")
        print(f"  item_id: {item.item_id}")
        print(f"  room_name: {item.criteria.get('room_name')}")
        print(f"  stay_type: {item.criteria.get('stay_type')}")
        if not details:
            print("  matches: 0")
            return
        print(f"  matches: {len(details)}")
        for idx, detail in enumerate(details, 1):
            print(f"  match_{idx}: {detail.get('h2_text')}")
            print(f"    container_found: {detail.get('container_found')}")
            print(f"    has_book: {detail.get('has_book')}")
            print(f"    has_closed: {detail.get('has_closed')}")
            print(f"    stay_type_match: {detail.get('stay_type_match')}")
            print(f"    stay_type_nodes: {detail.get('stay_type_nodes')}")
            print(f"    stay_type_scopes: {detail.get('stay_type_scopes')}")


class HtmlNode:
    def __init__(self, tag: str, attrs: dict[str, str], parent: "HtmlNode | None") -> None:
        self.tag = tag
        self.attrs = attrs
        self.parent = parent
        self.children: list[HtmlNode] = []
        self.text_parts: list[str] = []

    def add_child(self, node: "HtmlNode") -> None:
        self.children.append(node)

    def add_text(self, text: str) -> None:
        if text.strip():
            self.text_parts.append(text)

    def get_text(self) -> str:
        parts = list(self.text_parts)
        for child in self.children:
            parts.append(child.get_text())
        return "".join(parts)

    @staticmethod
    def _normalize_text(value: str) -> str:
        return " ".join(value.split())

    def ancestors(self, count: int) -> "HtmlNode | None":
        node: HtmlNode | None = self
        for _ in range(count):
            if node is None:
                return None
            node = node.parent
        return node

    def has_ancestor_id_contains(self, token: str) -> bool:
        token_lower = token.lower()
        node: HtmlNode | None = self
        while node is not None:
            node_id = node.attrs.get("id", "")
            if token_lower in node_id.lower():
                return True
            node = node.parent
        return False

    def descendants(self) -> list["HtmlNode"]:
        nodes = []
        stack = list(self.children)
        while stack:
            node = stack.pop()
            nodes.append(node)
            stack.extend(node.children)
        return nodes

    def has_descendant_text(self, needle: str) -> bool:
        if not needle:
            return False
        for node in self.descendants():
            if needle in node.get_text():
                return True
        return False

    def has_descendant_exact_text(self, needle: str) -> bool:
        if not needle:
            return False
        for node in self.descendants():
            if self._normalize_text(node.get_text()) == needle:
                return True
        return False

    def find_descendants_with_text(self, needle: str) -> list["HtmlNode"]:
        if not needle:
            return []
        hits = []
        for node in self.descendants():
            if needle in node.get_text():
                hits.append(node)
        return hits

    def find_descendants_with_exact_text(self, needle: str) -> list["HtmlNode"]:
        if not needle:
            return []
        hits = []
        for node in self.descendants():
            if self._normalize_text(node.get_text()) == needle:
                hits.append(node)
        return hits

    def find_nearest_ancestor_with_descendant_text(
        self,
        needle: str,
        limit: int = 12,
        boundary: "HtmlNode | None" = None,
    ) -> "HtmlNode | None":
        node: HtmlNode | None = self.parent
        steps = 0
        while node is not None and steps < limit:
            if node.has_descendant_text(needle):
                return node
            if boundary is not None and node == boundary:
                break
            node = node.parent
            steps += 1
        return None

    def find_nearest_ancestor_with_descendant_exact_text(
        self,
        needle: str,
        limit: int = 12,
        boundary: "HtmlNode | None" = None,
    ) -> "HtmlNode | None":
        node: HtmlNode | None = self.parent
        steps = 0
        while node is not None and steps < limit:
            if node.has_descendant_exact_text(needle):
                return node
            if boundary is not None and node == boundary:
                break
            node = node.parent
            steps += 1
        return None


class RoomAvailabilityDetector(HTMLParser):
    def __init__(self, room_name: str | None) -> None:
        super().__init__(convert_charrefs=True)
        self.room_name = room_name or ""
        self.root = HtmlNode("root", {}, None)
        self.current = self.root
        self.h2_nodes: list[HtmlNode] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = {k: (v or "") for k, v in attrs}
        node = HtmlNode(tag, attrs_dict, self.current)
        self.current.add_child(node)
        self.current = node
        if tag.lower() == "h2":
            self.h2_nodes.append(node)

    def handle_endtag(self, tag: str) -> None:
        if self.current.parent is not None:
            self.current = self.current.parent

    def handle_data(self, data: str) -> None:
        self.current.add_text(data)

    def evaluate(
        self, stay_type: str | None = None, scan_all: bool = False
    ) -> tuple[bool, list[dict[str, Any]]]:
        if not self.room_name and not scan_all:
            return False, []
        excluded_h2_phrases = [
            "NOL AI로 최근 후기를 요약했어요",
        ]
        excluded_normalized = [
            "".join(phrase.split()) for phrase in excluded_h2_phrases
        ]
        target_nodes = []
        for h2 in self.h2_nodes:
            if not h2.has_ancestor_id_contains("PLACE_SECTION"):
                continue
            h2_text = h2.get_text()
            normalized = " ".join(h2_text.split())
            normalized_compact = "".join(h2_text.split())
            if any(phrase in normalized for phrase in excluded_h2_phrases):
                continue
            if any(phrase in normalized_compact for phrase in excluded_normalized):
                continue
            if scan_all or (
                self.room_name and (self.room_name in h2_text or self.room_name in normalized)
            ):
                target_nodes.append(h2)
        details: list[dict[str, Any]] = []
        available = False
        other_type = None
        if stay_type == "숙박":
            other_type = "대실"
        elif stay_type == "대실":
            other_type = "숙박"
        for h2 in target_nodes:
            container = h2.ancestors(5)
            if stay_type and container is not None:
                candidate = h2.find_nearest_ancestor_with_descendant_exact_text(
                    stay_type, boundary=container
                )
                if candidate is not None:
                    container = candidate
            if container is None:
                details.append(
                    {
                        "h2_text": h2.get_text().strip(),
                        "container_found": False,
                        "has_book": False,
                        "has_closed": False,
                    }
                )
                continue
            stay_nodes: list[HtmlNode] = []
            stay_match = True
            if stay_type:
                stay_nodes = container.find_descendants_with_exact_text(stay_type)
                stay_match = bool(stay_nodes)
            scope_nodes = stay_nodes if stay_type else [container]
            if not scope_nodes and stay_type:
                details.append(
                    {
                        "h2_text": h2.get_text().strip(),
                        "container_found": True,
                        "has_book": False,
                        "has_closed": False,
                        "stay_type_match": False,
                        "stay_type_nodes": 0,
                    }
                )
                continue
            has_book = False
            has_closed = False
            matched_scopes = 0
            for scope in scope_nodes:
                reservation_scope = scope
                if stay_type:
                    reservation_scope = self._find_reservation_scope(
                        scope, stay_type, other_type, boundary=container
                    )
                    if reservation_scope is None:
                        continue
                matched_scopes += 1
                nodes = [reservation_scope] + reservation_scope.descendants()
                for node in nodes:
                    text = node.get_text()
                    if "예약하기" in text or "예약가능" in text:
                        has_book = True
                    if "예약마감" in text:
                        has_closed = True
            details.append(
                {
                    "h2_text": h2.get_text().strip(),
                    "container_found": True,
                    "has_book": has_book,
                    "has_closed": has_closed,
                    "stay_type_match": stay_match,
                    "stay_type_nodes": len(stay_nodes) if stay_type else None,
                    "stay_type_scopes": matched_scopes if stay_type else None,
                }
            )
            if stay_match and has_book:
                available = True
        return available, details

    def _find_reservation_scope(
        self,
        node: HtmlNode,
        stay_type: str,
        other_type: str | None,
        limit: int = 8,
        boundary: HtmlNode | None = None,
    ) -> HtmlNode | None:
        current = node
        steps = 0
        while current is not None and steps < limit:
            text = current.get_text()
            has_reservation = "예약하기" in text or "예약가능" in text or "예약마감" in text
            has_other = other_type in text if other_type else False
            if has_reservation and not has_other:
                return current
            if current.parent is None:
                break
            if boundary is not None and current == boundary:
                break
            current = current.parent
            steps += 1
        return None


class ConsoleNotifier:
    def __init__(self, ntfy_topic: str | None = None) -> None:
        self.ntfy_topic = ntfy_topic

    def notify(self, item: WatchItem, availability: Availability) -> None:
        price = f"{availability.price}" if availability.price is not None else "unknown"
        timestamp = datetime.now(timezone.utc).isoformat()
        print("\n[ALERT] Refund availability detected")
        print(f"  time_utc: {timestamp}")
        print(f"  item_id: {item.item_id}")
        print(f"  type: {item.item_type}")
        print(f"  name: {item.name}")
        print(f"  date: {item.date}")
        print(f"  price: {price}")
        print(f"  source: {availability.source}")
        self._notify_ntfy(item)

    def _notify_ntfy(self, item: WatchItem) -> None:
        if not self.ntfy_topic:
            return
        stay_type = str(item.criteria.get("stay_type") or "").strip()
        stay_label = f"[{stay_type}] " if stay_type else ""
        message = f"예약가능 {stay_label}{item.name}".strip()
        result = send_ntfy_message(self.ntfy_topic, message)
        if not result.get("ok"):
            print(f"  ntfy_error: {result.get('error')}")


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


def load_watchlist(path: Path) -> list[WatchItem]:
    data = json.loads(path.read_text())
    items = []
    for raw in data:
        items.append(
            WatchItem(
                item_id=raw["id"],
                item_type=raw["type"],
                name=raw["name"],
                date=raw["date"],
                criteria=raw.get("criteria", {}),
            )
        )
    return items


def load_state(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def save_state(path: Path, state: dict[str, dict[str, Any]]) -> None:
    path.write_text(json.dumps(state, indent=2, sort_keys=True))


def poll_once(
    items: list[WatchItem],
    provider: MockProvider | YanoljaHtmlProvider,
    notifier: ConsoleNotifier,
    state: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    now = datetime.now(timezone.utc).isoformat()
    updated = dict(state)
    for item in items:
        availability = provider.get_availability(item)
        prev = state.get(item.item_id, {})
        prev_available = bool(prev.get("available", False)) if prev else False
        if availability.available and not prev_available:
            notifier.notify(item, availability)
        updated[item.item_id] = {
            "available": availability.available,
            "price": availability.price,
            "checked_at": now,
            "source": availability.source,
        }
    return updated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refund availability alert prototype")
    parser.add_argument("--interval", type=float, default=5.0, help="poll interval in seconds")
    parser.add_argument("--iterations", type=int, default=0, help="number of polls to run (0 = infinite)")
    parser.add_argument("--once", action="store_true", help="run a single poll and exit")
    parser.add_argument(
        "--provider",
        type=str,
        default="auto",
        choices=["auto", "mock", "yanolja-html"],
        help="availability provider",
    )
    parser.add_argument("--debug", action="store_true", help="print room-level scan details")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent
    load_env(base_dir / ".env")
    try:
        data_dir = base_dir / "data"

        watchlist_path = data_dir / "watchlist.json"
        state_path = data_dir / "state.json"
        timeline_path = data_dir / "mock_timeline.json"

        items = load_watchlist(watchlist_path)
        state = load_state(state_path)

        if args.provider == "mock":
            provider: MockProvider | YanoljaHtmlProvider = MockProvider(
                timeline_path, start_monotonic=time.monotonic()
            )
        elif args.provider == "yanolja-html":
            provider = YanoljaHtmlProvider(debug=args.debug)
        else:
            # auto: if any item has a url, use YanoljaHtmlProvider, else mock
            if any("url" in item.criteria for item in items):
                provider = YanoljaHtmlProvider(debug=args.debug)
            else:
                provider = MockProvider(timeline_path, start_monotonic=time.monotonic())
        ntfy_topic = os.environ.get("NTFY_TOPIC", "").strip() or None
        notifier = ConsoleNotifier(ntfy_topic=ntfy_topic)

        if args.once:
            updated = poll_once(items, provider, notifier, state)
            save_state(state_path, updated)
            return

        count = 0
        while True:
            updated = poll_once(items, provider, notifier, state)
            save_state(state_path, updated)
            state = updated
            count += 1
            if args.iterations and count >= args.iterations:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n모니터링을 중지합니다.")


if __name__ == "__main__":
    main()
