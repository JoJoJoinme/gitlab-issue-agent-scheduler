from __future__ import annotations

import asyncio
import json
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import Issue
from .state import StateStore


class EventSink:
    """Append-only JSONL event sink with a global and per-attempt stream."""

    def __init__(self, state: StateStore, *, stdout: bool = True) -> None:
        self.state = state
        self.stdout = stdout
        self._lock = asyncio.Lock()
        self.global_path = state.root / "events.jsonl"
        self.global_path.parent.mkdir(parents=True, exist_ok=True)

    async def emit(
        self,
        event_type: str,
        *,
        issue: Issue | None = None,
        issue_id: str | None = None,
        identifier: str | None = None,
        attempt_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if issue is not None:
            issue_id = issue.id
            identifier = issue.identifier
        event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(UTC).isoformat(),
            "event_type": event_type,
            "issue_id": issue_id,
            "issue_identifier": identifier,
            "attempt_id": attempt_id,
            "details": details or {},
        }
        encoded = json.dumps(event, ensure_ascii=False, separators=(",", ":"), default=str)
        async with self._lock:
            self._append(self.global_path, encoded)
            if identifier:
                issue_events = self.state.issue_dir(identifier) / "events.jsonl"
                self._append(issue_events, encoded)
                if attempt_id:
                    attempt_events = self.state.attempt_dir(identifier, attempt_id) / "events.jsonl"
                    self._append(attempt_events, encoded)
            if self.stdout:
                print(encoded, file=sys.stdout, flush=True)
        return event

    @staticmethod
    def _append(path: Path, line: str) -> None:
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(line)
            handle.write("\n")
            handle.flush()
