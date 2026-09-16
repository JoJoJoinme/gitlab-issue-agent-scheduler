from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import Issue, IssueState, LocalPhase


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utc_now().isoformat()


def workspace_key(identifier: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]", "_", identifier)
    if sanitized == identifier:
        return sanitized
    digest = hashlib.sha256(identifier.encode("utf-8")).hexdigest()[:16]
    return f"{sanitized}-{digest}"


class StateStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.issues_root = root / "issues"
        self.issues_root.mkdir(parents=True, exist_ok=True)

    def state_path(self, identifier: str) -> Path:
        return self.issues_root / workspace_key(identifier) / "state.json"

    def issue_dir(self, identifier: str) -> Path:
        path = self.issues_root / workspace_key(identifier)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def attempt_dir(self, identifier: str, attempt_id: str) -> Path:
        path = self.issue_dir(identifier) / "attempts" / attempt_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get(self, identifier: str) -> IssueState | None:
        path = self.state_path(identifier)
        if not path.exists():
            return None
        return self._read(path)

    def find_by_issue_id(self, issue_id: str) -> IssueState | None:
        return next((state for state in self.load_all() if state.issue_id == issue_id), None)

    def for_issue(self, issue: Issue) -> IssueState:
        existing = self.get(issue.identifier)
        if existing is None:
            existing = IssueState(
                issue_id=issue.id,
                project_id=issue.project_id,
                iid=issue.iid,
                identifier=issue.identifier,
                tracker_state=issue.state,
                tracker_labels=list(issue.labels),
            )
        else:
            existing.issue_id = issue.id
            existing.project_id = issue.project_id
            existing.iid = issue.iid
            existing.tracker_state = issue.state
            existing.tracker_labels = list(issue.labels)
        return self.save(existing)

    def save(self, state: IssueState) -> IssueState:
        state.updated_at = iso_now()
        path = self.state_path(state.identifier)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(state.to_dict(), ensure_ascii=False, indent=2, sort_keys=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_text(payload + "\n", encoding="utf-8")
        os.replace(temporary, path)
        return state

    def load_all(self) -> list[IssueState]:
        states: list[IssueState] = []
        for path in sorted(self.issues_root.glob("*/state.json")):
            states.append(self._read(path))
        return states

    def _read(self, path: Path) -> IssueState:
        try:
            payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
            state = IssueState.from_dict(payload)
            self._recover_interrupted_attempt_metadata(state, path.parent)
            return state
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as error:
            raise RuntimeError(f"cannot load durable state {path}: {error}") from error

    @staticmethod
    def _recover_interrupted_attempt_metadata(state: IssueState, issue_dir: Path) -> None:
        """Replay small, durable attempt facts that may be newer than state.json.

        State snapshots are written before an executor starts, while attempt events are
        appended as execution progresses. A hard scheduler crash can therefore leave a
        RUNNING/BLOCKED snapshot without the attempt number or native session ID that was
        already observed. Only interrupted execution states are repaired here; completed
        READY/WAIT/RELEASED snapshots remain authoritative so old events cannot resurrect
        a deliberately cleared session.
        """
        if state.phase not in {LocalPhase.RUNNING, LocalPhase.BLOCKED}:
            return
        if state.phase is LocalPhase.BLOCKED and state.last_outcome == "placement_blocked":
            return
        attempt_id = state.last_attempt_id
        if not attempt_id:
            return
        events_path = issue_dir / "attempts" / attempt_id / "events.jsonl"
        if not events_path.exists():
            return

        try:
            lines = events_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return

        for line in lines:
            try:
                event = json.loads(line)
            except (json.JSONDecodeError, TypeError):
                # A crash can leave an incomplete final line. Earlier flushed events
                # remain useful recovery evidence.
                continue
            if not isinstance(event, dict) or event.get("attempt_id") != attempt_id:
                continue
            details = event.get("details")
            if not isinstance(details, dict):
                continue

            attempt_number = details.get("attempt_number")
            if (
                isinstance(attempt_number, int)
                and not isinstance(attempt_number, bool)
                and attempt_number > state.total_attempts
            ):
                state.total_attempts = attempt_number

            event_type = event.get("event_type")
            if event_type in {"agent.session_observed", "agent.exited"}:
                session_id = details.get("session_id")
                if isinstance(session_id, str) and session_id.strip():
                    state.backend_session_id = session_id.strip()
            elif event_type == "continuation.native_resume_abandoned":
                # A failed native continuation intentionally invalidates the session.
                # Replaying an earlier session-observed event must not revive it.
                state.backend_session_id = None


def copy_state(state: IssueState, **changes: Any) -> IssueState:
    return replace(state, **changes)
