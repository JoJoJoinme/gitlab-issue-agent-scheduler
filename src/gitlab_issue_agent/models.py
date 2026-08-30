from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any


class IssueDisposition(StrEnum):
    ACTIVE = "active"
    NON_ACTIVE = "non_active"
    TERMINAL = "terminal"
    MISSING = "missing"


class AttemptOutcome(StrEnum):
    CLEAN_EXIT = "clean_exit"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"


class ContinuationMode(StrEnum):
    FIRST = "first"
    NATIVE = "native_resume"
    STATELESS = "stateless_reconstruction"


class LocalPhase(StrEnum):
    READY = "ready"
    RUNNING = "running"
    CONTINUATION_WAIT = "continuation_wait"
    RETRY_WAIT = "retry_wait"
    RELEASED = "released"


@dataclass(frozen=True, slots=True)
class Issue:
    id: str
    project_id: str
    iid: int
    identifier: str
    title: str
    description: str | None
    state: str
    labels: tuple[str, ...] = ()
    web_url: str | None = None
    updated_at: str | None = None
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    def to_dict(self, *, include_raw: bool = False) -> dict[str, Any]:
        result = asdict(self)
        result["labels"] = list(self.labels)
        if not include_raw:
            result.pop("raw", None)
        return result


@dataclass(frozen=True, slots=True)
class WorkspaceInfo:
    path: Path
    branch: str
    created_now: bool


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    pid: int
    create_time: float
    attempt_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AttemptContext:
    issue: Issue
    workspace: WorkspaceInfo
    attempt_id: str
    attempt_number: int
    failure_count: int
    continuation_index: int
    mode: ContinuationMode
    prompt: str
    session_id: str | None = None


@dataclass(frozen=True, slots=True)
class AgentResult:
    outcome: AttemptOutcome
    exit_code: int | None
    session_id: str | None
    summary: str
    error: str | None = None
    duration_seconds: float = 0.0


@dataclass(slots=True)
class IssueState:
    issue_id: str
    project_id: str
    iid: int
    identifier: str
    phase: LocalPhase = LocalPhase.READY
    tracker_state: str = ""
    tracker_labels: list[str] = field(default_factory=list)
    workspace_path: str | None = None
    branch: str | None = None
    total_attempts: int = 0
    failure_count: int = 0
    continuation_index: int = 0
    backend_session_id: str | None = None
    process: ProcessIdentity | None = None
    next_run_at: str | None = None
    last_attempt_id: str | None = None
    last_outcome: str | None = None
    last_error: str | None = None
    last_summary: str = ""
    recovery_count: int = 0
    updated_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["phase"] = self.phase.value
        return result

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> IssueState:
        data = dict(value)
        data["phase"] = LocalPhase(data.get("phase", LocalPhase.READY))
        process = data.get("process")
        data["process"] = ProcessIdentity(**process) if process else None
        known = {item.name for item in cls.__dataclass_fields__.values()}
        return cls(**{key: item for key, item in data.items() if key in known})
