from __future__ import annotations

import asyncio
import subprocess
import sys
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from gitlab_issue_agent.backend import AgentBackend, BackendCallbacks
from gitlab_issue_agent.config import SchedulerConfig
from gitlab_issue_agent.models import (
    AgentResult,
    AttemptContext,
    AttemptOutcome,
    Issue,
    IssueDisposition,
)


def git(*args: str, cwd: Path | None = None) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


@pytest.fixture
def origin_repo(tmp_path: Path) -> Path:
    repository = tmp_path / "origin"
    repository.mkdir()
    git("init", "--initial-branch", "main", cwd=repository)
    git("config", "user.email", "scheduler-tests@example.invalid", cwd=repository)
    git("config", "user.name", "Scheduler Tests", cwd=repository)
    (repository / "README.md").write_text("# fixture\n", encoding="utf-8")
    git("add", "README.md", cwd=repository)
    git("commit", "-m", "initial", cwd=repository)
    return repository


def make_issue(
    *, iid: int = 1, labels: tuple[str, ...] = ("agent::ready",), state: str = "opened"
) -> Issue:
    project = "group/project"
    return Issue(
        id=f"{project}:{iid}",
        project_id=project,
        iid=iid,
        identifier=f"{project}#{iid}",
        title="Implement durable scheduling",
        description="Ship code and tests, then hand off for human review.",
        state=state,
        labels=labels,
        web_url=f"https://gitlab.example/{project}/-/issues/{iid}",
    )


class FakeTracker:
    def __init__(self, *issues: Issue) -> None:
        self.issues = {issue.id: issue for issue in issues}
        self.closed = False

    async def list_active_issues(self) -> list[Issue]:
        return [
            issue
            for issue in self.issues.values()
            if self.disposition(issue) is IssueDisposition.ACTIVE
        ]

    async def get_issue(self, issue_id: str) -> Issue | None:
        return self.issues.get(issue_id)

    def disposition(self, issue: Issue | None) -> IssueDisposition:
        if issue is None:
            return IssueDisposition.MISSING
        labels = {label.casefold() for label in issue.labels}
        if issue.state.casefold() == "closed" or labels.intersection(
            {"agent::done", "agent::cancelled"}
        ):
            return IssueDisposition.TERMINAL
        if issue.state.casefold() != "opened" or "agent::ready" not in labels:
            return IssueDisposition.NON_ACTIVE
        return IssueDisposition.ACTIVE

    def handoff(self, issue_id: str) -> None:
        self.issues[issue_id] = replace(self.issues[issue_id], labels=("agent::human-review",))

    async def close(self) -> None:
        self.closed = True


class ScriptedBackend(AgentBackend):
    def __init__(
        self,
        results: list[AgentResult],
        *,
        on_run: Callable[[AttemptContext, int], Any] | None = None,
        native_resume: bool = True,
    ) -> None:
        self.results = results
        self.on_run = on_run
        self.native_resume = native_resume
        self.contexts: list[AttemptContext] = []

    @property
    def supports_native_resume(self) -> bool:
        return self.native_resume

    async def run(
        self,
        context: AttemptContext,
        *,
        cancel_event: asyncio.Event,
        callbacks: BackendCallbacks,
    ) -> AgentResult:
        self.contexts.append(context)
        index = len(self.contexts) - 1
        await callbacks.emit("fake_agent.started", {"index": index})
        if self.on_run:
            result = self.on_run(context, index)
            if asyncio.iscoroutine(result):
                await result
        if cancel_event.is_set():
            return AgentResult(AttemptOutcome.CANCELLED, None, context.session_id, "", "cancelled")
        if index >= len(self.results):
            raise AssertionError("scripted backend exhausted")
        return self.results[index]


def make_config(
    tmp_path: Path,
    origin_repo: Path,
    *,
    command: str | None = None,
    args: list[str] | None = None,
    native_resume_args: list[str] | None = None,
    agent_env: dict[str, str] | None = None,
    retry_initial: float = 0.08,
    continuation_delay: float = 0.02,
    timeout_seconds: float = 10.0,
) -> SchedulerConfig:
    workflow = tmp_path / "WORKFLOW.md"
    workflow.write_text(
        """# Agent workflow

Implement the issue in the assigned branch, run tests, push the branch, and create or update a merge request. Never merge it. When the MR is ready, replace `agent::ready` with `agent::human-review`.
""",
        encoding="utf-8",
    )
    raw: dict[str, Any] = {
        "workflow_file": str(workflow),
        "scheduler": {
            "state_root": str(tmp_path / "state"),
            "poll_interval_seconds": 0.03,
            "max_concurrent_agents": 2,
        },
        "tracker": {
            "base_url": "https://gitlab.example",
            "token": "test-token",
            "project": "group/project",
            "active_labels": ["agent::ready"],
            "terminal_labels": ["agent::done", "agent::cancelled"],
        },
        "repository": {
            "clone_url": str(origin_repo),
            "default_branch": "main",
        },
        "agent": {
            "command": command or sys.executable,
            "args": args or ["-c", "print('unused')", "{prompt}"],
            "native_resume_args": native_resume_args
            or [
                "-c",
                "print('unused resume')",
                "{session_id}",
                "{prompt}",
            ],
            "timeout_seconds": timeout_seconds,
            "cancel_grace_seconds": 0.5,
            "env": agent_env or {},
        },
        "retry": {
            "initial_seconds": retry_initial,
            "max_seconds": 0.5,
            "multiplier": 2,
        },
        "continuation": {
            "delay_seconds": continuation_delay,
            "max_consecutive": 5,
            "yield_seconds": 0.1,
        },
        "observability": {"stdout_json": False},
    }
    return SchedulerConfig.from_mapping(raw, config_path=tmp_path / "scheduler.yaml")


async def wait_until(
    predicate: Callable[[], bool], *, timeout: float = 10.0, interval: float = 0.02
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(interval)
