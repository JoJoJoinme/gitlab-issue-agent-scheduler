from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import psutil
import pytest
from conftest import FakeTracker, ScriptedBackend, make_config, make_issue, wait_until

from gitlab_issue_agent.backend import CommandAgentBackend
from gitlab_issue_agent.events import EventSink
from gitlab_issue_agent.models import (
    AgentResult,
    AttemptOutcome,
    ContinuationMode,
    IssueState,
    LocalPhase,
)
from gitlab_issue_agent.orchestrator import Orchestrator
from gitlab_issue_agent.process_guard import ProcessGuard
from gitlab_issue_agent.prompts import PromptBuilder
from gitlab_issue_agent.state import StateStore
from gitlab_issue_agent.workspace import WorkspaceManager


def build_orchestrator(config, tracker, backend):
    state = StateStore(config.state_root)
    return Orchestrator(
        config,
        tracker=tracker,
        workspace=WorkspaceManager(config.state_root, config.repository),
        backend=backend,
        state=state,
        events=EventSink(state, stdout=False),
        prompts=PromptBuilder(config.workflow_file),
    )


@pytest.mark.asyncio
async def test_clean_exit_continues_natively_without_entering_failure_backoff(
    tmp_path: Path, origin_repo: Path
) -> None:
    issue = make_issue()
    tracker = FakeTracker(issue)

    def handoff_after_second_run(_context, index: int) -> None:
        if index == 1:
            tracker.handoff(issue.id)

    backend = ScriptedBackend(
        [
            AgentResult(AttemptOutcome.CLEAN_EXIT, 0, "session-1", "first turn"),
            AgentResult(AttemptOutcome.CLEAN_EXIT, 0, "session-1", "second turn"),
        ],
        on_run=handoff_after_second_run,
    )
    config = make_config(tmp_path, origin_repo, continuation_delay=0.01)
    orchestrator = build_orchestrator(config, tracker, backend)

    await orchestrator.recover()
    await orchestrator.tick()
    await wait_until(lambda: len(backend.contexts) == 1 and not orchestrator.running)
    first_state = orchestrator.state.get(issue.identifier)
    assert first_state is not None
    assert first_state.phase is LocalPhase.CONTINUATION_WAIT
    assert first_state.failure_count == 0

    await asyncio.sleep(0.02)
    await orchestrator.tick()
    await wait_until(lambda: len(backend.contexts) == 2 and not orchestrator.running)

    assert backend.contexts[0].mode is ContinuationMode.FIRST
    assert backend.contexts[1].mode is ContinuationMode.NATIVE
    assert backend.contexts[1].session_id == "session-1"
    assert backend.contexts[0].workspace.path == backend.contexts[1].workspace.path
    final_state = orchestrator.state.get(issue.identifier)
    assert final_state is not None
    assert final_state.phase is LocalPhase.RELEASED
    assert final_state.failure_count == 0

    events = [
        json.loads(line)
        for line in orchestrator.events.global_path.read_text(encoding="utf-8").splitlines()
    ]
    event_types = [event["event_type"] for event in events]
    assert "continuation.scheduled" in event_types
    assert "retry.scheduled" not in event_types
    attempt_events = list(
        (orchestrator.state.issue_dir(issue.identifier) / "attempts").glob("*/events.jsonl")
    )
    assert len(attempt_events) == 2
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_failure_uses_backoff_then_stateless_reconstruction(
    tmp_path: Path, origin_repo: Path
) -> None:
    issue = make_issue()
    tracker = FakeTracker(issue)

    def handoff_on_retry(_context, index: int) -> None:
        if index == 1:
            tracker.handoff(issue.id)

    backend = ScriptedBackend(
        [
            AgentResult(AttemptOutcome.FAILED, 7, None, "partial edit", "exit 7"),
            AgentResult(AttemptOutcome.CLEAN_EXIT, 0, None, "recovered"),
        ],
        on_run=handoff_on_retry,
        native_resume=False,
    )
    config = make_config(tmp_path, origin_repo, retry_initial=0.12)
    orchestrator = build_orchestrator(config, tracker, backend)

    await orchestrator.recover()
    await orchestrator.tick()
    await wait_until(lambda: len(backend.contexts) == 1 and not orchestrator.running)
    retry_state = orchestrator.state.get(issue.identifier)
    assert retry_state is not None
    assert retry_state.phase is LocalPhase.RETRY_WAIT
    assert retry_state.failure_count == 1

    await orchestrator.tick()
    assert len(backend.contexts) == 1
    await asyncio.sleep(0.14)
    await orchestrator.tick()
    await wait_until(lambda: len(backend.contexts) == 2 and not orchestrator.running)

    assert backend.contexts[1].mode is ContinuationMode.STATELESS
    assert "partial edit" in backend.contexts[1].prompt
    assert "Current git worktree evidence" in backend.contexts[1].prompt
    final_state = orchestrator.state.get(issue.identifier)
    assert final_state is not None
    assert final_state.phase is LocalPhase.RELEASED
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_reconcile_non_active_issue_terminates_real_executor_immediately(
    tmp_path: Path, origin_repo: Path
) -> None:
    script = tmp_path / "blocking_agent.py"
    script.write_text(
        """import json, time
print(json.dumps({"session_id": "real-session", "message": "started"}), flush=True)
time.sleep(60)
""",
        encoding="utf-8",
    )
    config = make_config(
        tmp_path,
        origin_repo,
        command=sys.executable,
        args=[str(script), "-p", "{prompt}"],
        native_resume_args=[str(script), "--resume", "{session_id}", "-p", "{prompt}"],
        timeout_seconds=30,
    )
    issue = make_issue()
    tracker = FakeTracker(issue)
    backend = CommandAgentBackend(config.agent)
    orchestrator = build_orchestrator(config, tracker, backend)

    await orchestrator.recover()
    await orchestrator.tick()
    await wait_until(
        lambda: (
            (state := orchestrator.state.get(issue.identifier)) is not None
            and state.process is not None
        )
    )
    running_state = orchestrator.state.get(issue.identifier)
    assert running_state is not None and running_state.process is not None
    pid = running_state.process.pid
    assert psutil.pid_exists(pid)

    tracker.handoff(issue.id)
    await orchestrator.tick()
    await wait_until(lambda: not orchestrator.running)

    final_state = orchestrator.state.get(issue.identifier)
    assert final_state is not None
    assert final_state.phase is LocalPhase.RELEASED
    assert final_state.process is None
    assert not ProcessGuard.matches(running_state.process)
    event_text = orchestrator.events.global_path.read_text(encoding="utf-8")
    assert '"event_type":"reconcile.cancel_requested"' in event_text
    assert '"event_type":"retry.scheduled"' not in event_text
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_cold_start_reaps_orphan_and_tracker_overrides_stale_running_state(
    tmp_path: Path, origin_repo: Path
) -> None:
    sleeper = tmp_path / "orphan.py"
    sleeper.write_text("import time\ntime.sleep(60)\n", encoding="utf-8")
    process = await asyncio.create_subprocess_exec(sys.executable, str(sleeper))
    try:
        issue = make_issue(labels=("agent::human-review",))
        tracker = FakeTracker(issue)
        backend = ScriptedBackend([])
        config = make_config(tmp_path, origin_repo)
        orchestrator = build_orchestrator(config, tracker, backend)
        identity = ProcessGuard.capture(process.pid, "crashed-attempt")
        orchestrator.state.save(
            IssueState(
                issue_id=issue.id,
                project_id=issue.project_id,
                iid=issue.iid,
                identifier=issue.identifier,
                phase=LocalPhase.RUNNING,
                tracker_state="opened",
                tracker_labels=["agent::ready"],
                total_attempts=1,
                process=identity,
            )
        )

        await orchestrator.recover()
        await asyncio.wait_for(process.wait(), timeout=5)
        recovered = orchestrator.state.get(issue.identifier)
        assert recovered is not None
        assert recovered.phase is LocalPhase.RELEASED
        assert recovered.process is None
        assert recovered.tracker_labels == ["agent::human-review"]
        assert backend.contexts == []
        events = orchestrator.events.global_path.read_text(encoding="utf-8")
        assert '"event_type":"cold_start.orphan_reaped"' in events
        assert '"local_phase":"released"' in events
        await orchestrator.shutdown()
    finally:
        if process.returncode is None:
            process.kill()
            await asyncio.wait_for(process.wait(), timeout=5)


@pytest.mark.asyncio
async def test_cold_start_active_issue_reaps_old_executor_then_reconstructs(
    tmp_path: Path, origin_repo: Path
) -> None:
    sleeper = tmp_path / "active_orphan.py"
    sleeper.write_text("import time\ntime.sleep(60)\n", encoding="utf-8")
    process = await asyncio.create_subprocess_exec(sys.executable, str(sleeper))
    try:
        issue = make_issue()
        tracker = FakeTracker(issue)

        def handoff(_context, _index: int) -> None:
            tracker.handoff(issue.id)

        backend = ScriptedBackend(
            [AgentResult(AttemptOutcome.CLEAN_EXIT, 0, None, "reconstructed")],
            on_run=handoff,
            native_resume=False,
        )
        config = make_config(tmp_path, origin_repo)
        orchestrator = build_orchestrator(config, tracker, backend)
        identity = ProcessGuard.capture(process.pid, "interrupted-attempt")
        orchestrator.state.save(
            IssueState(
                issue_id=issue.id,
                project_id=issue.project_id,
                iid=issue.iid,
                identifier=issue.identifier,
                phase=LocalPhase.RUNNING,
                tracker_state=issue.state,
                tracker_labels=list(issue.labels),
                total_attempts=1,
                last_summary="work before scheduler crash",
                process=identity,
            )
        )

        await orchestrator.recover()
        await asyncio.wait_for(process.wait(), timeout=5)
        recovered = orchestrator.state.get(issue.identifier)
        assert recovered is not None and recovered.phase is LocalPhase.READY

        await orchestrator.tick()
        await wait_until(lambda: len(backend.contexts) == 1 and not orchestrator.running)
        assert backend.contexts[0].mode is ContinuationMode.STATELESS
        assert "work before scheduler crash" in backend.contexts[0].prompt
        final_state = orchestrator.state.get(issue.identifier)
        assert final_state is not None and final_state.phase is LocalPhase.RELEASED
        await orchestrator.shutdown()
    finally:
        if process.returncode is None:
            process.kill()
            await asyncio.wait_for(process.wait(), timeout=5)


@pytest.mark.asyncio
async def test_workspace_is_a_durable_reused_git_worktree(
    tmp_path: Path, origin_repo: Path
) -> None:
    config = make_config(tmp_path, origin_repo)
    manager = WorkspaceManager(config.state_root, config.repository)
    issue = make_issue(iid=42)
    first = await manager.ensure(issue)
    marker = first.path / "durable.txt"
    marker.write_text("survives\n", encoding="utf-8")

    second = await manager.ensure(issue)
    assert first.created_now is True
    assert second.created_now is False
    assert first.path == second.path
    assert first.branch == "agent/issue-42"
    assert marker.read_text(encoding="utf-8") == "survives\n"
