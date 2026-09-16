from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest
from conftest import FakeTracker, ScriptedBackend, make_config, make_issue, wait_until

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
async def test_cold_start_recovers_interrupted_session_and_continues_natively(
    tmp_path: Path, origin_repo: Path
) -> None:
    sleeper = tmp_path / "interrupted_agent.py"
    sleeper.write_text("import time\ntime.sleep(60)\n", encoding="utf-8")
    process = await asyncio.create_subprocess_exec(sys.executable, str(sleeper))
    try:
        issue = make_issue()
        tracker = FakeTracker(issue)

        def handoff(_context, _index: int) -> None:
            tracker.handoff(issue.id)

        backend = ScriptedBackend(
            [AgentResult(AttemptOutcome.CLEAN_EXIT, 0, "interrupted-session", "continued")],
            on_run=handoff,
            native_resume=True,
        )
        config = make_config(tmp_path, origin_repo)
        orchestrator = build_orchestrator(config, tracker, backend)
        attempt_id = "interrupted-attempt"
        identity = ProcessGuard.capture(process.pid, attempt_id)
        orchestrator.state.save(
            IssueState(
                issue_id=issue.id,
                project_id=issue.project_id,
                iid=issue.iid,
                identifier=issue.identifier,
                phase=LocalPhase.RUNNING,
                tracker_state=issue.state,
                tracker_labels=list(issue.labels),
                total_attempts=0,
                last_attempt_id=attempt_id,
                process=identity,
            )
        )
        await orchestrator.events.emit(
            "attempt.started",
            issue=issue,
            attempt_id=attempt_id,
            details={"attempt_number": 1},
        )
        await orchestrator.events.emit(
            "agent.session_observed",
            issue=issue,
            attempt_id=attempt_id,
            details={"attempt_number": 1, "session_id": "interrupted-session"},
        )

        journal_recovered = orchestrator.state.get(issue.identifier)
        assert journal_recovered is not None
        assert journal_recovered.total_attempts == 1
        assert journal_recovered.backend_session_id == "interrupted-session"

        await orchestrator.recover()
        await asyncio.wait_for(process.wait(), timeout=5)
        recovered = orchestrator.state.get(issue.identifier)
        assert recovered is not None
        assert recovered.phase is LocalPhase.READY
        assert recovered.total_attempts == 1
        assert recovered.backend_session_id == "interrupted-session"

        await orchestrator.tick()
        await wait_until(lambda: len(backend.contexts) == 1 and not orchestrator.running)
        assert backend.contexts[0].attempt_number == 2
        assert backend.contexts[0].mode is ContinuationMode.NATIVE
        assert backend.contexts[0].session_id == "interrupted-session"

        final_state = orchestrator.state.get(issue.identifier)
        assert final_state is not None
        assert final_state.phase is LocalPhase.RELEASED
        assert final_state.total_attempts == 2
        await orchestrator.shutdown()
    finally:
        if process.returncode is None:
            process.kill()
            await asyncio.wait_for(process.wait(), timeout=5)


@pytest.mark.asyncio
async def test_interrupted_journal_respects_native_resume_invalidation_and_bad_tail(
    tmp_path: Path, origin_repo: Path
) -> None:
    issue = make_issue()
    config = make_config(tmp_path, origin_repo)
    state = StateStore(config.state_root)
    events = EventSink(state, stdout=False)
    attempt_id = "failed-native-attempt"
    state.save(
        IssueState(
            issue_id=issue.id,
            project_id=issue.project_id,
            iid=issue.iid,
            identifier=issue.identifier,
            phase=LocalPhase.RUNNING,
            tracker_state=issue.state,
            tracker_labels=list(issue.labels),
            total_attempts=1,
            last_attempt_id=attempt_id,
        )
    )
    await events.emit(
        "attempt.started",
        issue=issue,
        attempt_id=attempt_id,
        details={"attempt_number": 2},
    )
    await events.emit(
        "agent.session_observed",
        issue=issue,
        attempt_id=attempt_id,
        details={"attempt_number": 2, "session_id": "stale-session"},
    )
    await events.emit(
        "continuation.native_resume_abandoned",
        issue=issue,
        attempt_id=attempt_id,
        details={"attempt_number": 2},
    )
    attempt_events = state.attempt_dir(issue.identifier, attempt_id) / "events.jsonl"
    with attempt_events.open("a", encoding="utf-8") as handle:
        handle.write('{"event_type":"partial"')

    recovered = state.get(issue.identifier)
    assert recovered is not None
    assert recovered.total_attempts == 2
    assert recovered.backend_session_id is None


@pytest.mark.asyncio
async def test_completed_state_does_not_replay_old_session_events(
    tmp_path: Path, origin_repo: Path
) -> None:
    issue = make_issue()
    config = make_config(tmp_path, origin_repo)
    state = StateStore(config.state_root)
    events = EventSink(state, stdout=False)
    attempt_id = "completed-attempt"
    state.save(
        IssueState(
            issue_id=issue.id,
            project_id=issue.project_id,
            iid=issue.iid,
            identifier=issue.identifier,
            phase=LocalPhase.READY,
            tracker_state=issue.state,
            tracker_labels=list(issue.labels),
            total_attempts=1,
            last_attempt_id=attempt_id,
            backend_session_id=None,
        )
    )
    await events.emit(
        "agent.session_observed",
        issue=issue,
        attempt_id=attempt_id,
        details={"attempt_number": 1, "session_id": "must-not-return"},
    )

    recovered = state.get(issue.identifier)
    assert recovered is not None
    assert recovered.backend_session_id is None
