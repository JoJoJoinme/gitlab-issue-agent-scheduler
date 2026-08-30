from __future__ import annotations

import asyncio
import contextlib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from .backend import AgentBackend, BackendCallbacks
from .config import SchedulerConfig
from .events import EventSink
from .models import (
    AgentResult,
    AttemptContext,
    AttemptOutcome,
    ContinuationMode,
    Issue,
    IssueDisposition,
    IssueState,
    LocalPhase,
    ProcessIdentity,
)
from .process_guard import ProcessGuard
from .prompts import PromptBuilder
from .state import StateStore, iso_now, utc_now
from .tracker import IssueTracker, TrackerError
from .workspace import WorkspaceManager


@dataclass(slots=True)
class RunningEntry:
    issue: Issue
    cancel_event: asyncio.Event
    task: asyncio.Task[None]
    cancel_reason: str | None = None


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


class Orchestrator:
    """Single scheduling authority for dispatch, continuation, retry, and reconciliation."""

    def __init__(
        self,
        config: SchedulerConfig,
        *,
        tracker: IssueTracker,
        workspace: WorkspaceManager,
        backend: AgentBackend,
        state: StateStore,
        events: EventSink,
        prompts: PromptBuilder,
        process_guard: type[ProcessGuard] = ProcessGuard,
    ) -> None:
        self.config = config
        self.tracker = tracker
        self.workspace = workspace
        self.backend = backend
        self.state = state
        self.events = events
        self.prompts = prompts
        self.process_guard = process_guard
        self.running: dict[str, RunningEntry] = {}
        self._tick_lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._stopping = False

    async def recover(self) -> None:
        """Cold-start reconciliation: stop orphan executors, then trust GitLab over local state."""
        states = self.state.load_all()
        await self.events.emit("cold_start.begin", details={"durable_issue_count": len(states)})
        for local in states:
            if local.process is not None:
                matched = self.process_guard.matches(local.process)
                terminated = False
                if matched:
                    terminated = await self.process_guard.terminate_tree(
                        local.process, grace_seconds=self.config.agent.cancel_grace_seconds
                    )
                await self.events.emit(
                    "cold_start.orphan_reaped",
                    issue_id=local.issue_id,
                    identifier=local.identifier,
                    attempt_id=local.process.attempt_id,
                    details={
                        "pid": local.process.pid,
                        "identity_matched": matched,
                        "terminated": terminated,
                    },
                )
                local.process = None

            try:
                issue = await self.tracker.get_issue(local.issue_id)
            except TrackerError as error:
                local.phase = LocalPhase.READY
                local.next_run_at = None
                local.last_error = f"cold-start tracker refresh failed: {error}"
                local.recovery_count += 1
                self.state.save(local)
                await self.events.emit(
                    "cold_start.tracker_unavailable",
                    issue_id=local.issue_id,
                    identifier=local.identifier,
                    details={"error": str(error)},
                )
                continue

            disposition = self.tracker.disposition(issue)
            local.recovery_count += 1
            local.next_run_at = None
            if issue is not None:
                self._refresh_local_snapshot(local, issue)
            if disposition is IssueDisposition.ACTIVE:
                local.phase = LocalPhase.READY
                local.last_outcome = "cold_start_recovered"
                local.last_error = None
            else:
                local.phase = LocalPhase.RELEASED
                local.last_outcome = "cold_start_tracker_override"
                local.last_error = None
            self.state.save(local)
            await self.events.emit(
                "cold_start.reconciled",
                issue=issue,
                issue_id=local.issue_id,
                identifier=local.identifier,
                details={
                    "disposition": disposition.value,
                    "local_phase": local.phase.value,
                },
            )
        await self.events.emit("cold_start.complete", details={})

    async def tick(self) -> None:
        async with self._tick_lock:
            await self._reconcile_running()
            if self._stopping:
                return
            try:
                candidates = await self.tracker.list_active_issues()
            except TrackerError as error:
                await self.events.emit("tracker.poll_failed", details={"error": str(error)})
                return

            active_by_id = {issue.id: issue for issue in candidates}
            await self._reconcile_waiting(active_by_id)
            for issue in candidates:
                local = self.state.for_issue(issue)
                if local.phase is LocalPhase.RELEASED:
                    local.phase = LocalPhase.READY
                    local.next_run_at = None
                    local.last_outcome = "tracker_reactivated"
                    self.state.save(local)
                    await self.events.emit("tracker.issue_reactivated", issue=issue, details={})

            slots = self.config.max_concurrent_agents - len(self.running)
            if slots <= 0:
                return
            now = utc_now()
            for issue in sorted(candidates, key=lambda item: (item.iid, item.id)):
                if slots <= 0:
                    break
                if issue.id in self.running:
                    continue
                local = self.state.for_issue(issue)
                if local.phase is LocalPhase.RUNNING:
                    local.phase = LocalPhase.READY
                    local.process = None
                    self.state.save(local)
                    await self.events.emit("state.stale_running_released", issue=issue, details={})
                if local.phase not in {
                    LocalPhase.READY,
                    LocalPhase.CONTINUATION_WAIT,
                    LocalPhase.RETRY_WAIT,
                }:
                    continue
                due = _parse_time(local.next_run_at)
                if due is not None and due > now:
                    continue
                self._dispatch(issue)
                slots -= 1

    async def run_forever(self) -> None:
        await self.recover()
        await self.events.emit(
            "scheduler.started",
            details={
                "poll_interval_seconds": self.config.poll_interval_seconds,
                "max_concurrent_agents": self.config.max_concurrent_agents,
            },
        )
        try:
            while not self._stopping:
                self._wake.clear()
                await self.tick()
                timeout = self._next_wait_seconds()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), timeout=timeout)
        finally:
            await self.shutdown()
            await self.tracker.close()

    async def shutdown(self) -> None:
        if self._stopping and not self.running:
            return
        self._stopping = True
        self._wake.set()
        entries = list(self.running.values())
        for entry in entries:
            entry.cancel_reason = "scheduler_shutdown"
            entry.cancel_event.set()
            await self.events.emit(
                "scheduler.shutdown_cancel_requested", issue=entry.issue, details={}
            )
        if entries:
            timeout = self.config.agent.cancel_grace_seconds + 5
            _done, pending = await asyncio.wait([entry.task for entry in entries], timeout=timeout)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        await self.events.emit("scheduler.stopped", details={})

    def _dispatch(self, issue: Issue) -> None:
        cancel_event = asyncio.Event()
        task = asyncio.create_task(self._execute(issue, cancel_event))
        entry = RunningEntry(issue=issue, cancel_event=cancel_event, task=task)
        self.running[issue.id] = entry

        def completed(done_task: asyncio.Task[None], issue_id: str = issue.id) -> None:
            current = self.running.get(issue_id)
            if current is not None and current.task is done_task:
                self.running.pop(issue_id, None)
            with contextlib.suppress(asyncio.CancelledError, Exception):
                done_task.exception()
            self._wake.set()

        task.add_done_callback(completed)

    async def _execute(self, issue: Issue, cancel_event: asyncio.Event) -> None:
        local = self.state.for_issue(issue)
        attempt_id = str(uuid.uuid4())
        attempt_number = local.total_attempts + 1
        local.phase = LocalPhase.RUNNING
        local.next_run_at = None
        local.last_attempt_id = attempt_id
        local.process = None
        self.state.save(local)
        await self.events.emit(
            "attempt.started",
            issue=issue,
            attempt_id=attempt_id,
            details={
                "attempt_number": attempt_number,
                "failure_count": local.failure_count,
                "continuation_index": local.continuation_index,
            },
        )

        try:
            workspace = await self.workspace.ensure(issue)
            local.workspace_path = str(workspace.path)
            local.branch = workspace.branch
            self.state.save(local)
            await self.events.emit(
                "workspace.ready",
                issue=issue,
                attempt_id=attempt_id,
                details={
                    "path": str(workspace.path),
                    "branch": workspace.branch,
                    "created_now": workspace.created_now,
                },
            )
            mode = self._continuation_mode(local)
            if mode is ContinuationMode.FIRST:
                prompt = self.prompts.first(issue)
            elif mode is ContinuationMode.NATIVE:
                prompt = self.prompts.native_continuation(issue, turn=local.continuation_index + 1)
            else:
                snapshot = await self.workspace.snapshot(workspace.path)
                prompt = self.prompts.stateless_continuation(
                    issue,
                    previous_summary=local.last_summary,
                    workspace_snapshot=snapshot,
                    attempt_number=attempt_number,
                )

            context = AttemptContext(
                issue=issue,
                workspace=workspace,
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                failure_count=local.failure_count,
                continuation_index=local.continuation_index,
                mode=mode,
                prompt=prompt,
                session_id=local.backend_session_id,
            )

            async def emit(event_type: str, details: dict[str, Any]) -> None:
                await self.events.emit(
                    event_type,
                    issue=issue,
                    attempt_id=attempt_id,
                    details={"attempt_number": attempt_number, **details},
                )

            async def process_started(identity: ProcessIdentity) -> None:
                local.process = identity
                self.state.save(local)
                await emit(
                    "attempt.process_started",
                    {"pid": identity.pid, "create_time": identity.create_time},
                )

            if cancel_event.is_set():
                result = AgentResult(
                    outcome=AttemptOutcome.CANCELLED,
                    exit_code=None,
                    session_id=local.backend_session_id,
                    summary=local.last_summary,
                    error="cancelled before executor launch",
                )
            else:
                result = await self.backend.run(
                    context,
                    cancel_event=cancel_event,
                    callbacks=BackendCallbacks(emit=emit, process_started=process_started),
                )
            await self._finish_attempt(
                issue=issue,
                local=local,
                result=result,
                mode=mode,
                attempt_id=attempt_id,
                attempt_number=attempt_number,
            )
        except asyncio.CancelledError:
            local.process = None
            local.phase = LocalPhase.READY
            local.next_run_at = iso_now()
            local.last_outcome = AttemptOutcome.CANCELLED.value
            local.last_error = "scheduler task cancelled"
            self.state.save(local)
            await self.events.emit(
                "attempt.task_cancelled", issue=issue, attempt_id=attempt_id, details={}
            )
            raise
        except Exception as error:  # noqa: BLE001 - one failed issue must not kill the scheduler
            local.process = None
            local.total_attempts = attempt_number
            await self.events.emit(
                "attempt.internal_error",
                issue=issue,
                attempt_id=attempt_id,
                details={"error": f"{type(error).__name__}: {error}"},
            )
            await self._schedule_failure(
                issue,
                local,
                attempt_id=attempt_id,
                error=f"scheduler attempt error: {type(error).__name__}: {error}",
                summary=local.last_summary,
            )

    async def _finish_attempt(
        self,
        *,
        issue: Issue,
        local: IssueState,
        result: AgentResult,
        mode: ContinuationMode,
        attempt_id: str,
        attempt_number: int,
    ) -> None:
        local.process = None
        local.total_attempts = attempt_number
        local.last_outcome = result.outcome.value
        local.last_error = result.error
        local.last_summary = result.summary
        if result.session_id:
            local.backend_session_id = result.session_id
        self.state.save(local)

        try:
            refreshed = await self.tracker.get_issue(issue.id)
        except TrackerError as error:
            await self.events.emit(
                "attempt.tracker_refresh_failed",
                issue=issue,
                attempt_id=attempt_id,
                details={"error": str(error), "agent_outcome": result.outcome.value},
            )
            await self._schedule_failure(
                issue,
                local,
                attempt_id=attempt_id,
                error=f"post-attempt tracker refresh failed: {error}",
                summary=result.summary,
            )
            return

        disposition = self.tracker.disposition(refreshed)
        if refreshed is not None:
            self._refresh_local_snapshot(local, refreshed)
        if disposition is not IssueDisposition.ACTIVE:
            local.phase = LocalPhase.RELEASED
            local.next_run_at = None
            local.process = None
            local.last_error = None if result.outcome is AttemptOutcome.CLEAN_EXIT else result.error
            self.state.save(local)
            await self.events.emit(
                "attempt.released_by_tracker",
                issue=refreshed,
                issue_id=issue.id,
                identifier=issue.identifier,
                attempt_id=attempt_id,
                details={
                    "disposition": disposition.value,
                    "agent_outcome": result.outcome.value,
                },
            )
            return

        current_issue = refreshed or issue
        entry = self.running.get(issue.id)
        cancel_reason = entry.cancel_reason if entry else None
        if result.outcome is AttemptOutcome.CLEAN_EXIT:
            await self._schedule_clean_continuation(current_issue, local, attempt_id=attempt_id)
        elif result.outcome is AttemptOutcome.CANCELLED and cancel_reason == "scheduler_shutdown":
            local.phase = LocalPhase.READY
            local.next_run_at = iso_now()
            local.last_error = None
            self.state.save(local)
            await self.events.emit(
                "attempt.paused_for_shutdown",
                issue=current_issue,
                attempt_id=attempt_id,
                details={},
            )
        elif result.outcome is AttemptOutcome.CANCELLED:
            local.phase = LocalPhase.READY
            local.next_run_at = iso_now()
            self.state.save(local)
            await self.events.emit(
                "attempt.cancelled_but_tracker_active",
                issue=current_issue,
                attempt_id=attempt_id,
                details={"cancel_reason": cancel_reason},
            )
        else:
            if mode is ContinuationMode.NATIVE:
                local.backend_session_id = None
                await self.events.emit(
                    "continuation.native_resume_abandoned",
                    issue=current_issue,
                    attempt_id=attempt_id,
                    details={"next_mode": ContinuationMode.STATELESS.value},
                )
            await self._schedule_failure(
                current_issue,
                local,
                attempt_id=attempt_id,
                error=result.error or result.outcome.value,
                summary=result.summary,
            )

    async def _schedule_clean_continuation(
        self, issue: Issue, local: IssueState, *, attempt_id: str
    ) -> None:
        local.failure_count = 0
        local.continuation_index += 1
        delay = self.config.continuation.delay_seconds
        yielded = False
        if local.continuation_index % self.config.continuation.max_consecutive == 0:
            delay = max(delay, self.config.continuation.yield_seconds)
            yielded = True
        local.phase = LocalPhase.CONTINUATION_WAIT
        local.next_run_at = (utc_now() + timedelta(seconds=delay)).isoformat()
        local.last_error = None
        self.state.save(local)
        await self.events.emit(
            "continuation.scheduled",
            issue=issue,
            attempt_id=attempt_id,
            details={
                "delay_seconds": delay,
                "continuation_index": local.continuation_index,
                "failure_count": local.failure_count,
                "yielded_at_cap": yielded,
                "next_path": (
                    ContinuationMode.NATIVE.value
                    if local.backend_session_id and self.backend.supports_native_resume
                    else ContinuationMode.STATELESS.value
                ),
            },
        )

    async def _schedule_failure(
        self,
        issue: Issue,
        local: IssueState,
        *,
        attempt_id: str,
        error: str,
        summary: str,
    ) -> None:
        local.failure_count += 1
        local.continuation_index = 0
        delay = self.config.retry.delay(local.failure_count)
        local.phase = LocalPhase.RETRY_WAIT
        local.next_run_at = (utc_now() + timedelta(seconds=delay)).isoformat()
        local.last_error = error
        local.last_summary = summary
        local.process = None
        self.state.save(local)
        await self.events.emit(
            "retry.scheduled",
            issue=issue,
            attempt_id=attempt_id,
            details={
                "delay_seconds": delay,
                "failure_count": local.failure_count,
                "error": error,
            },
        )

    async def _reconcile_running(self) -> None:
        entries = list(self.running.values())
        if not entries:
            return
        results = await asyncio.gather(
            *(self.tracker.get_issue(entry.issue.id) for entry in entries),
            return_exceptions=True,
        )
        to_cancel: list[RunningEntry] = []
        for entry, result in zip(entries, results, strict=True):
            if isinstance(result, Exception):
                await self.events.emit(
                    "reconcile.refresh_failed",
                    issue=entry.issue,
                    details={"error": str(result)},
                )
                continue
            disposition = self.tracker.disposition(result)
            if disposition is IssueDisposition.ACTIVE and result is not None:
                entry.issue = result
                continue
            entry.cancel_reason = f"tracker_{disposition.value}"
            entry.cancel_event.set()
            to_cancel.append(entry)
            await self.events.emit(
                "reconcile.cancel_requested",
                issue=result,
                issue_id=entry.issue.id,
                identifier=entry.issue.identifier,
                details={"disposition": disposition.value},
            )

        if to_cancel:
            timeout = self.config.agent.cancel_grace_seconds + 5
            _done, pending = await asyncio.wait(
                [entry.task for entry in to_cancel], timeout=timeout
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def _reconcile_waiting(self, active_by_id: dict[str, Issue]) -> None:
        for local in self.state.load_all():
            if local.issue_id in self.running or local.phase is LocalPhase.RELEASED:
                continue
            active = active_by_id.get(local.issue_id)
            if active is not None:
                self._refresh_local_snapshot(local, active)
                self.state.save(local)
                continue
            try:
                refreshed = await self.tracker.get_issue(local.issue_id)
            except TrackerError as error:
                await self.events.emit(
                    "reconcile.waiting_refresh_failed",
                    issue_id=local.issue_id,
                    identifier=local.identifier,
                    details={"error": str(error)},
                )
                continue
            disposition = self.tracker.disposition(refreshed)
            if disposition is IssueDisposition.ACTIVE:
                continue
            if refreshed is not None:
                self._refresh_local_snapshot(local, refreshed)
            local.phase = LocalPhase.RELEASED
            local.next_run_at = None
            local.process = None
            self.state.save(local)
            await self.events.emit(
                "reconcile.waiting_released",
                issue=refreshed,
                issue_id=local.issue_id,
                identifier=local.identifier,
                details={"disposition": disposition.value},
            )

    def _continuation_mode(self, local: IssueState) -> ContinuationMode:
        if local.total_attempts == 0:
            return ContinuationMode.FIRST
        if local.backend_session_id and self.backend.supports_native_resume:
            return ContinuationMode.NATIVE
        return ContinuationMode.STATELESS

    def _next_wait_seconds(self) -> float:
        poll = self.config.poll_interval_seconds
        if len(self.running) >= self.config.max_concurrent_agents:
            return poll
        now = utc_now()
        waits: list[float] = []
        for local in self.state.load_all():
            if local.phase not in {
                LocalPhase.CONTINUATION_WAIT,
                LocalPhase.RETRY_WAIT,
                LocalPhase.READY,
            }:
                continue
            due = _parse_time(local.next_run_at)
            if due is not None:
                waits.append(max(0.01, (due - now).total_seconds()))
        return min([poll, *waits]) if waits else poll

    @staticmethod
    def _refresh_local_snapshot(local: IssueState, issue: Issue) -> None:
        local.issue_id = issue.id
        local.project_id = issue.project_id
        local.iid = issue.iid
        local.identifier = issue.identifier
        local.tracker_state = issue.state
        local.tracker_labels = list(issue.labels)
