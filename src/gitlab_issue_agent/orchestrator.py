from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from .backend import AgentBackend, BackendCallbacks
from .config import SchedulerConfig
from .events import EventSink
from .execution import ExecutionRouter, ExecutionTarget, PlacementError
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
    target_id: str
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
        workspace: WorkspaceManager | None = None,
        backend: AgentBackend | None = None,
        execution: ExecutionRouter | None = None,
        state: StateStore,
        events: EventSink,
        prompts: PromptBuilder,
        process_guard: type[ProcessGuard] = ProcessGuard,
    ) -> None:
        self.config = config
        self.tracker = tracker
        if execution is None:
            if workspace is None or backend is None:
                raise ValueError("workspace and backend are required without an execution router")
            target_config = config.execution.target(config.execution.default_target)
            target = ExecutionTarget(
                config=target_config,
                workspace=workspace,
                backend=backend,
            )
            execution = ExecutionRouter(
                targets={target.id: target},
                default_target=target.id,
                label_prefix=config.execution.label_prefix,
            )
        self.execution = execution
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
            needs_reap = local.process is not None or local.phase in {
                LocalPhase.RUNNING,
                LocalPhase.BLOCKED,
            }
            if needs_reap and local.last_outcome != "placement_blocked":
                reap = await self._reap_state(local, event_prefix="cold_start")
                if not reap:
                    # Keep reconciling tracker metadata below, but never turn
                    # an unconfirmed remote executor into dispatchable state.
                    local.phase = LocalPhase.BLOCKED
                else:
                    local.phase = LocalPhase.READY

            try:
                issue = await self.tracker.get_issue(local.issue_id)
            except TrackerError as error:
                if local.phase is not LocalPhase.BLOCKED:
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
            if local.phase is LocalPhase.BLOCKED and local.last_outcome == "placement_blocked":
                if disposition is not IssueDisposition.ACTIVE:
                    local.phase = LocalPhase.RELEASED
                    local.last_outcome = "cold_start_tracker_override"
                    local.last_error = None
            elif local.phase is LocalPhase.BLOCKED:
                local.last_outcome = "executor_stop_unconfirmed"
            elif disposition is IssueDisposition.ACTIVE:
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

    async def _reap_state(self, local: IssueState, *, event_prefix: str) -> bool:
        target_id = self._state_target_id(local)
        attempt_id = local.last_attempt_id or (
            local.process.attempt_id if local.process is not None else None
        )
        if not attempt_id:
            local.phase = LocalPhase.BLOCKED
            local.last_error = "cannot prove executor stopped: durable attempt_id is absent"
            self.state.save(local)
            await self.events.emit(
                f"{event_prefix}.orphan_reap_unconfirmed",
                issue_id=local.issue_id,
                identifier=local.identifier,
                details={"execution_target": target_id, "error": local.last_error},
            )
            return False
        try:
            target = self.execution.target(target_id)
        except PlacementError as error:
            local.phase = LocalPhase.BLOCKED
            local.last_error = str(error)
            self.state.save(local)
            await self.events.emit(
                f"{event_prefix}.orphan_reap_unconfirmed",
                issue_id=local.issue_id,
                identifier=local.identifier,
                attempt_id=attempt_id,
                details={"execution_target": target_id, "error": str(error)},
            )
            return False
        result = await target.reap(
            attempt_id=attempt_id,
            identity=local.process,
            process_guard=self.process_guard,
        )
        event_type = (
            f"{event_prefix}.orphan_reaped"
            if result.confirmed_safe
            else f"{event_prefix}.orphan_reap_unconfirmed"
        )
        await self.events.emit(
            event_type,
            issue_id=local.issue_id,
            identifier=local.identifier,
            attempt_id=attempt_id,
            details={
                "execution_target": target_id,
                "pid": local.process.pid if local.process else None,
                "identity_matched": result.identity_matched,
                "terminated": result.terminated,
                "confirmed_safe": result.confirmed_safe,
                "error": result.error,
            },
        )
        if result.confirmed_safe:
            local.process = None
            local.last_error = None
            self.state.save(local)
            return True
        local.phase = LocalPhase.BLOCKED
        local.next_run_at = None
        local.last_error = f"remote executor stop is unconfirmed: {result.error or 'unknown'}"
        self.state.save(local)
        return False

    def _state_target_id(self, local: IssueState) -> str:
        if local.execution_target:
            return local.execution_target
        if local.process and local.process.host_id != "local":
            return local.process.host_id
        return self.execution.default_target

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
            await self._reconcile_blocked(active_by_id)
            await self._reconcile_waiting(active_by_id)
            for issue in candidates:
                local = self.state.for_issue(issue)
                if local.phase is LocalPhase.RELEASED:
                    local.phase = LocalPhase.READY
                    local.next_run_at = None
                    local.last_outcome = "tracker_reactivated"
                    self.state.save(local)
                    await self.events.emit("tracker.issue_reactivated", issue=issue, details={})

            global_slots = self.config.max_concurrent_agents - len(self.running)
            if global_slots <= 0:
                return
            target_counts = Counter(entry.target_id for entry in self.running.values())
            now = utc_now()
            for issue in sorted(candidates, key=lambda item: (item.iid, item.id)):
                if global_slots <= 0:
                    break
                if issue.id in self.running:
                    continue
                local = self.state.for_issue(issue)
                if local.phase is LocalPhase.RUNNING:
                    safe = await self._reap_state(local, event_prefix="reconcile")
                    if not safe:
                        continue
                    local.phase = LocalPhase.READY
                    local.next_run_at = None
                    self.state.save(local)
                    await self.events.emit("state.stale_running_released", issue=issue, details={})
                try:
                    target_id = self.execution.placement(issue)
                    target = self.execution.target(target_id)
                except PlacementError as error:
                    local.phase = LocalPhase.BLOCKED
                    local.last_outcome = "placement_blocked"
                    local.last_error = str(error)
                    local.next_run_at = None
                    self.state.save(local)
                    await self.events.emit(
                        "placement.blocked",
                        issue=issue,
                        details={"error": str(error)},
                    )
                    continue
                if local.phase is LocalPhase.BLOCKED:
                    if local.last_outcome != "placement_blocked":
                        continue
                    local.phase = LocalPhase.READY
                    local.last_error = None
                    local.last_outcome = "placement_unblocked"
                    self.state.save(local)
                    await self.events.emit(
                        "placement.unblocked",
                        issue=issue,
                        details={"execution_target": target_id},
                    )
                if local.phase not in {
                    LocalPhase.READY,
                    LocalPhase.CONTINUATION_WAIT,
                    LocalPhase.RETRY_WAIT,
                }:
                    continue
                due = _parse_time(local.next_run_at)
                if due is not None and due > now:
                    continue
                if target_counts[target_id] >= target.max_concurrent_agents:
                    continue
                if local.execution_target and local.execution_target != target_id:
                    previous_target = local.execution_target
                    local.backend_session_id = None
                    local.workspace_path = None
                    local.branch = None
                    local.continuation_index = 0
                    await self.events.emit(
                        "placement.changed",
                        issue=issue,
                        details={
                            "previous_target": previous_target,
                            "execution_target": target_id,
                            "native_session_cleared": True,
                        },
                    )
                local.execution_target = target_id
                self.state.save(local)
                self._dispatch(issue, target_id)
                target_counts[target_id] += 1
                global_slots -= 1

    async def run_forever(self) -> None:
        await self.recover()
        await self.events.emit(
            "scheduler.started",
            details={
                "poll_interval_seconds": self.config.poll_interval_seconds,
                "max_concurrent_agents": self.config.max_concurrent_agents,
                "execution_targets": {
                    target_id: {
                        "kind": target.config.kind,
                        "max_concurrent_agents": target.max_concurrent_agents,
                    }
                    for target_id, target in self.execution.targets.items()
                },
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
            timeout = (
                max(
                    self.execution.target(entry.target_id).config.agent.cancel_grace_seconds
                    for entry in entries
                )
                + 15
            )
            _done, pending = await asyncio.wait([entry.task for entry in entries], timeout=timeout)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        await self.events.emit("scheduler.stopped", details={})

    def _dispatch(self, issue: Issue, target_id: str) -> None:
        cancel_event = asyncio.Event()
        task = asyncio.create_task(self._execute(issue, target_id, cancel_event))
        entry = RunningEntry(
            issue=issue,
            target_id=target_id,
            cancel_event=cancel_event,
            task=task,
        )
        self.running[issue.id] = entry

        def completed(done_task: asyncio.Task[None], issue_id: str = issue.id) -> None:
            current = self.running.get(issue_id)
            if current is not None and current.task is done_task:
                self.running.pop(issue_id, None)
            with contextlib.suppress(asyncio.CancelledError, Exception):
                done_task.exception()
            self._wake.set()

        task.add_done_callback(completed)

    async def _execute(self, issue: Issue, target_id: str, cancel_event: asyncio.Event) -> None:
        target = self.execution.target(target_id)
        local = self.state.for_issue(issue)
        attempt_id = str(uuid.uuid4())
        attempt_number = local.total_attempts + 1
        local.phase = LocalPhase.RUNNING
        local.next_run_at = None
        local.last_attempt_id = attempt_id
        local.execution_target = target_id
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
                "execution_target": target_id,
            },
        )

        try:
            workspace = await target.workspace.ensure(issue, attempt_id=attempt_id)
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
                    "execution_target": target_id,
                },
            )
            mode = self._continuation_mode(local, target)
            if mode is ContinuationMode.FIRST:
                prompt = self.prompts.first(issue)
            elif mode is ContinuationMode.NATIVE:
                prompt = self.prompts.native_continuation(issue, turn=local.continuation_index + 1)
            else:
                snapshot = await target.workspace.snapshot(workspace.path)
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
                    {
                        "pid": identity.pid,
                        "create_time": identity.create_time,
                        "execution_target": identity.host_id,
                    },
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
                result = await target.backend.run(
                    context,
                    cancel_event=cancel_event,
                    callbacks=BackendCallbacks(emit=emit, process_started=process_started),
                )
            await self._finish_attempt(
                issue=issue,
                local=local,
                target=target,
                result=result,
                mode=mode,
                attempt_id=attempt_id,
                attempt_number=attempt_number,
            )
        except asyncio.CancelledError:
            reap = await target.reap(
                attempt_id=attempt_id,
                identity=local.process,
                process_guard=self.process_guard,
            )
            if reap.confirmed_safe:
                local.process = None
                local.phase = LocalPhase.READY
                local.next_run_at = iso_now()
            else:
                local.phase = LocalPhase.BLOCKED
                local.next_run_at = None
            local.last_outcome = AttemptOutcome.CANCELLED.value
            local.last_error = (
                "scheduler task cancelled"
                if reap.confirmed_safe
                else f"scheduler task cancelled; remote stop unconfirmed: {reap.error}"
            )
            self.state.save(local)
            await self.events.emit(
                "attempt.task_cancelled",
                issue=issue,
                attempt_id=attempt_id,
                details={
                    "execution_target": target_id,
                    "executor_safe": reap.confirmed_safe,
                },
            )
            raise
        except Exception as error:  # noqa: BLE001 - one failed issue must not kill the scheduler
            reap = await target.reap(
                attempt_id=attempt_id,
                identity=local.process,
                process_guard=self.process_guard,
            )
            local.total_attempts = attempt_number
            await self.events.emit(
                "attempt.internal_error",
                issue=issue,
                attempt_id=attempt_id,
                details={"error": f"{type(error).__name__}: {error}"},
            )
            if not reap.confirmed_safe:
                local.phase = LocalPhase.BLOCKED
                local.next_run_at = None
                local.last_outcome = "executor_stop_unconfirmed"
                local.last_error = (
                    f"scheduler attempt error and executor stop unconfirmed: "
                    f"{type(error).__name__}: {error}; {reap.error or 'unknown'}"
                )
                self.state.save(local)
                return
            local.process = None
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
        target: ExecutionTarget,
        result: AgentResult,
        mode: ContinuationMode,
        attempt_id: str,
        attempt_number: int,
    ) -> None:
        local.total_attempts = attempt_number
        local.last_outcome = result.outcome.value
        local.last_error = result.error
        local.last_summary = result.summary
        if result.session_id:
            local.backend_session_id = result.session_id
        if not result.executor_safe:
            local.phase = LocalPhase.BLOCKED
            local.next_run_at = None
            local.last_outcome = "executor_stop_unconfirmed"
            local.last_error = result.error or "execution target could not confirm executor stop"
            try:
                refreshed = await self.tracker.get_issue(issue.id)
                if refreshed is not None:
                    self._refresh_local_snapshot(local, refreshed)
            except TrackerError:
                pass
            self.state.save(local)
            await self.events.emit(
                "attempt.executor_stop_unconfirmed",
                issue=issue,
                attempt_id=attempt_id,
                details={
                    "execution_target": target.id,
                    "outcome": result.outcome.value,
                    "error": result.error,
                },
            )
            return
        local.process = None
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
            await self._schedule_clean_continuation(
                current_issue,
                local,
                target=target,
                attempt_id=attempt_id,
            )
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
        self,
        issue: Issue,
        local: IssueState,
        *,
        target: ExecutionTarget,
        attempt_id: str,
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
                    if local.backend_session_id and target.backend.supports_native_resume
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
                try:
                    requested_target = self.execution.placement(result)
                except PlacementError as error:
                    entry.cancel_reason = "placement_invalid"
                    cancel_details = {
                        "disposition": disposition.value,
                        "reason": "placement_invalid",
                        "error": str(error),
                    }
                else:
                    if requested_target == entry.target_id:
                        continue
                    entry.cancel_reason = "execution_target_changed"
                    cancel_details = {
                        "disposition": disposition.value,
                        "reason": "execution_target_changed",
                        "previous_target": entry.target_id,
                        "requested_target": requested_target,
                    }
            else:
                entry.cancel_reason = f"tracker_{disposition.value}"
                cancel_details = {"disposition": disposition.value}
            entry.cancel_event.set()
            to_cancel.append(entry)
            await self.events.emit(
                "reconcile.cancel_requested",
                issue=result,
                issue_id=entry.issue.id,
                identifier=entry.issue.identifier,
                details=cancel_details,
            )

        if to_cancel:
            timeout = (
                max(
                    self.execution.target(entry.target_id).config.agent.cancel_grace_seconds
                    for entry in to_cancel
                )
                + 15
            )
            _done, pending = await asyncio.wait(
                [entry.task for entry in to_cancel], timeout=timeout
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def _reconcile_waiting(self, active_by_id: dict[str, Issue]) -> None:
        for local in self.state.load_all():
            if local.issue_id in self.running or local.phase in {
                LocalPhase.RELEASED,
                LocalPhase.BLOCKED,
            }:
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

    async def _reconcile_blocked(self, active_by_id: dict[str, Issue]) -> None:
        for local in self.state.load_all():
            if local.phase is not LocalPhase.BLOCKED:
                continue
            if local.last_outcome == "placement_blocked":
                issue = active_by_id.get(local.issue_id)
                if issue is None:
                    try:
                        issue = await self.tracker.get_issue(local.issue_id)
                    except TrackerError:
                        continue
                disposition = self.tracker.disposition(issue)
                if disposition is not IssueDisposition.ACTIVE:
                    if issue is not None:
                        self._refresh_local_snapshot(local, issue)
                    local.phase = LocalPhase.RELEASED
                    local.next_run_at = None
                    local.last_outcome = "placement_block_released_by_tracker"
                    local.last_error = None
                    self.state.save(local)
                    await self.events.emit(
                        "placement.block_released_by_tracker",
                        issue=issue,
                        issue_id=local.issue_id,
                        identifier=local.identifier,
                        details={"disposition": disposition.value},
                    )
                continue
            safe = await self._reap_state(local, event_prefix="reconcile")
            if not safe:
                continue
            issue = active_by_id.get(local.issue_id)
            if issue is None:
                try:
                    issue = await self.tracker.get_issue(local.issue_id)
                except TrackerError as error:
                    local.phase = LocalPhase.READY
                    local.last_error = f"executor is safe; tracker refresh failed: {error}"
                    self.state.save(local)
                    continue
            disposition = self.tracker.disposition(issue)
            if issue is not None:
                self._refresh_local_snapshot(local, issue)
            local.phase = (
                LocalPhase.READY if disposition is IssueDisposition.ACTIVE else LocalPhase.RELEASED
            )
            local.next_run_at = None
            local.last_outcome = "executor_stop_confirmed"
            local.last_error = None
            self.state.save(local)
            await self.events.emit(
                "reconcile.executor_unblocked",
                issue=issue,
                issue_id=local.issue_id,
                identifier=local.identifier,
                details={
                    "execution_target": self._state_target_id(local),
                    "disposition": disposition.value,
                    "local_phase": local.phase.value,
                },
            )

    @staticmethod
    def _continuation_mode(local: IssueState, target: ExecutionTarget) -> ContinuationMode:
        if local.total_attempts == 0:
            return ContinuationMode.FIRST
        if local.backend_session_id and target.backend.supports_native_resume:
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
