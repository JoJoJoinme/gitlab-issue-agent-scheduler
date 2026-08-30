from __future__ import annotations

import asyncio
import time
from typing import Any

from .backend import AgentBackend, BackendCallbacks
from .config import AgentConfig, RepositoryConfig
from .models import (
    AgentResult,
    AttemptContext,
    AttemptOutcome,
    ProcessIdentity,
    ReapResult,
    WorkspaceInfo,
)
from .remote_protocol import (
    PROTOCOL_VERSION,
    agent_payload,
    issue_payload,
    repository_payload,
)
from .ssh_transport import SSHTransport, SSHTransportError


class RemoteWorkspaceManager:
    def __init__(
        self,
        *,
        target_id: str,
        state_root: str,
        repository: RepositoryConfig,
        transport: SSHTransport,
    ) -> None:
        self.target_id = target_id
        self.state_root = state_root
        self.repository = repository
        self.transport = transport

    def _base(self, operation: str) -> dict[str, Any]:
        return {
            "protocol_version": PROTOCOL_VERSION,
            "operation": operation,
            "target_id": self.target_id,
            "state_root": self.state_root,
            "repository": repository_payload(self.repository),
        }

    async def ensure(self, issue: Any, *, attempt_id: str | None = None) -> WorkspaceInfo:
        result = await self.transport.request(
            {
                **self._base("prepare"),
                "issue": issue_payload(issue),
                "attempt_id": attempt_id,
            }
        )
        return WorkspaceInfo(
            path=str(result["path"]),
            branch=str(result["branch"]),
            created_now=bool(result.get("created_now", False)),
        )

    async def snapshot(self, path: str, *, max_chars: int = 12000) -> str:
        result = await self.transport.request(
            {
                **self._base("snapshot"),
                "workspace_path": str(path),
                "max_chars": max_chars,
            }
        )
        return str(result.get("snapshot", ""))


class SSHAgentBackend(AgentBackend):
    def __init__(
        self,
        *,
        target_id: str,
        state_root: str,
        repository: RepositoryConfig,
        agent: AgentConfig,
        transport: SSHTransport,
    ) -> None:
        self.target_id = target_id
        self.state_root = state_root
        self.repository = repository
        self.agent = agent
        self.transport = transport

    @property
    def supports_native_resume(self) -> bool:
        return self.agent.supports_native_resume

    def _base(self, operation: str) -> dict[str, Any]:
        return {
            "protocol_version": PROTOCOL_VERSION,
            "operation": operation,
            "target_id": self.target_id,
            "state_root": self.state_root,
        }

    async def reap(self, attempt_id: str) -> ReapResult:
        try:
            result = await self.transport.request(
                {
                    **self._base("cancel"),
                    "attempt_id": attempt_id,
                    "grace_seconds": self.agent.cancel_grace_seconds,
                }
            )
            return ReapResult(
                confirmed_safe=bool(result.get("confirmed_safe", False)),
                identity_matched=bool(result.get("identity_matched", False)),
                terminated=bool(result.get("terminated", False)),
                error=(str(result["error"]) if result.get("error") else None),
            )
        except SSHTransportError as error:
            return ReapResult(confirmed_safe=False, error=str(error))

    async def run(
        self,
        context: AttemptContext,
        *,
        cancel_event: asyncio.Event,
        callbacks: BackendCallbacks,
    ) -> AgentResult:
        started = time.monotonic()

        async def on_message(message: dict[str, Any]) -> None:
            message_type = message.get("type")
            if message_type == "event":
                details = message.get("details", {})
                if not isinstance(details, dict):
                    details = {"remote_details": details}
                await callbacks.emit(
                    str(message.get("event_type", "remote.unknown")),
                    {"execution_target": self.target_id, "remote": True, **details},
                )
                return
            if message_type == "process_started":
                identity_raw = message.get("identity")
                if not isinstance(identity_raw, dict):
                    raise SSHTransportError("remote process identity is malformed")
                identity = ProcessIdentity(**identity_raw)
                if identity.attempt_id != context.attempt_id or identity.host_id != self.target_id:
                    raise SSHTransportError("remote process identity ownership mismatch")
                await callbacks.process_started(identity)

        request = {
            **self._base("run"),
            "repository": repository_payload(self.repository),
            "agent": agent_payload(self.agent),
            "issue": issue_payload(context.issue),
            "context": {
                "attempt_id": context.attempt_id,
                "attempt_number": context.attempt_number,
                "failure_count": context.failure_count,
                "continuation_index": context.continuation_index,
                "mode": context.mode.value,
                "prompt": context.prompt,
                "session_id": context.session_id,
            },
        }
        request_task = asyncio.create_task(self.transport.request(request, on_message=on_message))
        cancel_task = asyncio.create_task(cancel_event.wait())
        transport_timeout = self.agent.timeout_seconds + self.agent.cancel_grace_seconds + 30.0
        try:
            done, _ = await asyncio.wait(
                {request_task, cancel_task},
                timeout=transport_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if cancel_task in done and cancel_event.is_set():
                reap = await self.reap(context.attempt_id)
                await self._settle_request(request_task)
                return AgentResult(
                    outcome=AttemptOutcome.CANCELLED,
                    exit_code=None,
                    session_id=context.session_id,
                    summary="",
                    error=(
                        "cancelled by scheduler"
                        if reap.confirmed_safe
                        else f"cancel requested but remote stop is unconfirmed: {reap.error}"
                    ),
                    duration_seconds=time.monotonic() - started,
                    executor_safe=reap.confirmed_safe,
                )
            if request_task not in done:
                reap = await self.reap(context.attempt_id)
                await self._settle_request(request_task)
                return AgentResult(
                    outcome=AttemptOutcome.TIMED_OUT,
                    exit_code=None,
                    session_id=context.session_id,
                    summary="",
                    error="SSH execution exceeded its control-plane timeout",
                    duration_seconds=time.monotonic() - started,
                    executor_safe=reap.confirmed_safe,
                )

            result = request_task.result()
            raw = result.get("agent_result")
            if not isinstance(raw, dict):
                raise SSHTransportError("remote run response lacks agent_result")
            return AgentResult(
                outcome=AttemptOutcome(str(raw["outcome"])),
                exit_code=(int(raw["exit_code"]) if raw.get("exit_code") is not None else None),
                session_id=(str(raw["session_id"]) if raw.get("session_id") is not None else None),
                summary=str(raw.get("summary", "")),
                error=str(raw["error"]) if raw.get("error") is not None else None,
                duration_seconds=float(raw.get("duration_seconds", time.monotonic() - started)),
                executor_safe=bool(raw.get("executor_safe", True)),
            )
        except (SSHTransportError, KeyError, TypeError, ValueError) as error:
            reap = await self.reap(context.attempt_id)
            return AgentResult(
                outcome=AttemptOutcome.FAILED,
                exit_code=None,
                session_id=context.session_id,
                summary="",
                error=f"SSH execution failed: {error}",
                duration_seconds=time.monotonic() - started,
                executor_safe=reap.confirmed_safe,
            )
        except asyncio.CancelledError:
            await self.reap(context.attempt_id)
            await self._settle_request(request_task)
            raise
        finally:
            cancel_task.cancel()
            await asyncio.gather(cancel_task, return_exceptions=True)

    async def _settle_request(self, task: asyncio.Task[dict[str, Any]]) -> None:
        try:
            await asyncio.wait_for(
                asyncio.shield(task), timeout=self.agent.cancel_grace_seconds + 5.0
            )
        except (TimeoutError, SSHTransportError, asyncio.CancelledError):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
