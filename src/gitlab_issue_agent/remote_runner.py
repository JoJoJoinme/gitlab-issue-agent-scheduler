from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .backend import BackendCallbacks, CommandAgentBackend
from .models import AttemptContext, ContinuationMode, ProcessIdentity
from .process_guard import ProcessGuard
from .remote_protocol import (
    PROTOCOL_VERSION,
    agent_from_payload,
    issue_from_payload,
    repository_from_payload,
)
from .workspace import WorkspaceManager

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class RemoteRequestError(RuntimeError):
    pass


class ProtocolWriter:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.connected = True

    async def send(self, value: dict[str, Any]) -> None:
        if not self.connected:
            return
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
        async with self._lock:
            try:
                print(encoded, flush=True)
            except (BrokenPipeError, OSError):
                # The scheduler may have been SIGKILLed. Keep supervising the
                # agent and retain the lease until it really exits.
                self.connected = False


class LeaseStore:
    def __init__(self, root: Path) -> None:
        self.root = root / "leases"
        self.root.mkdir(parents=True, exist_ok=True)
        self.cancellations_root = root / "cancellations"
        self.cancellations_root.mkdir(parents=True, exist_ok=True)

    def path(self, attempt_id: str) -> Path:
        if not _SAFE_ID.fullmatch(attempt_id):
            raise RemoteRequestError("attempt_id contains unsupported characters")
        return self.root / f"{attempt_id}.json"

    def save(
        self,
        attempt_id: str,
        *,
        target_id: str,
        status: str,
        identity: ProcessIdentity,
        workspace_path: str | None = None,
    ) -> None:
        payload = {
            "protocol_version": PROTOCOL_VERSION,
            "attempt_id": attempt_id,
            "target_id": target_id,
            "status": status,
            "identity": identity.to_dict(),
            "workspace_path": workspace_path,
        }
        path = self.path(attempt_id)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)

    def load(self, attempt_id: str) -> dict[str, Any] | None:
        path = self.path(attempt_id)
        if not path.exists():
            return None
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RemoteRequestError(f"cannot read attempt lease: {error}") from error
        if not isinstance(value, dict):
            raise RemoteRequestError("attempt lease is not an object")
        return value

    def remove(self, attempt_id: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            self.path(attempt_id).unlink()

    def mark_cancelled(self, attempt_id: str, *, target_id: str) -> None:
        path = self.cancellations_root / self.path(attempt_id).name
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_text(
            json.dumps({"attempt_id": attempt_id, "target_id": target_id}) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)

    def is_cancelled(self, attempt_id: str, *, target_id: str) -> bool:
        path = self.cancellations_root / self.path(attempt_id).name
        if not path.exists():
            return False
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RemoteRequestError(f"cannot read cancellation tombstone: {error}") from error
        return value.get("attempt_id") == attempt_id and value.get("target_id") == target_id


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RemoteRequestError(f"{name} must be an object")
    return value


def _state_root(request: dict[str, Any]) -> Path:
    value = str(request.get("state_root", "")).strip()
    if not value:
        raise RemoteRequestError("state_root is required")
    root = Path(value).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _target_id(request: dict[str, Any]) -> str:
    value = str(request.get("target_id", "")).strip()
    if not _SAFE_ID.fullmatch(value):
        raise RemoteRequestError("target_id contains unsupported characters")
    return value


def _workspace_path(root: Path, raw_path: Any) -> Path:
    path = Path(str(raw_path)).expanduser().resolve()
    workspaces_root = (root / "workspaces").resolve()
    try:
        path.relative_to(workspaces_root)
    except ValueError as error:
        raise RemoteRequestError("workspace path is outside the configured state root") from error
    return path


async def _prepare(request: dict[str, Any]) -> dict[str, Any]:
    root = _state_root(request)
    repository = repository_from_payload(_mapping(request.get("repository"), "repository"))
    issue = issue_from_payload(_mapping(request.get("issue"), "issue"))
    attempt_value = request.get("attempt_id")
    if attempt_value is None:
        workspace = await WorkspaceManager(root, repository).ensure(issue)
        return {
            "path": str(workspace.path),
            "branch": workspace.branch,
            "created_now": workspace.created_now,
        }
    attempt_id = str(attempt_value)
    target_id = _target_id(request)
    leases = LeaseStore(root)
    runner_identity = ProcessGuard.capture(os.getpid(), attempt_id, host_id=target_id)
    leases.save(
        attempt_id,
        target_id=target_id,
        status="preparing_workspace",
        identity=runner_identity,
    )
    try:
        if leases.is_cancelled(attempt_id, target_id=target_id):
            raise RemoteRequestError("attempt was cancelled before workspace preparation")
        workspace = await WorkspaceManager(root, repository).ensure(issue)
        return {
            "path": str(workspace.path),
            "branch": workspace.branch,
            "created_now": workspace.created_now,
        }
    finally:
        leases.remove(attempt_id)


async def _snapshot(request: dict[str, Any]) -> dict[str, Any]:
    root = _state_root(request)
    repository = repository_from_payload(_mapping(request.get("repository"), "repository"))
    path = _workspace_path(root, request.get("workspace_path"))
    snapshot = await WorkspaceManager(root, repository).snapshot(
        path, max_chars=int(request.get("max_chars", 12000))
    )
    return {"snapshot": snapshot}


async def _cancel(request: dict[str, Any]) -> dict[str, Any]:
    root = _state_root(request)
    target_id = _target_id(request)
    attempt_id = str(request.get("attempt_id", ""))
    leases = LeaseStore(root)
    leases.mark_cancelled(attempt_id, target_id=target_id)
    lease = leases.load(attempt_id)
    if lease is None:
        return {
            "confirmed_safe": True,
            "identity_matched": False,
            "terminated": False,
            "reason": "lease_absent",
        }
    if lease.get("target_id") != target_id or lease.get("attempt_id") != attempt_id:
        raise RemoteRequestError("attempt lease ownership does not match the request")
    identity_raw = _mapping(lease.get("identity"), "lease.identity")
    identity = ProcessIdentity(**identity_raw)
    matched = ProcessGuard.matches(identity)
    terminated = False
    if matched:
        terminated = await ProcessGuard.terminate_tree(
            identity,
            grace_seconds=float(request.get("grace_seconds", 5)),
        )
    confirmed_safe = not ProcessGuard.matches(identity)
    if confirmed_safe:
        leases.remove(attempt_id)
    return {
        "confirmed_safe": confirmed_safe,
        "identity_matched": matched,
        "terminated": terminated,
        "pid": identity.pid,
        "create_time": identity.create_time,
    }


async def _run(request: dict[str, Any], writer: ProtocolWriter) -> dict[str, Any]:
    root = _state_root(request)
    target_id = _target_id(request)
    repository = repository_from_payload(_mapping(request.get("repository"), "repository"))
    agent = agent_from_payload(_mapping(request.get("agent"), "agent"))
    issue = issue_from_payload(_mapping(request.get("issue"), "issue"))
    context_raw = _mapping(request.get("context"), "context")
    attempt_id = str(context_raw.get("attempt_id", ""))
    if not _SAFE_ID.fullmatch(attempt_id):
        raise RemoteRequestError("attempt_id contains unsupported characters")
    leases = LeaseStore(root)
    runner_identity = ProcessGuard.capture(os.getpid(), attempt_id, host_id=target_id)
    leases.save(
        attempt_id,
        target_id=target_id,
        status="launching",
        identity=runner_identity,
        workspace_path=None,
    )

    try:
        session_id = (
            str(context_raw["session_id"]) if context_raw.get("session_id") is not None else None
        )

        def cancelled_result() -> dict[str, Any]:
            return {
                "agent_result": {
                    "outcome": "cancelled",
                    "exit_code": None,
                    "session_id": session_id,
                    "summary": "",
                    "error": "cancelled before remote executor launch",
                    "duration_seconds": 0.0,
                    "executor_safe": True,
                }
            }

        if leases.is_cancelled(attempt_id, target_id=target_id):
            return cancelled_result()
        workspace = await WorkspaceManager(root, repository).ensure(issue)
        leases.save(
            attempt_id,
            target_id=target_id,
            status="launching",
            identity=runner_identity,
            workspace_path=str(workspace.path),
        )
        if leases.is_cancelled(attempt_id, target_id=target_id):
            return cancelled_result()
        context = AttemptContext(
            issue=issue,
            workspace=workspace,
            attempt_id=attempt_id,
            attempt_number=int(context_raw["attempt_number"]),
            failure_count=int(context_raw.get("failure_count", 0)),
            continuation_index=int(context_raw.get("continuation_index", 0)),
            mode=ContinuationMode(str(context_raw["mode"])),
            prompt=str(context_raw["prompt"]),
            session_id=session_id,
        )

        async def emit(event_type: str, details: dict[str, Any]) -> None:
            await writer.send({"type": "event", "event_type": event_type, "details": details})

        async def process_started(identity: ProcessIdentity) -> None:
            leases.save(
                attempt_id,
                target_id=target_id,
                status="running",
                identity=identity,
                workspace_path=str(workspace.path),
            )
            await writer.send({"type": "process_started", "identity": identity.to_dict()})

        result = await CommandAgentBackend(agent, host_id=target_id).run(
            context,
            cancel_event=asyncio.Event(),
            callbacks=BackendCallbacks(emit=emit, process_started=process_started),
        )
        payload = asdict(result)
        payload["outcome"] = result.outcome.value
        return {"agent_result": payload}
    finally:
        leases.remove(attempt_id)


async def handle(request: dict[str, Any], writer: ProtocolWriter) -> dict[str, Any]:
    if request.get("protocol_version") != PROTOCOL_VERSION:
        raise RemoteRequestError("unsupported protocol_version")
    operation = request.get("operation")
    if operation == "prepare":
        return await _prepare(request)
    if operation == "snapshot":
        return await _snapshot(request)
    if operation == "cancel":
        return await _cancel(request)
    if operation == "run":
        return await _run(request, writer)
    raise RemoteRequestError(f"unsupported operation: {operation!r}")


async def _main() -> int:
    writer = ProtocolWriter()
    try:
        line = await asyncio.to_thread(sys.stdin.readline)
        if not line:
            raise RemoteRequestError("request is absent")
        request = json.loads(line)
        if not isinstance(request, dict):
            raise RemoteRequestError("request must be an object")
        result = await handle(request, writer)
        await writer.send({"type": "response", "ok": True, "result": result})
        return 0
    except Exception as error:  # noqa: BLE001 - protocol errors become structured responses
        await writer.send(
            {
                "type": "response",
                "ok": False,
                "error": f"{type(error).__name__}: {error}",
            }
        )
        return 1


def main() -> None:
    raise SystemExit(asyncio.run(_main()))


if __name__ == "__main__":
    main()
