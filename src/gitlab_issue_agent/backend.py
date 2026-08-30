from __future__ import annotations

import abc
import asyncio
import json
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, ClassVar

import psutil

from .config import AgentConfig
from .models import (
    AgentResult,
    AttemptContext,
    AttemptOutcome,
    ContinuationMode,
    ProcessIdentity,
)
from .process_guard import ProcessGuard

EmitCallback = Callable[[str, dict[str, Any]], Awaitable[None]]
ProcessStartedCallback = Callable[[ProcessIdentity], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class BackendCallbacks:
    emit: EmitCallback
    process_started: ProcessStartedCallback


class AgentBackend(abc.ABC):
    @property
    @abc.abstractmethod
    def supports_native_resume(self) -> bool: ...

    @abc.abstractmethod
    async def run(
        self,
        context: AttemptContext,
        *,
        cancel_event: asyncio.Event,
        callbacks: BackendCallbacks,
    ) -> AgentResult: ...


class CommandAgentBackend(AgentBackend):
    """Generic no-shell command backend for csc/custom-claude and other `-p` CLIs."""

    _BASE_ENV: ClassVar[set[str]] = {
        "PATH",
        "HOME",
        "USER",
        "LOGNAME",
        "SHELL",
        "LANG",
        "LC_ALL",
        "TERM",
        "TMPDIR",
        "TEMP",
        "TMP",
        "SSH_AUTH_SOCK",
        "SYSTEMROOT",
        "COMSPEC",
        "PATHEXT",
        "USERPROFILE",
        "APPDATA",
        "LOCALAPPDATA",
        "PROGRAMDATA",
    }

    def __init__(self, config: AgentConfig, *, process_guard: type[ProcessGuard] = ProcessGuard):
        self.config = config
        self.process_guard = process_guard

    @property
    def supports_native_resume(self) -> bool:
        return self.config.supports_native_resume

    async def run(
        self,
        context: AttemptContext,
        *,
        cancel_event: asyncio.Event,
        callbacks: BackendCallbacks,
    ) -> AgentResult:
        started = time.monotonic()
        args = self._arguments(context)
        env = self._environment(context)
        command_preview = [self.config.command, *self._redacted_arguments(args, context.prompt)]
        await callbacks.emit(
            "agent.launching",
            {
                "mode": context.mode.value,
                "command": command_preview,
                "cwd": str(context.workspace.path),
            },
        )
        spawn_options: dict[str, Any] = {}
        if os.name == "nt":
            spawn_options["creationflags"] = 0x00000200  # CREATE_NEW_PROCESS_GROUP
        else:
            spawn_options["start_new_session"] = True
        try:
            process = await asyncio.create_subprocess_exec(
                self.config.command,
                *args,
                cwd=str(context.workspace.path),
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=1024 * 1024,
                **spawn_options,
            )
        except OSError as error:
            message = f"cannot launch agent backend: {error}"
            await callbacks.emit("agent.spawn_failed", {"error": message})
            return AgentResult(
                outcome=AttemptOutcome.FAILED,
                exit_code=None,
                session_id=context.session_id,
                summary="",
                error=message,
                duration_seconds=time.monotonic() - started,
            )

        identity: ProcessIdentity | None = None
        try:
            try:
                identity = self.process_guard.capture(process.pid, context.attempt_id)
                await callbacks.process_started(identity)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                await callbacks.emit("agent.process_identity_unavailable", {"pid": process.pid})

            session_id = context.session_id
            summary_parts: list[str] = []

            async def read_stream(stream: asyncio.StreamReader | None, stream_name: str) -> None:
                nonlocal session_id, summary_parts
                if stream is None:
                    return
                while True:
                    raw_line = await stream.readline()
                    if not raw_line:
                        return
                    line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
                    if not line:
                        continue
                    parsed = self._parse_line(line)
                    observed_session = self._extract_session_id(parsed) if parsed else None
                    if observed_session and observed_session != session_id:
                        session_id = observed_session
                        await callbacks.emit(
                            "agent.session_observed", {"session_id": observed_session}
                        )
                    details: dict[str, Any] = {"stream": stream_name}
                    if parsed is not None:
                        details.update({"kind": "json", "record": parsed})
                        summary_piece = self._summary_from_json(parsed)
                    else:
                        details.update({"kind": "text", "text": line[:65536]})
                        summary_piece = line
                    await callbacks.emit("agent.output", details)
                    if summary_piece:
                        summary_parts.append(summary_piece)
                        summary_parts = self._trim_summary(summary_parts)

            stdout_task = asyncio.create_task(read_stream(process.stdout, "stdout"))
            stderr_task = asyncio.create_task(read_stream(process.stderr, "stderr"))
            wait_task = asyncio.create_task(process.wait())
            cancel_task = asyncio.create_task(cancel_event.wait())
            done, _ = await asyncio.wait(
                {wait_task, cancel_task},
                timeout=self.config.timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if cancel_task in done and cancel_event.is_set():
                if identity:
                    await self.process_guard.terminate_tree(
                        identity, grace_seconds=self.config.cancel_grace_seconds
                    )
                elif process.returncode is None:
                    process.terminate()
                await process.wait()
                outcome = AttemptOutcome.CANCELLED
                error = "cancelled by scheduler"
            elif wait_task in done:
                outcome = (
                    AttemptOutcome.CLEAN_EXIT if process.returncode == 0 else AttemptOutcome.FAILED
                )
                error = (
                    None
                    if process.returncode == 0
                    else f"agent exited with code {process.returncode}"
                )
            else:
                if identity:
                    await self.process_guard.terminate_tree(
                        identity, grace_seconds=self.config.cancel_grace_seconds
                    )
                elif process.returncode is None:
                    process.kill()
                await process.wait()
                outcome = AttemptOutcome.TIMED_OUT
                error = f"agent exceeded timeout of {self.config.timeout_seconds}s"

            cancel_task.cancel()
            if not wait_task.done():
                wait_task.cancel()
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            summary = "\n".join(summary_parts)[-self.config.summary_chars :]
            await callbacks.emit(
                "agent.exited",
                {
                    "outcome": outcome.value,
                    "exit_code": process.returncode,
                    "session_id": session_id,
                    "duration_seconds": time.monotonic() - started,
                    "error": error,
                },
            )
            return AgentResult(
                outcome=outcome,
                exit_code=process.returncode,
                session_id=session_id,
                summary=summary,
                error=error,
                duration_seconds=time.monotonic() - started,
            )
        except asyncio.CancelledError:
            if identity and self.process_guard.matches(identity):
                await self.process_guard.terminate_tree(
                    identity, grace_seconds=self.config.cancel_grace_seconds
                )
            elif process.returncode is None:
                process.kill()
                await process.wait()
            raise

    def _arguments(self, context: AttemptContext) -> list[str]:
        if context.mode is ContinuationMode.NATIVE:
            if not context.session_id or not self.config.native_resume_args:
                raise ValueError("native resume requested without a session id/resume template")
            template = self.config.native_resume_args
        else:
            template = self.config.args
        values = {
            "{prompt}": context.prompt,
            "{session_id}": context.session_id or "",
            "{workspace}": str(context.workspace.path),
            "{issue_id}": context.issue.id,
            "{attempt_id}": context.attempt_id,
        }
        rendered: list[str] = []
        for argument in template:
            for placeholder, value in values.items():
                argument = argument.replace(placeholder, value)
            rendered.append(argument)
        return rendered

    def _environment(self, context: AttemptContext) -> dict[str, str]:
        allowed = {name.upper() for name in self._BASE_ENV.union(self.config.pass_env)}
        env = {name: value for name, value in os.environ.items() if name.upper() in allowed}
        env.update(self.config.env)
        env.update(
            {
                "ISSUE_AGENT_ATTEMPT_ID": context.attempt_id,
                "ISSUE_AGENT_ISSUE_ID": context.issue.id,
                "ISSUE_AGENT_ISSUE_IDENTIFIER": context.issue.identifier,
                "ISSUE_AGENT_WORKSPACE": str(context.workspace.path),
                "ISSUE_AGENT_CONTINUATION_MODE": context.mode.value,
            }
        )
        return env

    def _parse_line(self, line: str) -> dict[str, Any] | None:
        if self.config.output_format == "text":
            return None
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else {"value": parsed}

    def _extract_session_id(self, record: dict[str, Any]) -> str | None:
        for path in self.config.session_id_paths:
            value: Any = record
            for segment in path.split("."):
                if not isinstance(value, dict) or segment not in value:
                    value = None
                    break
                value = value[segment]
            if isinstance(value, str) and value.strip():
                return value.strip()
        return self._recursive_session_id(record)

    def _recursive_session_id(self, value: Any) -> str | None:
        if isinstance(value, dict):
            for key, item in value.items():
                if (
                    key in {"session_id", "sessionId", "thread_id", "threadId"}
                    and isinstance(item, str)
                    and item.strip()
                ):
                    return item.strip()
                nested = self._recursive_session_id(item)
                if nested:
                    return nested
        elif isinstance(value, list):
            for item in value:
                nested = self._recursive_session_id(item)
                if nested:
                    return nested
        return None

    @staticmethod
    def _summary_from_json(record: dict[str, Any]) -> str:
        for key in ("result", "message", "text", "content", "summary"):
            value = record.get(key)
            if isinstance(value, str):
                return value
        return json.dumps(record, ensure_ascii=False, separators=(",", ":"))

    def _trim_summary(self, parts: list[str]) -> list[str]:
        total = 0
        kept: list[str] = []
        for part in reversed(parts):
            total += len(part) + 1
            kept.append(part)
            if total >= self.config.summary_chars:
                break
        return list(reversed(kept))

    @staticmethod
    def _redacted_arguments(arguments: list[str], prompt: str) -> list[str]:
        return [argument.replace(prompt, "<prompt>") for argument in arguments]
