from __future__ import annotations

import asyncio
import json
import re
import shlex
from collections.abc import Awaitable, Callable
from typing import Any

from .config import SSHConfig


class SSHTransportError(RuntimeError):
    pass


MessageCallback = Callable[[dict[str, Any]], Awaitable[None]]

_HOST_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_USER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class SSHTransport:
    """One JSON request per OpenSSH connection to a fixed remote helper.

    Only trusted configuration is placed in the SSH command. Issue text,
    prompts, paths, and credentials are sent as JSON over stdin.
    """

    def __init__(self, config: SSHConfig, *, max_line_bytes: int = 4 * 1024 * 1024) -> None:
        self.config = config
        self.max_line_bytes = max_line_bytes
        if not _HOST_PATTERN.fullmatch(config.host):
            raise SSHTransportError(f"unsafe SSH host syntax: {config.host!r}")
        if config.user and not _USER_PATTERN.fullmatch(config.user):
            raise SSHTransportError(f"unsafe SSH user syntax: {config.user!r}")

    @property
    def destination(self) -> str:
        return f"{self.config.user}@{self.config.host}" if self.config.user else self.config.host

    def command(self) -> list[str]:
        config = self.config
        arguments = [
            config.executable,
            "-T",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            f"ConnectTimeout={max(1, int(config.connect_timeout_seconds))}",
            "-o",
            f"ServerAliveInterval={config.server_alive_interval_seconds}",
            "-o",
            f"ServerAliveCountMax={config.server_alive_count_max}",
            "-p",
            str(config.port),
        ]
        if config.identity_file:
            arguments.extend(["-i", str(config.identity_file)])
        if config.known_hosts_file:
            arguments.extend(["-o", f"UserKnownHostsFile={config.known_hosts_file}"])
        for option in config.options:
            arguments.extend(["-o", option])
        # OpenSSH sends a command string to the server. shlex.join quotes the
        # static, administrator-owned argv; no Issue-controlled value is here.
        remote_command = shlex.join(config.remote_command)
        arguments.extend([self.destination, remote_command])
        return arguments

    async def request(
        self,
        payload: dict[str, Any],
        *,
        on_message: MessageCallback | None = None,
    ) -> dict[str, Any]:
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        try:
            process = await asyncio.create_subprocess_exec(
                *self.command(),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=self.max_line_bytes,
            )
        except OSError as error:
            raise SSHTransportError(f"cannot launch OpenSSH: {error}") from error

        stderr_parts: list[str] = []

        async def read_stderr() -> None:
            assert process.stderr is not None
            while line := await process.stderr.readline():
                stderr_parts.append(line.decode("utf-8", errors="replace"))
                if sum(map(len, stderr_parts)) > 65536:
                    del stderr_parts[:-8]

        stderr_task = asyncio.create_task(read_stderr())
        response: dict[str, Any] | None = None
        try:
            assert process.stdin is not None
            process.stdin.write(encoded.encode("utf-8"))
            await process.stdin.drain()
            process.stdin.close()

            assert process.stdout is not None
            while raw_line := await process.stdout.readline():
                try:
                    message = json.loads(raw_line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise SSHTransportError("remote helper emitted invalid JSON") from error
                if not isinstance(message, dict):
                    raise SSHTransportError("remote helper emitted a non-object message")
                if message.get("type") == "response":
                    response = message
                elif on_message is not None:
                    await on_message(message)

            return_code = await process.wait()
            await stderr_task
            stderr = "".join(stderr_parts)[-8000:].strip()
            if response is None:
                suffix = f": {stderr}" if stderr else ""
                raise SSHTransportError(
                    f"remote helper exited with code {return_code} without a response{suffix}"
                )
            if return_code != 0 or not response.get("ok", False):
                error = str(response.get("error") or stderr or f"exit {return_code}")
                raise SSHTransportError(f"remote helper request failed: {error}")
            result = response.get("result", {})
            if not isinstance(result, dict):
                raise SSHTransportError("remote helper returned a non-object result")
            return result
        except asyncio.CancelledError:
            if process.returncode is None:
                process.kill()
                await process.wait()
            stderr_task.cancel()
            await asyncio.gather(stderr_task, return_exceptions=True)
            raise
        except Exception:
            if process.returncode is None:
                process.kill()
                await process.wait()
            stderr_task.cancel()
            await asyncio.gather(stderr_task, return_exceptions=True)
            raise
