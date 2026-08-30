from __future__ import annotations

import asyncio
import contextlib
import os

import psutil

from .models import ProcessIdentity


class ProcessGuard:
    """Captures process birth identity and terminates only the matching process tree."""

    @staticmethod
    def capture(pid: int, attempt_id: str, *, host_id: str = "local") -> ProcessIdentity:
        process = psutil.Process(pid)
        return ProcessIdentity(
            pid=pid,
            create_time=process.create_time(),
            attempt_id=attempt_id,
            host_id=host_id,
        )

    @staticmethod
    def matches(identity: ProcessIdentity) -> bool:
        try:
            process = psutil.Process(identity.pid)
            return abs(process.create_time() - identity.create_time) < 0.01
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False

    @classmethod
    async def terminate_tree(cls, identity: ProcessIdentity, *, grace_seconds: float = 5.0) -> bool:
        return await asyncio.to_thread(cls._terminate_tree, identity, grace_seconds)

    @classmethod
    def _terminate_tree(cls, identity: ProcessIdentity, grace_seconds: float) -> bool:
        if identity.pid == os.getpid() or not cls.matches(identity):
            return False
        try:
            parent = psutil.Process(identity.pid)
            processes = parent.children(recursive=True)
            processes.append(parent)
            for process in reversed(processes):
                with contextlib.suppress(psutil.NoSuchProcess, psutil.AccessDenied):
                    process.terminate()
            _, alive = psutil.wait_procs(processes, timeout=grace_seconds)
            for process in alive:
                with contextlib.suppress(psutil.NoSuchProcess, psutil.AccessDenied):
                    process.kill()
            if alive:
                psutil.wait_procs(alive, timeout=max(1.0, grace_seconds))
            return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False
