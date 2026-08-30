from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .backend import AgentBackend, CommandAgentBackend
from .config import ExecutionTargetConfig, SchedulerConfig
from .models import Issue, ProcessIdentity, ReapResult, WorkspaceInfo
from .process_guard import ProcessGuard
from .remote_execution import RemoteWorkspaceManager, SSHAgentBackend
from .ssh_transport import SSHTransport
from .workspace import WorkspaceManager


class PlacementError(RuntimeError):
    pass


class WorkspaceProvider(Protocol):
    async def ensure(self, issue: Issue, *, attempt_id: str | None = None) -> WorkspaceInfo: ...

    async def snapshot(self, path: object, *, max_chars: int = 12000) -> str: ...


@dataclass(slots=True)
class ExecutionTarget:
    config: ExecutionTargetConfig
    workspace: WorkspaceProvider
    backend: AgentBackend
    remote_backend: SSHAgentBackend | None = None

    @property
    def id(self) -> str:
        return self.config.id

    @property
    def max_concurrent_agents(self) -> int:
        return self.config.max_concurrent_agents

    async def reap(
        self,
        *,
        attempt_id: str,
        identity: ProcessIdentity | None,
        process_guard: type[ProcessGuard] = ProcessGuard,
    ) -> ReapResult:
        if self.remote_backend is not None:
            return await self.remote_backend.reap(attempt_id)
        if identity is None:
            return ReapResult(confirmed_safe=True)
        if identity.host_id not in {"local", self.id}:
            return ReapResult(
                confirmed_safe=False,
                error=(
                    f"process identity belongs to {identity.host_id}, not execution target {self.id}"
                ),
            )
        matched = process_guard.matches(identity)
        terminated = False
        if matched:
            terminated = await process_guard.terminate_tree(
                identity,
                grace_seconds=self.config.agent.cancel_grace_seconds,
            )
        return ReapResult(
            confirmed_safe=not process_guard.matches(identity),
            identity_matched=matched,
            terminated=terminated,
        )


class ExecutionRouter:
    def __init__(
        self,
        *,
        targets: dict[str, ExecutionTarget],
        default_target: str,
        label_prefix: str,
    ) -> None:
        self.targets = targets
        self.default_target = default_target
        self.label_prefix = label_prefix

    def placement(self, issue: Issue) -> str:
        requested = {
            label[len(self.label_prefix) :]
            for label in issue.labels
            if label.startswith(self.label_prefix)
        }
        if len(requested) > 1:
            raise PlacementError(
                f"Issue has multiple execution-target labels: {', '.join(sorted(requested))}"
            )
        target_id = next(iter(requested), self.default_target)
        if target_id not in self.targets:
            raise PlacementError(f"Issue requests unknown execution target: {target_id}")
        return target_id

    def target(self, target_id: str) -> ExecutionTarget:
        try:
            return self.targets[target_id]
        except KeyError as error:
            raise PlacementError(
                f"execution target is no longer configured: {target_id}"
            ) from error


def build_execution_router(config: SchedulerConfig) -> ExecutionRouter:
    targets: dict[str, ExecutionTarget] = {}
    single_legacy_local = (
        len(config.execution.targets) == 1
        and config.execution.default_target == "local"
        and config.execution.targets["local"].kind == "local"
    )
    for target_config in config.execution.targets.values():
        if target_config.kind == "local":
            root = (
                config.state_root
                if single_legacy_local
                else config.state_root / "execution-targets" / target_config.id
            )
            targets[target_config.id] = ExecutionTarget(
                config=target_config,
                workspace=WorkspaceManager(root, target_config.repository),
                backend=CommandAgentBackend(
                    target_config.agent,
                    host_id=target_config.id,
                ),
            )
            continue

        assert target_config.ssh is not None
        assert target_config.remote_state_root is not None
        transport = SSHTransport(target_config.ssh)
        remote_backend = SSHAgentBackend(
            target_id=target_config.id,
            state_root=target_config.remote_state_root,
            repository=target_config.repository,
            agent=target_config.agent,
            transport=transport,
        )
        targets[target_config.id] = ExecutionTarget(
            config=target_config,
            workspace=RemoteWorkspaceManager(
                target_id=target_config.id,
                state_root=target_config.remote_state_root,
                repository=target_config.repository,
                transport=transport,
            ),
            backend=remote_backend,
            remote_backend=remote_backend,
        )
    return ExecutionRouter(
        targets=targets,
        default_target=config.execution.default_target,
        label_prefix=config.execution.label_prefix,
    )
