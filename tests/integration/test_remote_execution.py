from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pytest
from conftest import FakeTracker, make_config, make_issue, wait_until

from gitlab_issue_agent.backend import BackendCallbacks
from gitlab_issue_agent.config import ExecutionTargetConfig, SSHConfig
from gitlab_issue_agent.events import EventSink
from gitlab_issue_agent.execution import ExecutionRouter, ExecutionTarget
from gitlab_issue_agent.models import (
    AttemptContext,
    AttemptOutcome,
    ContinuationMode,
    IssueState,
    LocalPhase,
    ProcessIdentity,
)
from gitlab_issue_agent.orchestrator import Orchestrator
from gitlab_issue_agent.process_guard import ProcessGuard
from gitlab_issue_agent.prompts import PromptBuilder
from gitlab_issue_agent.remote_execution import RemoteWorkspaceManager, SSHAgentBackend
from gitlab_issue_agent.remote_protocol import (
    PROTOCOL_VERSION,
    agent_payload,
    issue_payload,
    repository_payload,
)
from gitlab_issue_agent.ssh_transport import SSHTransport, SSHTransportError
from gitlab_issue_agent.state import StateStore


class DirectRemoteTransport:
    """Runs the SSH remote helper locally while preserving its JSON wire protocol."""

    async def request(self, payload, *, on_message=None):
        environment = dict(os.environ)
        source_root = str(Path(__file__).parents[2] / "src")
        existing = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            source_root if not existing else os.pathsep.join([source_root, existing])
        )
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "gitlab_issue_agent.remote_runner",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=environment,
            limit=4 * 1024 * 1024,
        )
        try:
            assert process.stdin is not None
            process.stdin.write((json.dumps(payload, separators=(",", ":")) + "\n").encode())
            await process.stdin.drain()
            process.stdin.close()
            response = None
            assert process.stdout is not None
            while line := await process.stdout.readline():
                message = json.loads(line)
                if message.get("type") == "response":
                    response = message
                elif on_message:
                    await on_message(message)
            stderr = (await process.stderr.read() if process.stderr is not None else b"").decode(
                errors="replace"
            )
            return_code = await process.wait()
            if not response or return_code != 0 or not response.get("ok"):
                error = response.get("error") if response else stderr
                raise SSHTransportError(str(error or f"remote exit {return_code}"))
            return response["result"]
        except asyncio.CancelledError:
            if process.returncode is None:
                process.kill()
                await process.wait()
            raise


class UnreachableTransport:
    async def request(self, payload, *, on_message=None):
        del payload, on_message
        raise SSHTransportError("execution host is unreachable")


def remote_components(config, remote_root: Path, transport):
    target_id = "build-01"
    backend = SSHAgentBackend(
        target_id=target_id,
        state_root=str(remote_root),
        repository=config.repository,
        agent=config.agent,
        transport=transport,
    )
    workspace = RemoteWorkspaceManager(
        target_id=target_id,
        state_root=str(remote_root),
        repository=config.repository,
        transport=transport,
    )
    target_config = ExecutionTargetConfig(
        id=target_id,
        kind="ssh",
        repository=config.repository,
        agent=config.agent,
        max_concurrent_agents=1,
        remote_state_root=str(remote_root),
        ssh=None,
    )
    target = ExecutionTarget(
        config=target_config,
        workspace=workspace,
        backend=backend,
        remote_backend=backend,
    )
    router = ExecutionRouter(
        targets={target_id: target},
        default_target=target_id,
        label_prefix="agent-host::",
    )
    return workspace, backend, router


@pytest.mark.asyncio
async def test_remote_protocol_reuses_worktree_and_cancels_csc_process(
    tmp_path: Path, origin_repo: Path
) -> None:
    script = tmp_path / "remote_agent.py"
    script.write_text(
        """import json, time
print(json.dumps({"session_id": "remote-session", "message": "started"}), flush=True)
time.sleep(60)
""",
        encoding="utf-8",
    )
    config = make_config(
        tmp_path,
        origin_repo,
        command=sys.executable,
        args=[str(script), "-p", "{prompt}"],
        timeout_seconds=30,
    )
    remote_root = tmp_path / "remote-state"
    workspace, backend, _router = remote_components(config, remote_root, DirectRemoteTransport())
    issue = make_issue(labels=("agent::ready", "agent-host::build-01"))
    first = await workspace.ensure(issue)
    second = await workspace.ensure(issue)
    assert first.path == second.path
    assert first.created_now is True
    assert second.created_now is False

    cancel_event = asyncio.Event()
    identities = []
    emitted = []

    async def emit(event_type: str, details: dict[str, Any]) -> None:
        emitted.append((event_type, details))

    async def process_started(identity) -> None:
        identities.append(identity)

    context = AttemptContext(
        issue=issue,
        workspace=first,
        attempt_id=str(uuid.uuid4()),
        attempt_number=1,
        failure_count=0,
        continuation_index=0,
        mode=ContinuationMode.FIRST,
        prompt="work on the issue",
    )
    task = asyncio.create_task(
        backend.run(
            context,
            cancel_event=cancel_event,
            callbacks=BackendCallbacks(emit=emit, process_started=process_started),
        )
    )
    await wait_until(lambda: bool(identities))
    identity = identities[0]
    assert identity.host_id == "build-01"
    assert ProcessGuard.matches(identity)
    cancel_event.set()
    result = await asyncio.wait_for(task, timeout=10)
    assert result.outcome is AttemptOutcome.CANCELLED
    assert result.executor_safe is True
    assert not ProcessGuard.matches(identity)
    assert any(event_type == "agent.output" for event_type, _details in emitted)
    assert not (remote_root / "leases" / f"{context.attempt_id}.json").exists()


@pytest.mark.asyncio
async def test_cold_start_uses_remote_lease_then_tracker_overrides_local_state(
    tmp_path: Path, origin_repo: Path
) -> None:
    sleeper = tmp_path / "remote_orphan.py"
    sleeper.write_text("import time\ntime.sleep(60)\n", encoding="utf-8")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(sleeper),
        start_new_session=os.name != "nt",
    )
    try:
        issue = make_issue(labels=("agent::human-review",))
        tracker = FakeTracker(issue)
        config = make_config(tmp_path, origin_repo)
        remote_root = tmp_path / "remote-state"
        _workspace, _backend, router = remote_components(
            config, remote_root, DirectRemoteTransport()
        )
        attempt_id = str(uuid.uuid4())
        identity = ProcessGuard.capture(process.pid, attempt_id, host_id="build-01")
        leases = remote_root / "leases"
        leases.mkdir(parents=True)
        (leases / f"{attempt_id}.json").write_text(
            json.dumps(
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "attempt_id": attempt_id,
                    "target_id": "build-01",
                    "status": "running",
                    "identity": asdict(identity),
                    "workspace_path": None,
                }
            ),
            encoding="utf-8",
        )
        state = StateStore(config.state_root)
        state.save(
            IssueState(
                issue_id=issue.id,
                project_id=issue.project_id,
                iid=issue.iid,
                identifier=issue.identifier,
                phase=LocalPhase.RUNNING,
                tracker_state="opened",
                tracker_labels=["agent::ready"],
                execution_target="build-01",
                last_attempt_id=attempt_id,
                process=identity,
            )
        )
        orchestrator = Orchestrator(
            config,
            tracker=tracker,
            execution=router,
            state=state,
            events=EventSink(state, stdout=False),
            prompts=PromptBuilder(config.workflow_file),
        )

        await orchestrator.recover()
        await asyncio.wait_for(process.wait(), timeout=10)
        recovered = state.get(issue.identifier)
        assert recovered is not None
        assert recovered.phase is LocalPhase.RELEASED
        assert recovered.process is None
        assert recovered.tracker_labels == ["agent::human-review"]
        event_text = orchestrator.events.global_path.read_text(encoding="utf-8")
        assert '"event_type":"cold_start.orphan_reaped"' in event_text
        assert '"execution_target":"build-01"' in event_text
        await orchestrator.shutdown()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


@pytest.mark.asyncio
async def test_unreachable_remote_reaper_blocks_even_when_tracker_is_terminal(
    tmp_path: Path, origin_repo: Path
) -> None:
    config = make_config(tmp_path, origin_repo)
    issue = make_issue(state="closed", labels=("agent::done",))
    tracker = FakeTracker(issue)
    remote_root = tmp_path / "remote-state"
    _workspace, backend, router = remote_components(config, remote_root, UnreachableTransport())
    attempt_id = str(uuid.uuid4())
    state = StateStore(config.state_root)
    state.save(
        IssueState(
            issue_id=issue.id,
            project_id=issue.project_id,
            iid=issue.iid,
            identifier=issue.identifier,
            phase=LocalPhase.RUNNING,
            tracker_state="opened",
            tracker_labels=["agent::ready"],
            execution_target="build-01",
            last_attempt_id=attempt_id,
        )
    )
    orchestrator = Orchestrator(
        config,
        tracker=tracker,
        execution=router,
        state=state,
        events=EventSink(state, stdout=False),
        prompts=PromptBuilder(config.workflow_file),
    )

    await orchestrator.recover()
    recovered = state.get(issue.identifier)
    assert recovered is not None
    assert recovered.phase is LocalPhase.BLOCKED
    assert recovered.last_attempt_id == attempt_id
    assert recovered.tracker_state == "closed"
    assert "unconfirmed" in (recovered.last_error or "")
    await orchestrator.tick()
    assert state.get(issue.identifier).phase is LocalPhase.BLOCKED  # type: ignore[union-attr]
    backend.transport = DirectRemoteTransport()
    await orchestrator.tick()
    assert state.get(issue.identifier).phase is LocalPhase.RELEASED  # type: ignore[union-attr]
    await orchestrator.shutdown()


@pytest.mark.skipif(
    os.environ.get("ISSUE_AGENT_TEST_SSH") != "1",
    reason="loopback OpenSSH test is enabled explicitly in CI",
)
@pytest.mark.asyncio
async def test_real_openssh_round_trip_and_remote_cancellation(
    tmp_path: Path, origin_repo: Path
) -> None:
    identity_file = Path(os.environ["ISSUE_AGENT_TEST_SSH_IDENTITY"])
    known_hosts_file = Path(os.environ["ISSUE_AGENT_TEST_SSH_KNOWN_HOSTS"])
    ssh = SSHConfig(
        host=os.environ.get("ISSUE_AGENT_TEST_SSH_HOST", "127.0.0.1"),
        user=os.environ["ISSUE_AGENT_TEST_SSH_USER"],
        port=int(os.environ["ISSUE_AGENT_TEST_SSH_PORT"]),
        identity_file=identity_file,
        known_hosts_file=known_hosts_file,
        connect_timeout_seconds=5,
        remote_command=(sys.executable, "-m", "gitlab_issue_agent.remote_runner"),
    )
    script = tmp_path / "ssh_agent.py"
    script.write_text(
        """import json, time
print(json.dumps({"session_id": "ssh-session", "message": "started"}), flush=True)
time.sleep(60)
""",
        encoding="utf-8",
    )
    config = make_config(
        tmp_path,
        origin_repo,
        command=sys.executable,
        args=[str(script), "-p", "{prompt}"],
        timeout_seconds=30,
    )
    remote_root = tmp_path / "real-ssh-state"
    transport = SSHTransport(ssh)
    workspace = RemoteWorkspaceManager(
        target_id="loopback",
        state_root=str(remote_root),
        repository=config.repository,
        transport=transport,
    )
    backend = SSHAgentBackend(
        target_id="loopback",
        state_root=str(remote_root),
        repository=config.repository,
        agent=config.agent,
        transport=transport,
    )
    issue = make_issue(labels=("agent::ready", "agent-host::loopback"))
    prepared = await workspace.ensure(issue)
    identities = []
    cancel_event = asyncio.Event()

    async def ignore_event(_event_type: str, _details: dict[str, Any]) -> None:
        return None

    async def process_started(identity) -> None:
        identities.append(identity)

    context = AttemptContext(
        issue=issue,
        workspace=prepared,
        attempt_id=str(uuid.uuid4()),
        attempt_number=1,
        failure_count=0,
        continuation_index=0,
        mode=ContinuationMode.FIRST,
        prompt="run through real OpenSSH",
    )
    task = asyncio.create_task(
        backend.run(
            context,
            cancel_event=cancel_event,
            callbacks=BackendCallbacks(
                emit=ignore_event,
                process_started=process_started,
            ),
        )
    )
    await wait_until(lambda: bool(identities), timeout=15)
    cancel_event.set()
    result = await asyncio.wait_for(task, timeout=15)
    assert result.outcome is AttemptOutcome.CANCELLED
    assert result.executor_safe is True
    assert identities[0].host_id == "loopback"


@pytest.mark.skipif(
    os.name == "nt" or os.environ.get("ISSUE_AGENT_TEST_SSH") != "1",
    reason="real SSH SIGKILL recovery runs in Linux CI",
)
@pytest.mark.asyncio
async def test_real_openssh_scheduler_sigkill_then_remote_lease_reap(
    tmp_path: Path, origin_repo: Path
) -> None:
    script = tmp_path / "ssh_orphan_agent.py"
    script.write_text("import time\ntime.sleep(60)\n", encoding="utf-8")
    config = make_config(
        tmp_path,
        origin_repo,
        command=sys.executable,
        args=[str(script), "-p", "{prompt}"],
        timeout_seconds=30,
    )
    target_id = "loopback-kill9"
    attempt_id = str(uuid.uuid4())
    remote_root = tmp_path / "ssh-kill9-state"
    issue = make_issue(labels=("agent::ready", f"agent-host::{target_id}"))
    request = {
        "protocol_version": PROTOCOL_VERSION,
        "operation": "run",
        "target_id": target_id,
        "state_root": str(remote_root),
        "repository": repository_payload(config.repository),
        "agent": agent_payload(config.agent),
        "issue": issue_payload(issue),
        "context": {
            "attempt_id": attempt_id,
            "attempt_number": 1,
            "failure_count": 0,
            "continuation_index": 0,
            "mode": ContinuationMode.FIRST.value,
            "prompt": "survive the scheduler process",
            "session_id": None,
        },
    }
    request_path = tmp_path / "request.json"
    identity_path = tmp_path / "remote-process.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")
    helper = Path(__file__).parents[1] / "helpers" / "ssh_controller.py"
    controller = await asyncio.create_subprocess_exec(
        sys.executable,
        str(helper),
        "--host",
        os.environ.get("ISSUE_AGENT_TEST_SSH_HOST", "127.0.0.1"),
        "--user",
        os.environ["ISSUE_AGENT_TEST_SSH_USER"],
        "--port",
        os.environ["ISSUE_AGENT_TEST_SSH_PORT"],
        "--identity",
        os.environ["ISSUE_AGENT_TEST_SSH_IDENTITY"],
        "--known-hosts",
        os.environ["ISSUE_AGENT_TEST_SSH_KNOWN_HOSTS"],
        "--python",
        sys.executable,
        "--request",
        str(request_path),
        "--process-identity",
        str(identity_path),
    )
    try:
        await wait_until(identity_path.exists, timeout=15)
        identity = ProcessIdentity(**json.loads(identity_path.read_text(encoding="utf-8")))
        assert identity.host_id == target_id
        assert ProcessGuard.matches(identity)

        controller.kill()  # SIGKILL on POSIX: no backend cleanup path can run.
        await asyncio.wait_for(controller.wait(), timeout=5)
        assert ProcessGuard.matches(identity)

        ssh = SSHConfig(
            host=os.environ.get("ISSUE_AGENT_TEST_SSH_HOST", "127.0.0.1"),
            user=os.environ["ISSUE_AGENT_TEST_SSH_USER"],
            port=int(os.environ["ISSUE_AGENT_TEST_SSH_PORT"]),
            identity_file=Path(os.environ["ISSUE_AGENT_TEST_SSH_IDENTITY"]),
            known_hosts_file=Path(os.environ["ISSUE_AGENT_TEST_SSH_KNOWN_HOSTS"]),
            remote_command=(sys.executable, "-m", "gitlab_issue_agent.remote_runner"),
        )
        replacement = SSHAgentBackend(
            target_id=target_id,
            state_root=str(remote_root),
            repository=config.repository,
            agent=config.agent,
            transport=SSHTransport(ssh),
        )
        reaped = await replacement.reap(attempt_id)
        assert reaped.confirmed_safe is True
        assert reaped.identity_matched is True
        assert not ProcessGuard.matches(identity)
    finally:
        if controller.returncode is None:
            controller.kill()
            await controller.wait()
