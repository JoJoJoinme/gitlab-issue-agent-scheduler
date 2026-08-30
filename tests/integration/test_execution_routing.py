from __future__ import annotations

import sys
from pathlib import Path

import pytest
from conftest import make_issue

from gitlab_issue_agent.config import SchedulerConfig
from gitlab_issue_agent.execution import PlacementError, build_execution_router


def test_multi_host_config_inherits_agent_and_routes_by_issue_label(tmp_path: Path) -> None:
    workflow = tmp_path / "WORKFLOW.md"
    workflow.write_text("Do the work and stop at human review.\n", encoding="utf-8")
    config = SchedulerConfig.from_mapping(
        {
            "workflow_file": str(workflow),
            "scheduler": {"state_root": str(tmp_path / "state")},
            "tracker": {"token": "token", "project": "group/project"},
            "repository": {"clone_url": "git@example.invalid:group/project.git"},
            "agent": {
                "command": sys.executable,
                "args": ["-c", "print('ok')", "{prompt}"],
            },
            "execution": {
                "default_target": "scheduler-local",
                "label_prefix": "agent-host::",
                "targets": {
                    "scheduler-local": {
                        "kind": "local",
                        "max_concurrent_agents": 1,
                    },
                    "gpu-01": {
                        "kind": "ssh",
                        "max_concurrent_agents": 2,
                        "remote_state_root": "/var/lib/issue-agent",
                        "ssh": {
                            "host": "gpu-01.internal",
                            "user": "issue-agent",
                            "known_hosts_file": str(tmp_path / "known_hosts"),
                        },
                        "agent": {"command": "csc"},
                    },
                },
            },
        },
        config_path=tmp_path / "scheduler.yaml",
    )
    assert config.execution.targets["gpu-01"].agent.command == "csc"
    assert config.execution.targets["gpu-01"].repository.clone_url.endswith("group/project.git")
    assert config.execution.targets["gpu-01"].max_concurrent_agents == 2

    router = build_execution_router(config)
    assert router.placement(make_issue()) == "scheduler-local"
    assert router.placement(make_issue(labels=("agent::ready", "agent-host::gpu-01"))) == "gpu-01"
    with pytest.raises(PlacementError, match="unknown"):
        router.placement(make_issue(labels=("agent::ready", "agent-host::missing")))
    with pytest.raises(PlacementError, match="multiple"):
        router.placement(
            make_issue(
                labels=(
                    "agent::ready",
                    "agent-host::gpu-01",
                    "agent-host::scheduler-local",
                )
            )
        )
