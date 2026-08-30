from __future__ import annotations

from dataclasses import asdict
from typing import Any

from .config import AgentConfig, RepositoryConfig
from .models import Issue

PROTOCOL_VERSION = 1


def repository_payload(config: RepositoryConfig) -> dict[str, Any]:
    return asdict(config)


def repository_from_payload(value: dict[str, Any]) -> RepositoryConfig:
    return RepositoryConfig(
        clone_url=str(value["clone_url"]),
        default_branch=str(value.get("default_branch", "main")),
        branch_prefix=str(value.get("branch_prefix", "agent/issue-")),
        fetch_on_create=bool(value.get("fetch_on_create", True)),
    )


def agent_payload(config: AgentConfig) -> dict[str, Any]:
    return asdict(config)


def agent_from_payload(value: dict[str, Any]) -> AgentConfig:
    env = value.get("env", {})
    if not isinstance(env, dict):
        raise TypeError("agent.env must be an object")
    return AgentConfig(
        command=str(value["command"]),
        args=tuple(str(item) for item in value.get("args", ["-p", "{prompt}"])),
        native_resume_args=tuple(str(item) for item in value.get("native_resume_args", [])),
        prefer_native_resume=bool(value.get("prefer_native_resume", True)),
        output_format=str(value.get("output_format", "auto")),
        session_id_paths=tuple(
            str(item) for item in value.get("session_id_paths", ["session_id", "session.id"])
        ),
        timeout_seconds=float(value.get("timeout_seconds", 3600)),
        cancel_grace_seconds=float(value.get("cancel_grace_seconds", 5)),
        pass_env=tuple(str(item) for item in value.get("pass_env", [])),
        env={str(key): str(item) for key, item in env.items()},
        summary_chars=int(value.get("summary_chars", 8000)),
    )


def issue_payload(issue: Issue) -> dict[str, Any]:
    return issue.to_dict()


def issue_from_payload(value: dict[str, Any]) -> Issue:
    return Issue(
        id=str(value["id"]),
        project_id=str(value["project_id"]),
        iid=int(value["iid"]),
        identifier=str(value["identifier"]),
        title=str(value["title"]),
        description=(str(value["description"]) if value.get("description") is not None else None),
        state=str(value["state"]),
        labels=tuple(str(item) for item in value.get("labels", [])),
        web_url=str(value["web_url"]) if value.get("web_url") is not None else None,
        updated_at=(str(value["updated_at"]) if value.get("updated_at") is not None else None),
    )
