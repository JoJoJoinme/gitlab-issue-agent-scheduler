from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


class ConfigError(ValueError):
    pass


_ENV_PATTERN = re.compile(
    r"^\$(?:\{(?P<braced>[A-Za-z_][A-Za-z0-9_]*)\}|(?P<plain>[A-Za-z_][A-Za-z0-9_]*))$"
)


def _resolve_env(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _resolve_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_resolve_env(item) for item in value]
    if not isinstance(value, str):
        return value
    match = _ENV_PATTERN.match(value)
    if not match:
        return value
    name = match.group("braced") or match.group("plain")
    if name not in os.environ:
        raise ConfigError(f"environment variable {name} is required")
    return os.environ[name]


def _path(base: Path, value: str | Path) -> Path:
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = base / candidate
    return candidate.resolve()


def _string_list(value: Any, name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ConfigError(f"{name} must be a list of strings")
    return list(value)


@dataclass(frozen=True, slots=True)
class GitLabConfig:
    base_url: str
    token: str
    project: str
    active_states: tuple[str, ...] = ("opened",)
    terminal_states: tuple[str, ...] = ("closed",)
    required_labels: tuple[str, ...] = ()
    active_labels: tuple[str, ...] = ()
    terminal_labels: tuple[str, ...] = ("agent::done", "agent::cancelled")
    per_page: int = 100
    request_timeout_seconds: float = 20.0


@dataclass(frozen=True, slots=True)
class RepositoryConfig:
    clone_url: str
    default_branch: str = "main"
    branch_prefix: str = "agent/issue-"
    fetch_on_create: bool = True


@dataclass(frozen=True, slots=True)
class AgentConfig:
    command: str
    args: tuple[str, ...] = ("-p", "{prompt}")
    native_resume_args: tuple[str, ...] = ()
    prefer_native_resume: bool = True
    output_format: str = "auto"
    session_id_paths: tuple[str, ...] = ("session_id", "session.id")
    timeout_seconds: float = 3600.0
    cancel_grace_seconds: float = 5.0
    pass_env: tuple[str, ...] = ()
    env: dict[str, str] = field(default_factory=dict)
    summary_chars: int = 8000

    @property
    def supports_native_resume(self) -> bool:
        return self.prefer_native_resume and bool(self.native_resume_args)


@dataclass(frozen=True, slots=True)
class RetryConfig:
    initial_seconds: float = 10.0
    max_seconds: float = 300.0
    multiplier: float = 2.0

    def delay(self, failure_count: int) -> float:
        exponent = max(0, failure_count - 1)
        return min(self.initial_seconds * (self.multiplier**exponent), self.max_seconds)


@dataclass(frozen=True, slots=True)
class ContinuationConfig:
    delay_seconds: float = 1.0
    max_consecutive: int = 20
    yield_seconds: float = 30.0


@dataclass(frozen=True, slots=True)
class SchedulerConfig:
    config_path: Path
    state_root: Path
    workflow_file: Path
    poll_interval_seconds: float
    max_concurrent_agents: int
    tracker: GitLabConfig
    repository: RepositoryConfig
    agent: AgentConfig
    retry: RetryConfig
    continuation: ContinuationConfig
    stdout_events: bool = True

    @classmethod
    def load(cls, path: str | Path) -> SchedulerConfig:
        config_path = Path(path).expanduser().resolve()
        try:
            raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        except OSError as error:
            raise ConfigError(f"cannot read config {config_path}: {error}") from error
        except yaml.YAMLError as error:
            raise ConfigError(f"invalid YAML in {config_path}: {error}") from error
        if not isinstance(raw, dict):
            raise ConfigError("config root must be a mapping")
        return cls.from_mapping(_resolve_env(raw), config_path=config_path)

    @classmethod
    def from_mapping(
        cls, raw: dict[str, Any], *, config_path: str | Path = "scheduler.yaml"
    ) -> SchedulerConfig:
        config_path = Path(config_path).resolve()
        base = config_path.parent
        tracker_raw = raw.get("tracker", {})
        repo_raw = raw.get("repository", {})
        agent_raw = raw.get("agent", {})
        scheduler_raw = raw.get("scheduler", {})
        retry_raw = raw.get("retry", {})
        continuation_raw = raw.get("continuation", {})
        observability_raw = raw.get("observability", {})

        for name, value in (
            ("tracker", tracker_raw),
            ("repository", repo_raw),
            ("agent", agent_raw),
            ("scheduler", scheduler_raw),
            ("retry", retry_raw),
            ("continuation", continuation_raw),
            ("observability", observability_raw),
        ):
            if not isinstance(value, dict):
                raise ConfigError(f"{name} must be a mapping")

        required = {
            "tracker.token": tracker_raw.get("token"),
            "tracker.project": tracker_raw.get("project"),
            "repository.clone_url": repo_raw.get("clone_url"),
            "agent.command": agent_raw.get("command"),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ConfigError("missing required configuration: " + ", ".join(missing))

        args = _string_list(agent_raw.get("args", ["-p", "{prompt}"]), "agent.args")
        resume_args = _string_list(
            agent_raw.get("native_resume_args", []), "agent.native_resume_args"
        )
        if not any("{prompt}" in item for item in args):
            raise ConfigError("agent.args must contain a {prompt} placeholder")
        if resume_args and (
            not any("{prompt}" in item for item in resume_args)
            or not any("{session_id}" in item for item in resume_args)
        ):
            raise ConfigError(
                "agent.native_resume_args must contain {session_id} and {prompt} placeholders"
            )

        agent_env = agent_raw.get("env", {})
        if not isinstance(agent_env, dict) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in agent_env.items()
        ):
            raise ConfigError("agent.env must be a string-to-string mapping")

        tracker = GitLabConfig(
            base_url=str(tracker_raw.get("base_url", "https://gitlab.com")).rstrip("/"),
            token=str(tracker_raw["token"]),
            project=str(tracker_raw["project"]),
            active_states=tuple(
                _string_list(tracker_raw.get("active_states", ["opened"]), "tracker.active_states")
            ),
            terminal_states=tuple(
                _string_list(
                    tracker_raw.get("terminal_states", ["closed"]), "tracker.terminal_states"
                )
            ),
            required_labels=tuple(
                _string_list(tracker_raw.get("required_labels", []), "tracker.required_labels")
            ),
            active_labels=tuple(
                _string_list(tracker_raw.get("active_labels", []), "tracker.active_labels")
            ),
            terminal_labels=tuple(
                _string_list(
                    tracker_raw.get("terminal_labels", ["agent::done", "agent::cancelled"]),
                    "tracker.terminal_labels",
                )
            ),
            per_page=int(tracker_raw.get("per_page", 100)),
            request_timeout_seconds=float(tracker_raw.get("request_timeout_seconds", 20)),
        )
        repository = RepositoryConfig(
            clone_url=str(repo_raw["clone_url"]),
            default_branch=str(repo_raw.get("default_branch", "main")),
            branch_prefix=str(repo_raw.get("branch_prefix", "agent/issue-")),
            fetch_on_create=bool(repo_raw.get("fetch_on_create", True)),
        )
        agent = AgentConfig(
            command=str(agent_raw["command"]),
            args=tuple(args),
            native_resume_args=tuple(resume_args),
            prefer_native_resume=bool(agent_raw.get("prefer_native_resume", True)),
            output_format=str(agent_raw.get("output_format", "auto")),
            session_id_paths=tuple(
                _string_list(
                    agent_raw.get("session_id_paths", ["session_id", "session.id"]),
                    "agent.session_id_paths",
                )
            ),
            timeout_seconds=float(agent_raw.get("timeout_seconds", 3600)),
            cancel_grace_seconds=float(agent_raw.get("cancel_grace_seconds", 5)),
            pass_env=tuple(_string_list(agent_raw.get("pass_env", []), "agent.pass_env")),
            env=dict(agent_env),
            summary_chars=int(agent_raw.get("summary_chars", 8000)),
        )
        retry = RetryConfig(
            initial_seconds=float(retry_raw.get("initial_seconds", 10)),
            max_seconds=float(retry_raw.get("max_seconds", 300)),
            multiplier=float(retry_raw.get("multiplier", 2)),
        )
        continuation = ContinuationConfig(
            delay_seconds=float(continuation_raw.get("delay_seconds", 1)),
            max_consecutive=int(continuation_raw.get("max_consecutive", 20)),
            yield_seconds=float(continuation_raw.get("yield_seconds", 30)),
        )

        config = cls(
            config_path=config_path,
            state_root=_path(base, scheduler_raw.get("state_root", ".scheduler")),
            workflow_file=_path(base, raw.get("workflow_file", "WORKFLOW.md")),
            poll_interval_seconds=float(scheduler_raw.get("poll_interval_seconds", 30)),
            max_concurrent_agents=int(scheduler_raw.get("max_concurrent_agents", 4)),
            tracker=tracker,
            repository=repository,
            agent=agent,
            retry=retry,
            continuation=continuation,
            stdout_events=bool(observability_raw.get("stdout_json", True)),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.poll_interval_seconds <= 0:
            raise ConfigError("scheduler.poll_interval_seconds must be positive")
        if self.max_concurrent_agents <= 0:
            raise ConfigError("scheduler.max_concurrent_agents must be positive")
        if self.retry.initial_seconds < 0 or self.retry.max_seconds < self.retry.initial_seconds:
            raise ConfigError("retry delays are invalid")
        if self.retry.multiplier < 1:
            raise ConfigError("retry.multiplier must be at least 1")
        if self.continuation.delay_seconds < 0 or self.continuation.max_consecutive <= 0:
            raise ConfigError("continuation settings are invalid")
        if self.agent.timeout_seconds <= 0 or self.agent.cancel_grace_seconds < 0:
            raise ConfigError("agent timeout settings are invalid")
        if self.agent.output_format not in {"auto", "jsonl", "text"}:
            raise ConfigError("agent.output_format must be auto, jsonl, or text")
