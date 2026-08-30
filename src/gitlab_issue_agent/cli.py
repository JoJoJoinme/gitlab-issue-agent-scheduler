from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from pathlib import Path

from filelock import FileLock, Timeout

from .config import ConfigError, SchedulerConfig
from .events import EventSink
from .execution import build_execution_router
from .orchestrator import Orchestrator
from .prompts import PromptBuilder
from .state import StateStore
from .tracker import GitLabIssueTracker


def build_orchestrator(config: SchedulerConfig) -> Orchestrator:
    state = StateStore(config.state_root)
    events = EventSink(state, stdout=config.stdout_events)
    return Orchestrator(
        config,
        tracker=GitLabIssueTracker(config.tracker),
        execution=build_execution_router(config),
        state=state,
        events=events,
        prompts=PromptBuilder(config.workflow_file),
    )


async def _run(config: SchedulerConfig) -> None:
    orchestrator = build_orchestrator(config)
    loop = asyncio.get_running_loop()
    for signal_name in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(
                signal_name, lambda: asyncio.create_task(orchestrator.shutdown())
            )
        except (NotImplementedError, RuntimeError):
            pass
    await orchestrator.run_forever()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gitlab-agent-scheduler",
        description="Run a GitLab Issue-driven long-running coding-agent scheduler.",
    )
    parser.add_argument(
        "--config", type=Path, default=Path("scheduler.yaml"), help="scheduler YAML file"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate configuration and workflow files without contacting GitLab",
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    try:
        config = SchedulerConfig.load(args.config)
        if not config.workflow_file.is_file():
            raise ConfigError(f"workflow file does not exist: {config.workflow_file}")
        if args.check:
            print(f"configuration valid: {config.config_path}")
            print(f"workflow valid: {config.workflow_file}")
            return
        config.state_root.mkdir(parents=True, exist_ok=True)
        lock = FileLock(str(config.state_root / "scheduler.lock"), timeout=0)
        with lock:
            asyncio.run(_run(config))
    except Timeout:
        print("another scheduler instance already owns this state root", file=sys.stderr)
        raise SystemExit(2) from None
    except (ConfigError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
