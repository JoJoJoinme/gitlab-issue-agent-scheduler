from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import pytest
from conftest import wait_until

from gitlab_issue_agent.models import ProcessIdentity
from gitlab_issue_agent.process_guard import ProcessGuard

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SIGKILL is a POSIX crash boundary")


def _issue_payload(labels: list[str]) -> dict[str, object]:
    return {
        "id": 1001,
        "iid": 1,
        "title": "Survive scheduler crash",
        "description": "Keep the worktree and obey current tracker state.",
        "state": "opened",
        "labels": labels,
        "web_url": "http://gitlab.invalid/group/project/-/issues/1",
        "updated_at": "2026-08-30T00:00:00Z",
    }


@pytest.mark.asyncio
async def test_real_sigkill_then_cold_start_reaps_orphan_and_obeys_tracker(
    tmp_path: Path, origin_repo: Path
) -> None:
    tracker_file = tmp_path / "tracker.json"
    tracker_file.write_text(json.dumps(_issue_payload(["agent::ready"])), encoding="utf-8")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            issue = json.loads(tracker_file.read_text(encoding="utf-8"))
            path = urlparse(self.path).path
            if path.endswith("/issues/1"):
                body = issue
                status = 200
            elif path.endswith("/issues"):
                body = [issue]
                status = 200
            else:
                body = {"message": "not found"}
                status = 404
            encoded = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.send_header("X-Next-Page", "")
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server_task = asyncio.create_task(asyncio.to_thread(server.serve_forever))
    port = server.server_address[1]

    agent_script = tmp_path / "agent.py"
    agent_script.write_text(
        """import json, time
print(json.dumps({"session_id": "kill9-session", "message": "running"}), flush=True)
time.sleep(120)
""",
        encoding="utf-8",
    )
    workflow = tmp_path / "WORKFLOW.md"
    workflow.write_text("Implement the issue. Never merge.\n", encoding="utf-8")
    state_root = tmp_path / "state"
    config = tmp_path / "scheduler.yaml"
    config.write_text(
        f"""workflow_file: {workflow}
scheduler:
  state_root: {state_root}
  poll_interval_seconds: 0.1
  max_concurrent_agents: 1
tracker:
  base_url: http://127.0.0.1:{port}
  token: test-token
  project: group/project
  active_labels: ["agent::ready"]
  terminal_labels: ["agent::done", "agent::cancelled"]
repository:
  clone_url: {origin_repo}
  default_branch: main
agent:
  command: {sys.executable}
  args: ["{agent_script}", "-p", "{{prompt}}"]
  output_format: jsonl
  timeout_seconds: 300
  cancel_grace_seconds: 0.5
retry:
  initial_seconds: 0.1
  max_seconds: 1
continuation:
  delay_seconds: 0.1
  max_consecutive: 5
  yield_seconds: 0.2
observability:
  stdout_json: false
""",
        encoding="utf-8",
    )

    scheduler: asyncio.subprocess.Process | None = None
    replacement: asyncio.subprocess.Process | None = None
    orphan: ProcessIdentity | None = None

    async def start_scheduler() -> asyncio.subprocess.Process:
        return await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "gitlab_issue_agent",
            "--config",
            str(config),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    def current_state() -> dict[str, object] | None:
        paths = list(state_root.glob("issues/*/state.json"))
        if not paths:
            return None
        return json.loads(paths[0].read_text(encoding="utf-8"))

    try:
        scheduler = await start_scheduler()
        await wait_until(
            lambda: bool(
                (state := current_state())
                and state.get("phase") == "running"
                and state.get("process")
            ),
            timeout=15,
        )
        running_state = current_state()
        assert running_state is not None
        orphan = ProcessIdentity(**running_state["process"])  # type: ignore[arg-type]
        assert ProcessGuard.matches(orphan)

        scheduler.send_signal(signal.SIGKILL)
        await asyncio.wait_for(scheduler.wait(), timeout=5)
        assert ProcessGuard.matches(orphan), "executor should outlive the killed scheduler"

        next_tracker = tmp_path / "tracker.next.json"
        next_tracker.write_text(
            json.dumps(_issue_payload(["agent::human-review"])), encoding="utf-8"
        )
        os.replace(next_tracker, tracker_file)

        replacement = await start_scheduler()
        await wait_until(
            lambda: bool(
                (state := current_state())
                and state.get("phase") == "released"
                and state.get("process") is None
            ),
            timeout=15,
        )
        assert not ProcessGuard.matches(orphan)
        events = (state_root / "events.jsonl").read_text(encoding="utf-8")
        assert '"event_type":"cold_start.orphan_reaped"' in events
        assert '"local_phase":"released"' in events
    finally:
        for child in (scheduler, replacement):
            if child is not None and child.returncode is None:
                child.send_signal(signal.SIGTERM)
                try:
                    await asyncio.wait_for(child.wait(), timeout=5)
                except TimeoutError:
                    child.kill()
                    await child.wait()
        if orphan is not None and ProcessGuard.matches(orphan):
            await ProcessGuard.terminate_tree(orphan, grace_seconds=0.5)
        server.shutdown()
        server.server_close()
        await server_task
