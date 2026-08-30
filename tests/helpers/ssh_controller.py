from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from gitlab_issue_agent.config import SSHConfig
from gitlab_issue_agent.ssh_transport import SSHTransport


async def run(args: argparse.Namespace) -> None:
    config = SSHConfig(
        host=args.host,
        user=args.user,
        port=args.port,
        identity_file=Path(args.identity),
        known_hosts_file=Path(args.known_hosts),
        remote_command=(args.python, "-m", "gitlab_issue_agent.remote_runner"),
    )
    payload = json.loads(Path(args.request).read_text(encoding="utf-8"))

    async def message(value) -> None:
        if value.get("type") != "process_started":
            return
        path = Path(args.process_identity)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(value["identity"]), encoding="utf-8")
        os.replace(temporary, path)

    await SSHTransport(config).request(payload, on_message=message)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--identity", required=True)
    parser.add_argument("--known-hosts", required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--request", required=True)
    parser.add_argument("--process-identity", required=True)
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
