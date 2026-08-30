from __future__ import annotations

from typing import Any

import httpx
import pytest

from gitlab_issue_agent.config import GitLabConfig
from gitlab_issue_agent.models import IssueDisposition
from gitlab_issue_agent.tracker import GitLabIssueTracker


def payload(iid: int, *, labels: list[str], state: str = "opened") -> dict[str, Any]:
    return {
        "id": 1000 + iid,
        "iid": iid,
        "title": f"Issue {iid}",
        "description": "objective",
        "state": state,
        "labels": labels,
        "web_url": f"https://gitlab.example/group/project/-/issues/{iid}",
        "updated_at": "2026-08-30T00:00:00Z",
    }


@pytest.mark.asyncio
async def test_gitlab_v4_paths_and_label_disposition_are_authoritative() -> None:
    seen_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.raw_path.decode())
        assert request.headers["PRIVATE-TOKEN"] == "test-token"
        if request.url.path.endswith("/issues/1"):
            return httpx.Response(200, json=payload(1, labels=["agent::ready"]))
        if request.url.path.endswith("/issues/404"):
            return httpx.Response(404, json={"message": "404 Not found"})
        return httpx.Response(
            200,
            json=[
                payload(1, labels=["agent::ready"]),
                payload(2, labels=["agent::human-review"]),
            ],
            headers={"X-Next-Page": ""},
        )

    client = httpx.AsyncClient(
        base_url="https://gitlab.example/api/v4/",
        headers={"PRIVATE-TOKEN": "test-token"},
        transport=httpx.MockTransport(handler),
    )
    config = GitLabConfig(
        base_url="https://gitlab.example",
        token="test-token",
        project="group/project",
        active_labels=("agent::ready",),
        terminal_labels=("agent::done", "agent::cancelled"),
    )
    tracker = GitLabIssueTracker(config, client=client)
    try:
        active = await tracker.list_active_issues()
        assert [issue.iid for issue in active] == [1]
        refreshed = await tracker.get_issue("group/project:1")
        assert refreshed is not None
        assert tracker.disposition(refreshed) is IssueDisposition.ACTIVE
        assert await tracker.get_issue("group/project:404") is None
        assert all(
            path.startswith("/api/v4/projects/group%2Fproject/issues") for path in seen_paths
        )
    finally:
        await client.aclose()
