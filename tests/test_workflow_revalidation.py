from __future__ import annotations

from gitlab_issue_agent.models import Issue
from gitlab_issue_agent.prompts import PromptBuilder


def _issue(*, title: str = "Keep the agent running") -> Issue:
    return Issue(
        id="issue-101",
        project_id="project-1",
        iid=101,
        identifier="group/project#101",
        title=title,
        description="Implement the remaining work",
        state="opened",
        labels=("agent::ready",),
        web_url="https://gitlab.example/group/project/-/issues/101",
    )


def test_native_continuation_restates_workflow_only_after_change(tmp_path) -> None:
    workflow_file = tmp_path / "WORKFLOW.md"
    workflow_file.write_text("Follow workflow v1 for {{ issue.title }}.", encoding="utf-8")
    builder = PromptBuilder(workflow_file)
    issue = _issue()

    first = builder.first(issue)
    unchanged = builder.native_continuation(issue, turn=1)

    assert "Follow workflow v1 for Keep the agent running." in first
    assert "## Repository workflow refresh" not in unchanged

    workflow_file.write_text("Follow workflow v2 for {{ issue.title }}.", encoding="utf-8")
    refreshed = builder.native_continuation(issue, turn=2)
    stable = builder.native_continuation(issue, turn=3)

    assert "## Repository workflow refresh" in refreshed
    assert "Follow workflow v2 for Keep the agent running." in refreshed
    assert "Follow workflow v1" not in refreshed
    assert "## Repository workflow refresh" not in stable


def test_native_continuation_refreshes_conservatively_after_scheduler_restart(tmp_path) -> None:
    workflow_file = tmp_path / "WORKFLOW.md"
    workflow_file.write_text("Current workflow.", encoding="utf-8")
    issue = _issue()

    before_restart = PromptBuilder(workflow_file)
    before_restart.first(issue)

    after_restart = PromptBuilder(workflow_file)
    refreshed = after_restart.native_continuation(issue, turn=2)

    assert "## Repository workflow refresh" in refreshed
    assert "Current workflow." in refreshed


def test_stateless_continuation_records_the_current_rendered_workflow(tmp_path) -> None:
    workflow_file = tmp_path / "WORKFLOW.md"
    workflow_file.write_text("Work on {{ issue.title }}.", encoding="utf-8")
    builder = PromptBuilder(workflow_file)
    issue = _issue(title="Version one")

    builder.first(issue)
    updated_issue = _issue(title="Version two")
    stateless = builder.stateless_continuation(
        updated_issue,
        previous_summary="checkpoint",
        workspace_snapshot="clean",
        attempt_number=2,
    )
    native = builder.native_continuation(updated_issue, turn=2)

    assert "Work on Version two." in stateless
    assert "## Repository workflow refresh" not in native
