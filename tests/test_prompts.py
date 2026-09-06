from pathlib import Path

from gitlab_issue_agent.models import Issue
from gitlab_issue_agent.prompts import PromptBuilder


def _issue() -> Issue:
    return Issue(
        id="42",
        project_id="7",
        iid=12,
        identifier="proj#12",
        title="Make the worker durable",
        description="Keep progress across clean continuations.",
        state="opened",
        labels=("agent::ready",),
        web_url="https://gitlab.example/proj/-/issues/12",
    )


def _builder(tmp_path: Path) -> PromptBuilder:
    workflow = tmp_path / "WORKFLOW.md"
    workflow.write_text("Work only on {{ issue.identifier }} and run tests.", encoding="utf-8")
    return PromptBuilder(workflow)


def test_first_prompt_declares_persistent_goal_and_checkpoint(tmp_path: Path) -> None:
    prompt = _builder(tmp_path).first(_issue())

    assert "Treat the Issue as one persistent goal across process turns" in prompt
    assert "## Continuation checkpoint" in prompt
    assert "Verification evidence" in prompt
    assert "The checkpoint is working memory only" in prompt


def test_native_continuation_repeats_handoff_contract(tmp_path: Path) -> None:
    prompt = _builder(tmp_path).native_continuation(_issue(), turn=3)

    assert "clean continuation turn 3" in prompt
    assert "## Persistent goal and handoff contract" in prompt
    assert "## Continuation checkpoint" in prompt


def test_stateless_continuation_treats_tail_as_fallible_evidence(tmp_path: Path) -> None:
    prompt = _builder(tmp_path).stateless_continuation(
        _issue(),
        previous_summary="## Continuation checkpoint\n- Goal status: half done",
        workspace_snapshot="[status]\n M src/worker.py",
        attempt_number=4,
    )

    assert "Latest bounded backend tail" in prompt
    assert "fallible working note, not an authoritative record" in prompt
    assert "half done" in prompt
    assert "M src/worker.py" in prompt
