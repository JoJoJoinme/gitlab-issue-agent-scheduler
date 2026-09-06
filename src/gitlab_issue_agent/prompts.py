from __future__ import annotations

import json
from pathlib import Path

from .models import Issue


class PromptError(RuntimeError):
    pass


class PromptBuilder:
    _CHECKPOINT_MARKER = "## Continuation checkpoint"
    _MAX_CHECKPOINT_CHARS = 2500

    def __init__(self, workflow_file: Path) -> None:
        self.workflow_file = workflow_file

    def first(self, issue: Issue) -> str:
        return self._full(issue, continuation_context=None)

    def native_continuation(self, issue: Issue, *, turn: int) -> str:
        return f"""Continuation guidance:

- GitLab Issue {issue.identifier} remains active and is still the authoritative objective.
- Continue the existing agent session in its current durable worktree.
- This is clean continuation turn {turn}; a previous process exit did not mean the issue was done.
- Inspect current files and tests, then continue the remaining work without restarting completed work.
- Do not merge. Stop only after moving the issue to the configured human-review/non-active gate, a terminal state, or when genuinely blocked.

## Persistent goal and handoff contract

{self._goal_contract()}

Refreshed tracker snapshot:
{self._issue_json(issue)}
"""

    def stateless_continuation(
        self,
        issue: Issue,
        *,
        previous_summary: str,
        workspace_snapshot: str,
        attempt_number: int,
    ) -> str:
        checkpoint = self.extract_continuation_checkpoint(previous_summary)
        if checkpoint:
            prior_context = f"""Recovered structured continuation checkpoint:
```text
{checkpoint}
```"""
        else:
            prior_context = f"""No structured continuation checkpoint was recovered. Fall back to the previous bounded backend tail:
```text
{previous_summary or "(no usable prior output)"}
```"""
        context = f"""The backend session cannot be resumed natively. Reconstruct state from durable evidence.

Attempt number: {attempt_number}

{prior_context}

Current git worktree evidence:
```text
{workspace_snapshot}
```

Treat generated handoff text as a fallible working note, not an authoritative record. It can be incomplete or stale. Re-check the current issue, worktree, git history, and tests before acting; concrete repository evidence overrides the note.
"""
        return self._full(issue, continuation_context=context)

    @classmethod
    def extract_continuation_checkpoint(cls, previous_summary: str) -> str:
        """Recover only the latest structured handoff from a bounded backend tail.

        The backend intentionally stays model-neutral and stores a bounded tail. This
        parser gives stateless reconstruction a deterministic semantic boundary without
        trusting or persisting arbitrary generated prose as scheduler authority.
        """
        marker_index = previous_summary.rfind(cls._CHECKPOINT_MARKER)
        if marker_index < 0:
            return ""
        checkpoint = previous_summary[marker_index:].strip()
        if len(checkpoint) > cls._MAX_CHECKPOINT_CHARS:
            checkpoint = checkpoint[: cls._MAX_CHECKPOINT_CHARS].rstrip()
        return checkpoint

    def _full(self, issue: Issue, *, continuation_context: str | None) -> str:
        workflow = self._workflow_body()
        replacements = {
            "{{ issue.identifier }}": issue.identifier,
            "{{ issue.title }}": issue.title,
            "{{ issue.description }}": issue.description or "",
            "{{ issue.url }}": issue.web_url or "",
            "{{ issue.state }}": issue.state,
        }
        for placeholder, value in replacements.items():
            workflow = workflow.replace(placeholder, value)
        continuation = (
            f"\n\n## Durable continuation context\n\n{continuation_context.strip()}"
            if continuation_context
            else ""
        )
        return f"""# Scheduler execution contract

The GitLab Issue snapshot below is the authoritative objective. Local scheduler state, a prior agent claim, and a clean process exit never override it. Work only in the assigned git worktree. Do not merge; hand off for human review according to the repository workflow.

## Repository workflow

{workflow}

## Persistent goal and handoff contract

{self._goal_contract()}

## Authoritative GitLab Issue snapshot

```json
{self._issue_json(issue)}
```
{continuation}
"""

    @staticmethod
    def _goal_contract() -> str:
        return """Treat the Issue as one persistent goal across process turns, not as a sequence of unrelated prompts.

Before acting, keep these four items explicit from the Issue and repository evidence:

1. desired outcome;
2. constraints and non-goals;
3. verification evidence that can prove progress or completion;
4. the stop condition for handing work to human review or declaring a genuine blocker.

Do not invent unsupported acceptance criteria. If the Issue is ambiguous in a way that can materially change the result, preserve the ambiguity in the handoff rather than silently choosing a new objective.

If this process turn exits cleanly while the Issue is still active, end the final output with the following concise checkpoint. Keep it under 2,000 characters and do not include secrets:

## Continuation checkpoint
- Goal status: <what is complete vs. incomplete>
- Decisions and constraints: <important choices that a fresh agent must preserve>
- Verification evidence: <tests, measurements, or concrete observations already obtained>
- Failed approaches / risks: <what was tried or remains risky>
- Remaining work: <specific unfinished items>
- Next action: <the highest-value next step>

The checkpoint is working memory only. It never overrides the GitLab Issue, repository state, tests, or the configured human-review gate."""

    def _workflow_body(self) -> str:
        try:
            content = self.workflow_file.read_text(encoding="utf-8").strip()
        except OSError as error:
            raise PromptError(f"cannot read workflow file {self.workflow_file}: {error}") from error
        if not content:
            raise PromptError(f"workflow file is empty: {self.workflow_file}")
        if content.startswith("---\n"):
            parts = content.split("---", 2)
            if len(parts) == 3:
                content = parts[2].strip()
        return content

    @staticmethod
    def _issue_json(issue: Issue) -> str:
        return json.dumps(issue.to_dict(), ensure_ascii=False, indent=2, sort_keys=True)
