from __future__ import annotations

import json
from pathlib import Path

from .models import Issue


class PromptError(RuntimeError):
    pass


class PromptBuilder:
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
        context = f"""The backend session cannot be resumed natively. Reconstruct state from durable evidence.

Attempt number: {attempt_number}

Previous bounded agent summary:
```text
{previous_summary or "(no usable prior summary)"}
```

Current git worktree evidence:
```text
{workspace_snapshot}
```

Do not assume the summary is complete. Inspect the worktree, git history, tests, and the current issue before acting.
"""
        return self._full(issue, continuation_context=context)

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

## Authoritative GitLab Issue snapshot

```json
{self._issue_json(issue)}
```
{continuation}
"""

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
