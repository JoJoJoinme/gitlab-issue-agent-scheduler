# Issue implementation workflow

Work only on GitLab Issue `{{ issue.identifier }}` in the assigned worktree and branch.

1. Re-read the current issue and inspect existing code, tests, and repository guidance.
2. Make the desired outcome, constraints/non-goals, verification evidence, and stop condition explicit before implementing.
3. Implement the smallest complete change that satisfies the issue and its acceptance criteria.
4. Run relevant tests and record concrete verification evidence.
5. Commit and push the issue branch.
6. Create or update a merge request targeting the configured default branch.
7. Do **not** merge the merge request.
8. When implementation and verification are ready, replace `agent::ready` with `agent::human-review` and leave a concise evidence comment.

If the process turn ends while the issue is still active, finish the output with the scheduler's `## Continuation checkpoint` handoff. That checkpoint is working memory for the next turn; it does not prove completion and does not override the current Issue, repository state, tests, or human-review gate.

If blocked, leave the issue active only when another continuation can make progress. Otherwise document the blocker for a human without claiming completion.
