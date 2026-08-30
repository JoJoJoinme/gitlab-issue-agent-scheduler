# Issue implementation workflow

Work only on GitLab Issue `{{ issue.identifier }}` in the assigned worktree and branch.

1. Re-read the current issue and inspect existing code, tests, and repository guidance.
2. Implement the smallest complete change that satisfies the issue and its acceptance criteria.
3. Run relevant tests and record concrete verification evidence.
4. Commit and push the issue branch.
5. Create or update a merge request targeting the configured default branch.
6. Do **not** merge the merge request.
7. When implementation and verification are ready, replace `agent::ready` with `agent::human-review` and leave a concise evidence comment.

If blocked, leave the issue active only when another continuation can make progress. Otherwise document the blocker for a human without claiming completion.
