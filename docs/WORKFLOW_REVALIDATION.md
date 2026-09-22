# Native-session workflow revalidation

## Problem

A native backend session can survive many clean continuation turns. Before this change, the scheduler refreshed the GitLab Issue snapshot on every native continuation but did not re-state `WORKFLOW.md`. If the repository workflow changed while the native session was alive, the backend could keep following the workflow snapshot it saw on its first turn.

That is different from stateless reconstruction, which already rebuilds the full scheduler execution contract from the current `WORKFLOW.md`.

## Design

`PromptBuilder` now hashes the **rendered workflow text actually delivered to each Issue**. First turns and stateless reconstruction record the current digest. A native continuation reads and renders the current workflow and compares it with the last digest delivered by this scheduler process:

- unchanged workflow: keep the native continuation prompt small;
- changed workflow: append a `Repository workflow refresh` section and mark the new digest delivered;
- scheduler restart with an existing native backend session: the delivery cache is intentionally empty, so the next native continuation re-states the current workflow once rather than assuming the session already saw it.

The cache is deliberately process-local. It is an optimization that suppresses redundant re-statements, not an authority source. Losing it is safe because the fallback is to re-state current instructions.

The digest uses SHA-256 from the Python standard library and covers the rendered workflow after Issue placeholders are expanded. This means a workflow that embeds `{{ issue.title }}` or similar fields is re-stated when that rendered instruction changes even if the file itself did not.

## Boundaries

This mechanism does not make the scheduler responsible for a backend's internal skill or tool registry. It only revalidates `WORKFLOW.md`, which is already scheduler-owned instruction input.

It also does not change the existing authority model:

- the current GitLab Issue remains the objective authority;
- work continues only in the durable per-Issue worktree;
- reconciliation and cancellation still govern executor lifetime;
- no automatic merge or Issue mutation is introduced;
- the human-review gate remains mandatory.

## Why this shape

Google ADK introduced an analogous opt-in `SkillLifecycleConfig(revalidate_skills=True)` for long-lived sessions: it records a digest of a loaded skill's instructions/frontmatter/resources and re-states or rematerializes that skill when its current definition differs from what the session loaded. The transferable principle is that a long-lived agent session should not silently treat mutable scheduler-owned instructions as immutable forever.

The scheduler does not copy ADK's skill lifecycle machinery because it has no structured access to backend-internal skills. Instead it applies the invariant only at the instruction boundary it actually owns and can verify.

First-party reference: https://github.com/google/adk-python/commit/f848b7e3bd4943d245b324fd233483c9857804cf
