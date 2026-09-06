# Long-running goal and handoff contract

## Why this exists

The scheduler already externalizes long-running control state: the GitLab Issue is authoritative, process exit is not completion, the worktree is durable, and continuation/retry/reconciliation are scheduler concerns.

Recent agent runtimes make two complementary ideas explicit:

- a long-running task should be represented as a persistent goal with an outcome, constraints, verification loop, and stop condition;
- continuation memory should preserve useful working notes without letting generated memory replace authoritative instructions or concrete evidence.

The current P0 implementation had the control-plane half of that design, but stateless reconstruction only received a bounded tail of the previous backend output plus a git snapshot. That tail was incidental rather than an intentional handoff artifact.

## P0.1 adaptation

This repository now adds a backend-neutral goal and handoff contract:

1. Every turn treats the GitLab Issue as one persistent goal, not a fresh prompt.
2. Before acting, the agent keeps four items explicit: desired outcome, constraints/non-goals, verification evidence, and stop condition.
3. If a clean process turn ends while the Issue is still active, the agent ends its output with a concise `## Continuation checkpoint` containing goal status, decisions/constraints, verification evidence, failed approaches/risks, remaining work, and the next action.
4. The stateless-reconstruction boundary deterministically extracts the **latest** structured checkpoint from the bounded backend tail. Older/noisy output is not replayed when a checkpoint is present.
5. If no checkpoint exists, reconstruction falls back to the old bounded-tail behavior for backward compatibility.
6. In both paths the generated handoff is explicitly treated as fallible working memory and re-validated against the current Issue, worktree, git history, and tests.

This is deliberately small. The existing command backend already persists a bounded tail of agent output in `IssueState.last_summary`; the checkpoint marker adds a semantic boundary on top of that durable state without adding a model-specific memory service or changing the remote protocol. The extracted checkpoint is bounded again before prompt injection.

## Authority and safety boundary

The checkpoint is never authoritative. Precedence remains:

1. current GitLab Issue and configured tracker disposition;
2. repository/worktree state and concrete test evidence;
3. repository workflow and human-review boundary;
4. generated continuation checkpoint and other backend output.

Do not store secrets in checkpoints. Treat scheduler state and event logs as sensitive build/agent logs, consistent with the existing repository guidance.

## What this does not implement yet

This change does **not** add a semantic memory database, embeddings, cross-issue memory, automatic self-modification, or a model-specific `/goal` integration. It also does not yet inject selected older attempt evidence into a fresh continuation.

A later change can add an explicit episodic-evidence index over attempt events if field testing shows that a single latest checkpoint loses important older observations. That should remain deterministic, bounded, inspectable, and subordinate to tracker/repository evidence.

## Upstream references

- OpenAI Codex long-running goals: https://learn.chatgpt.com/zh-Hans/use-cases/follow-goals
- OpenAI long-running work guidance: https://learn.chatgpt.com/docs/long-running-work
- OpenAI Codex memories: https://learn.chatgpt.com/docs/customization/memories
- OpenAI Codex configuration reference (`features.context_management.experimental_mode`): https://learn.chatgpt.com/docs/config-file/config-reference
